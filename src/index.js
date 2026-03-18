const DEFAULT_PAGE_SIZE = 100;
const MAX_FETCH_SIZE    = 100;
const MAX_INPUT_LENGTH = 200;
const MAX_PAGE = 10000;

const SORTABLE = new Set([
  "employer_name",
  "job_title",
  "wage_rate_of_pay_from",
  "worksite_state",
  "worksite_city",
  "begin_date",
  "end_date",
]);

const BLOCKED_PREFIXES = ["/src/", "/scripts/", "/migrations/"];

const SUGGESTIONS_INDEX_KEY = "suggestions_index.json";
const MAX_SUGGEST_RESULTS = 100;

let suggestionsIndexCache = null;

async function getSuggestionsIndex(env, request) {
  if (suggestionsIndexCache) return suggestionsIndexCache;
  if (env.SUGGESTIONS_INDEX) {
    try {
      const obj = await env.SUGGESTIONS_INDEX.get(SUGGESTIONS_INDEX_KEY);
      if (obj && obj.body) {
        const body = await obj.arrayBuffer();
        suggestionsIndexCache = JSON.parse(new TextDecoder().decode(body));
        return suggestionsIndexCache;
      }
    } catch (e) {
      console.error("R2 suggestions index:", e);
    }
  }
  if (env.ASSETS) {
    try {
      const assetUrl = new URL("/suggestions_index.json", request.url);
      const res = await env.ASSETS.fetch(assetUrl.toString());
      if (res.ok) {
        suggestionsIndexCache = await res.json();
        return suggestionsIndexCache;
      }
    } catch (e) {
      console.error("Fallback suggestions index (ASSETS):", e);
    }
  }
  return null;
}

function searchFromIndex(index, field, q, contextEmployer, contextJob) {
  const col = field === "employer" ? "employers" : "jobs";
  const list = index[col];
  if (!list || !Array.isArray(list)) return [];
  const ql = q.trim().toLowerCase();
  if (!ql) {
    if (field === "employer" && contextJob) {
      const arr = index.jobToEmployers[contextJob.toLowerCase()];
      return Array.isArray(arr) ? [...new Set(arr)].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" })).slice(0, MAX_SUGGEST_RESULTS) : [];
    }
    if (field === "job" && contextEmployer) {
      const arr = index.employerToJobs[contextEmployer.toLowerCase()];
      return Array.isArray(arr) ? [...new Set(arr)].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" })).slice(0, MAX_SUGGEST_RESULTS) : [];
    }
    return [];
  }
  const prefix = list.filter((s) => s.toLowerCase().startsWith(ql));
  const substr = list.filter((s) => {
    const sl = s.toLowerCase();
    return sl.includes(ql) && !sl.startsWith(ql);
  });
  const seen = new Set();
  const out = [];
  for (const s of prefix) {
    if (!seen.has(s)) { seen.add(s); out.push(s); }
  }
  for (const s of substr) {
    if (!seen.has(s)) { seen.add(s); out.push(s); }
  }
  return out.slice(0, MAX_SUGGEST_RESULTS);
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const staticRouteMap = {
      "/": "/index.html",
      "/record": "/record.html",
    };

    if (BLOCKED_PREFIXES.some((p) => url.pathname.startsWith(p))) {
      return new Response("Not Found", { status: 404 });
    }

    if (url.pathname.startsWith("/api/")) {
      if (!isSameOrigin(request)) {
        return jsonResponse({ error: "Forbidden" }, 403, {});
      }

      if (request.method === "OPTIONS") {
        return new Response(null, {
          status: 204,
          headers: buildCorsHeaders(request),
        });
      }

      const cors = buildCorsHeaders(request);

      if (request.method !== "GET") {
        return jsonResponse({ error: "Method not allowed" }, 405, cors);
      }

      if (env.API_TOKEN) {
        const token = request.headers.get("X-API-Token");
        if (token !== env.API_TOKEN) {
          return jsonResponse({ error: "Forbidden" }, 403, cors);
        }
      }

      const ip = request.headers.get("cf-connecting-ip") || "unknown";
      if (env.API_RATE_LIMITER) {
        const { success } = await env.API_RATE_LIMITER.limit({ key: ip });
        if (!success) {
          return jsonResponse(
            { error: "Too many requests. Please try again later." },
            429,
            { ...cors, "Retry-After": "60" }
          );
        }
      }

      if (url.pathname === "/api/search") {
        return handleSearch(url.searchParams, env.DB, cors, env, ctx);
      }
      if (url.pathname === "/api/record") {
        return handleRecord(url.searchParams, env.DB, cors);
      }
      if (url.pathname === "/api/suggest") {
        return handleSuggest(url.searchParams, env.DB, cors, env, ctx, request);
      }

      return jsonResponse({ error: "Not found" }, 404, cors);
    }

    if (staticRouteMap[url.pathname]) {
      const assetUrl = new URL(request.url);
      assetUrl.pathname = staticRouteMap[url.pathname];
      const response = await env.ASSETS.fetch(new Request(assetUrl.toString(), request));
      if (env.API_TOKEN) {
        const html = await response.text();
        const injected = html.replace(
          '<meta charset="UTF-8">',
          `<meta charset="UTF-8"><meta name="api-token" content="${env.API_TOKEN}">`
        );
        const newHeaders = new Headers(response.headers);
        newHeaders.delete("content-length");
        return new Response(injected, { status: response.status, headers: newHeaders });
      }
      return response;
    }

    return env.ASSETS.fetch(request);
  },
};

const CACHE_TTL_EDGE = 86400;
const CACHE_TTL_KV   = 7776000;

function buildSearchCacheKey(employer, job, location, sort, dir, page, pageSize) {
  return `search:${employer}:${job}:${location}:${sort}:${dir}:${page}:${pageSize}`;
}

async function handleSearch(params, db, cors, env, ctx) {
  const employer = (params.get("employer") || "").trim().slice(0, MAX_INPUT_LENGTH);
  const job = (params.get("job") || "").trim().slice(0, MAX_INPUT_LENGTH);
  const location = (params.get("location") || "").trim().slice(0, MAX_INPUT_LENGTH);

  if (!employer && !job && !location) {
    return jsonResponse({ error: "Please provide at least one search term." }, 400, cors);
  }

  const page = Math.min(MAX_PAGE, Math.max(1, parseInt(params.get("page") || "1", 10) || 1));
  const requestedSize = parseInt(params.get("pageSize") || String(DEFAULT_PAGE_SIZE), 10);
  const pageSize = Math.min(MAX_FETCH_SIZE, Math.max(1, Number.isFinite(requestedSize) ? requestedSize : DEFAULT_PAGE_SIZE));
  const sortParam = params.get("sort") || "wage_rate_of_pay_from";
  const dirParam = (params.get("dir") || "DESC").toUpperCase();

  const sort = SORTABLE.has(sortParam) ? sortParam : "wage_rate_of_pay_from";
  const dir = dirParam === "ASC" ? "ASC" : "DESC";
  const offset = (page - 1) * pageSize;

  const kvKey = buildSearchCacheKey(employer, job, location, sort, dir, page, pageSize);
  const cacheUrl = new Request(`https://cache.internal/${kvKey}`);
  const searchMeta = { employer: employer || null, job: job || null, location: location || null, sort, dir, page, page_size: pageSize };

  const t0 = Date.now();
  try {
    const edgeCached = await caches.default.match(cacheUrl);
    if (edgeCached) {
      if (ctx) {
        ctx.waitUntil(logSearch(env.SEARCH_LOGS, { ...searchMeta, cache_tier: "edge", duration_ms: Date.now() - t0 }));
      }
      return edgeCached;
    }
  } catch {}

  try {
    if (env.SEARCH_CACHE) {
      const kvData = await env.SEARCH_CACHE.get(kvKey, { type: "json" });
      if (kvData) {
        const kvDuration = Date.now() - t0;
        const response = jsonResponse(kvData, 200, cors);
        if (ctx) {
          ctx.waitUntil(Promise.all([
            caches.default.put(cacheUrl, response.clone()),
            logSearch(env.SEARCH_LOGS, { ...searchMeta, cache_tier: "kv", duration_ms: kvDuration }),
          ]));
        }
        return response;
      }
    }
  } catch {}

  const where = [];
  const bindings = [];

  if (employer) {
    where.push("LOWER(employer_name) = LOWER(?)");
    bindings.push(employer);
  }
  if (job) {
    where.push("LOWER(job_title) = LOWER(?)");
    bindings.push(job);
  }
  if (location) {
    where.push("(LOWER(worksite_city) = LOWER(?) OR LOWER(worksite_state) = LOWER(?))");
    bindings.push(location, location);
  }

  const whereClause = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const countSQL = `SELECT COUNT(*) AS cnt FROM h1b_wages ${whereClause}`;
  const dataSQL = `SELECT id, employer_name, job_title, wage_rate_of_pay_from, worksite_city, worksite_state, begin_date, end_date FROM h1b_wages ${whereClause} ORDER BY ${sort} ${dir} NULLS LAST LIMIT ${pageSize} OFFSET ${offset}`;

  try {
    const [countRow, rows] = await Promise.all([
      db.prepare(countSQL).bind(...bindings).first(),
      db.prepare(dataSQL).bind(...bindings).all(),
    ]);
    const duration = Date.now() - t0;
    const total = countRow?.cnt ?? 0;
    const payload = { total, page, pageSize, results: rows.results };
    const response = jsonResponse(payload, 200, cors);

    if (ctx) {
      ctx.waitUntil(Promise.all([
        caches.default.put(cacheUrl, response.clone()),
        env.SEARCH_CACHE ? env.SEARCH_CACHE.put(kvKey, JSON.stringify(payload), { expirationTtl: CACHE_TTL_KV }) : Promise.resolve(),
        logSQL(env.SQL_LOGS, {
          route: "/api/search",
          sql: resolveSQL(dataSQL, bindings),
          duration_ms: duration,
          rows_returned: total,
        }),
        logSearch(env.SEARCH_LOGS, { ...searchMeta, cache_tier: "d1", total_results: total, duration_ms: duration }),
      ]));
    }

    return response;
  } catch (err) {
    console.error("Search error:", err);
    return jsonResponse({ error: "Search failed. Please try again." }, 500, cors);
  }
}

const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

async function handleRecord(params, db, cors) {
  const id = (params.get("id") || "").trim().slice(0, 50);
  if (!id || !UUID_REGEX.test(id)) {
    return jsonResponse({ error: "Invalid record ID." }, 400, cors);
  }
  try {
    const row = await db
      .prepare(
        `SELECT id, case_number, job_title, soc_code, soc_title,
                begin_date, end_date,
                employer_name, employer_address1, employer_address2,
                employer_city, employer_state, employer_postal_code, employer_country,
                worksite_address1, worksite_address2, worksite_city, worksite_county,
                worksite_state, worksite_postal_code,
                wage_rate_of_pay_from, wage_rate_of_pay_to, prevailing_wage, pw_wage_level
         FROM h1b_wages WHERE id = ?`
      )
      .bind(id)
      .first();

    if (!row) {
      return jsonResponse({ error: "Record not found." }, 404, cors);
    }
    return jsonResponse({ result: row }, 200, cors);
  } catch (err) {
    console.error("Record error:", err);
    return jsonResponse({ error: "Failed to load record." }, 500, cors);
  }
}

function buildSuggestCacheKey(field, q, ctxEmp, ctxJob, ctxLoc) {
  return `suggest:${field}:${q}:${ctxEmp}:${ctxJob}:${ctxLoc}`;
}

async function handleSuggest(params, db, cors, env, ctx, request) {
  const field    = params.get("field") || "";
  const q        = (params.get("q")        || "").trim().slice(0, MAX_INPUT_LENGTH);
  const ctxEmp   = (params.get("employer") || "").trim().slice(0, MAX_INPUT_LENGTH);
  const ctxJob   = (params.get("job")      || "").trim().slice(0, MAX_INPUT_LENGTH);
  const ctxLoc   = (params.get("location") || "").trim().slice(0, MAX_INPUT_LENGTH);

  if (field === "employer" || field === "job") {
    const index = await getSuggestionsIndex(env, request);
    if (index) {
      const results = searchFromIndex(index, field, q, ctxEmp, ctxJob);
      return jsonResponse({ results }, 200, cors);
    }
    return jsonResponse({ results: [] }, 200, cors);
  }

  const cfg = SUGGEST_FIELDS[field];
  if (!cfg) return jsonResponse({ results: [] }, 200, cors);

  const { col, table } = cfg;
  const hasContext = (field !== "employer" && ctxEmp) ||
                     (field !== "job"      && ctxJob) ||
                     (field !== "location" && ctxLoc);

  if (!hasContext && q.length < 2) {
    return jsonResponse({ results: [] }, 200, cors);
  }

  const kvKey = buildSuggestCacheKey(field, q, ctxEmp, ctxJob, ctxLoc);
  const cacheUrl = new Request(`https://cache.internal/${kvKey}`);
  const useKV = q.length <= SUGGEST_KV_MAX_Q_LEN;

  const t0 = Date.now();
  try {
    const edgeCached = await caches.default.match(cacheUrl);
    if (edgeCached) return edgeCached;
  } catch {}

  try {
    if (useKV && env.SEARCH_CACHE) {
      const kvData = await env.SEARCH_CACHE.get(kvKey, { type: "json" });
      if (kvData) {
        const response = jsonResponse(kvData, 200, cors);
        if (ctx) {
          ctx.waitUntil(caches.default.put(cacheUrl, response.clone()).catch(() => {}));
        }
        return response;
      }
    }
  } catch {}

  try {
    let stmt;

    if (hasContext) {
      const ctxWhere    = [];
      const ctxBindings = [];

      if (field !== "employer" && ctxEmp) {
        ctxWhere.push("employer_name = ?");
        ctxBindings.push(ctxEmp);
      }
      if (field !== "job" && ctxJob) {
        ctxWhere.push("job_title = ?");
        ctxBindings.push(ctxJob);
      }
      if (field !== "location" && ctxLoc) {
        ctxWhere.push("(worksite_city = ? OR worksite_state = ?)");
        ctxBindings.push(ctxLoc, ctxLoc);
      }

      if (q.length >= 2) {
        ctxWhere.push(`LOWER(${col}) = LOWER(?)`);
        ctxBindings.push(q);
        stmt = db.prepare(
          `SELECT DISTINCT ${col} AS value
           FROM h1b_wages
           WHERE ${ctxWhere.join(" AND ")} AND ${col} != ''
           ORDER BY ${col}
           LIMIT 8`
        ).bind(...ctxBindings);
      } else {
        stmt = db.prepare(
          `SELECT DISTINCT ${col} AS value
           FROM h1b_wages
           WHERE ${ctxWhere.join(" AND ")} AND ${col} != ''
           ORDER BY ${col}
           LIMIT 8`
        ).bind(...ctxBindings);
      }
    } else {
      stmt = db.prepare(
        `SELECT ${col} AS value
         FROM ${table}
         WHERE LOWER(${col}) = LOWER(?)
         ORDER BY ${col}
         LIMIT 8`
      ).bind(q);
    }

    const { results } = await stmt.all();
    const payload = { results: results.map((r) => r.value).filter(Boolean) };
    const response = jsonResponse(payload, 200, cors);

    if (ctx) {
      const bgTasks = [caches.default.put(cacheUrl, response.clone()).catch(() => {})];
      if (useKV && env.SEARCH_CACHE) {
        bgTasks.push(env.SEARCH_CACHE.put(kvKey, JSON.stringify(payload), { expirationTtl: CACHE_TTL_KV_SUGGEST }).catch(() => {}));
      }
      ctx.waitUntil(Promise.all(bgTasks));
    }

    return response;
  } catch (err) {
    console.error("Suggest error:", err);
    return jsonResponse({ error: "Suggestion lookup failed." }, 500, cors);
  }
}

function jsonResponse(body, status, extraHeaders) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...extraHeaders },
  });
}

function normHost(host) {
  if (!host) return "";
  return host.replace(/^www\./, "");
}

function isSameOrigin(request) {
  const origin = request.headers.get("Origin");
  const referer = request.headers.get("Referer");

  const workerHost = normHost(new URL(request.url).host);

  if (origin) {
    try {
      if (normHost(new URL(origin).host) === workerHost) return true;
    } catch {}
  }

  if (referer) {
    try {
      if (normHost(new URL(referer).host) === workerHost) return true;
    } catch {}
  }

  // Same-host fallback: browsers sometimes omit Origin/Referer for same-origin fetch.
  // Still requires valid API token when env.API_TOKEN is set.
  if (!origin && !referer && workerHost) return true;

  return false;
}

function buildCorsHeaders(request) {
  const origin = request.headers.get("Origin") || "";
  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-API-Token",
    "Vary": "Origin",
  };
}

function r2Key(prefix) {
  const now = new Date();
  const yyyy = now.getUTCFullYear();
  const mm   = String(now.getUTCMonth() + 1).padStart(2, "0");
  const dd   = String(now.getUTCDate()).padStart(2, "0");
  const hh   = String(now.getUTCHours()).padStart(2, "0");
  const min  = String(now.getUTCMinutes()).padStart(2, "0");
  const ss   = String(now.getUTCSeconds()).padStart(2, "0");
  const rand = Math.random().toString(36).slice(2, 8);
  return `${prefix}/${yyyy}/${mm}/${dd}/${hh}-${min}-${ss}-${rand}.json`;
}

function resolveSQL(template, params) {
  let i = 0;
  return template.replace(/\?/g, () => {
    const v = params[i++];
    if (v == null) return "NULL";
    if (typeof v === "string") return `'${v.replace(/'/g, "''")}'`;
    return String(v);
  });
}

async function logSQL(bucket, data) {
  if (!bucket) return;
  try {
    const body = JSON.stringify({ ts: new Date().toISOString(), ...data });
    await bucket.put(r2Key("sql"), body, { httpMetadata: { contentType: "application/json" } });
  } catch (e) {
    console.error("SQL log error:", e);
  }
}

async function logSearch(bucket, data) {
  if (!bucket) return;
  try {
    const body = JSON.stringify({ ts: new Date().toISOString(), ...data });
    await bucket.put(r2Key("search"), body, { httpMetadata: { contentType: "application/json" } });
  } catch (e) {
    console.error("Search log error:", e);
  }
}
