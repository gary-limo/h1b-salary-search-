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
const BLOCKED_PATHS = new Set(["/suggestions_index.json"]);

const SUGGESTIONS_INDEX_KEY = "suggestions_index.json";
const MAX_SUGGEST_RESULTS = 100;
const SUGGESTIONS_CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 1 day: R2 updates visible within a day

let suggestionsIndexCache = null;
let suggestionsIndexFetchedAt = 0;

async function getSuggestionsIndex(env, request) {
  const now = Date.now();
  if (suggestionsIndexCache && now - suggestionsIndexFetchedAt < SUGGESTIONS_CACHE_TTL_MS) {
    return suggestionsIndexCache;
  }
  suggestionsIndexCache = null;
  suggestionsIndexFetchedAt = 0;

  if (env.SUGGESTIONS_INDEX) {
    try {
      const obj = await env.SUGGESTIONS_INDEX.get(SUGGESTIONS_INDEX_KEY);
      if (obj && obj.body) {
        const body = await obj.arrayBuffer();
        suggestionsIndexCache = JSON.parse(new TextDecoder().decode(body));
        suggestionsIndexFetchedAt = now;
        return suggestionsIndexCache;
      }
    } catch (e) {
      logError(env, "R2 suggestions index load failed", e);
    }
  }
  if (env.ASSETS) {
    try {
      const assetUrl = new URL("/suggestions_index.json", request.url);
      const res = await env.ASSETS.fetch(assetUrl.toString());
      if (res.ok) {
        suggestionsIndexCache = await res.json();
        suggestionsIndexFetchedAt = now;
        return suggestionsIndexCache;
      }
    } catch (e) {
      logError(env, "Fallback suggestions index load failed", e);
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
  const seen = new Set();
  const prefix = [];
  const substr = [];

  for (const s of list) {
    if (seen.has(s)) continue;
    const sl = s.toLowerCase();
    if (sl.startsWith(ql)) {
      seen.add(s);
      prefix.push(s);
      // Prefix matches have highest rank; we can return immediately once full.
      if (prefix.length >= MAX_SUGGEST_RESULTS) {
        return prefix;
      }
      continue;
    }
    if (sl.includes(ql)) {
      seen.add(s);
      if (prefix.length + substr.length < MAX_SUGGEST_RESULTS) {
        substr.push(s);
      }
    }
  }

  return prefix.concat(substr).slice(0, MAX_SUGGEST_RESULTS);
}

/** wrangler dev uses localhost — skip API rate limits so local smoke tests can burst. Production still limited. */
function isLocalDevHostname(hostname) {
  const h = (hostname || "").toLowerCase();
  return h === "localhost" || h === "127.0.0.1" || h === "::1" || h.endsWith(".localhost");
}

function shouldRequireApiToken(env, request) {
  if (env.API_TOKEN) return true;
  try {
    const { hostname } = new URL(request.url);
    return !isLocalDevHostname(hostname);
  } catch {
    // Fail closed if URL parsing fails in production-like traffic.
    return true;
  }
}

/** Apply ratelimit only in production-like traffic. Optional: SKIP_API_RATE_LIMIT=true in .dev.vars (tunnels). */
function shouldApplyApiRateLimit(request, env) {
  if (!env.API_RATE_LIMITER) return false;
  if (env.SKIP_API_RATE_LIMIT === "true") return false;
  try {
    const { hostname } = new URL(request.url);
    if (isLocalDevHostname(hostname)) return false;
  } catch {
    /* fall through */
  }
  return true;
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const staticRouteMap = {
      "/": "/index.html",
      "/record": "/record.html",
    };

    if (BLOCKED_PREFIXES.some((p) => url.pathname.startsWith(p)) || BLOCKED_PATHS.has(url.pathname)) {
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

      if (shouldRequireApiToken(env, request)) {
        if (!env.API_TOKEN) {
          logError(env, "API_TOKEN is not set for non-localhost API traffic");
          return jsonResponse({ error: "Service unavailable." }, 503, cors);
        }
        const token = request.headers.get("X-API-Token");
        if (token !== env.API_TOKEN) {
          return jsonResponse({ error: "Forbidden" }, 403, cors);
        }
      }

      const ip = request.headers.get("cf-connecting-ip") || "unknown";
      if (shouldApplyApiRateLimit(request, env)) {
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
        ctx.waitUntil(logSearch(env, env.SEARCH_LOGS, { ...searchMeta, cache_tier: "edge", duration_ms: Date.now() - t0 }));
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
            logSearch(env, env.SEARCH_LOGS, { ...searchMeta, cache_tier: "kv", duration_ms: kvDuration }),
          ]));
        }
        return response;
      }
    }
  } catch {}

  const where = [];
  const bindings = [];

  const employerNorm = employer.toLowerCase();
  const jobNorm = job.toLowerCase();
  const locationNorm = location.toLowerCase();

  if (employerNorm) {
    where.push("employer_name = ?");
    bindings.push(employerNorm);
  }
  if (jobNorm) {
    where.push("job_title = ?");
    bindings.push(jobNorm);
  }
  if (locationNorm) {
    where.push("(worksite_city = ? OR worksite_state = ?)");
    bindings.push(locationNorm, locationNorm);
  }

  const whereClause = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const countSQL = `SELECT COUNT(*) AS cnt FROM h1b_wages ${whereClause}`;
  const dataSQL = `SELECT id, employer_name, job_title, wage_rate_of_pay_from, worksite_city, worksite_state, begin_date, end_date FROM h1b_wages ${whereClause} ORDER BY ${sort} ${dir} NULLS LAST LIMIT ${pageSize} OFFSET ${offset}`;

  try {
    const session = db.withSession();
    const [countRow, rows] = await Promise.all([
      session.prepare(countSQL).bind(...bindings).first(),
      session.prepare(dataSQL).bind(...bindings).all(),
    ]);
    const duration = Date.now() - t0;
    const total = countRow?.cnt ?? 0;
    const payload = { total, page, pageSize, results: rows.results };
    const response = jsonResponse(payload, 200, cors);

    if (ctx) {
      ctx.waitUntil(Promise.all([
        caches.default.put(cacheUrl, response.clone()),
        env.SEARCH_CACHE ? env.SEARCH_CACHE.put(kvKey, JSON.stringify(payload), { expirationTtl: CACHE_TTL_KV }) : Promise.resolve(),
        logSQL(env, env.SQL_LOGS, {
          route: "/api/search",
          query_name: "search_results",
          query_template: "search_results_by_filters",
          has_employer: !!employerNorm,
          has_job: !!jobNorm,
          has_location: !!locationNorm,
          sort,
          dir,
          page,
          page_size: pageSize,
          duration_ms: duration,
          rows_returned: total,
        }),
        logSearch(env, env.SEARCH_LOGS, { ...searchMeta, cache_tier: "d1", total_results: total, duration_ms: duration }),
      ]));
    }

    return response;
  } catch (err) {
    logError(env, "Search handler failed", err);
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
    const session = db.withSession();
    const row = await session
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
    logError(null, "Record handler failed", err);
    return jsonResponse({ error: "Failed to load record." }, 500, cors);
  }
}

async function handleSuggest(params, db, cors, env, ctx, request) {
  const field = params.get("field") || "";
  const q = (params.get("q") || "").trim().slice(0, MAX_INPUT_LENGTH);
  const ctxEmp = (params.get("employer") || "").trim().slice(0, MAX_INPUT_LENGTH);
  const ctxJob = (params.get("job") || "").trim().slice(0, MAX_INPUT_LENGTH);

  if (field === "employer" || field === "job") {
    const index = await getSuggestionsIndex(env, request);
    if (index) {
      const results = searchFromIndex(index, field, q, ctxEmp, ctxJob);
      return jsonResponse({ results }, 200, cors);
    }
    return jsonResponse({ results: [] }, 200, cors);
  }

  // Location and other fields: no server-side suggest (UI types location free-form).
  return jsonResponse({ results: [] }, 200, cors);
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

  const { host, hostname } = new URL(request.url);
  const workerHost = normHost(host);

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

  // Local dev/smoke tests from Node often omit Origin/Referer.
  // Keep strict checks in production: this fallback is localhost-only.
  if (!origin && !referer && isLocalDevHostname(hostname)) return true;

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

function shouldLogErrors(env) {
  // Default true for operational visibility; can disable with LOG_ERRORS=false.
  return (env?.LOG_ERRORS || "true").toLowerCase() !== "false";
}

function shouldLogSearchTerms(env) {
  // Default false to reduce log-injection / sensitive-input exposure.
  return (env?.LOG_SEARCH_TERMS || "false").toLowerCase() === "true";
}

function sanitizeForLog(value, maxLen = 256) {
  if (value == null) return value;
  const s = String(value)
    .replace(/[\u0000-\u001F\u007F]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return s.length > maxLen ? s.slice(0, maxLen) : s;
}

function sanitizeLogObject(obj) {
  if (obj == null || typeof obj !== "object") return obj;
  const out = Array.isArray(obj) ? [] : {};
  for (const [k, v] of Object.entries(obj)) {
    const key = sanitizeForLog(k, 64);
    if (v == null || typeof v === "number" || typeof v === "boolean") {
      out[key] = v;
    } else if (typeof v === "string") {
      out[key] = sanitizeForLog(v);
    } else if (Array.isArray(v)) {
      out[key] = v.map((x) => (typeof x === "string" ? sanitizeForLog(x) : x));
    } else if (typeof v === "object") {
      out[key] = sanitizeLogObject(v);
    } else {
      out[key] = sanitizeForLog(v);
    }
  }
  return out;
}

function logError(env, message, err) {
  if (!shouldLogErrors(env)) return;
  const msg = sanitizeForLog(message, 120);
  const errMsg = sanitizeForLog(err?.message || err, 200);
  console.error(msg, errMsg || "");
}

async function logSQL(env, bucket, data) {
  if (!bucket) return;
  try {
    const body = JSON.stringify(sanitizeLogObject({ ts: new Date().toISOString(), ...data }));
    await bucket.put(r2Key("sql"), body, { httpMetadata: { contentType: "application/json" } });
  } catch (e) {
    logError(env, "SQL log write failed", e);
  }
}

async function logSearch(env, bucket, data) {
  if (!bucket) return;
  try {
    const safeData = { ...data };
    if (!shouldLogSearchTerms(env)) {
      safeData.employer = safeData.employer ? "[REDACTED]" : null;
      safeData.job = safeData.job ? "[REDACTED]" : null;
      safeData.location = safeData.location ? "[REDACTED]" : null;
    }
    const body = JSON.stringify(sanitizeLogObject({ ts: new Date().toISOString(), ...safeData }));
    await bucket.put(r2Key("search"), body, { httpMetadata: { contentType: "application/json" } });
  } catch (e) {
    logError(env, "Search log write failed", e);
  }
}
