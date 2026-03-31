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
const TURNSTILE_SESSION_COOKIE = "h1b_ts_sess";
const TURNSTILE_SESSION_MAX_AGE_SEC = 2 * 60 * 60;
const SITEVERIFY_URL = "https://challenges.cloudflare.com/turnstile/v0/siteverify";
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

  // Non-empty query: search only within the contextual set (small, in-memory) when the other
  // dimension is known — not the full global employers/jobs arrays.
  let candidates;
  if (field === "job" && contextEmployer) {
    const arr = index.employerToJobs[contextEmployer.toLowerCase()];
    if (!Array.isArray(arr) || arr.length === 0) return [];
    candidates = [...new Set(arr)].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
  } else if (field === "employer" && contextJob) {
    const arr = index.jobToEmployers[contextJob.toLowerCase()];
    if (!Array.isArray(arr) || arr.length === 0) return [];
    candidates = [...new Set(arr)].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
  } else {
    candidates = list;
  }

  const seen = new Set();
  const prefix = [];
  const substr = [];

  for (const s of candidates) {
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

function isTurnstileConfigured(env) {
  const site = (env.TURNSTILE_SITE_KEY || "").trim();
  const secret = (env.TURNSTILE_SECRET_KEY || "").trim();
  return !!(site && secret);
}

/** Only `GET /api/search` needs a Turnstile session (salary table); suggest/record are ungated. */
function shouldRequireTurnstileSession(env, request) {
  if (env.SKIP_TURNSTILE === "true") return false;
  if (!isTurnstileConfigured(env)) return false;
  try {
    const { hostname, pathname } = new URL(request.url);
    if (isLocalDevHostname(hostname)) return false;
    if (pathname !== "/api/search") return false;
  } catch {
    return false;
  }
  return true;
}

/** Avoid loading the Turnstile iframe on localhost (http) — mismatched with challenges.cloudflare.com (https) causes 110200 / frame errors. */
function shouldExposeTurnstileWidget(request, env) {
  if (!isTurnstileConfigured(env)) return false;
  if (env.TURNSTILE_SHOW_ON_LOCALHOST === "true") return true;
  try {
    const { hostname } = new URL(request.url);
    if (isLocalDevHostname(hostname)) return false;
  } catch {
    /* fall through */
  }
  return true;
}

function escapeHtmlAttr(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/</g, "&lt;");
}

function getCookie(request, name) {
  const header = request.headers.get("Cookie");
  if (!header) return null;
  const parts = header.split(";");
  for (const part of parts) {
    const idx = part.indexOf("=");
    if (idx === -1) continue;
    const k = part.slice(0, idx).trim();
    if (k === name) return decodeURIComponent(part.slice(idx + 1).trim());
  }
  return null;
}

function bufToHex(buf) {
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function hmacSha256Hex(message, secret) {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(message));
  return bufToHex(sig);
}

async function verifyTurnstileSessionCookie(raw, secret) {
  if (!raw || !secret) return false;
  const dot = raw.indexOf(".");
  if (dot === -1) return false;
  const expStr = raw.slice(0, dot);
  const sig = raw.slice(dot + 1);
  const exp = parseInt(expStr, 10);
  if (!Number.isFinite(exp) || exp < Math.floor(Date.now() / 1000)) return false;
  const expected = await hmacSha256Hex(expStr, secret);
  return sig === expected;
}

function buildSessionSetCookie(value, maxAgeSec, request) {
  const secure = new URL(request.url).protocol === "https:";
  const parts = [
    `${TURNSTILE_SESSION_COOKIE}=${encodeURIComponent(value)}`,
    "HttpOnly",
    "Path=/",
    `Max-Age=${maxAgeSec}`,
    "SameSite=Lax",
  ];
  if (secure) parts.push("Secure");
  return parts.join("; ");
}

async function verifyTurnstileSiteverify(token, remoteip, secret) {
  const body = JSON.stringify({
    secret,
    response: token,
    ...(remoteip ? { remoteip } : {}),
  });
  const res = await fetch(SITEVERIFY_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
  });
  if (!res.ok) {
    return { success: false, "error-codes": ["siteverify-http"] };
  }
  return res.json();
}

async function handleTurnstileSession(request, env, cors) {
  if (!isTurnstileConfigured(env)) {
    return jsonResponse({ error: "Turnstile is not configured." }, 503, cors);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return jsonResponse({ error: "Invalid JSON." }, 400, cors);
  }
  const token = body?.response;
  if (!token || typeof token !== "string" || token.length > 2048) {
    return jsonResponse({ error: "Missing or invalid Turnstile response." }, 400, cors);
  }
  const remoteip = request.headers.get("cf-connecting-ip") || "";
  const result = await verifyTurnstileSiteverify(token, remoteip, env.TURNSTILE_SECRET_KEY);
  if (!result.success) {
    logError(env, "Turnstile siteverify failed", (result["error-codes"] || []).join(","));
    return jsonResponse({ error: "Verification failed." }, 400, cors);
  }
  const exp = Math.floor(Date.now() / 1000) + TURNSTILE_SESSION_MAX_AGE_SEC;
  const expStr = String(exp);
  const sig = await hmacSha256Hex(expStr, env.TURNSTILE_SECRET_KEY);
  const cookieVal = `${expStr}.${sig}`;
  const res = jsonResponse({ ok: true }, 200, cors);
  res.headers.set("Set-Cookie", buildSessionSetCookie(cookieVal, TURNSTILE_SESSION_MAX_AGE_SEC, request));
  return res;
}

async function hasValidTurnstileSession(request, env) {
  const raw = getCookie(request, TURNSTILE_SESSION_COOKIE);
  if (!raw) return false;
  return verifyTurnstileSessionCookie(raw, env.TURNSTILE_SECRET_KEY);
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

      if (url.pathname === "/api/turnstile/config" && request.method === "GET") {
        const siteKey = (env.TURNSTILE_SITE_KEY || "").trim();
        const expose = shouldExposeTurnstileWidget(request, env);
        return jsonResponse({ siteKey: expose ? siteKey : null }, 200, cors);
      }

      if (url.pathname === "/api/turnstile/session" && request.method === "POST") {
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
        return handleTurnstileSession(request, env, cors);
      }

      if (request.method !== "GET") {
        return jsonResponse({ error: "Method not allowed" }, 405, cors);
      }

      if (shouldRequireTurnstileSession(env, request)) {
        if (!(await hasValidTurnstileSession(request, env))) {
          return jsonResponse(
            { error: "Verification required.", code: "turnstile_required" },
            403,
            cors
          );
        }
      }

      // Rate limit DB search only; /api/suggest is high-frequency while typing.
      const ip = request.headers.get("cf-connecting-ip") || "unknown";
      if (
        url.pathname === "/api/search" &&
        shouldApplyApiRateLimit(request, env)
      ) {
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
      const siteKey = (env.TURNSTILE_SITE_KEY || "").trim();
      const exposeTurnstile = shouldExposeTurnstileWidget(request, env);
      // Turnstile only for search (/); /api/record is ungated — do not inject on record detail page.
      if (siteKey && exposeTurnstile && assetUrl.pathname !== "/record.html") {
        const html = await response.text();
        let inject = '<meta charset="UTF-8">';
        inject += `<meta name="turnstile-site-key" content="${escapeHtmlAttr(siteKey)}">`;
        const injected = html.replace("<meta charset=\"UTF-8\">", inject);
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

/** Bump wrangler var SEARCH_CACHE_VERSION and redeploy for global search cache invalidation (KV + edge). */
function searchCacheVersion(env) {
  const s = String(env?.SEARCH_CACHE_VERSION ?? "1").trim();
  return s || "1";
}

function buildSearchCacheKey(version, employer, job, location, sort, dir, page, pageSize, jobMatch) {
  return `search:v${version}:${employer}:${job}:${location}:${sort}:${dir}:${page}:${pageSize}:${jobMatch}`;
}

/** Escape `%`, `_`, `\` for SQL LIKE with ESCAPE '\' */
function escapeSqlLikePattern(s) {
  return s.replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_");
}

/** `contains` only when employer + job present (substring match on job_title); else `exact`. */
function resolvedJobMatch(params, employerNorm, jobNorm) {
  const raw = (params.get("job_match") || params.get("jobMatch") || "exact").trim().toLowerCase();
  if (raw === "contains" && employerNorm && jobNorm) return "contains";
  return "exact";
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

  const employerNorm = employer.toLowerCase();
  const jobNorm = job.toLowerCase();
  const locationNorm = location.toLowerCase();
  const jobMatch = resolvedJobMatch(params, employerNorm, jobNorm);

  const cacheVer = searchCacheVersion(env);
  const kvKey = buildSearchCacheKey(cacheVer, employer, job, location, sort, dir, page, pageSize, jobMatch);
  const cacheUrl = new Request(`https://cache.internal/${encodeURIComponent(kvKey)}`);
  const searchMeta = {
    employer: employer || null,
    job: job || null,
    location: location || null,
    job_match: jobMatch,
    sort,
    dir,
    page,
    page_size: pageSize,
  };

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

  if (employerNorm) {
    where.push("employer_name = ?");
    bindings.push(employerNorm);
  }
  if (jobNorm) {
    if (jobMatch === "contains") {
      const likePat = `%${escapeSqlLikePattern(jobNorm)}%`;
      where.push("job_title LIKE ? ESCAPE '\\'");
      bindings.push(likePat);
    } else {
      where.push("job_title = ?");
      bindings.push(jobNorm);
    }
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
          job_match: jobMatch,
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
  const idRaw = (params.get("id") || "").trim().slice(0, 50);
  if (!idRaw || !UUID_REGEX.test(idRaw)) {
    return jsonResponse({ error: "Invalid record ID." }, 400, cors);
  }
  // SQLite TEXT equality is case-sensitive; normalize so URL case matches stored id.
  const id = idRaw.toLowerCase();
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
    return false;
  }

  if (referer) {
    try {
      if (normHost(new URL(referer).host) === workerHost) return true;
    } catch {}
    return false;
  }

  // Local dev/smoke tests from Node often omit Origin/Referer.
  if (isLocalDevHostname(hostname)) return true;

  // Same-origin GET/fetch often omits Origin; Referer may be stripped (privacy / policies).
  // Reject obvious cross-site navigations; allow when Host matches the request URL.
  if (request.headers.get("Sec-Fetch-Site") === "cross-site") return false;

  const hostHeader = request.headers.get("Host");
  if (hostHeader && normHost(hostHeader) === workerHost) return true;

  return false;
}

function buildCorsHeaders(request) {
  const origin = request.headers.get("Origin") || "";
  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
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
    const body = JSON.stringify(sanitizeLogObject({ ts: new Date().toISOString(), ...data }));
    await bucket.put(r2Key("search"), body, { httpMetadata: { contentType: "application/json" } });
  } catch (e) {
    logError(env, "Search log write failed", e);
  }
}
