/**
 * H1B Salary Search – Cloudflare Worker
 *
 * Serves the static frontend (via env.ASSETS) and a read-only JSON API
 * that queries the D1 `h1b_wages` table.
 *
 * API routes:
 *   GET /api/search   – paginated full-text search
 *   GET /api/suggest  – autocomplete suggestions
 *
 * Cross-origin policy: API requests are only accepted when the Origin
 * or Referer header matches this Worker's own hostname (same-site enforcement).
 * Requests with no matching header are rejected (403).
 */

const DEFAULT_PAGE_SIZE = 100;
const MAX_FETCH_SIZE    = 10000;
const MAX_INPUT_LENGTH = 200;
const MAX_PAGE = 10000;

// Columns that can be used in ORDER BY (whitelist to prevent SQL injection)
const SORTABLE = new Set([
  "employer_name",
  "job_title",
  "wage_rate_of_pay_from",
  "worksite_state",
  "worksite_city",
  "begin_date",
  "end_date",
]);

// Suggestion field mapping (URL param → DB column)
const SUGGEST_FIELDS = {
  employer: "employer_name",
  job: "job_title",
  location: "worksite_city",
};

// Block direct access to server-side source files
const BLOCKED_PREFIXES = ["/src/", "/scripts/", "/migrations/"];

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const staticRouteMap = {
      "/": "/index.html",
      "/compare": "/compare.html",
      "/record": "/record.html",
    };

    // Block server-side files from being served as static assets
    if (BLOCKED_PREFIXES.some((p) => url.pathname.startsWith(p))) {
      return new Response("Not Found", { status: 404 });
    }

    // Delegate API requests to handlers (including CORS preflight)
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
      if (request.method !== "GET") {
        return jsonResponse({ error: "Method not allowed" }, 405, buildCorsHeaders(request));
      }

      // Rate limit: 60 API requests per minute per IP
      const ip = request.headers.get("cf-connecting-ip") || "unknown";
      if (env.API_RATE_LIMITER) {
        const { success } = await env.API_RATE_LIMITER.limit({ key: ip });
        if (!success) {
          return jsonResponse({ error: "Too many requests. Please try again later." }, 429, buildCorsHeaders(request));
        }
      }

      const cors = buildCorsHeaders(request);

      if (url.pathname === "/api/search") {
        return handleSearch(url.searchParams, env.DB, cors);
      }
      if (url.pathname === "/api/suggest") {
        return handleSuggest(url.searchParams, env.DB, cors);
      }
      if (url.pathname === "/api/record") {
        return handleRecord(url.searchParams, env.DB, cors);
      }
      if (url.pathname === "/api/compare") {
        return handleCompare(url.searchParams, env.DB, cors);
      }

      return jsonResponse({ error: "Not found" }, 404, cors);
    }

    // Serve all other paths as static assets (index.html, compare.html, etc.)
    if (staticRouteMap[url.pathname]) {
      const assetUrl = new URL(request.url);
      assetUrl.pathname = staticRouteMap[url.pathname];
      return env.ASSETS.fetch(new Request(assetUrl.toString(), request));
    }

    // Fallback to direct static asset lookup
    return env.ASSETS.fetch(request);
  },
};

// ─── Handlers ────────────────────────────────────────────────────────────────

async function handleSearch(params, db, cors) {
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

  const where = [];
  const bindings = [];

  if (employer) {
    // Prefix match only - enables index use on employer_name (users typically type start of name)
    where.push("w.employer_name LIKE ?");
    bindings.push(`${employer.toLowerCase()}%`);
  }
  if (job) {
    where.push("f.job_title LIKE ?");
    bindings.push(`%${job.toLowerCase()}%`);
  }
  if (location) {
    where.push("(f.worksite_city LIKE ? OR f.worksite_state LIKE ?)");
    bindings.push(`%${location.toLowerCase()}%`, `%${location.toLowerCase()}%`);
  }

  const whereClause = where.length ? `WHERE ${where.join(" AND ")}` : "";

  try {
    const [countRow, rows] = await Promise.all([
      db
        .prepare(
          `SELECT COUNT(*) AS total
           FROM h1b_wages_fts f
           JOIN h1b_wages w ON w.id = f.rowid
           ${whereClause}`
        )
        .bind(...bindings)
        .first(),
      db
        .prepare(
          `SELECT w.id, w.employer_name, w.job_title,
                  w.wage_rate_of_pay_from,
                  w.worksite_city, w.worksite_state,
                  w.begin_date, w.end_date
           FROM h1b_wages_fts f
           JOIN h1b_wages w ON w.id = f.rowid
           ${whereClause}
           ORDER BY w.${sort} ${dir} NULLS LAST
           LIMIT ${pageSize} OFFSET ${offset}`
        )
        .bind(...bindings)
        .all(),
    ]);

    return jsonResponse(
      {
        total: countRow?.total ?? 0,
        page,
        pageSize,
        results: rows.results,
      },
      200,
      cors
    );
  } catch (err) {
    console.error("Search error:", err);
    return jsonResponse({ error: "Search failed. Please try again." }, 500, cors);
  }
}

async function handleRecord(params, db, cors) {
  const id = parseInt(params.get("id") || "0", 10);
  if (!id || id < 1 || !Number.isFinite(id)) {
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

async function handleSuggest(params, db, cors) {
  const field = params.get("field") || "";
  const q = (params.get("q") || "").trim().slice(0, MAX_INPUT_LENGTH);
  const col = SUGGEST_FIELDS[field];

  if (!col || q.length < 2) {
    return jsonResponse({ results: [] }, 200, cors);
  }

  try {
    const { results } = await db
      .prepare(
        `SELECT DISTINCT ${col} AS value
         FROM h1b_wages
         WHERE ${col} LIKE ?
         ORDER BY ${col}
         LIMIT 8`
      )
      .bind(`${q.toLowerCase()}%`)
      .all();

    return jsonResponse(
      { results: results.map((r) => r.value).filter(Boolean) },
      200,
      cors
    );
  } catch (err) {
    console.error("Suggest error:", err);
    return jsonResponse({ error: "Suggestion lookup failed." }, 500, cors);
  }
}

async function handleCompare(params, db, cors) {
  const raw = (params.get("employers") || "").trim();
  if (!raw) {
    return jsonResponse({ error: "Provide at least one employer." }, 400, cors);
  }

  const employers = raw.split("||").map(e => e.trim()).filter(Boolean).slice(0, 5);
  if (employers.length === 0) {
    return jsonResponse({ error: "Provide at least one employer." }, 400, cors);
  }

  const placeholders = employers.map(() => "?").join(",");

  try {
    const { results } = await db
      .prepare(
        `SELECT employer_name, ROUND(avg_wage, 0) AS avg_wage,
                std_career_level
         FROM h1b_salary_summary
         WHERE employer_name IN (${placeholders})
         ORDER BY employer_name, std_career_level`
      )
      .bind(...employers)
      .all();

    return jsonResponse({ results: results || [] }, 200, cors);
  } catch (err) {
    console.error("Compare error:", err);
    const msg = String(err && err.message ? err.message : err);
    if (msg.includes("no such table: h1b_salary_summary")) {
      return jsonResponse({ error: "Compare data is not initialized. Run migration 0002 to create h1b_salary_summary." }, 500, cors);
    }
    return jsonResponse({ error: "Comparison failed. Please try again." }, 500, cors);
  }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function jsonResponse(body, status, extraHeaders) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...extraHeaders },
  });
}

/**
 * Allow requests from our own site. Normalize www/non-www so both work.
 * Reject: curl, Postman, scripts (no Origin/Referer), other sites.
 */
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

  return false;
}

function buildCorsHeaders(request) {
  const origin = request.headers.get("Origin") || "";
  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Vary": "Origin",
  };
}
