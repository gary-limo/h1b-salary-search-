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
 * header matches this Worker's own hostname (same-site enforcement).
 * Requests with no Origin header (e.g. curl, server-to-server) are allowed.
 */

const PAGE_SIZE = 15;

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

    // Block server-side files from being served as static assets
    if (BLOCKED_PREFIXES.some((p) => url.pathname.startsWith(p))) {
      return new Response("Not Found", { status: 404 });
    }

    // Handle CORS preflight for API routes
    if (request.method === "OPTIONS" && url.pathname.startsWith("/api/")) {
      return new Response(null, {
        status: 204,
        headers: buildCorsHeaders(request),
      });
    }

    // Delegate API requests to handlers
    if (url.pathname.startsWith("/api/")) {
      if (!isSameOrigin(request)) {
        return jsonResponse({ error: "Forbidden" }, 403, {});
      }
      if (request.method !== "GET") {
        return jsonResponse({ error: "Method not allowed" }, 405, buildCorsHeaders(request));
      }

      const cors = buildCorsHeaders(request);

      if (url.pathname === "/api/search") {
        return handleSearch(url.searchParams, env.DB, cors);
      }
      if (url.pathname === "/api/suggest") {
        return handleSuggest(url.searchParams, env.DB, cors);
      }

      return jsonResponse({ error: "Not found" }, 404, cors);
    }

    // Serve all other paths as static assets (index.html, favicon, etc.)
    return env.ASSETS.fetch(request);
  },
};

// ─── Handlers ────────────────────────────────────────────────────────────────

async function handleSearch(params, db, cors) {
  const employer = (params.get("employer") || "").trim();
  const job = (params.get("job") || "").trim();
  const location = (params.get("location") || "").trim();
  const page = Math.max(1, parseInt(params.get("page") || "1", 10));
  const sortParam = params.get("sort") || "wage_rate_of_pay_from";
  const dirParam = (params.get("dir") || "DESC").toUpperCase();

  const sort = SORTABLE.has(sortParam) ? sortParam : "wage_rate_of_pay_from";
  const dir = dirParam === "ASC" ? "ASC" : "DESC";
  const offset = (page - 1) * PAGE_SIZE;

  const where = [];
  const bindings = [];

  if (employer) {
    where.push("LOWER(employer_name) LIKE ?");
    bindings.push(`%${employer.toLowerCase()}%`);
  }
  if (job) {
    where.push("LOWER(job_title) LIKE ?");
    bindings.push(`%${job.toLowerCase()}%`);
  }
  if (location) {
    where.push("(LOWER(worksite_city) LIKE ? OR LOWER(worksite_state) LIKE ?)");
    bindings.push(`%${location.toLowerCase()}%`, `%${location.toLowerCase()}%`);
  }

  const whereClause = where.length ? `WHERE ${where.join(" AND ")}` : "";

  try {
    const [countRow, rows] = await Promise.all([
      db
        .prepare(`SELECT COUNT(*) AS total FROM h1b_wages ${whereClause}`)
        .bind(...bindings)
        .first(),
      db
        .prepare(
          `SELECT employer_name, job_title,
                  wage_rate_of_pay_from,
                  worksite_city, worksite_state,
                  begin_date, end_date
           FROM h1b_wages ${whereClause}
           ORDER BY ${sort} ${dir} NULLS LAST
           LIMIT ${PAGE_SIZE} OFFSET ${offset}`
        )
        .bind(...bindings)
        .all(),
    ]);

    return jsonResponse(
      {
        total: countRow?.total ?? 0,
        page,
        pageSize: PAGE_SIZE,
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

async function handleSuggest(params, db, cors) {
  const field = params.get("field") || "";
  const q = (params.get("q") || "").trim();
  const col = SUGGEST_FIELDS[field];

  if (!col || q.length < 2) {
    return jsonResponse({ results: [] }, 200, cors);
  }

  try {
    const { results } = await db
      .prepare(
        `SELECT DISTINCT ${col} AS value
         FROM h1b_wages
         WHERE LOWER(${col}) LIKE ?
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

// ─── Helpers ─────────────────────────────────────────────────────────────────

function jsonResponse(body, status, extraHeaders) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...extraHeaders },
  });
}

/**
 * Returns true when the request either has no Origin header (direct/same-origin)
 * or the Origin's hostname matches the Worker's own hostname.
 */
function isSameOrigin(request) {
  const origin = request.headers.get("Origin");
  if (!origin) return true;
  try {
    const workerHost = new URL(request.url).host;
    const originHost = new URL(origin).host;
    return originHost === workerHost;
  } catch {
    return false;
  }
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
