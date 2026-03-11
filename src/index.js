/**
 * H1B Salary Search – Cloudflare Worker
 *
 * Serves the static frontend (via env.ASSETS) and a read-only JSON API
 * that queries the D1 `h1b_wages` table.
 *
 * API routes:
 *   GET  /api/search      – paginated full-text search
 *   GET  /api/suggest     – autocomplete suggestions
 *   POST /api/compare-ai  – AI-powered salary comparison (rate-limited)
 *
 * Cross-origin policy: API requests are only accepted when the Origin
 * or Referer header matches this Worker's own hostname (same-site enforcement).
 * Requests with no matching header are rejected (403).
 */

const DEFAULT_PAGE_SIZE = 100;
const MAX_FETCH_SIZE    = 10000;
const MAX_INPUT_LENGTH = 200;
const MAX_PAGE = 10000;
const DEFAULT_COMPARE_ENABLED = true;

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

// Suggestion field mapping (URL param → DB column + dedicated suggest table)
const SUGGEST_FIELDS = {
  employer:  { col: "employer_name", table: "h1b_suggest_employers" },
  job:       { col: "job_title",     table: "h1b_suggest_jobs"      },
  location:  { col: "worksite_city", table: "h1b_suggest_locations" },
};

// Block direct access to server-side source files
const BLOCKED_PREFIXES = ["/src/", "/scripts/", "/migrations/"];

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const compareEnabled = isCompareEnabled(env);
    const staticRouteMap = {
      "/": "/index.html",
      "/compare": "/compare.html",
      "/record": "/record.html",
    };

    // Block server-side files from being served as static assets
    if (BLOCKED_PREFIXES.some((p) => url.pathname.startsWith(p))) {
      return new Response("Not Found", { status: 404 });
    }

    // Feature-gate compare so hidden pages are not directly reachable.
    const COMPARE_PATHS = ["/compare", "/compare.html", "/api/compare", "/api/compare-ai"];
    if (!compareEnabled && COMPARE_PATHS.includes(url.pathname)) {
      return jsonResponse({ error: "Not found" }, 404, {});
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

      const cors = buildCorsHeaders(request);

      // /api/compare-ai is POST-only; everything else is GET-only
      if (url.pathname === "/api/compare-ai") {
        if (request.method !== "POST") {
          return jsonResponse({ error: "Method not allowed" }, 405, cors);
        }
        const ip = request.headers.get("cf-connecting-ip") || "unknown";
        if (env.AI_RATE_LIMITER) {
          const { success } = await env.AI_RATE_LIMITER.limit({ key: ip });
          if (!success) {
            return jsonResponse({ error: "AI limit reached (5/min). Please wait." }, 429, cors);
          }
        }
        return handleCompareAI(request, env.DB, env.AI, cors);
      }

      if (request.method !== "GET") {
        return jsonResponse({ error: "Method not allowed" }, 405, cors);
      }

      const ip = request.headers.get("cf-connecting-ip") || "unknown";
      if (env.API_RATE_LIMITER) {
        const { success } = await env.API_RATE_LIMITER.limit({ key: ip });
        if (!success) {
          return jsonResponse({ error: "Too many requests. Please try again later." }, 429, cors);
        }
      }

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
    // Prefix match — uses idx_h1b_employer (B-tree range scan)
    where.push("employer_name LIKE ?");
    bindings.push(`${employer.toLowerCase()}%`);
  }
  if (job) {
    where.push("job_title LIKE ?");
    bindings.push(`%${job.toLowerCase()}%`);
  }
  if (location) {
    where.push("(worksite_city LIKE ? OR worksite_state LIKE ?)");
    bindings.push(`%${location.toLowerCase()}%`, `%${location.toLowerCase()}%`);
  }

  const whereClause = where.length ? `WHERE ${where.join(" AND ")}` : "";

  try {
    const [countRow, rows] = await Promise.all([
      db
        .prepare(
          `SELECT COUNT(*) AS total
           FROM h1b_wages
           ${whereClause}`
        )
        .bind(...bindings)
        .first(),
      db
        .prepare(
          `SELECT id, employer_name, job_title,
                  wage_rate_of_pay_from,
                  worksite_city, worksite_state,
                  begin_date, end_date
           FROM h1b_wages
           ${whereClause}
           ORDER BY ${sort} ${dir} NULLS LAST
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
  const field    = params.get("field") || "";
  const q        = (params.get("q")        || "").trim().slice(0, MAX_INPUT_LENGTH);
  const ctxEmp   = (params.get("employer") || "").trim().slice(0, MAX_INPUT_LENGTH);
  const ctxJob   = (params.get("job")      || "").trim().slice(0, MAX_INPUT_LENGTH);
  const ctxLoc   = (params.get("location") || "").trim().slice(0, MAX_INPUT_LENGTH);
  const cfg      = SUGGEST_FIELDS[field];

  if (!cfg) return jsonResponse({ results: [] }, 200, cors);

  const { col, table } = cfg;
  const hasContext = (field !== "employer" && ctxEmp) ||
                     (field !== "job"      && ctxJob) ||
                     (field !== "location" && ctxLoc);

  // Require at least 2 chars unless context is set (then show hints on focus too)
  if (!hasContext && q.length < 2) {
    return jsonResponse({ results: [] }, 200, cors);
  }

  try {
    let stmt;

    if (hasContext) {
      // Context-aware path: anchor on the exact selected value (composite index seek),
      // use LIKE '%q%' (contains) for flexibility. When q is empty (focus trigger),
      // return random samples so user sees what's available without typing.
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
        // Contains match — user is actively typing; composite index handles the context filter
        ctxWhere.push(`${col} LIKE ?`);
        ctxBindings.push(`%${q}%`);
        stmt = db.prepare(
          `SELECT DISTINCT ${col} AS value
           FROM h1b_wages
           WHERE ${ctxWhere.join(" AND ")} AND ${col} != ''
           ORDER BY ${col}
           LIMIT 8`
        ).bind(...ctxBindings);
      } else {
        // No query yet (focus trigger): return random samples scoped to context
        // so user gets a preview of what exists without typing anything
        stmt = db.prepare(
          `SELECT value FROM (
             SELECT DISTINCT ${col} AS value
             FROM h1b_wages
             WHERE ${ctxWhere.join(" AND ")} AND ${col} != ''
           ) ORDER BY RANDOM() LIMIT 8`
        ).bind(...ctxBindings);
      }
    } else {
      // No context: fast prefix scan on the small suggest table (index range scan)
      stmt = db.prepare(
        `SELECT ${col} AS value
         FROM ${table}
         WHERE ${col} LIKE ?
         ORDER BY ${col}
         LIMIT 8`
      ).bind(`${q}%`);
    }

    const { results } = await stmt.all();
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

  const employers = raw.split("||").map(e => e.trim()).filter(Boolean).slice(0, 4);
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

const AI_MAX_EMPLOYERS = 4;
const AI_MAX_ROLES = 20;
const AI_MODEL = "@cf/meta/llama-3.1-8b-instruct";
const AI_MAX_TOKENS = 1024;
const AI_MAX_HISTORY = 10;

async function handleCompareAI(request, db, ai, cors) {
  let body;
  try {
    body = await request.json();
  } catch {
    return jsonResponse({ error: "Invalid JSON body." }, 400, cors);
  }

  const raw = Array.isArray(body?.employers) ? body.employers : [];
  const employers = raw
    .map(e => (typeof e === "string" ? e.trim() : ""))
    .filter(Boolean)
    .slice(0, AI_MAX_EMPLOYERS);

  if (employers.length < 2) {
    return jsonResponse({ error: "Provide 2–4 employer names." }, 400, cors);
  }

  const userMessages = Array.isArray(body?.messages) ? body.messages : [];
  const isChat = userMessages.length > 0;
  const wantsStream = body?.stream === true;

  try {
    const placeholders = employers.map(() => "?").join(",");

    const [summaryResult, rolesResult] = await Promise.all([
      db.prepare(
        `SELECT employer_name, ROUND(avg_wage, 0) AS avg_wage, std_career_level
         FROM h1b_salary_summary
         WHERE employer_name IN (${placeholders})
         ORDER BY employer_name, std_career_level`
      ).bind(...employers).all(),
      db.prepare(
        `SELECT employer_name, job_title, ROUND(AVG(wage_rate_of_pay_from), 0) AS avg_wage,
                COUNT(*) AS filings
         FROM h1b_wages
         WHERE employer_name IN (${placeholders})
         GROUP BY employer_name, job_title
         ORDER BY employer_name, avg_wage DESC
         LIMIT ${AI_MAX_ROLES}`
      ).bind(...employers).all(),
    ]);

    const summaryData = summaryResult?.results || [];
    const rolesData = rolesResult?.results || [];

    if (rolesData.length === 0) {
      return jsonResponse({ error: "No salary data found for those employers." }, 404, cors);
    }

    const dataBlock = rolesData
      .map(r => `${r.employer_name} | ${r.job_title} | $${r.avg_wage} | ${r.filings} filings`)
      .join("\n");

    const systemMsg = [
      "You are a friendly, knowledgeable H-1B salary analyst. You have the following real salary data ",
      "(US Dept of Labor, FY2025). Each row: Employer | Job Title | Avg Annual Wage | Filings.",
      "",
      dataBlock,
      "",
      "Rules:",
      "- Answer questions about this data conversationally and concisely.",
      "- Explain what roles do, find equivalent roles across employers, compare pay.",
      "- Use only the data above. Do not invent numbers.",
      "- Format dollar amounts with commas (e.g. $120,000).",
      "- Keep responses short (2-4 paragraphs max) unless user asks for detail.",
      "- Be engaging and helpful, like a smart colleague explaining salary data.",
    ].join("\n");

    const messages = [{ role: "system", content: systemMsg }];

    if (isChat) {
      const history = userMessages.slice(-AI_MAX_HISTORY);
      for (const m of history) {
        const role = m.role === "assistant" ? "assistant" : "user";
        const content = String(m.content || "").slice(0, 500);
        if (content) messages.push({ role, content });
      }
    } else {
      messages.push({
        role: "user",
        content: "Give me a quick, engaging overview of how these employers compare on H-1B salaries. " +
          "What roles do they hire for, and who pays best?"
      });
    }

    if (wantsStream) {
      const stream = await ai.run(AI_MODEL, {
        messages,
        max_tokens: AI_MAX_TOKENS,
        temperature: 0.5,
        stream: true,
      });
      return new Response(stream, {
        headers: { "content-type": "text/event-stream", ...cors },
      });
    }

    const aiResult = await ai.run(AI_MODEL, {
      messages,
      max_tokens: AI_MAX_TOKENS,
      temperature: 0.5,
    });

    const response = { analysis: aiResult?.response ?? "", data: summaryData };
    if (!isChat) response.data = summaryData;
    return jsonResponse(response, 200, cors);
  } catch (err) {
    console.error("Compare-AI error:", err);
    return jsonResponse({ error: "AI comparison failed. Please try again." }, 500, cors);
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
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Vary": "Origin",
  };
}

function isCompareEnabled(env) {
  const raw = env?.COMPARE_ENABLED;
  if (raw == null) return DEFAULT_COMPARE_ENABLED;
  return String(raw).toLowerCase() === "true";
}
