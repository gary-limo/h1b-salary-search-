#!/usr/bin/env node
/**
 * Local smoke test — run while `npm run dev` is up.
 *
 *   npm run test
 *   SMOKE_QUIET=1 npm run test   # summary only (no per-case expected/actual)
 *
 * By default refuses non-localhost BASE_URL (use ALLOW_REMOTE_SMOKE=1 to override).
 */

const BASE = process.env.BASE_URL || "http://127.0.0.1:8787";
const QUIET = process.env.SMOKE_QUIET === "1";
const SAMPLE_ROWS = Math.min(15, Math.max(1, Number(process.env.SMOKE_SAMPLE_ROWS) || 3));

function apiHeaders() {
  return { Accept: "application/json" };
}

function log(...args) {
  if (!QUIET) console.log(...args);
}

function assertLocalBaseUrl() {
  let hostname;
  try {
    hostname = new URL(BASE).hostname.toLowerCase();
  } catch {
    throw new Error(`Invalid BASE_URL: ${BASE}`);
  }
  const local =
    hostname === "localhost" ||
    hostname === "127.0.0.1" ||
    hostname === "::1" ||
    hostname.endsWith(".localhost");
  if (!local && process.env.ALLOW_REMOTE_SMOKE !== "1") {
    console.error(
      `Refusing to run smoke tests against non-local host (${hostname}).\n` +
        `Use BASE_URL=http://127.0.0.1:8787 (or set ALLOW_REMOTE_SMOKE=1 to override).`,
    );
    process.exit(1);
  }
}

function truncate(s, max = 72) {
  if (s == null) return "";
  const t = String(s);
  return t.length <= max ? t : `${t.slice(0, max)}…`;
}

function summarizeRow(row) {
  return {
    employer_name: row.employer_name,
    job_title: truncate(row.job_title, 56),
    wage_rate_of_pay_from: row.wage_rate_of_pay_from,
    worksite_city: row.worksite_city,
    worksite_state: row.worksite_state,
    begin_date: row.begin_date,
    end_date: row.end_date,
  };
}

async function suggestPool(field, seeds) {
  const set = new Set();
  for (const q of seeds) {
    const u = new URL("/api/suggest", BASE);
    u.searchParams.set("field", field);
    u.searchParams.set("q", q);
    const r = await fetch(u, { headers: apiHeaders() });
    if (!r.ok) {
      throw new Error(`GET ${u.pathname}${u.search} → HTTP ${r.status}`);
    }
    const data = await r.json();
    const results = Array.isArray(data.results) ? data.results : [];
    for (const x of results) {
      if (typeof x === "string" && x.trim()) set.add(x.trim());
    }
  }
  return [...set];
}

function pickRandomUnique(arr, n) {
  const shuffled = [...arr].sort(() => Math.random() - 0.5);
  return shuffled.slice(0, Math.min(n, shuffled.length));
}

const SUGGEST_SEEDS = ["a", "e", "i", "o", "s", "m", "t", "p", "r", "c", "d", "g", "h", "n", "l"];

const LOCATIONS = [
  "CA",
  "TX",
  "NY",
  "WA",
  "FL",
  "IL",
  "Seattle",
  "San Francisco",
  "Boston",
  "Chicago",
];

/** Exact strings that must return ≥1 row — exercises & and similar in query encoding. */
const SPECIAL_CASE_EMPLOYER = "E&K Sunrise Inc";
const SPECIAL_CASE_JOB = "R&D Analyst";

/** Exercises exact employer + `job_match=contains` (typed keyword vs full suggested title). */
const KLA_CORPORATION_EXAMPLE_EMPLOYER = "KLA Corporation";
const KLA_CORPORATION_EXAMPLE_JOB_KEYWORD = "mechanical";

function rowMatchesLocation(row, loc) {
  const l = loc.toLowerCase();
  const city = (row.worksite_city || "").toLowerCase();
  const state = (row.worksite_state || "").toLowerCase();
  return city === l || state === l;
}

async function assertAsset(path, label) {
  const url = new URL(path, BASE);
  const r = await fetch(url, { headers: { Accept: "*/*" } });
  if (r.status !== 200) {
    throw new Error(`${label} ${path}: expected HTTP 200, got ${r.status}`);
  }
  const ct = r.headers.get("content-type") || "";
  log(`  [${label}] ${path}`);
  log(`    Expected: HTTP 200`);
  log(`    Actual:   HTTP ${r.status}, Content-Type: ${ct || "(none)"}`);
}

function printExpectedActual(kind, params, body) {
  log(`    Expected:`);
  if (kind === "employer") {
    log(`      Every row: employer_name === ${JSON.stringify(params.employer)} (case-insensitive)`);
  } else if (kind === "job") {
    log(`      Every row: job_title === ${JSON.stringify(params.job)} (case-insensitive)`);
  } else if (kind === "location") {
    log(
      `      Every row: worksite_city === ${JSON.stringify(params.location)} OR worksite_state === ${JSON.stringify(params.location)} (case-insensitive)`,
    );
  } else if (kind === "employer_job_contains") {
    log(
      `      Every row: employer_name === ${JSON.stringify(params.employer)} (case-insensitive); job_title contains ${JSON.stringify(params.job)} (case-insensitive); job_match=contains`,
    );
  }
  log(`    Actual:`);
  log(`      total=${body.total}, page=${body.page}, pageSize=${body.pageSize}, rows_in_page=${body.results.length}`);
  if (body.results.length === 0) {
    log(`      (no rows in this page — ${body.total === 0 ? "no DB matches for this query" : "pagination"})`);
    return;
  }
  const n = Math.min(SAMPLE_ROWS, body.results.length);
  log(`      Sample rows (first ${n}):`);
  for (let i = 0; i < n; i++) {
    log(`        ${i + 1}. ${JSON.stringify(summarizeRow(body.results[i]))}`);
  }
}

async function assertSearch(label, params, kind, assertRow) {
  const u = new URL("/api/search", BASE);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null) u.searchParams.set(k, String(v));
  });
  log(`\n  --- ${label} ---`);
  log(`    Request: GET ${u.pathname}${u.search}`);

  const r = await fetch(u, { headers: apiHeaders() });
  log(`    HTTP: ${r.status} (expected 200)`);
  if (r.status !== 200) {
    throw new Error(`${label}: HTTP ${r.status}`);
  }
  const body = await r.json();
  if (typeof body.total !== "number" || !Array.isArray(body.results)) {
    throw new Error(`${label}: expected { total: number, results: array[] }`);
  }
  if (typeof body.page !== "number" || typeof body.pageSize !== "number") {
    throw new Error(`${label}: expected page and pageSize`);
  }

  printExpectedActual(kind, params, body);

  if (body.total > 0 && assertRow) {
    for (const row of body.results) {
      assertRow(row, params);
    }
    log(`    Check:    ✓ all ${body.results.length} rows on this page match expected`);
  } else if (body.total === 0 && (kind === "employer" || kind === "job")) {
    log(`    Check:    ⚠ total=0 — unexpected for exact match from /api/suggest`);
  } else if (body.total === 0 && kind === "employer_job_contains") {
    log(`    Check:    ⚠ total=0 — unexpected for employer + job substring search`);
  } else if (body.total === 0 && kind === "location") {
    log(`    Check:    (skipped row match — no results for this location term)`);
  }

  return body;
}

async function main() {
  assertLocalBaseUrl();
  console.log(`Smoke test (local) → ${BASE}`);
  if (QUIET) console.log("(SMOKE_QUIET=1: detail hidden)\n");
  else console.log("");

  await assertAsset("/", "asset");
  await assertAsset("/index.html", "asset");
  await assertAsset("/favicon.svg", "asset");
  await assertAsset("/insights/", "insights");
  await assertAsset("/insights/list-of-h1b-concurrent-employers-2026/", "insights");
  log(`\n  ✓ Assets OK (5 requests above)`);

  log(`\n  Building suggest pools (parallel employer + job)…`);
  const [employerPool, jobPool] = await Promise.all([
    suggestPool("employer", SUGGEST_SEEDS),
    suggestPool("job", SUGGEST_SEEDS),
  ]);
  log(`    Unique employers collected: ${employerPool.length}`);
  log(`    Unique job titles collected: ${jobPool.length}`);

  if (employerPool.length === 0) {
    throw new Error("No employers from /api/suggest — check suggestions index / R2 / dev.");
  }
  if (jobPool.length === 0) {
    throw new Error("No jobs from /api/suggest — check suggestions index / R2 / dev.");
  }

  const employers = pickRandomUnique(employerPool, 10);
  const jobs = pickRandomUnique(jobPool, 10);

  if (employers.length < 10) {
    console.warn(
      `  ⚠ Only ${employers.length} unique employers from suggest (expected 10); running fewer checks.`,
    );
  }
  if (jobs.length < 10) {
    console.warn(
      `  ⚠ Only ${jobs.length} unique jobs from suggest (expected 10); running fewer checks.`,
    );
  }

  log(`\n  === /api/search by employer (${employers.length} cases) ===`);
  let ei = 0;
  for (const employer of employers) {
    ei++;
    await assertSearch(
      `Employer ${ei}/${employers.length}: ${truncate(employer, 60)}`,
      { employer, pageSize: 5 },
      "employer",
      (row, p) => {
        if (row.employer_name.toLowerCase() !== p.employer.toLowerCase()) {
          throw new Error(`employer mismatch: got ${JSON.stringify(row.employer_name)}`);
        }
      },
    );
  }
  log(`\n  ✓ /api/search employer — ${employers.length} queries passed`);

  log(`\n  === /api/search by job (${jobs.length} cases) ===`);
  let ji = 0;
  for (const job of jobs) {
    ji++;
    await assertSearch(
      `Job ${ji}/${jobs.length}: ${truncate(job, 60)}`,
      { job, pageSize: 5 },
      "job",
      (row, p) => {
        if (row.job_title.toLowerCase() !== p.job.toLowerCase()) {
          throw new Error(`job_title mismatch: got ${JSON.stringify(row.job_title)}`);
        }
      },
    );
  }
  log(`\n  ✓ /api/search job — ${jobs.length} queries passed`);

  log(`\n  === /api/search by location (${LOCATIONS.length} cases) ===`);
  let locNonZero = 0;
  let li = 0;
  for (const location of LOCATIONS) {
    li++;
    const body = await assertSearch(
      `Location ${li}/${LOCATIONS.length}: ${location}`,
      { location, pageSize: 5 },
      "location",
      (row, p) => {
        if (!rowMatchesLocation(row, p.location)) {
          throw new Error(
            `location mismatch for ${JSON.stringify(p.location)}: row city/state ${JSON.stringify(
              row.worksite_city,
            )}/${JSON.stringify(row.worksite_state)}`,
          );
        }
      },
    );
    if (body.total > 0) locNonZero++;
  }
  log(`\n  ✓ /api/search location — ${LOCATIONS.length} queries passed (${locNonZero} with total > 0)`);

  log(`\n  === /api/search special cases (& in employer / job title) ===`);
  log(`    (require total > 0 — URLSearchParams encodes & in query values)`);

  const specialEmployerBody = await assertSearch(
    `Special employer: ${SPECIAL_CASE_EMPLOYER}`,
    { employer: SPECIAL_CASE_EMPLOYER, pageSize: 5 },
    "employer",
    (row, p) => {
      if (row.employer_name.toLowerCase() !== p.employer.toLowerCase()) {
        throw new Error(`employer mismatch: got ${JSON.stringify(row.employer_name)}`);
      }
    },
  );
  if (specialEmployerBody.total === 0) {
    throw new Error(
      `Special employer ${JSON.stringify(SPECIAL_CASE_EMPLOYER)} returned no rows — expected ≥1 (check DB spelling / data load).`,
    );
  }

  const specialJobBody = await assertSearch(
    `Special job: ${SPECIAL_CASE_JOB}`,
    { job: SPECIAL_CASE_JOB, pageSize: 5 },
    "job",
    (row, p) => {
      if (row.job_title.toLowerCase() !== p.job.toLowerCase()) {
        throw new Error(`job_title mismatch: got ${JSON.stringify(row.job_title)}`);
      }
    },
  );
  if (specialJobBody.total === 0) {
    throw new Error(
      `Special job ${JSON.stringify(SPECIAL_CASE_JOB)} returned no rows — expected ≥1 (check DB spelling / data load).`,
    );
  }

  log(`\n  ✓ /api/search special cases passed (${SPECIAL_CASE_EMPLOYER}, ${SPECIAL_CASE_JOB})`);

  log(`\n  === /api/search job_match=contains (KLA + mechanical keyword) ===`);
  const klaMechanicalBody = await assertSearch(
    `Employer exact + job keyword: ${KLA_CORPORATION_EXAMPLE_EMPLOYER} / ${KLA_CORPORATION_EXAMPLE_JOB_KEYWORD}`,
    {
      employer: KLA_CORPORATION_EXAMPLE_EMPLOYER,
      job: KLA_CORPORATION_EXAMPLE_JOB_KEYWORD,
      job_match: "contains",
      pageSize: 5,
    },
    "employer_job_contains",
    (row, p) => {
      if (row.employer_name.toLowerCase() !== p.employer.toLowerCase()) {
        throw new Error(`employer mismatch: got ${JSON.stringify(row.employer_name)}`);
      }
      const needle = p.job.toLowerCase();
      if (!(row.job_title || "").toLowerCase().includes(needle)) {
        throw new Error(`job_title missing substring ${JSON.stringify(needle)}: got ${JSON.stringify(row.job_title)}`);
      }
    },
  );
  if (klaMechanicalBody.total === 0) {
    throw new Error(
      `KLA + mechanical (job_match=contains) returned no rows — expected ≥1 (load data with KLA mechanical titles or check spelling).`,
    );
  }

  log(`\n  ✓ /api/search job_match=contains example passed (${KLA_CORPORATION_EXAMPLE_EMPLOYER} / ${KLA_CORPORATION_EXAMPLE_JOB_KEYWORD})`);

  console.log("\nAll smoke checks passed.");
}

main().catch((e) => {
  console.error("\nSmoke test failed:", e.message || e);
  process.exit(1);
});
