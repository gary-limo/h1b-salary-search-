const TURNSTILE_SITE_KEY = document.querySelector('meta[name="turnstile-site-key"]')?.content?.trim() || '';
const DEFAULT_PAGE_SIZE = 25;
const PAGE_SIZE_OPTIONS = [25, 50, 75];

const POPULAR = [
  { label: "Google",            employer: "Google LLC",               job: "",                  loc: "" },
  { label: "Amazon",            employer: "Amazon com Services LLC",  job: "",                  loc: "" },
  { label: "Microsoft",         employer: "Microsoft Corporation",    job: "",                  loc: "" },
  { label: "Meta Platforms",    employer: "Meta Platforms Inc",       job: "",                  loc: "" },
  { label: "Apple",             employer: "Apple Inc",                job: "",                  loc: "" },
  { label: "Accenture",         employer: "Accenture LLP",            job: "",                  loc: "" },
  { label: "Data Scientist",    employer: "",                         job: "data scientist",    loc: "" },
  { label: "AI Engineer", employer: "",                         job: "ai engineer", loc: "" },
  { label: "San Francisco",     employer: "",                         job: "",                  loc: "san francisco" },
  { label: "Seattle",           employer: "",                         job: "",                  loc: "seattle" },
  { label: "New York",          employer: "",                         job: "",                  loc: "new york" },
];

const SORT_COLS = [
  { key: "employer_name",         label: "Employer"    },
  { key: "job_title",             label: "Job Title"   },
  { key: "wage_rate_of_pay_from", label: "Base Salary" },
  { key: "worksite_state",        label: "Location"    },
  { key: "begin_date",            label: "Start"       },
  { key: "end_date",              label: "End"         },
];

let currentPage     = 1;
let currentPageSize = DEFAULT_PAGE_SIZE;
let totalCount      = 0;
let hasSearched     = false;
let sortCol         = "wage_rate_of_pay_from";
let sortDir         = "DESC";

let pageResults    = [];
let filterEmp      = "";
let filterJob      = "";
let filterLoc      = "";
let openFilterCol  = "";
let filterDebounce;

const $ = (id) => document.getElementById(id);
const empInput = $("empInput"), jobInput = $("jobInput"), locInput = $("locInput");
const empFieldInner = $("empFieldInner"), jobFieldInner = $("jobFieldInner"), locFieldInner = $("locFieldInner");
const searchBtn = $("searchBtn"), searchBtnText = $("searchBtnText");

let turnstileReady = !TURNSTILE_SITE_KEY;
/** Set by explicit `turnstile.render` so we can `remove()` after success. */
let turnstileWidgetId = null;

function submitTurnstileSession(token) {
  return fetch("/api/turnstile/session", {
    method: "POST",
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify({ response: token }),
  });
}

function hideTurnstileWidget() {
  const tw = $("turnstile-widget");
  if (!tw) return;
  if (window.turnstile && turnstileWidgetId != null) {
    try {
      window.turnstile.remove(turnstileWidgetId);
    } catch (e) {
      /* ignore */
    }
    turnstileWidgetId = null;
  }
  tw.innerHTML = "";
  tw.classList.add("turnstile-hidden");
  tw.setAttribute("aria-hidden", "true");
}

function mountTurnstileWidget() {
  const tw = $("turnstile-widget");
  if (!tw || !TURNSTILE_SITE_KEY || !window.turnstile) return;
  if (turnstileWidgetId != null) {
    try {
      window.turnstile.remove(turnstileWidgetId);
    } catch (e) {
      /* ignore */
    }
    turnstileWidgetId = null;
  }
  tw.innerHTML = "";
  tw.classList.remove("turnstile-hidden");
  tw.setAttribute("aria-hidden", "false");
  turnstileWidgetId = window.turnstile.render(tw, {
    sitekey: TURNSTILE_SITE_KEY,
    callback: onTurnstileCallback,
    theme: "light",
    size: "flexible",
  });
}

function onTurnstileCallback(token) {
  submitTurnstileSession(token)
    .then(async (res) => {
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || `Verification failed (${res.status})`);
      }
      turnstileReady = true;
      if (searchBtn) searchBtn.disabled = false;
      hideTurnstileWidget();
      window.dispatchEvent(new CustomEvent("turnstile-ready"));
    })
    .catch((e) => {
      console.error(e);
      $("resultsWrap")?.classList.add("show");
      if ($("errorMsg")) $("errorMsg").textContent = "Security check failed. Please refresh the page.";
      $("errorBox")?.classList.remove("hidden");
    });
}

function waitForTurnstile() {
  if (!TURNSTILE_SITE_KEY || turnstileReady) return Promise.resolve();
  return new Promise((resolve) => {
    if (turnstileReady) return resolve();
    window.addEventListener("turnstile-ready", () => resolve(), { once: true });
  });
}

if (TURNSTILE_SITE_KEY) {
  if (searchBtn) searchBtn.disabled = true;
  const s = document.createElement("script");
  s.src = "https://challenges.cloudflare.com/turnstile/v0/api.js";
  s.async = true;
  s.defer = true;
  s.onload = () => mountTurnstileWidget();
  document.head.appendChild(s);
}

function applyResultFilters(records) {
  const emp  = filterEmp.trim().toLowerCase();
  const job  = filterJob.trim().toLowerCase();
  const loc  = filterLoc.trim().toLowerCase();
  if (!emp && !job && !loc) return records;
  return records.filter((r) => {
    if (emp && !(r.employer_name || "").toLowerCase().includes(emp)) return false;
    if (job && !(r.job_title || "").toLowerCase().includes(job)) return false;
    if (loc && !fmtLoc(r).toLowerCase().includes(loc)) return false;
    return true;
  });
}

async function fetchPage(emp, job, loc) {
  const params = new URLSearchParams({
    employer: emp.trim(), job: job.trim(), location: loc.trim(),
    page: currentPage, pageSize: currentPageSize, sort: sortCol, dir: sortDir,
  });
  const res = await fetch(`/api/search?${params}`, {
    credentials: "include",
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    if (res.status === 403 && err.code === "turnstile_required") {
      turnstileReady = false;
      if (TURNSTILE_SITE_KEY && searchBtn) searchBtn.disabled = true;
      if (window.turnstile) mountTurnstileWidget();
      throw new Error("Session expired. Complete the security check again.");
    }
    throw new Error(err.error || `Request failed (${res.status})`);
  }
  const data = await res.json();
  totalCount  = data.total ?? 0;
  pageResults = data.results || [];
}


const usd = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
function fmtUSD(v) { return v == null ? "\u2014" : usd.format(v); }
function fmtLoc(r) {
  const city = r.worksite_city ? toTitleCase(r.worksite_city) : null;
  const state = r.worksite_state ? String(r.worksite_state).toUpperCase() : null;
  const parts = [city, state].filter(Boolean);
  return parts.join(", ") || "\u2014";
}
function html(s) {
  if (s == null) return "\u2014";
  const d = document.createElement("div");
  d.textContent = String(s);
  return d.innerHTML;
}
function toTitleCase(s) {
  if (!s) return s;
  return s.replace(/\b\w/g, c => c.toUpperCase());
}

function renderEmptyState(isFilterEmpty) {
  if (isFilterEmpty) {
    return `<div class="empty-state"><div class="empty-state-msg">No matching records.</div><div class="empty-state-hint">Clear or change your filter above.</div><button type="button" class="empty-state-btn" data-action="clear-filters">Clear Filters</button></div>`;
  }
  return `<div class="empty-state"><div class="empty-state-msg">No records found.</div><div class="empty-state-hint">Try different keywords or filters.</div><button type="button" class="empty-state-btn" data-action="clear-search">Clear Search</button></div>`;
}

function renderTable(records, isFilterEmpty) {
  const recs = records || [];
  const emptyStateHtml = renderEmptyState(isFilterEmpty);
  const rows = recs.length
    ? recs.map((r, i) => `<tr class="clickable" data-id="${html(r.id)}" style="animation-delay:${i * 25}ms">
    <td class="emp"><a href="/record?id=${encodeURIComponent(r.id || "")}" class="emp-link" target="_blank" rel="noopener noreferrer" onclick="event.stopPropagation()">${html(toTitleCase(r.employer_name))}</a></td>
    <td class="job">${html(toTitleCase(r.job_title))}</td>
    <td class="salary">${fmtUSD(r.wage_rate_of_pay_from)}</td>
    <td class="loc">${html(fmtLoc(r))}</td>
    <td class="date">${html(r.begin_date)}</td>
    <td class="date">${html(r.end_date)}</td>
  </tr>`).join("")
    : `<tr><td colspan="6" class="no-filter-match"><div class="empty-state-wrap">${emptyStateHtml}</div></td></tr>`;

  const cards = recs.length ? recs.map((r, i) => `<div class="result-card clickable" data-id="${html(r.id)}" style="animation-delay:${i * 30}ms;cursor:pointer">
    <a href="/record?id=${encodeURIComponent(r.id || "")}" class="rc-emp emp-link" target="_blank" rel="noopener noreferrer" onclick="event.stopPropagation()">${html(toTitleCase(r.employer_name))}</a>
    <div class="rc-job">${html(toTitleCase(r.job_title))}</div>
    <div class="rc-grid">
      <div><div class="rc-label">Base Salary</div><div class="rc-val money">${fmtUSD(r.wage_rate_of_pay_from)}</div></div>
      <div><div class="rc-label">Location</div><div class="rc-val">${html(fmtLoc(r))}</div></div>
      <div><div class="rc-label">Start</div><div class="rc-val">${html(r.begin_date)}</div></div>
      <div><div class="rc-label">End</div><div class="rc-val">${html(r.end_date)}</div></div>
    </div>
  </div>`).join("") : emptyStateHtml;

  const funnelSvg = `<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3"/></svg>`;
  const FILTERABLE = ["employer_name", "job_title", "worksite_state"];
  const ths = SORT_COLS.map(c => {
    const active = sortCol === c.key;
    const arrow  = active ? (sortDir === "ASC" ? "\u25B2" : "\u25BC") : "\u25B2";
    if (!FILTERABLE.includes(c.key)) {
      return `<th scope="col" class="${active ? "sorted" : ""}" data-col="${c.key}">${c.label}<span class="sort-arrow">${arrow}</span></th>`;
    }
    const fval    = c.key === "employer_name" ? filterEmp : c.key === "job_title" ? filterJob : filterLoc;
    const fActive = fval.trim() !== "";
    const safeVal = fval.replace(/&/g, "&amp;").replace(/"/g, "&quot;");
    return `<th scope="col" class="${active ? "sorted" : ""} th-filterable" data-col="${c.key}">` +
      `<div class="th-inner"><span>${c.label}<span class="sort-arrow">${arrow}</span></span>` +
      `<button class="th-filter-btn${fActive ? " active" : ""}" data-fcol="${c.key}" aria-label="Filter ${c.label}" title="Filter ${c.label}">${funnelSvg}</button></div>` +
      `<div class="th-filter-pop" id="fpop-${c.key}">` +
      `<input type="text" placeholder="Type and hit Enter" value="${safeVal}" autocomplete="off" data-fcol="${c.key}">` +
      (fActive ? `<button class="fpop-clear" data-fcol="${c.key}">Clear filter</button>` : "") +
      `</div></th>`;
  }).join("");

  return `<div class="tbl-wrap"><div class="tbl-scroll"><table>
    <thead><tr>${ths}</tr></thead>
    <tbody>${rows}</tbody></table></div></div>
    <div class="results-cards">${cards}</div>`;
}

function renderSkeleton() {
  const colCount = SORT_COLS.length;
  const rows = Array.from({ length: 6 }, () =>
    `<tr>${Array(colCount).fill(0).map(() =>
      `<td><div class="skeleton" style="height:14px;width:${50 + Math.random() * 80}px"></div></td>`
    ).join("")}</tr>`
  ).join("");
  const ths = SORT_COLS.map(c => `<th>${c.label}</th>`).join("");
  return `<div class="tbl-wrap"><div class="tbl-scroll"><table>
    <thead><tr>${ths}</tr></thead>
    <tbody>${rows}</tbody></table></div></div>`;
}

function renderPager(displayCount) {
  const count = displayCount ?? totalCount;
  const showPager = count > 0;
  $("pagerContainer").classList.toggle("hidden", !showPager);
  if (!showPager) return;

  const tp = Math.ceil(count / currentPageSize);
  const s  = (currentPage - 1) * currentPageSize + 1;
  const e  = Math.min(currentPage * currentPageSize, count);

  const sizeOptions = PAGE_SIZE_OPTIONS.map(n =>
    `<option value="${n}" ${n === currentPageSize ? "selected" : ""}>${n}</option>`
  ).join("");

  $("pagerContainer").innerHTML = `
    <div class="pager-left">
      <span class="pager-info">${s}\u2013${e} of ${count.toLocaleString()}</span>
      <label class="pager-size-wrap">
        <span class="pager-size-label">per page</span>
        <select class="pager-size-select" id="pageSizeSelect" aria-label="Results per page">
          ${sizeOptions}
        </select>
      </label>
    </div>
    <div class="pager-btns">
      <button class="pager-btn" id="prevBtn" ${currentPage <= 1 ? "disabled" : ""} aria-label="Previous page">\u2190</button>
      <span class="pager-page">${currentPage} / ${tp.toLocaleString()}</span>
      <button class="pager-btn" id="nextBtn" ${currentPage >= tp ? "disabled" : ""} aria-label="Next page">\u2192</button>
    </div>`;

  $("prevBtn").onclick = () => { if (currentPage > 1) { currentPage--; runPage(); } };
  $("nextBtn").onclick = () => { if (currentPage < tp) { currentPage++; runPage(); } };
  $("pageSizeSelect").onchange = () => {
    currentPageSize = parseInt($("pageSizeSelect").value, 10);
    currentPage = 1;
    runPage();
  };
}

function bindFilterIcons() {
  const container = $("tableContainer");
  if (!container) return;

  container.querySelectorAll(".th-filter-btn").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const col = btn.dataset.fcol;
      const pop = document.getElementById("fpop-" + col);
      if (!pop) return;
      const wasOpen = pop.classList.contains("open");
      container.querySelectorAll(".th-filter-pop.open").forEach(p => p.classList.remove("open"));
      if (!wasOpen) {
        pop.classList.add("open");
        openFilterCol = col;
        pop.querySelector("input")?.focus();
      } else {
        openFilterCol = "";
      }
    });
  });

  container.querySelectorAll(".th-filter-pop input").forEach(input => {
    input.addEventListener("input", () => {
      const col = input.dataset.fcol;
      if (col === "employer_name") filterEmp = input.value;
      else if (col === "job_title") filterJob = input.value;
      else if (col === "worksite_state") filterLoc = input.value;
      openFilterCol = col;
      clearTimeout(filterDebounce);
      filterDebounce = setTimeout(() => renderCurrentResults(), 200);
    });
    input.addEventListener("click", (e) => e.stopPropagation());
    input.addEventListener("keydown", (e) => {
      if (e.key === "Escape" || e.key === "Enter") {
        container.querySelectorAll(".th-filter-pop.open").forEach(p => p.classList.remove("open"));
        openFilterCol = "";
      }
    });
  });

  container.querySelectorAll(".fpop-clear").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const col = btn.dataset.fcol;
      if (col === "employer_name") filterEmp = "";
      else if (col === "job_title") filterJob = "";
      else if (col === "worksite_state") filterLoc = "";
      openFilterCol = "";
      renderCurrentResults();
    });
  });

  if (openFilterCol) {
    const pop = document.getElementById("fpop-" + openFilterCol);
    if (pop) {
      pop.classList.add("open");
      const inp = pop.querySelector("input");
      if (inp) { inp.focus(); const l = inp.value.length; inp.setSelectionRange(l, l); }
    }
  }
}

function bindSortHeaders() {
  $("tableContainer").querySelectorAll("th[data-col]").forEach(th => {
    th.addEventListener("click", (e) => {
      if (e.target.closest(".th-filter-btn, .th-filter-pop")) return;
      const col = th.dataset.col;
      if (sortCol === col) {
        sortDir = sortDir === "ASC" ? "DESC" : "ASC";
      } else {
        sortCol = col;
        sortDir = col === "wage_rate_of_pay_from" ? "DESC" : "ASC";
      }
      currentPage = 1;
      runPage();
    });
  });
}

function bindRowClicks() {
  $("tableContainer").querySelectorAll("tr.clickable[data-id]").forEach(tr => {
    tr.setAttribute("tabindex", "0");
    tr.setAttribute("role", "link");
    const openRecord = () => window.open(`/record?id=${encodeURIComponent(tr.dataset.id || "")}`, "_blank");
    tr.addEventListener("click", openRecord);
    tr.addEventListener("keydown", e => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); openRecord(); } });
  });
  document.querySelectorAll(".result-card.clickable[data-id]").forEach(card => {
    card.setAttribute("tabindex", "0");
    card.setAttribute("role", "link");
    const openRecord = () => window.open(`/record?id=${encodeURIComponent(card.dataset.id || "")}`, "_blank");
    card.addEventListener("click", openRecord);
    card.addEventListener("keydown", e => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); openRecord(); } });
  });
}

function clearSearch() {
  empInput.value = ""; jobInput.value = ""; locInput.value = "";
  hasSearched = false; currentPage = 1; totalCount = 0;
  $("resultsWrap").classList.remove("show");
  updateClearBtn();
}

function clearColumnFilters() {
  filterEmp = ""; filterJob = ""; filterLoc = "";
  const pop = document.getElementById("fpop-" + openFilterCol);
  if (pop) pop.classList.remove("open");
  openFilterCol = "";
  renderCurrentResults();
}

function bindEmptyStateButtons() {
  const container = $("tableContainer");
  if (!container) return;
  container.querySelectorAll(".empty-state-btn[data-action='clear-search']").forEach(btn => {
    btn.addEventListener("click", clearSearch);
  });
  container.querySelectorAll(".empty-state-btn[data-action='clear-filters']").forEach(btn => {
    btn.addEventListener("click", clearColumnFilters);
  });
}

function renderCurrentResults() {
  const filtered = applyResultFilters(pageResults);
  const filteredCount = filtered.length;
  const hasColumnFilter = !!(filterEmp || filterJob || filterLoc);

  $("tableContainer").innerHTML = renderTable(filtered, filteredCount === 0 && hasColumnFilter);

  const tblScroll = $("tableContainer").querySelector(".tbl-scroll");
  if (tblScroll) {
    tblScroll.classList.add("settling");
    requestAnimationFrame(() => requestAnimationFrame(() => tblScroll.classList.remove("settling")));
  }

  $("tableContainer").classList.remove("hidden");

  if (hasColumnFilter) {
    $("resultsBadge").textContent = `${filteredCount} of ${pageResults.length} on page \u00b7 ${totalCount.toLocaleString()} total`;
  } else {
    $("resultsBadge").textContent = `${totalCount.toLocaleString()} record${totalCount !== 1 ? "s" : ""}`;
  }

  renderPager(totalCount);
  bindSortHeaders();
  bindFilterIcons();
  bindRowClicks();
  bindEmptyStateButtons();
}

async function runPage() {
  const emp = empInput.value.trim(), job = jobInput.value.trim(), loc = locInput.value.trim();

  $("tableContainer").classList.add("hidden");
  $("pagerContainer").classList.add("hidden");
  $("skeletonTable").innerHTML = renderSkeleton();

  try {
    await fetchPage(emp, job, loc);
    $("skeletonTable").innerHTML = "";
    renderCurrentResults();
  } catch (err) {
    $("skeletonTable").innerHTML = "";
    $("errorMsg").textContent = err.message || "Search failed.";
    $("errorBox").classList.remove("hidden");
    $("resultsBadge").textContent = "0 records";
  }
}

async function executeSearch() {
  hasSearched      = true;
  currentPage      = 1;
  currentPageSize  = DEFAULT_PAGE_SIZE;
  totalCount       = 0;
  sortCol          = "wage_rate_of_pay_from";
  sortDir          = "DESC";
  filterEmp        = "";
  filterJob        = "";
  filterLoc        = "";
  openFilterCol    = "";

  searchBtn.disabled      = true;
  searchBtnText.textContent = "Searching\u2026";

  $("resultsWrap").classList.add("show");
  $("errorBox").classList.add("hidden");
  $("tableContainer").classList.add("hidden");
  $("pagerContainer").classList.add("hidden");
  $("skeletonTable").innerHTML  = renderSkeleton();
  $("resultsBadge").textContent = "";

  try {
    await runPage();
  } finally {
    searchBtn.disabled = TURNSTILE_SITE_KEY ? !turnstileReady : false;
    searchBtnText.textContent = "Search Salaries";
    $("skeletonTable").innerHTML = "";
  }
}

$("searchForm").addEventListener("submit", async ev => {
  ev.preventDefault();
  if (!empInput.value.trim() && !jobInput.value.trim() && !locInput.value.trim()) {
    $("resultsWrap").classList.add("show");
    $("errorMsg").textContent = "Please select employer, role or city.";
    $("errorBox").classList.remove("hidden");
    $("resultsBadge").textContent = "0 records";
    return;
  }
  await executeSearch();
});

[{input: empInput, inner: empFieldInner}, {input: jobInput, inner: jobFieldInner}, {input: locInput, inner: locFieldInner}]
  .forEach(({input, inner}) => {
    input.addEventListener("focus", () => inner.classList.add("focused"));
    input.addEventListener("blur",  () => inner.classList.remove("focused"));
  });

document.addEventListener("click", () => {
  if (!openFilterCol) return;
  const pop = document.getElementById("fpop-" + openFilterCol);
  if (pop) pop.classList.remove("open");
  openFilterCol = "";
});

function updateClearBtn() {
  $("clearBtn").classList.toggle(
    "show",
    !!(empInput.value.trim() || jobInput.value.trim() || locInput.value.trim())
  );
}
[empInput, jobInput, locInput].forEach(el => el.addEventListener("input", updateClearBtn));
$("clearBtn").addEventListener("click", () => {
  empInput.value = ""; jobInput.value = ""; locInput.value = "";
  hasSearched = false; currentPage = 1; totalCount = 0;
  $("resultsWrap").classList.remove("show");
  updateClearBtn();
});

const popularContainer = document.querySelector(".popular");
POPULAR.forEach(item => {
  const btn = document.createElement("button");
  btn.className = "popular-btn";
  btn.type      = "button";
  btn.textContent = item.label;
  btn.addEventListener("click", () => {
    selEmp = item.employer;
    selJob = item.job;
    selLoc = item.loc || "";
    empInput.value = item.employer;
    jobInput.value = toTitleCase(item.job);
    locInput.value = toTitleCase(item.loc || "");
    updateClearBtn();
    executeSearch();
  });
  popularContainer.appendChild(btn);
});

let selEmp = "", selJob = "", selLoc = "";
const acTimers = {};
/** Bumps on each suggest invocation; drop responses that are not for the latest query. */
const suggestSeq = { employer: 0, job: 0, location: 0 };

let apiSuggestReady = false;
let suggestProbeInFlight = null;
let suggestProbeNextAllowedAt = 0;
const SUGGEST_PROBE_COOLDOWN_MS = 10000;

function apiSuggestHeaders() {
  return { Accept: "application/json" };
}

async function fetchApiSuggest(field, q, contextEmployer, contextJob) {
  const params = new URLSearchParams({ field, q: q || "" });
  if (contextEmployer) params.set("employer", contextEmployer);
  if (contextJob) params.set("job", contextJob);
  const res = await fetch(`/api/suggest?${params}`, { credentials: "include", headers: apiSuggestHeaders() });
  if (!res.ok) return null;
  const data = await res.json();
  return Array.isArray(data.results) ? data.results : null;
}

async function ensureApiSuggestReady(force = false) {
  if (apiSuggestReady) return true;
  const now = Date.now();
  if (!force && now < suggestProbeNextAllowedAt) return false;
  if (suggestProbeInFlight) return suggestProbeInFlight;

  suggestProbeInFlight = (async () => {
    try {
      const res = await fetch("/api/suggest?field=employer&q=", { credentials: "include", headers: apiSuggestHeaders() });
      if (res.ok) {
        apiSuggestReady = true;
        return true;
      }
    } catch (_) {}
    suggestProbeNextAllowedAt = Date.now() + SUGGEST_PROBE_COOLDOWN_MS;
    return false;
  })();

  const ok = await suggestProbeInFlight;
  suggestProbeInFlight = null;
  return ok;
}

async function showRelatedSuggestions(selectedField, selectedValue) {
  const otherCol = selectedField === "employer" ? "job" : "employer";
  const otherDD = $(selectedField === "employer" ? "jobDropdown" : "empDropdown");
  const otherInput = selectedField === "employer" ? jobInput : empInput;
  const otherFieldInner = $(selectedField === "employer" ? "jobFieldInner" : "empFieldInner");

  if (!apiSuggestReady) {
    await ensureApiSuggestReady();
  }
  if (apiSuggestReady) {
    const ctxEmp = selectedField === "employer" ? selectedValue : undefined;
    const ctxJob = selectedField === "job" ? selectedValue : undefined;
    const items = await fetchApiSuggest(otherCol, "", ctxEmp, ctxJob);
    if (items && items.length) {
      otherDD.innerHTML = "";
      items.forEach(v => {
        const el = document.createElement("div");
        el.className = "dropdown-item";
        el.setAttribute("role", "option");
        el.setAttribute("data-val", v);
        el.textContent = toTitleCase(v);
        el.addEventListener("mousedown", e => {
          e.preventDefault();
          otherInput.value = otherCol === "job" ? toTitleCase(v) : v;
          if (otherCol === "employer") selEmp = v; else selJob = v;
          updateClearBtn();
          otherDD.className = "dropdown"; otherDD.innerHTML = "";
          otherInput.setAttribute("aria-expanded", "false");
        });
        otherDD.appendChild(el);
      });
      otherDD.className = "dropdown open";
      otherFieldInner.classList.add("focused");
      otherInput.setAttribute("aria-expanded", "true");
    } else {
      otherDD.className = "dropdown"; otherDD.innerHTML = "";
    }
    return;
  }

  otherDD.className = "dropdown";
  otherDD.innerHTML = "";
}

function acEffectiveEmployer() {
  return (selEmp || empInput.value.trim()).trim();
}
function acEffectiveJob() {
  return (selJob || jobInput.value.trim()).trim();
}

function acGetContext(field) {
  const ctx = {};
  const effEmp = acEffectiveEmployer();
  const effJob = acEffectiveJob();
  if (field !== "employer" && effEmp) ctx.employer = effEmp;
  if (field !== "job" && effJob) ctx.job = effJob;
  if (field !== "location" && selLoc) ctx.location = selLoc;
  return ctx;
}

function acCloseAll() {
  ["empDropdown", "jobDropdown", "locDropdown"].forEach(id => {
    const dd = $(id);
    if (dd) { dd.className = "dropdown"; dd.innerHTML = ""; }
  });
}

function acSetup(input, dropdownId, field) {
  const dd = $(dropdownId);
  let highlighted = -1;
  function closeDD() {
    dd.className = "dropdown";
    dd.innerHTML = "";
    highlighted = -1;
    input.setAttribute("aria-expanded", "false");
  }

  function renderItems(items) {
    if (!items || items.length === 0) { closeDD(); return; }
    dd.innerHTML = items.map(v =>
      `<div class="dropdown-item" role="option" data-val="${v.replace(/&/g,"&amp;").replace(/"/g,"&quot;")}">${html(toTitleCase(v))}</div>`
    ).join("");
    dd.className = "dropdown open";
    input.setAttribute("aria-expanded", "true");
    highlighted = -1;
    dd.querySelectorAll(".dropdown-item").forEach(item => {
      item.addEventListener("mousedown", e => {
        e.preventDefault();
        const val = item.getAttribute("data-val");
        if (field === "employer") { selEmp = val; input.value = toTitleCase(val); }
        else if (field === "job") { selJob = val; input.value = toTitleCase(val); }
        else { selLoc = val; input.value = toTitleCase(val); }
        updateClearBtn();
        closeDD();
      });
    });
  }

  function highlight(idx) {
    const items = dd.querySelectorAll(".dropdown-item");
    if (!items.length) return;
    highlighted = ((idx % items.length) + items.length) % items.length;
    items.forEach((it, i) => it.classList.toggle("ac-highlighted", i === highlighted));
    items[highlighted]?.scrollIntoView({ block: "nearest" });
  }

  async function fetchSuggest(q) {
    suggestSeq[field]++;
    const seq = suggestSeq[field];
    const effEmp = acEffectiveEmployer();
    const effJob = acEffectiveJob();
    const hasCtx = (field === "employer" && effJob) || (field === "job" && effEmp);
    if (!hasCtx && q.length < 2) { closeDD(); return; }
    if (!apiSuggestReady) {
      const ready = await ensureApiSuggestReady();
      if (!ready) { closeDD(); return; }
    }
    if (seq !== suggestSeq[field]) return;
    dd.innerHTML = '<div class="dropdown-loading">Searching\u2026</div>';
    dd.className = "dropdown open";
    clearTimeout(acTimers[field]);
    const ctxEmp = field === "job" ? effEmp || undefined : selEmp || undefined;
    const ctxJob = field === "employer" ? effJob || undefined : selJob || undefined;
    acTimers[field] = setTimeout(async () => {
      if (seq !== suggestSeq[field]) return;
      try {
        const results = await fetchApiSuggest(field, q, ctxEmp, ctxJob);
        if (seq !== suggestSeq[field]) return;
        if (results !== null) renderItems(results);
        else closeDD();
      } catch { closeDD(); }
    }, 50);
  }

  input.addEventListener("input", () => {
    if (field === "employer") selEmp = "";
    else if (field === "job") selJob = "";
    else selLoc = "";
    clearTimeout(acTimers[field]);
    fetchSuggest(input.value.trim());
  });

  input.addEventListener("focus", () => {
    const ctx = acGetContext(field);
    if (Object.keys(ctx).length > 0 && !dd.classList.contains("open")) {
      fetchSuggest(input.value.trim());
    }
  });

  input.addEventListener("keydown", e => {
    const items = dd.querySelectorAll(".dropdown-item");
    if (e.key === "ArrowDown") { e.preventDefault(); highlight(highlighted + 1); }
    else if (e.key === "ArrowUp") { e.preventDefault(); highlight(highlighted - 1); }
    else if (e.key === "Enter" && highlighted >= 0 && items[highlighted]) {
      e.preventDefault();
      const val = items[highlighted].getAttribute("data-val");
      if (field === "employer") { selEmp = val; input.value = toTitleCase(val); }
      else if (field === "job") { selJob = val; input.value = toTitleCase(val); }
      else { selLoc = val; input.value = toTitleCase(val); }
      updateClearBtn();
      closeDD();
    } else if (e.key === "Escape") { closeDD(); }
  });

  input.addEventListener("blur", () => setTimeout(closeDD, 150));
}

acSetup(empInput, "empDropdown", "employer");
acSetup(jobInput, "jobDropdown", "job");

(async function initSuggestions() {
  await ensureApiSuggestReady(true);
})();

const origClearClick = $("clearBtn").onclick;
$("clearBtn").addEventListener("click", () => {
  selEmp = ""; selJob = ""; selLoc = "";
  acCloseAll();
});

document.addEventListener("click", e => {
  if (!e.target.closest(".field-wrap")) acCloseAll();
});
