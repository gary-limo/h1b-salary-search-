const TURNSTILE_SITE_KEY = document.querySelector('meta[name="turnstile-site-key"]')?.content?.trim() || '';
let turnstileReady = !TURNSTILE_SITE_KEY;

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

function onTurnstileCallback(token) {
  submitTurnstileSession(token)
    .then(async (res) => {
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || `Verification failed (${res.status})`);
      }
      turnstileReady = true;
      window.dispatchEvent(new CustomEvent("turnstile-ready"));
    })
    .catch((e) => {
      console.error(e);
      const errBox = document.getElementById("errorBox");
      const errMsg = document.getElementById("errorMsg");
      if (errMsg) errMsg.textContent = "Security check failed. Please refresh the page.";
      if (errBox) errBox.style.display = "flex";
    });
}

window.onTurnstileCallback = onTurnstileCallback;

if (TURNSTILE_SITE_KEY) {
  const tw = document.getElementById("turnstile-widget");
  if (tw) {
    tw.setAttribute("class", "cf-turnstile turnstile-host");
    tw.setAttribute("data-sitekey", TURNSTILE_SITE_KEY);
    tw.setAttribute("data-callback", "onTurnstileCallback");
    tw.setAttribute("data-theme", "light");
    tw.setAttribute("data-size", "flexible");
  }
  const s = document.createElement("script");
  s.src = "https://challenges.cloudflare.com/turnstile/v0/api.js";
  s.async = true;
  s.defer = true;
  document.head.appendChild(s);
}

const usd = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
function fmtUSD(v) { return v == null ? null : usd.format(v); }
function toTitleCase(s) { if (!s) return s; return s.replace(/\b\w/g, c => c.toUpperCase()); }

function set(id, val) {
  const el = document.getElementById(id);
  if (!el) return;
  if (val == null || val === "") {
    el.textContent = "—";
    el.classList.add("empty");
  } else {
    const displayVal = (typeof val === "string") ? val.toUpperCase() : val;
    el.textContent = displayVal;
    el.classList.remove("empty");
  }
}

function addrLine(a1, a2) {
  return [a1, a2].filter(Boolean).join(", ") || null;
}

function cityStateZip(city, state, zip) {
  return [city, state, zip].filter(Boolean).join(", ") || null;
}

const fetchDedupe = new Map();
function dedupeFetch(url, init) {
  const method = (init && init.method) || "GET";
  const key = `${method}:${url}`;
  if (fetchDedupe.has(key)) return fetchDedupe.get(key);
  const p = fetch(url, init).finally(() => {
    fetchDedupe.delete(key);
  });
  fetchDedupe.set(key, p);
  return p;
}

async function loadRecord(id) {
  const res = await dedupeFetch(`/api/record?id=${encodeURIComponent(id)}`, {
    credentials: "include",
    headers: { Accept: "application/json" },
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    if (res.status === 403 && err.code === "turnstile_required") {
      turnstileReady = false;
      throw new Error("Session expired. Please refresh the page.");
    }
    throw new Error(err.error || `Error ${res.status}`);
  }
  return (await res.json()).result;
}

const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

async function init() {
  const params = new URLSearchParams(location.search);
  const id = (params.get("id") || "").trim();

  if (!id || !UUID_REGEX.test(id)) {
    document.getElementById("pageTitle").textContent = "Record not found";
    document.getElementById("errorMsg").textContent = "No valid record ID provided.";
    document.getElementById("errorBox").style.display = "flex";
    return;
  }

  if (TURNSTILE_SITE_KEY && !turnstileReady) {
    document.getElementById("pageTitle").textContent = "Verifying…";
    try {
      await new Promise((resolve, reject) => {
        const t = setTimeout(() => reject(new Error("Security check timed out. Please refresh.")), 120000);
        window.addEventListener("turnstile-ready", () => {
          clearTimeout(t);
          resolve();
        }, { once: true });
      });
    } catch (e) {
      document.getElementById("pageTitle").textContent = "Verification required";
      document.getElementById("errorMsg").textContent = e.message || "Please refresh the page.";
      document.getElementById("errorBox").style.display = "flex";
      return;
    }
  }

  try {
    const r = await loadRecord(id);

    const empCaps = (r.employer_name && String(r.employer_name).toUpperCase()) || "H1B Record";
    const jobCaps = (r.job_title && String(r.job_title).toUpperCase()) || "";
    document.title = `${empCaps} – ${jobCaps} | H1B Salary Search`;
    document.getElementById("pageTitle").textContent = empCaps;
    document.getElementById("pageSub").textContent = jobCaps;

    // Salary hero
    set("dWageFrom",  fmtUSD(r.wage_rate_of_pay_from));
    set("dWageTo",    fmtUSD(r.wage_rate_of_pay_to));
    set("dPrevWage",  fmtUSD(r.prevailing_wage));
    set("dWageLevel", r.pw_wage_level ? `Level ${r.pw_wage_level}` : null);

    ["dWageFrom","dWageTo","dPrevWage"].forEach(id => {
      const el = document.getElementById(id);
      if (el && !el.classList.contains("empty")) el.classList.add("money");
    });

    // Job
    set("dJobTitle",  r.job_title);
    set("dCaseNum",   r.case_number);
    set("dSocCode",   r.soc_code);
    set("dSocTitle",  r.soc_title);
    set("dBeginDate", r.begin_date);
    set("dEndDate",   r.end_date);

    // Employer
    set("dEmpName",      r.employer_name);
    set("dEmpCountry",   r.employer_country);
    set("dEmpAddr",      addrLine(r.employer_address1, r.employer_address2));
    set("dEmpCityState", cityStateZip(r.employer_city, r.employer_state, r.employer_postal_code));

    // Worksite
    set("dWorkAddr",  addrLine(r.worksite_address1, r.worksite_address2));
    set("dWorkCity",  [r.worksite_city, r.worksite_county].filter(Boolean).join(" / ") || null);
    set("dWorkState", r.worksite_state);
    set("dWorkZip",   r.worksite_postal_code);

    document.getElementById("recordContent").style.display = "block";

  } catch (err) {
    document.getElementById("pageTitle").textContent = "Error loading record";
    document.getElementById("errorMsg").textContent = err.message;
    document.getElementById("errorBox").style.display = "flex";
  }
}

init();
