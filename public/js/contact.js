(function () {
  const siteKey = document.querySelector('meta[name="turnstile-site-key"]')?.content?.trim() || "";
  const form = document.getElementById("contactForm");
  const statusEl = document.getElementById("contactStatus");
  const submitBtn = document.getElementById("contactSubmit");
  const turnstileHost = document.getElementById("contactTurnstile");
  const postUrl = (form?.dataset?.postUrl || "/api/x/st").trim() || "/api/x/st";

  let widgetId = null;

  function setStatus(msg, kind) {
    statusEl.textContent = msg || "";
    statusEl.className = "contact-status" + (kind ? " " + kind : "");
  }

  function resetTurnstile() {
    if (window.turnstile && widgetId != null) {
      try {
        window.turnstile.remove(widgetId);
      } catch (_) {}
      widgetId = null;
    }
    if (turnstileHost) turnstileHost.innerHTML = "";
  }

  function mountTurnstile() {
    if (!siteKey || !window.turnstile || !turnstileHost) return;
    resetTurnstile();
    widgetId = window.turnstile.render(turnstileHost, {
      sitekey: siteKey,
    });
  }

  if (siteKey) {
    const s = document.createElement("script");
    s.src = "https://challenges.cloudflare.com/turnstile/v0/api.js";
    s.async = true;
    s.defer = true;
    s.onload = () => mountTurnstile();
    document.head.appendChild(s);
  }

  form?.addEventListener("submit", async (e) => {
    e.preventDefault();
    setStatus("");

    const message = document.getElementById("contactMessage")?.value?.trim() || "";
    const honeypot = document.getElementById("contactHp")?.value?.trim() || "";

    if (!message) {
      setStatus("Please enter a message.", "err");
      return;
    }

    let turnstileToken = "";
    if (siteKey && window.turnstile && widgetId != null) {
      turnstileToken = window.turnstile.getResponse(widgetId) || "";
      if (!turnstileToken) {
        setStatus("Please complete the verification challenge.", "err");
        return;
      }
    }

    submitBtn.disabled = true;
    setStatus("Sending…");

    try {
      const res = await fetch(postUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          message,
          turnstileToken,
          _company_website: honeypot,
        }),
      });
      const data = await res.json().catch(() => ({}));

      if (res.ok && data.ok) {
        setStatus("Thanks — received.", "ok");
        form.reset();
        resetTurnstile();
        if (siteKey && window.turnstile) mountTurnstile();
      } else {
        setStatus(data.error || "Something went wrong. Please try again.", "err");
        if (window.turnstile && widgetId != null) {
          try {
            window.turnstile.reset(widgetId);
          } catch (_) {}
        }
      }
    } catch {
      setStatus("Network error. Please try again.", "err");
    } finally {
      submitBtn.disabled = false;
    }
  });
})();
