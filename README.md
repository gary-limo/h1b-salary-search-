# H1B Salary Search

For a full architectural breakdown (caching layers, ETL, data pipeline, and infrastructure choices), see: [I built an open-source H-1B salary search website using AI: here’s how](https://www.linkedin.com/pulse/i-built-open-source-h-1b-salary-search-website-using-ai-gaurav-muley-aivae).

Free, open-source search engine for H-1B visa salary data from the U.S. Department of Labor LCA disclosure records (FY2025). 570,000+ records.

**Live:** [h1b-salaries.com](https://h1b-salaries.com)

## API contract

The public JSON API (`/api/search`, `/api/record`, `/api/suggest`) is documented in **[`openapi/openapi.yaml`](openapi/openapi.yaml)** (OpenAPI 3). Lint locally with `npm run lint:openapi` (same check runs in GitHub Actions).

Behavior is implemented in [`src/index.js`](src/index.js). Smoke tests in [`tests/run-smoke.mjs`](tests/run-smoke.mjs) exercise `/api/search` and `/api/suggest` against a running dev server.

## Stack

- **Frontend:** Plain HTML/CSS/JS (no framework, no build step)
- **Backend:** Cloudflare Worker
- **Database:** Cloudflare D1 (SQLite)
- **Hosting:** Cloudflare (Worker + static assets)

## Local Development

```bash
# Install dependencies
npm install

# Run locally (requires Cloudflare account with D1 database)
npm run dev

# Smoke tests (same terminal or another; hits localhost only by default)
npm run test
# Quieter: SMOKE_QUIET=1 npm run test
# More sample rows per case: SMOKE_SAMPLE_ROWS=5 npm run test
```

## Python Scripts 

Python scripts (e.g. `scripts/data_parsing.py`, `scripts/create_db.py`, `scripts/build_suggestions_index.py`, ML training) run inside a virtual environment:

```bash
# Create venv (one-time)
python3 -m venv .venv

# Activate venv
source .venv/bin/activate   # macOS/Linux
# .venv\Scripts\activate    # Windows

# Install dependencies
pip install -r requirements.txt

# Run scripts (always with venv active)
python scripts/data_parsing.py   # Excel → parsed_output.csv (run from repo root)
python scripts/create_db.py
```

Full ETL (Excel → D1 export → suggestions JSON → local R2 upload) is documented in [`.github/pipeline_steps.txt`](.github/pipeline_steps.txt). Run `./scripts/run_pipeline.sh`; add `--prod` to also replace production D1 and upload the index to production R2.

## Deploy

```bash
npm run deploy
```

### Turnstile (optional)

To reduce automated abuse, enable [Cloudflare Turnstile](https://developers.cloudflare.com/turnstile/): create a widget in the dashboard, set **`TURNSTILE_SITE_KEY`** in `wrangler.jsonc` under `vars` (public site key), then store the **secret key** in the Worker:

```bash
npx wrangler secret put TURNSTILE_SECRET_KEY
```

If either value is missing, Turnstile stays **off** and behavior matches a normal deploy. With both set, browsers must complete the widget once; the Worker sets an **HttpOnly** session cookie (about **2 hours**) so legitimate users are not challenged on every search. **Localhost** and **`SKIP_TURNSTILE=true`** (e.g. in `.dev.vars`) skip the check for development and smoke tests.

### Site note form (`/reach-out`)

Messages are stored in **R2** as `inbox/{uuid}.json` (binding **`INBOX`**, bucket name **`inbox`**). Create the bucket once:

```bash
npx wrangler r2 bucket create inbox
```

The browser POSTs to **`/api/x/st`** (not advertised as a “contact” URL in the Worker). The Worker enforces **Turnstile** on the POST body (skipped on **localhost** for the API, same as search), a **honeypot** field, the shared **API rate limiter** (burst control), and **KV** caps: **100 submissions per UTC day** (global) and **2 per IP per UTC day**. Set **`TURNSTILE_SHOW_ON_LOCALHOST=true`** and **`TURNSTILE_SECRET_KEY`** in `.dev.vars` if you want the widget while testing on `http://127.0.0.1`. For tunnel hostnames, optional **`SKIP_CONTACT_RATE_LIMIT=true`** avoids KV caps during testing.

## Data Source

U.S. Department of Labor — H-1B Labor Condition Application (LCA) disclosure data. Wages shown do not include benefits or bonuses.

## License

[MIT](LICENSE)
