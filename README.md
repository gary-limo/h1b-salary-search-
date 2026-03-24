# H1B Salary Search

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

## Python Scripts (Data / ML)

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

## Data Source

U.S. Department of Labor — H-1B Labor Condition Application (LCA) disclosure data. Wages shown do not include benefits or bonuses.

## License

[MIT](LICENSE)
