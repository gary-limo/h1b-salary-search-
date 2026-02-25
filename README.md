# H1B Salary Search

Search H1B visa salaries. Free, fast, open source. Data source from US DoL.

Plain HTML/JS, DuckDB-WASM, Parquet. No backend, no API, no build step.


### Cloudflare Pages (free)

```bash
npx wrangler pages deploy . --project-name h1b-salary-search
```

## Local dev

```bash
python3 -m http.server 8080
# Open http://localhost:8080
```

## Data

- `h1b_data.parquet` — ~500k rows, ~8 MB (Employer, Job Title, Base Salary, Location, Start/End dates)
- Source: U.S. DOL H-1B LCA disclosures
