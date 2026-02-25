# H1B Salary Search

Search H1B visa salary data from the U.S. Department of Labor. Free, fast, open source.

**Tech:** Plain HTML/JS, DuckDB-WASM, Parquet. No backend, no API, no build step.

## Deploy (pick one)

### GitHub Pages (free, ~1 min)

1. Push this repo to GitHub
2. **Settings → Pages → Source:** Deploy from branch `main`, folder `/ (root)`
3. Site live at `https://<username>.github.io/<repo>/`

### Vercel (free)

```bash
npx vercel
```

### Netlify (free)

Drag the folder into [netlify.com/drop](https://app.netlify.com/drop)

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
