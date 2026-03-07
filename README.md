# H1B Salary Search

Free, open-source search engine for H-1B visa salary data from the U.S. Department of Labor LCA disclosure records (FY2025). 570,000+ records.

**Live:** [h1b-salaries.com](https://h1b-salaries.com)

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
```

## Deploy

```bash
npm run deploy
```

## Data Source

U.S. Department of Labor — H-1B Labor Condition Application (LCA) disclosure data. Wages shown do not include benefits or bonuses.

## License

[MIT](LICENSE)
