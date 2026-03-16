#!/bin/bash
# Seed local D1 database for development.
# Run once before first `npm run dev` (or when you need a fresh local DB).
#
# Local D1 persists in .wrangler/state/ — no Cloudflare API needed.
#
# Usage: ./scripts/seed_local_d1.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DB_NAME="h1b-wages"

cd "$PROJECT_DIR"

if [[ ! -f "h1b_wages_export.sql" ]]; then
  echo "h1b_wages_export.sql not found."
  echo "Run: python3 scripts/create_db.py && ./scripts/export_for_d1.sh"
  exit 1
fi

echo "Seeding local D1 (this may take a few minutes)..."
echo ""

echo "Step 1/6: Creating h1b_wages schema..."
npx wrangler d1 execute "$DB_NAME" --local --file=./migrations/0001_create_h1b_wages.sql

echo ""
echo "Step 2/6: Loading h1b_wages data..."
npx wrangler d1 execute "$DB_NAME" --local --file=./h1b_wages_export.sql

echo ""
echo "Step 3/6: Running data quality checks..."
npx wrangler d1 execute "$DB_NAME" --local --file=./migrations/0001c_data_quality.sql
npx wrangler d1 execute "$DB_NAME" --local --file=./migrations/0001c1_job_title_cleanup.sql
npx wrangler d1 execute "$DB_NAME" --local --file=./migrations/0001c2_data_quality_part2.sql

echo ""
echo "Step 4/5: Creating h1b_salary_compare table..."
npx wrangler d1 execute "$DB_NAME" --local --file=./migrations/0002_create_salary_compare.sql

echo ""
echo "Step 5/5: Verifying..."
npx wrangler d1 execute "$DB_NAME" --local --command "SELECT 'h1b_wages' as tbl, COUNT(*) as cnt FROM h1b_wages UNION ALL SELECT 'h1b_salary_compare', COUNT(*) FROM h1b_salary_compare UNION ALL SELECT 'h1b_salary_summary', COUNT(*) FROM h1b_salary_summary;"

echo ""
echo "Done. Local D1 is ready. Run: npm run dev"
