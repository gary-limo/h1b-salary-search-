#!/bin/bash
# Replace remote D1 app tables from the current local D1 database.
# Source of truth: local D1 SQLite file under .wrangler/state
#
# What this script does:
#   1. Export INSERT statements for h1b_wages from local D1
#   2. Export INSERT statements for employer_seo from local D1
#   3. Drop remote h1b_wages and employer_seo (destructive)
#   4. Recreate h1b_wages schema and load data
#   5. Recreate composite indexes
#   6. Create employer_seo schema and load data
#
# Usage:
#   ./scripts/load_d1_replace.sh
#   ./scripts/load_d1_replace.sh --yes

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DB_NAME="h1b-wages"
EXPORT_SQL="$PROJECT_DIR/h1b_wages_export.sql"
EXPORT_SEO_SQL="$PROJECT_DIR/employer_seo_export.sql"

cd "$PROJECT_DIR"

shopt -s nullglob
db_files=( "$PROJECT_DIR"/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/*.sqlite )
shopt -u nullglob

if [[ ${#db_files[@]} -eq 0 ]]; then
  echo "No local D1 SQLite file found under .wrangler/state."
  echo "Run your local seed first, then retry."
  exit 1
fi

if [[ ${#db_files[@]} -gt 1 ]]; then
  echo "Multiple local D1 SQLite files found:"
  printf '  %s\n' "${db_files[@]}"
  echo "Keep only the current one, then retry."
  exit 1
fi

LOCAL_D1_DB="${db_files[0]}"

if [[ "${1:-}" != "--yes" ]]; then
  echo "WARNING: This will REPLACE the remote D1 app tables in '$DB_NAME'."
  echo "Remote tables to be dropped and rebuilt:"
  echo "  - h1b_wages"
  echo "  - employer_seo"
  echo ""
  read -r -p "Type REPLACE to continue: " confirm
  if [[ "$confirm" != "REPLACE" ]]; then
    echo "Cancelled."
    exit 1
  fi
fi

echo "Step 1/8: Exporting h1b_wages INSERT statements from local D1..."
sqlite3 "$LOCAL_D1_DB" ".dump h1b_wages" | awk '/^INSERT INTO h1b_wages/' > "$EXPORT_SQL"

if [[ ! -s "$EXPORT_SQL" ]]; then
  echo "Export failed: $EXPORT_SQL is empty."
  exit 1
fi

echo ""
echo "Step 2/8: Exporting employer_seo INSERT statements from local D1..."
if ! sqlite3 "$LOCAL_D1_DB" \
  "SELECT name FROM sqlite_master WHERE type='table' AND name='employer_seo';" | grep -q employer_seo; then
  echo "Local database has no employer_seo table. Run scripts/create_db.py (step 4 builds it), then retry."
  exit 1
fi

sqlite3 "$LOCAL_D1_DB" ".dump employer_seo" | awk '/^INSERT INTO employer_seo/' > "$EXPORT_SEO_SQL"

if [[ ! -s "$EXPORT_SEO_SQL" ]]; then
  echo "Export failed: $EXPORT_SEO_SQL is empty."
  echo "Rebuild local D1 with scripts/create_db.py so employer_seo is populated."
  exit 1
fi

echo ""
echo "Step 3/8: Dropping remote app tables..."
npx wrangler d1 execute "$DB_NAME" --remote --command "
DROP TABLE IF EXISTS h1b_wages;
DROP TABLE IF EXISTS employer_seo;
"

echo ""
echo "Step 4/8: Recreating h1b_wages schema..."
npx wrangler d1 execute "$DB_NAME" --remote --file=./migrations/0001_create_h1b_wages.sql

echo ""
echo "Step 5/8: Loading h1b_wages data (this may take several minutes)..."
npx wrangler d1 execute "$DB_NAME" --remote --file=./h1b_wages_export.sql

echo ""
echo "Step 6/8: Recreating composite indexes..."
npx wrangler d1 execute "$DB_NAME" --remote --file=./migrations/0001d_composite_indexes.sql

echo ""
echo "Step 7/8: Creating employer_seo schema and loading rows..."
npx wrangler d1 execute "$DB_NAME" --remote --file=./migrations/0002_employer_seo.sql
npx wrangler d1 execute "$DB_NAME" --remote --file=./employer_seo_export.sql

echo ""
echo "Step 8/8: Verifying counts..."
npx wrangler d1 execute "$DB_NAME" --remote --command "
SELECT 'h1b_wages' as tbl, COUNT(*) as cnt FROM h1b_wages
UNION ALL
SELECT 'employer_seo', COUNT(*) FROM employer_seo
UNION ALL
SELECT 'distinct_employers_h1b', COUNT(DISTINCT employer_name) FROM h1b_wages;
"

echo ""
echo "Done. Remote D1 database '$DB_NAME' was replaced from local D1."
