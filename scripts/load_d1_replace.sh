#!/bin/bash
# Replace remote D1 app tables from the current local D1 database.
# Source of truth: local D1 SQLite file under .wrangler/state
#
# What this script does:
#   1. Export only INSERT statements for h1b_wages from local D1
#   2. Drop remote app tables (destructive)
#   3. Recreate h1b_wages schema
#   4. Load h1b_wages data
#   5. Recreate composite indexes
#   6. Rebuild salary compare and summary tables
#
# Usage:
#   ./scripts/load_d1_replace.sh
#   ./scripts/load_d1_replace.sh --yes

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DB_NAME="h1b-wages"
EXPORT_SQL="$PROJECT_DIR/h1b_wages_export.sql"

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
  echo "  - h1b_salary_compare"
  echo "  - h1b_salary_summary"
  echo ""
  read -r -p "Type REPLACE to continue: " confirm
  if [[ "$confirm" != "REPLACE" ]]; then
    echo "Cancelled."
    exit 1
  fi
fi

echo "Step 1/6: Exporting h1b_wages INSERT statements from local D1..."
sqlite3 "$LOCAL_D1_DB" ".dump h1b_wages" | awk '/^INSERT INTO h1b_wages/' > "$EXPORT_SQL"

if [[ ! -s "$EXPORT_SQL" ]]; then
  echo "Export failed: $EXPORT_SQL is empty."
  exit 1
fi

echo ""
echo "Step 2/6: Dropping remote app tables..."
npx wrangler d1 execute "$DB_NAME" --remote --command "
DROP TABLE IF EXISTS h1b_salary_summary;
DROP TABLE IF EXISTS h1b_salary_compare;
DROP TABLE IF EXISTS h1b_wages;
"

echo ""
echo "Step 3/6: Recreating h1b_wages schema..."
npx wrangler d1 execute "$DB_NAME" --remote --file=./migrations/0001_create_h1b_wages.sql

echo ""
echo "Step 4/6: Loading h1b_wages data (this may take several minutes)..."
npx wrangler d1 execute "$DB_NAME" --remote --file=./h1b_wages_export.sql

echo ""
echo "Step 5/6: Recreating composite indexes..."
npx wrangler d1 execute "$DB_NAME" --remote --file=./migrations/0001d_composite_indexes.sql


echo ""
echo "Verifying counts..."
npx wrangler d1 execute "$DB_NAME" --remote --command "
SELECT 'h1b_wages' as tbl, COUNT(*) as cnt FROM h1b_wages
UNION ALL
SELECT 'h1b_salary_compare', COUNT(*) FROM h1b_salary_compare
UNION ALL
SELECT 'h1b_salary_summary', COUNT(*) FROM h1b_salary_summary;
"

echo ""
echo "Done. Remote D1 database '$DB_NAME' was replaced from local D1."
