#!/bin/bash
# Push employer_seo from local Wrangler D1 SQLite to remote D1 only.
# Does NOT modify h1b_wages or any other remote tables.
#
# Preconditions:
#   - Local .wrangler D1 has employer_seo populated (migration 0002 + build_employer_seo_table.py).
#   - Remote should already have h1b_wages in sync with the employer names you expect; slugs/counts
#     come from your local table only.
#
# Usage:
#   ./scripts/load_employer_seo_remote_only.sh
#   ./scripts/load_employer_seo_remote_only.sh --yes

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DB_NAME="h1b-wages"
EXPORT_SEO_SQL="$PROJECT_DIR/employer_seo_export.sql"

cd "$PROJECT_DIR"

shopt -s nullglob
db_files=( "$PROJECT_DIR"/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/*.sqlite )
shopt -u nullglob

if [[ ${#db_files[@]} -eq 0 ]]; then
  echo "No local D1 SQLite file found under .wrangler/state."
  echo "Run npm run dev once, then apply 0002 + scripts/build_employer_seo_table.py."
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
  echo "This will REPLACE remote table employer_seo only (in '$DB_NAME')."
  echo "Remote h1b_wages and other tables are not modified."
  echo ""
  read -r -p "Type OK to continue: " confirm
  if [[ "$confirm" != "OK" ]]; then
    echo "Cancelled."
    exit 1
  fi
fi

if ! sqlite3 "$LOCAL_D1_DB" \
  "SELECT name FROM sqlite_master WHERE type='table' AND name='employer_seo';" | grep -q employer_seo; then
  echo "Local database has no employer_seo table."
  echo "Run: npx wrangler d1 execute $DB_NAME --local --file=./migrations/0002_employer_seo.sql"
  echo "Then: python3 scripts/build_employer_seo_table.py"
  exit 1
fi

echo "Exporting employer_seo INSERTs from local D1..."
sqlite3 "$LOCAL_D1_DB" ".dump employer_seo" | awk '/^INSERT INTO employer_seo/' > "$EXPORT_SEO_SQL"

if [[ ! -s "$EXPORT_SEO_SQL" ]]; then
  echo "Export failed: $EXPORT_SEO_SQL is empty."
  exit 1
fi

lines=$(wc -l < "$EXPORT_SEO_SQL" | tr -d ' ')
echo "Exported $lines INSERT row(s) -> $(basename "$EXPORT_SEO_SQL")"

echo ""
echo "Ensuring remote employer_seo schema exists..."
npx wrangler d1 execute "$DB_NAME" --remote --file=./migrations/0002_employer_seo.sql

echo ""
echo "Clearing remote employer_seo rows..."
npx wrangler d1 execute "$DB_NAME" --remote --command "DELETE FROM employer_seo;"

echo ""
echo "Loading remote employer_seo (may take a minute)..."
npx wrangler d1 execute "$DB_NAME" --remote --file=./employer_seo_export.sql

echo ""
echo "Verifying remote counts..."
npx wrangler d1 execute "$DB_NAME" --remote --command "
SELECT 'employer_seo' AS tbl, COUNT(*) AS cnt FROM employer_seo
UNION ALL
SELECT 'distinct_employers_h1b', COUNT(DISTINCT employer_name) FROM h1b_wages;
"

echo ""
echo "Done. Remote employer_seo was replaced from local D1 only."
