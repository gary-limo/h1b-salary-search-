#!/bin/bash
# Export SQLite database to .sql for Cloudflare D1 import.
# Usage: ./scripts/export_for_d1.sh
#
# Load to D1 (schema first, then data):
#   1. wrangler d1 execute <db-name> --remote --file=./migrations/0001_create_h1b_wages.sql
#   2. wrangler d1 execute <db-name> --remote --file=./h1b_wages_export.sql

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DB="$PROJECT_DIR/h1b_wages.db"
OUT="$PROJECT_DIR/h1b_wages_export.sql"

if [[ ! -f "$DB" ]]; then
  echo "Error: $DB not found. Run scripts/create_db.py first."
  exit 1
fi

echo "Exporting INSERT statements to $OUT..."
# Export only INSERT statements (schema is in migrations/)
sqlite3 "$DB" .dump | grep -E "^INSERT INTO h1b_wages" > "$OUT"

echo "Done. File size: $(du -h "$OUT" | cut -f1)"
echo ""
echo "To load into Cloudflare D1:"
echo "  1. wrangler d1 execute <db-name> --remote --file=./migrations/0001_create_h1b_wages.sql"
echo "  2. wrangler d1 execute <db-name> --remote --file=./h1b_wages_export.sql"
