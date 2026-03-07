#!/bin/bash
# Load H1B wages data into Cloudflare D1.
# Prerequisites: wrangler login (or CLOUDFLARE_API_TOKEN set)
#
# Usage:
#   1. Run: npx wrangler d1 create h1b-wages
#   2. Add the database_id to wrangler.jsonc (see instructions in output)
#   3. Run: ./scripts/load_d1.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DB_NAME="h1b-wages"

cd "$PROJECT_DIR"

# Ensure export file exists
if [[ ! -f "h1b_wages_export.sql" ]]; then
  echo "h1b_wages_export.sql not found. Running: python3 scripts/create_db.py && ./scripts/export_for_d1.sh"
  python3 scripts/create_db.py
  ./scripts/export_for_d1.sh
  echo ""
fi

echo "Step 1/3: Creating schema..."
npx wrangler d1 execute "$DB_NAME" --remote --file=./migrations/0001_create_h1b_wages.sql

echo ""
echo "Step 2/3: Loading data (this may take several minutes)..."
npx wrangler d1 execute "$DB_NAME" --remote --file=./h1b_wages_export.sql

echo ""
echo "Step 3/3: Verifying..."
npx wrangler d1 execute "$DB_NAME" --remote --command "SELECT COUNT(*) as total FROM h1b_wages;"

echo ""
echo "Done! D1 database '$DB_NAME' is ready."
