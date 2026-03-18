#!/usr/bin/env bash
# Upload public/suggestions_index.json to R2 for production.
# Run after build_suggestions_index.py (or full pipeline).
# First time: npx wrangler r2 bucket create h1b-suggestions-index

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
FILE="${PROJECT_DIR}/public/suggestions_index.json"
BUCKET="h1b-suggestions-index"
KEY="suggestions_index.json"

if [[ ! -f "$FILE" ]]; then
  echo "Missing $FILE. Run: python3 scripts/build_suggestions_index.py"
  exit 1
fi

echo "Uploading $FILE to R2 ${BUCKET}/${KEY}..."
npx wrangler r2 object put "${BUCKET}/${KEY}" --file="$FILE" --content-type="application/json"
echo "Done. Worker will load suggestions from R2 on next request."
