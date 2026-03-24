#!/usr/bin/env bash
# Upload public/suggestions_index.json to Cloudflare R2.
# Run after build_suggestions_index.py. scripts/run_pipeline.sh runs this automatically:
#   --local after building the index; --remote when using ./scripts/run_pipeline.sh --prod
#
# Usage:
#   ./scripts/upload_suggestions_to_r2.sh --remote   # production R2 (default)
#   ./scripts/upload_suggestions_to_r2.sh --local    # local wrangler dev R2 (Miniflare)
#
# First time (prod): npx wrangler r2 bucket create h1b-suggestions-index

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
FILE="${PROJECT_DIR}/public/suggestions_index.json"
BUCKET="h1b-suggestions-index"
KEY="suggestions_index.json"

MODE="--remote"
if [[ -n "${1:-}" ]]; then
  MODE="$1"
fi
if [[ "$MODE" != "--local" && "$MODE" != "--remote" ]]; then
  echo "Usage: $0 [--local|--remote]"
  echo "  --remote  Upload to production R2 (default)"
  echo "  --local   Upload to local dev R2 (use with wrangler dev)"
  exit 1
fi

if [[ ! -f "$FILE" ]]; then
  echo "Missing $FILE. Run: python3 scripts/build_suggestions_index.py"
  exit 1
fi

if [[ "$MODE" == "--remote" ]]; then
  echo "Uploading $FILE to R2 ${BUCKET}/${KEY} (remote/prod)..."
  npx wrangler r2 object put "${BUCKET}/${KEY}" --file="$FILE" --content-type="application/json" --remote
else
  echo "Uploading $FILE to R2 ${BUCKET}/${KEY} (local dev)..."
  npx wrangler r2 object put "${BUCKET}/${KEY}" --file="$FILE" --content-type="application/json" --local
fi
echo "Done. Worker will load suggestions from R2 on the next request."
