#!/usr/bin/env bash
#
# Warm KV cache by hitting each preset search URL.
# Run after data reload or deploy to pre-populate cache for common searches.
#
# Usage:
#   ./scripts/warm_cache.sh                          # default: production
#   ./scripts/warm_cache.sh https://h1b-salaries.com # explicit base URL
#   ./scripts/warm_cache.sh http://localhost:8787     # local dev
#
# You can add more URLs to PRESETS below — any search combo works.

set -euo pipefail

BASE="${1:-https://h1b-salaries.com}"
ORIGIN="$BASE"

PRESETS=(
  # Employer presets
  "employer=Google+LLC"
  "employer=Amazon.com+Services+LLC"
  "employer=Microsoft+Corporation"
  "employer=Meta+Platforms+Inc"
  "employer=Apple+Inc"
  "employer=Accenture+LLP"
  # Job title presets
  "job=data+scientist"
  "job=software+engineer"
  # Location presets
  "location=san+francisco"
  "location=seattle"
  "location=new+york"
)

echo "Warming cache at: $BASE"
echo "Presets: ${#PRESETS[@]}"
echo ""

SUCCESS=0
FAIL=0

for preset in "${PRESETS[@]}"; do
  URL="${BASE}/api/search?${preset}&page=1&pageSize=100&sort=wage_rate_of_pay_from&dir=DESC"
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Origin: ${ORIGIN}" \
    -H "Referer: ${ORIGIN}/" \
    "$URL")

  if [ "$STATUS" = "200" ]; then
    echo "  OK  $preset"
    SUCCESS=$((SUCCESS + 1))
  else
    echo "  FAIL ($STATUS)  $preset"
    FAIL=$((FAIL + 1))
  fi
done

echo ""
echo "Done. $SUCCESS succeeded, $FAIL failed."
