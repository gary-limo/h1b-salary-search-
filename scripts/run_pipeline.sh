#!/usr/bin/env bash
#
# End-to-end ETL pipeline for H1B Salary Search.
#
# Runs all steps from raw Excel files to a fully populated local D1,
# ready for testing with 'npm run dev'. Once validated locally, deploy
# to production with: ./scripts/load_d1_replace.sh
#
# Usage:
#   ./scripts/run_pipeline.sh           # run full pipeline (Steps 1-3)
#   ./scripts/run_pipeline.sh --prod    # run full pipeline + deploy to prod (Steps 1-4)
#
# Prerequisites:
#   - LCA Excel files in project root (LCA_Disclosure_Data_FY*.xlsx)
#   - Python 3 with pandas, openpyxl, textblob (pip install -r requirements.txt)
#   - Node.js with wrangler (npx wrangler)
#   - duckdb or pyarrow for parquet generation (pip install duckdb)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

DEPLOY_PROD=false
if [[ "${1:-}" == "--prod" ]]; then
  DEPLOY_PROD=true
fi

BOLD="\033[1m"
GREEN="\033[1;32m"
YELLOW="\033[1;33m"
CYAN="\033[1;36m"
RED="\033[1;31m"
RESET="\033[0m"

step_header() {
  echo ""
  echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
  echo -e "${BOLD}  $1${RESET}"
  echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
  echo ""
}

info()    { echo -e "  ${GREEN}✓${RESET} $1"; }
warn()    { echo -e "  ${YELLOW}⚠${RESET} $1"; }
progress(){ echo -e "  ${CYAN}→${RESET} $1"; }

fail() {
  echo -e "  ${RED}✗ $1${RESET}"
  exit 1
}

TOTAL_START=$SECONDS

# ─────────────────────────────────────────────────────────────
# Preflight checks
# ─────────────────────────────────────────────────────────────
step_header "Preflight checks"

progress "Checking for LCA Excel source files..."
LCA_COUNT=$(ls -1 LCA_Disclosure_Data_FY*.xlsx 2>/dev/null | wc -l | tr -d ' ')
if [[ "$LCA_COUNT" -eq 0 ]]; then
  fail "No LCA Excel files found in project root. Expected LCA_Disclosure_Data_FY*.xlsx"
fi
info "Found $LCA_COUNT LCA Excel file(s)"

progress "Checking Python..."
if ! command -v python3 &>/dev/null; then
  fail "python3 not found. Install Python 3 first."
fi
info "Python: $(python3 --version 2>&1)"

progress "Checking wrangler..."
if ! npx wrangler --version &>/dev/null; then
  fail "wrangler not found. Run: npm install"
fi
info "Wrangler: $(npx wrangler --version 2>&1 | head -1)"

progress "Checking Python dependencies..."
python3 -c "import pandas, openpyxl, textblob" 2>/dev/null || {
  fail "Missing Python packages. Run: pip install -r requirements.txt"
}
info "Python dependencies OK (pandas, openpyxl, textblob)"

progress "Checking parquet dependencies (duckdb or pyarrow)..."
python3 -c "import duckdb" 2>/dev/null || python3 -c "import pyarrow" 2>/dev/null || {
  fail "Missing parquet dependency. Run: pip install duckdb (or pip install pyarrow)"
}
info "Parquet dependencies OK (duckdb or pyarrow)"

# ─────────────────────────────────────────────────────────────
# Step 1: Parse LCA Excel files → parsed_output.csv
# ─────────────────────────────────────────────────────────────
step_header "Step 1/4: Parsing LCA Excel files"

progress "Running data_parsing.py (this may take several minutes)..."
STEP1_START=$SECONDS
python3 data_parsing.py
STEP1_TIME=$(( SECONDS - STEP1_START ))

if [[ ! -f "parsed_output.csv" ]]; then
  fail "parsed_output.csv was not created"
fi

ROW_COUNT=$(wc -l < parsed_output.csv | tr -d ' ')
CSV_SIZE=$(du -h parsed_output.csv | cut -f1 | tr -d ' ')
info "parsed_output.csv: $ROW_COUNT lines, $CSV_SIZE"
info "Step 1 completed in ${STEP1_TIME}s"

# ─────────────────────────────────────────────────────────────
# Step 2: Build local D1 from parsed_output.csv
# ─────────────────────────────────────────────────────────────
step_header "Step 2/4: Building local D1 database"

progress "Running create_db.py (flush → schema → data → indexes → suggest → pairs)..."
STEP2_START=$SECONDS
python3 scripts/create_db.py
STEP2_TIME=$(( SECONDS - STEP2_START ))

if [[ ! -f "h1b_wages_export.sql" ]]; then
  fail "h1b_wages_export.sql was not created"
fi
if [[ ! -f "distinct_employer_job_pairs.txt" ]]; then
  fail "distinct_employer_job_pairs.txt was not created"
fi

SQL_SIZE=$(du -h h1b_wages_export.sql | cut -f1 | tr -d ' ')
PAIR_COUNT=$(wc -l < distinct_employer_job_pairs.txt | tr -d ' ')
info "h1b_wages_export.sql: $SQL_SIZE"
info "distinct_employer_job_pairs.txt: $PAIR_COUNT pairs"
info "Local D1 populated and ready"
info "Step 2 completed in ${STEP2_TIME}s"

# ─────────────────────────────────────────────────────────────
# Step 3: Create parquet file
# ─────────────────────────────────────────────────────────────
step_header "Step 3/4: Creating parquet file"

progress "Running to_parquet.py..."
STEP3_START=$SECONDS
if ! python3 scripts/to_parquet.py; then
  fail "Step 3 failed: to_parquet.py could not create parquet. Ensure distinct_employer_job_pairs.txt exists and pip install duckdb (or pyarrow)."
fi
STEP3_TIME=$(( SECONDS - STEP3_START ))

if [[ ! -f "public/pairs_v2.parquet" ]]; then
  fail "public/pairs_v2.parquet was not created"
fi

PARQUET_SIZE=$(du -h public/pairs_v2.parquet | cut -f1 | tr -d ' ')
info "public/pairs_v2.parquet: $PARQUET_SIZE"
info "Step 3 completed in ${STEP3_TIME}s"

# ─────────────────────────────────────────────────────────────
# Step 3b: Build suggestions index (R2 + local fallback)
# ─────────────────────────────────────────────────────────────
progress "Running build_suggestions_index.py..."
if python3 scripts/build_suggestions_index.py; then
  if [[ -f "public/suggestions_index.json" ]]; then
    IDX_SIZE=$(du -h public/suggestions_index.json | cut -f1 | tr -d ' ')
    info "public/suggestions_index.json: $IDX_SIZE"
  fi
else
  warn "Suggestions index not built (optional). Worker will use parquet fallback or R2 when uploaded."
fi

# ─────────────────────────────────────────────────────────────
# Step 4: Deploy to production (optional)
# ─────────────────────────────────────────────────────────────
if [[ "$DEPLOY_PROD" == true ]]; then
  step_header "Step 4/4: Deploying local D1 to production"
  warn "This will REPLACE all production D1 tables!"
  progress "Running load_d1_replace.sh --yes..."
  STEP4_START=$SECONDS
  ./scripts/load_d1_replace.sh --yes
  STEP4_TIME=$(( SECONDS - STEP4_START ))
  info "Production D1 replaced from local D1"
  info "Step 4 completed in ${STEP4_TIME}s"
else
  echo ""
  echo -e "  ${YELLOW}Step 4 skipped${RESET} (production deploy)"
  echo -e "  To deploy to prod after testing, run:"
  echo -e "    ${BOLD}./scripts/load_d1_replace.sh${RESET}"
fi

# ─────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────
TOTAL_TIME=$(( SECONDS - TOTAL_START ))
MINUTES=$(( TOTAL_TIME / 60 ))
SECS=$(( TOTAL_TIME % 60 ))

step_header "Pipeline complete"

echo -e "  ${GREEN}Step 1${RESET}  Parse Excel → CSV                ${STEP1_TIME}s"
echo -e "  ${GREEN}Step 2${RESET}  Build local D1 + distinct pairs   ${STEP2_TIME}s"
echo -e "  ${GREEN}Step 3${RESET}  Create parquet                    ${STEP3_TIME}s"
if [[ "$DEPLOY_PROD" == true ]]; then
echo -e "  ${GREEN}Step 4${RESET}  Deploy to production              ${STEP4_TIME}s"
fi
echo ""
echo -e "  Total: ${BOLD}${MINUTES}m ${SECS}s${RESET}"
echo ""
echo -e "  ${CYAN}Next:${RESET} Test locally with ${BOLD}npm run dev${RESET}"
if [[ "$DEPLOY_PROD" != true ]]; then
echo -e "        Deploy to prod with ${BOLD}./scripts/load_d1_replace.sh${RESET}"
fi
echo ""
