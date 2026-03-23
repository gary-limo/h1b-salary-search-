#!/usr/bin/env bash
#
# End-to-end ETL pipeline for H1B Salary Search.
#
# Runs all steps from raw Excel files to a fully populated local D1,
# ready for testing with 'npm run dev'. Once validated locally, deploy
# to production with: ./scripts/load_d1_replace.sh
#
# Usage:
#   ./scripts/run_pipeline.sh              # Steps 1–3 (+ optional local R2)
#   ./scripts/run_pipeline.sh --prod       # Steps 1–5: full + prod D1 + prod R2
#
# Optional:
#   UPLOAD_LOCAL_R2=1 ./scripts/run_pipeline.sh   # also seed local dev R2 after building index
#
# Prerequisites:
#   - LCA Excel files in project root (LCA_Disclosure_Data_FY*.xlsx)
#   - Python 3 with pandas, openpyxl, textblob (pip install -r requirements.txt)
#   - Node.js with wrangler (npx wrangler)

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

# ─────────────────────────────────────────────────────────────
# Step 1: Parse LCA Excel files → parsed_output.csv
# ─────────────────────────────────────────────────────────────
step_header "Step 1/5: Parsing LCA Excel files"

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
step_header "Step 2/5: Building local D1 database"

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
# Step 3: Build suggestions index JSON (Worker + R2)
# ─────────────────────────────────────────────────────────────
step_header "Step 3/5: Building suggestions index (JSON)"

progress "Running build_suggestions_index.py..."
STEP3_START=$SECONDS
if ! python3 scripts/build_suggestions_index.py; then
  fail "Step 3 failed: build_suggestions_index.py. Ensure distinct_employer_job_pairs.txt exists."
fi
STEP3_TIME=$(( SECONDS - STEP3_START ))

if [[ ! -f "public/suggestions_index.json" ]]; then
  warn "public/suggestions_index.json was not created (check script output)"
else
  IDX_SIZE=$(du -h public/suggestions_index.json | cut -f1 | tr -d ' ')
  info "public/suggestions_index.json: $IDX_SIZE"
fi
info "Step 3 completed in ${STEP3_TIME}s"

# ─────────────────────────────────────────────────────────────
# Step 4: Upload suggestions index to local R2 (optional)
# ─────────────────────────────────────────────────────────────
if [[ "$DEPLOY_PROD" != true ]]; then
  if [[ "${UPLOAD_LOCAL_R2:-}" == "1" ]] && [[ -f "public/suggestions_index.json" ]]; then
    step_header "Step 4/5: Upload suggestions index to local R2"
    progress "Running upload_suggestions_to_r2.sh --local..."
    if ! ./scripts/upload_suggestions_to_r2.sh --local; then
      warn "Local R2 upload failed. Run manually: ./scripts/upload_suggestions_to_r2.sh --local"
    else
      info "Suggestions index uploaded to local R2 (wrangler dev)"
    fi
  else
    echo ""
    echo -e "  ${CYAN}Local R2:${RESET} To seed the dev bucket after building the index, run:"
    echo -e "    ${BOLD}UPLOAD_LOCAL_R2=1 ./scripts/run_pipeline.sh${RESET}  (next run)"
    echo -e "    or ${BOLD}./scripts/upload_suggestions_to_r2.sh --local${RESET}"
    echo ""
  fi
fi

# ─────────────────────────────────────────────────────────────
# Step 5: Deploy to production (optional)
# ─────────────────────────────────────────────────────────────
if [[ "$DEPLOY_PROD" == true ]]; then
  step_header "Step 4/5: Deploying local D1 to production"
  warn "This will REPLACE all production D1 tables!"
  progress "Running load_d1_replace.sh --yes..."
  STEP4_START=$SECONDS
  ./scripts/load_d1_replace.sh --yes
  STEP4_TIME=$(( SECONDS - STEP4_START ))
  info "Production D1 replaced from local D1"
  info "Step 4 completed in ${STEP4_TIME}s"

  step_header "Step 5/5: Upload suggestions index to R2 (prod)"
  if [[ -f "public/suggestions_index.json" ]]; then
    progress "Running upload_suggestions_to_r2.sh --remote..."
    if ./scripts/upload_suggestions_to_r2.sh --remote; then
      info "Suggestions index uploaded to production R2"
    else
      warn "R2 upload failed. Ensure bucket exists: npx wrangler r2 bucket create h1b-suggestions-index"
    fi
  else
    warn "public/suggestions_index.json missing; skipping R2 upload"
  fi
  info "Step 5 completed"
else
  echo ""
  echo -e "  ${YELLOW}Production deploy skipped${RESET}"
  echo -e "  To deploy D1 to prod: ${BOLD}./scripts/load_d1_replace.sh${RESET}"
  echo -e "  To upload suggestions to prod R2: ${BOLD}./scripts/upload_suggestions_to_r2.sh --remote${RESET}"
  echo ""
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
echo -e "  ${GREEN}Step 3${RESET}  Build suggestions index (JSON)    ${STEP3_TIME}s"
if [[ "$DEPLOY_PROD" == true ]]; then
echo -e "  ${GREEN}Step 4${RESET}  Deploy D1 to production           ${STEP4_TIME}s"
echo -e "  ${GREEN}Step 5${RESET}  Upload suggestions index to R2      (prod)"
fi
echo ""
echo -e "  Total: ${BOLD}${MINUTES}m ${SECS}s${RESET}"
echo ""
echo -e "  ${CYAN}Next:${RESET} Test locally with ${BOLD}npm run dev${RESET}"
if [[ "$DEPLOY_PROD" != true ]]; then
echo -e "        Prod: ${BOLD}./scripts/run_pipeline.sh --prod${RESET} or deploy D1 + R2 separately (see above)"
fi
echo ""
