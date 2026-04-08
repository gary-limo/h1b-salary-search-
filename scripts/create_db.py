#!/usr/bin/env python3
"""
Build local D1 database from parsed_output.csv.

Flow:
  1. Generate h1b_wages_export.sql (INSERT statements) from parsed_output.csv
  2. Flush local D1 (drop h1b_wages and related tables)
  3. Create schema + load data + indexes via wrangler
  4. Apply OpenRefine merge SQL (employer_name, job_title) if present under scripts/
  5. Export distinct employer|job_title pairs to distinct_employer_job_pairs.txt

No intermediate h1b_wages.db is created. Local D1 is the single source of truth.
"""

import csv
import glob
import os
import sqlite3
import subprocess
import sys
import uuid

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from wage_parse import parse_wage_float
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
CSV_PATH = os.path.join(PROJECT_DIR, "parsed_output.csv")
EXPORT_SQL_PATH = os.path.join(PROJECT_DIR, "h1b_wages_export.sql")
PAIRS_PATH = os.path.join(PROJECT_DIR, "distinct_employer_job_pairs.txt")
DB_NAME = "h1b-wages"

WRANGLER_D1_DIR = os.path.join(
    PROJECT_DIR, ".wrangler", "state", "v3", "d1", "miniflare-D1DatabaseObject"
)

CSV_COLUMNS = [
    "CASE_NUMBER", "JOB_TITLE", "SOC_CODE", "SOC_TITLE",
    "BEGIN_DATE", "END_DATE",
    "EMPLOYER_NAME", "EMPLOYER_ADDRESS1", "EMPLOYER_ADDRESS2",
    "EMPLOYER_CITY", "EMPLOYER_STATE", "EMPLOYER_POSTAL_CODE", "EMPLOYER_COUNTRY",
    "WORKSITE_ADDRESS1", "WORKSITE_ADDRESS2", "WORKSITE_CITY", "WORKSITE_COUNTY",
    "WORKSITE_STATE", "WORKSITE_POSTAL_CODE",
    "WAGE_RATE_OF_PAY_FROM", "WAGE_RATE_OF_PAY_TO", "PREVAILING_WAGE", "PW_WAGE_LEVEL",
]

DB_COLUMNS = [
    "case_number", "job_title", "soc_code", "soc_title",
    "begin_date", "end_date",
    "employer_name", "employer_address1", "employer_address2",
    "employer_city", "employer_state", "employer_postal_code", "employer_country",
    "worksite_address1", "worksite_address2", "worksite_city", "worksite_county",
    "worksite_state", "worksite_postal_code",
    "wage_rate_of_pay_from", "wage_rate_of_pay_to", "prevailing_wage", "pw_wage_level",
]

FLOAT_COLUMNS = {"WAGE_RATE_OF_PAY_FROM", "WAGE_RATE_OF_PAY_TO", "PREVAILING_WAGE"}


def sql_escape(value):
    """Escape a string value for SQL. Returns 'NULL' for empty/None."""
    if value is None:
        return "NULL"
    v = value.strip()
    if not v:
        return "NULL"
    return "'" + v.replace("'", "''") + "'"


def format_value(csv_col, raw_value):
    """Format a CSV value for SQL insertion."""
    if csv_col in FLOAT_COLUMNS:
        n = parse_wage_float(raw_value)
        if n is None:
            return "NULL"
        return str(n)
    return sql_escape(raw_value)


def run_wrangler(args, description=""):
    """Run a wrangler command and check for errors."""
    cmd = ["npx", "wrangler", "d1", "execute", DB_NAME, "--local"] + args
    print(f"  {description}")
    result = subprocess.run(cmd, cwd=PROJECT_DIR, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr.strip()}")
        sys.exit(1)
    return result.stdout


def find_local_d1_db():
    """Find the local D1 SQLite file under .wrangler/state/."""
    pattern = os.path.join(WRANGLER_D1_DIR, "*.sqlite")
    matches = glob.glob(pattern)
    if not matches:
        return None
    if len(matches) > 1:
        print(f"WARNING: Multiple local D1 SQLite files found, using first: {matches[0]}")
    return matches[0]


def main():
    if not os.path.exists(CSV_PATH):
        print(f"Error: {CSV_PATH} not found. Run scripts/data_parsing.py first.")
        sys.exit(1)

    # Step 1: Generate h1b_wages_export.sql from parsed_output.csv
    print("Step 1/5: Generating h1b_wages_export.sql from parsed_output.csv...")
    cols_str = "id, " + ", ".join(DB_COLUMNS)
    row_count = 0

    with open(CSV_PATH, "r", encoding="utf-8", newline="") as csvfile, \
         open(EXPORT_SQL_PATH, "w", encoding="utf-8") as sqlfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row_id = sql_escape(str(uuid.uuid4()))
            values = ", ".join(format_value(col, row.get(col, "")) for col in CSV_COLUMNS)
            sqlfile.write(f"INSERT INTO h1b_wages ({cols_str}) VALUES ({row_id}, {values});\n")
            row_count += 1
            if row_count % 50000 == 0:
                print(f"  Generated {row_count:,} INSERT statements...")

    sql_size_mb = os.path.getsize(EXPORT_SQL_PATH) / 1024 / 1024
    print(f"  Done: {row_count:,} rows -> {EXPORT_SQL_PATH} ({sql_size_mb:.1f} MB)")

    # Step 2: Flush and seed local D1
    print("\nStep 2/5: Flushing and seeding local D1...")
    run_wrangler(
        ["--command", "DROP TABLE IF EXISTS h1b_wages;"],
        "Dropping existing tables..."
    )
    run_wrangler(
        ["--file=./migrations/0001_create_h1b_wages.sql"],
        "Creating h1b_wages schema..."
    )
    run_wrangler(
        [f"--file={EXPORT_SQL_PATH}"],
        f"Loading {row_count:,} rows (this may take a few minutes)..."
    )
    run_wrangler(
        ["--file=./migrations/0001d_composite_indexes.sql"],
        "Creating composite indexes..."
    )

    # Step 3: OpenRefine cluster merges (employer then job title)
    print("\nStep 3/5: OpenRefine employer/job title updates (if scripts present)...")
    openrefine_files = (
        "openrefine_employer_name_updates.sql",
        "openrefine_job_title_updates.sql",
    )
    for sql_name in openrefine_files:
        sql_path = os.path.join(SCRIPT_DIR, sql_name)
        if not os.path.isfile(sql_path):
            print(f"  (skip) {sql_name} not found under scripts/")
            continue
        run_wrangler(
            [f"--file=./scripts/{sql_name}"],
            f"Applying {sql_name} (may take several minutes for large files)...",
        )

    # Verify row count after optional updates
    run_wrangler(
        ["--command", "SELECT 'h1b_wages' as tbl, COUNT(*) as cnt FROM h1b_wages;"],
        "Verifying row count..."
    )

    # Step 4: Export distinct employer-job pairs from local D1
    print("\nStep 4/5: Exporting distinct employer-job pairs...")
    local_db = find_local_d1_db()
    if not local_db:
        print("  WARNING: Could not find local D1 SQLite file. Skipping pairs export.")
        print("  Run 'npm run dev' once to initialize .wrangler/state, then re-run this script.")
    else:
        conn = sqlite3.connect(local_db)
        cur = conn.execute(
            "SELECT employer_name, job_title FROM h1b_wages "
            "GROUP BY employer_name, job_title ORDER BY employer_name, job_title"
        )
        pair_count = 0
        with open(PAIRS_PATH, "w", encoding="utf-8") as f:
            for emp, job in cur:
                emp = (emp or "").replace("|", " ")
                job = (job or "").replace("|", " ")
                if emp or job:
                    f.write(f"{emp}|{job}\n")
                    pair_count += 1
        conn.close()
        print(f"  Done: {pair_count:,} distinct pairs -> {os.path.basename(PAIRS_PATH)}")

    print("\nStep 5/5: Done.")
    print("\nLocal D1 is ready. Run: npm run dev")


if __name__ == "__main__":
    main()
