#!/usr/bin/env python3
"""
Export all h1b_wages rows where base salary (wage_rate_of_pay_from) is NULL.

Uses local Wrangler D1 SQLite (same layout as create_db.py).
Output: Excel workbook via pandas + openpyxl (already in requirements.txt).
"""

import glob
import os
import sqlite3
import sys
from datetime import datetime, timezone

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
WRANGLER_D1_DIR = os.path.join(
    PROJECT_DIR, ".wrangler", "state", "v3", "d1", "miniflare-D1DatabaseObject"
)
DEFAULT_OUT_DIR = os.path.join(PROJECT_DIR, "exports")


def find_local_d1_db():
    pattern = os.path.join(WRANGLER_D1_DIR, "*.sqlite")
    matches = glob.glob(pattern)
    if not matches:
        return None
    return matches[0]


def main():
    db_path = find_local_d1_db()
    if not db_path:
        print(
            "No local D1 database found under .wrangler/state/v3/d1/miniflare-D1DatabaseObject/",
            file=sys.stderr,
        )
        print("Run create_db.py / seed_local_d1.sh first.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(DEFAULT_OUT_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(DEFAULT_OUT_DIR, f"missing_wage_rate_of_pay_from_{ts}.xlsx")

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        df = pd.read_sql_query(
            """
            SELECT *
            FROM h1b_wages
            WHERE wage_rate_of_pay_from IS NULL
            ORDER BY employer_name, job_title, case_number
            """,
            conn,
        )
    finally:
        conn.close()

    # .xlsx (Excel); openpyxl is in requirements — legacy .xls is not supported without xlwt.
    df.to_excel(out_path, index=False, sheet_name="missing_wage_from")
    print(f"Rows: {len(df)}")
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
