#!/usr/bin/env python3
"""
Convert output_final.csv to h1b_data.parquet for the H1B Salary Search app.

Requires: pip install duckdb

Usage:
  python csv_to_parquet.py

Input:  output_final.csv (Employer, Job_Title, Base_Salary, Salary_Range, Location, Start_Date, End_Date)
Output: h1b_data.parquet (e, j, wf, loc, bd, ed) — Salary_Range excluded
"""

import duckdb
import os
import re
import glob
from datetime import datetime

CSV_PATH = "output_final.csv"
INDEX_PATH = "index.html"
PARQUET_PATH = f"h1b_data_{datetime.now().strftime('%m%d%Y')}.parquet"

def patch_index(parquet_name):
    with open(INDEX_PATH, "r") as f:
        html = f.read()
    updated = re.sub(
        r'const PARQUET_URL\s*=\s*"h1b_data_\d{8}\.parquet"',
        f'const PARQUET_URL = "{parquet_name}"',
        html,
    )
    if updated != html:
        with open(INDEX_PATH, "w") as f:
            f.write(updated)
        print(f"Patched {INDEX_PATH} → {parquet_name}")
    else:
        print(f"Warning: could not find PARQUET_URL to patch in {INDEX_PATH}")

def main():
    if not os.path.exists(CSV_PATH):
        print(f"Error: {CSV_PATH} not found")
        return 1

    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT
                Employer         AS e,
                Job_Title        AS j,
                CAST(Base_Salary AS INTEGER) AS wf,
                Location         AS loc,
                Start_Date       AS bd,
                End_Date         AS ed
            FROM read_csv_auto('{CSV_PATH}', ignore_errors=true)
        ) TO '{PARQUET_PATH}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    row_count = con.execute(f"SELECT count(*) FROM '{PARQUET_PATH}'").fetchone()[0]
    size_mb = os.path.getsize(PARQUET_PATH) / 1024 / 1024

    for old in glob.glob("h1b_data_*.parquet"):
        if old != PARQUET_PATH:
            os.remove(old)
            print(f"Removed old: {old}")

    patch_index(PARQUET_PATH)

    print(f"Created {PARQUET_PATH}")
    print(f"  Rows: {row_count:,}")
    print(f"  Size: {size_mb:.1f} MB")
    return 0

if __name__ == "__main__":
    exit(main())
