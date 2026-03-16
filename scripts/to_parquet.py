#!/usr/bin/env python3
"""
Convert distinct_employer_job_pairs.txt to Parquet format.
Output: public/pairs_v2.parquet with columns [employer, job].
Also updates public/index.html with a cache-busting ?v=YYMMDD query param.
Uses DuckDB (pip install duckdb) or pyarrow as fallback.
"""

import os
import re
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
INPUT_FILE = os.path.join(PROJECT_DIR, "distinct_employer_job_pairs.txt")
OUTPUT_FILE = os.path.join(PROJECT_DIR, "public", "pairs_v2.parquet")
INDEX_HTML = os.path.join(PROJECT_DIR, "public", "index.html")


def update_index_html_version():
    """Replace pairs_v2.parquet?v=XXXXXX with today's YYMMDD in index.html."""
    if not os.path.exists(INDEX_HTML):
        print(f"  WARNING: {INDEX_HTML} not found, skipping version update")
        return

    version = datetime.now().strftime("%y%m%d")
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        html = f.read()

    updated, count = re.subn(
        r'pairs_v2\.parquet\?v=\d{6}',
        f'pairs_v2.parquet?v={version}',
        html,
    )
    if count == 0:
        print(f"  WARNING: No pairs_v2.parquet?v=XXXXXX pattern found in index.html")
        return

    updated, cache_count = re.subn(
        r'h1b-parquet-\d{6}',
        f'h1b-parquet-{version}',
        updated,
    )
    if cache_count:
        print(f"  Updated cache name: h1b-parquet-{version} ({cache_count} occurrence(s))")

    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(updated)
    print(f"  Updated index.html: pairs_v2.parquet?v={version} ({count} occurrence(s))")


def main():
    employers = []
    jobs = []
    with open(INPUT_FILE, encoding="utf-8", errors="replace") as f:
        lines = f.read().strip().splitlines()
    for line in lines:
        if "|" not in line:
            continue
        emp, job = line.split("|", 1)
        emp, job = emp.strip(), job.strip()
        if emp and job:
            employers.append(emp)
            jobs.append(job)

    out_path = os.path.abspath(OUTPUT_FILE).replace("\\", "/")

    try:
        import duckdb
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE pairs(employer VARCHAR, job VARCHAR)")
        for i in range(0, len(employers), 10000):
            batch = list(zip(employers[i:i+10000], jobs[i:i+10000]))
            conn.executemany("INSERT INTO pairs VALUES (?, ?)", batch)
        conn.execute(f"COPY pairs TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    except ImportError:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            table = pa.table({"employer": employers, "job": jobs})
            pq.write_table(table, OUTPUT_FILE, compression="zstd")
        except ImportError:
            print("Install duckdb or pyarrow: pip install duckdb")
            raise SystemExit(1)

    n = len(employers)
    size_mb = os.path.getsize(OUTPUT_FILE) / 1024 / 1024
    print(f"Rows: {n:,}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Size: {size_mb:.2f} MB")

    update_index_html_version()


if __name__ == "__main__":
    main()
