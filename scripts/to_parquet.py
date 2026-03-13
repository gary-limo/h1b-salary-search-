#!/usr/bin/env python3
"""
Convert distinct_employer_job_pairs.txt to Parquet format.
Output: pairs_v2.parquet with columns [employer, job].
Uses DuckDB (pip install duckdb) or pyarrow as fallback.
"""

from pathlib import Path

INPUT_FILE = Path(__file__).resolve().parents[1] / "distinct_employer_job_pairs.txt"
OUTPUT_FILE = Path(__file__).resolve().parents[1] / "public" / "pairs_v2.parquet"


def main():
    employers = []
    jobs = []
    for line in INPUT_FILE.read_text(encoding="utf-8", errors="replace").strip().splitlines():
        if "|" not in line:
            continue
        emp, job = line.split("|", 1)
        emp, job = emp.strip(), job.strip()
        if emp and job:
            employers.append(emp)
            jobs.append(job)

    out_path = str(OUTPUT_FILE.resolve()).replace("\\", "/")

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
    size_mb = OUTPUT_FILE.stat().st_size / 1024 / 1024
    print(f"Rows: {n:,}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Size: {size_mb:.2f} MB")


if __name__ == "__main__":
    main()
