#!/usr/bin/env python3
"""Create SQLite database and load H1B wage data for Cloudflare D1 migration."""

import csv
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "h1b_wages.db"
CSV_PATH = Path(__file__).resolve().parent.parent / "parsed_output.csv"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS h1b_wages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_number TEXT,
    job_title TEXT,
    soc_code TEXT,
    soc_title TEXT,
    begin_date TEXT,
    end_date TEXT,
    employer_name TEXT,
    employer_address1 TEXT,
    employer_address2 TEXT,
    employer_city TEXT,
    employer_state TEXT,
    employer_postal_code TEXT,
    employer_country TEXT,
    worksite_address1 TEXT,
    worksite_address2 TEXT,
    worksite_city TEXT,
    worksite_county TEXT,
    worksite_state TEXT,
    worksite_postal_code TEXT,
    wage_rate_of_pay_from REAL,
    wage_rate_of_pay_to REAL,
    prevailing_wage REAL,
    pw_wage_level TEXT
);

CREATE INDEX IF NOT EXISTS idx_h1b_case_number ON h1b_wages(case_number);
CREATE INDEX IF NOT EXISTS idx_h1b_job_title ON h1b_wages(job_title);
CREATE INDEX IF NOT EXISTS idx_h1b_employer ON h1b_wages(employer_name);
CREATE INDEX IF NOT EXISTS idx_h1b_worksite_state ON h1b_wages(worksite_state);
CREATE INDEX IF NOT EXISTS idx_h1b_wage_from ON h1b_wages(wage_rate_of_pay_from);
CREATE INDEX IF NOT EXISTS idx_h1b_begin_date ON h1b_wages(begin_date);
"""

INSERT_SQL = """
INSERT INTO h1b_wages (
    case_number, job_title, soc_code, soc_title,
    begin_date, end_date,
    employer_name, employer_address1, employer_address2,
    employer_city, employer_state, employer_postal_code, employer_country,
    worksite_address1, worksite_address2, worksite_city, worksite_county,
    worksite_state, worksite_postal_code,
    wage_rate_of_pay_from, wage_rate_of_pay_to, prevailing_wage, pw_wage_level
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def parse_float(value: str) -> float | None:
    if not value or value.strip() == "":
        return None
    try:
        return float(value.replace(",", ""))
    except ValueError:
        return None


def clean(value: str) -> str | None:
    v = value.strip()
    return v if v else None


def main():
    print(f"Creating database at {DB_PATH}")

    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA_SQL)

    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        count = 0
        batch = []
        batch_size = 10000

        for row in reader:
            batch.append((
                clean(row.get("CASE_NUMBER", "")),
                clean(row.get("JOB_TITLE", "")),
                clean(row.get("SOC_CODE", "")),
                clean(row.get("SOC_TITLE", "")),
                clean(row.get("BEGIN_DATE", "")),
                clean(row.get("END_DATE", "")),
                clean(row.get("EMPLOYER_NAME", "")),
                clean(row.get("EMPLOYER_ADDRESS1", "")),
                clean(row.get("EMPLOYER_ADDRESS2", "")),
                clean(row.get("EMPLOYER_CITY", "")),
                clean(row.get("EMPLOYER_STATE", "")),
                clean(row.get("EMPLOYER_POSTAL_CODE", "")),
                clean(row.get("EMPLOYER_COUNTRY", "")),
                clean(row.get("WORKSITE_ADDRESS1", "")),
                clean(row.get("WORKSITE_ADDRESS2", "")),
                clean(row.get("WORKSITE_CITY", "")),
                clean(row.get("WORKSITE_COUNTY", "")),
                clean(row.get("WORKSITE_STATE", "")),
                clean(row.get("WORKSITE_POSTAL_CODE", "")),
                parse_float(row.get("WAGE_RATE_OF_PAY_FROM", "")),
                parse_float(row.get("WAGE_RATE_OF_PAY_TO", "")),
                parse_float(row.get("PREVAILING_WAGE", "")),
                clean(row.get("PW_WAGE_LEVEL", "")),
            ))

            if len(batch) >= batch_size:
                conn.executemany(INSERT_SQL, batch)
                count += len(batch)
                print(f"  Inserted {count:,} rows...")
                batch = []

        if batch:
            conn.executemany(INSERT_SQL, batch)
            count += len(batch)

    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM h1b_wages").fetchone()[0]
    print(f"Done. Total records in h1b_wages: {total:,}")
    conn.close()
    print(f"Database saved to {DB_PATH}")


if __name__ == "__main__":
    main()
