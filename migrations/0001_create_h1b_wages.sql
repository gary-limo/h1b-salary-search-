-- Schema for Cloudflare D1 (compatible with SQLite)
-- Run: wrangler d1 execute <db-name> --remote --file=./migrations/0001_create_h1b_wages.sql

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
