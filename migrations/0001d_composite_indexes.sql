-- Composite indexes for context-aware autocomplete suggest
-- When one field is selected, queries filter by that field (exact match, index seek)
-- and suggest values for the other field using the composite index — no full table scan.
-- Run: wrangler d1 execute <db-name> --local --file=./migrations/0001d_composite_indexes.sql

-- employer selected → suggest jobs in that employer
CREATE INDEX IF NOT EXISTS idx_h1b_emp_job  ON h1b_wages(employer_name, job_title);

-- employer selected → suggest cities for that employer
CREATE INDEX IF NOT EXISTS idx_h1b_emp_city ON h1b_wages(employer_name, worksite_city);

-- job selected → suggest employers that have that job
CREATE INDEX IF NOT EXISTS idx_h1b_job_emp  ON h1b_wages(job_title, employer_name);

-- job selected → suggest cities where that job exists
CREATE INDEX IF NOT EXISTS idx_h1b_job_city ON h1b_wages(job_title, worksite_city);

-- city selected → suggest employers in that city
CREATE INDEX IF NOT EXISTS idx_h1b_city_emp ON h1b_wages(worksite_city, employer_name);

-- city selected → suggest jobs in that city
CREATE INDEX IF NOT EXISTS idx_h1b_city_job ON h1b_wages(worksite_city, job_title);
