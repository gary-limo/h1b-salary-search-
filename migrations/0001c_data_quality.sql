-- Data quality checks for h1b_wages
-- Run after main table is loaded (after h1b_wages_export.sql), before 0002_create_salary_compare.sql
-- Run: wrangler d1 execute <db-name> --local --file=./migrations/0001c_data_quality.sql

-- Trim trailing periods from employer names (e.g. "Acme Inc." -> "Acme Inc")
UPDATE h1b_wages
SET employer_name = rtrim(employer_name, '.')
WHERE employer_name LIKE '%.';

-- Remove commas from employer names
UPDATE h1b_wages
SET employer_name = replace(employer_name, ',', '')
WHERE employer_name LIKE '%,%';

-- Trim leading and trailing whitespace from employer_name and job_title
UPDATE h1b_wages
SET employer_name = trim(employer_name),
    job_title = trim(job_title)
WHERE employer_name != trim(employer_name)
   OR job_title != trim(job_title);

-- Wage data errors: 8-digit values (e.g. 21300000 instead of 213000) -> set to 0.01 to exclude from aggregates
UPDATE h1b_wages
SET wage_rate_of_pay_from = 0.01
WHERE wage_rate_of_pay_from >= 10000000 AND wage_rate_of_pay_from < 100000000;

-- 7-digit values (e.g. 1200000 instead of 120000) -> multiply by 0.1 to correct
UPDATE h1b_wages
SET wage_rate_of_pay_from = wage_rate_of_pay_from * 0.1
WHERE wage_rate_of_pay_from >= 1000000 AND wage_rate_of_pay_from < 10000000;
