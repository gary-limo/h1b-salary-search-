-- Data quality part 2: lowercase normalization + wage fixes
-- Run AFTER 0001c1_job_title_cleanup.sql
-- Run: wrangler d1 execute <db-name> --local --file=./migrations/0001c2_data_quality_part2.sql

-- Normalize job_title and worksite_city to lowercase for consistent deduplication
-- (Frontend applies title-case formatting on display)
UPDATE h1b_wages
SET job_title = LOWER(job_title)
WHERE job_title != LOWER(job_title);

UPDATE h1b_wages
SET worksite_city = LOWER(worksite_city)
WHERE worksite_city != LOWER(worksite_city);

-- Wage data errors: 8-digit values (e.g. 21300000 instead of 213000) -> set to 0.01 to exclude from aggregates
UPDATE h1b_wages
SET wage_rate_of_pay_from = 0.01
WHERE wage_rate_of_pay_from >= 10000000 AND wage_rate_of_pay_from < 100000000;

-- 7-digit values (e.g. 1200000 instead of 120000) -> multiply by 0.1 to correct
UPDATE h1b_wages
SET wage_rate_of_pay_from = wage_rate_of_pay_from * 0.1
WHERE wage_rate_of_pay_from >= 1000000 AND wage_rate_of_pay_from < 10000000;
