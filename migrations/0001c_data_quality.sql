-- Data quality checks for h1b_wages
-- Run after main table is loaded (after h1b_wages_export.sql), before 0002_create_salary_compare.sql
-- Run: wrangler d1 execute <db-name> --local --file=./migrations/0001c_data_quality.sql

-- Trim trailing periods from employer names (e.g. "Acme Inc." -> "Acme Inc")
UPDATE h1b_wages
SET employer_name = rtrim(employer_name, '.')
WHERE employer_name LIKE '%.';

-- Fix typo: comapny -> company
UPDATE h1b_wages
SET employer_name = replace(employer_name, 'comapny', 'company')
WHERE employer_name LIKE '%comapny%';

-- Strip commas from employer names
UPDATE h1b_wages
SET employer_name = replace(employer_name, ',', '')
WHERE employer_name LIKE '%,%';

-- Normalize P.C. / p.c. -> PC / pc
UPDATE h1b_wages
SET employer_name = replace(replace(employer_name, 'P.C.', 'PC'), 'p.c.', 'pc')
WHERE employer_name LIKE '%P.C.%' OR employer_name LIKE '%p.c.%';

-- Trim leading and trailing whitespace from employer_name and job_title
UPDATE h1b_wages
SET employer_name = trim(employer_name),
    job_title = trim(job_title)
WHERE employer_name != trim(employer_name)
   OR job_title != trim(job_title);

-- Next: run 0001c1_job_title_cleanup.sql (job title mappings), then 0001c2_data_quality_part2.sql (lowercase + wage fixes)
