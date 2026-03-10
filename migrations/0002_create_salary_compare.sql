-- Materialized salary comparison table
-- Derived career level from soc_title (SOC major group) + pw_wage_level
-- Run: wrangler d1 execute <db-name> --remote --file=./migrations/0002_create_salary_compare.sql

DROP TABLE IF EXISTS h1b_salary_compare;

CREATE TABLE h1b_salary_compare (
    employer_name TEXT,
    soc_title TEXT,
    wage_rate_of_pay_from REAL,
    std_career_level TEXT
);

-- Tech-relevant management SOC codes (11-xxxx subset)
-- Excludes: Construction (11-9021), Facilities (11-3013), Security (11-3013.01),
--   Administrative Services (11-3012), Industrial Production (11-3051),
--   Food Service (11-9051), Lodging (11-9081), and other non-tech management

INSERT INTO h1b_salary_compare (employer_name, soc_title, wage_rate_of_pay_from, std_career_level)
SELECT
    employer_name,
    soc_title,
    wage_rate_of_pay_from,

    CASE
      -- Tech management (SOC 11): only tech-relevant sub-codes
      WHEN SUBSTR(soc_code, 1, 2) = '11'
           AND SUBSTR(soc_code, 1, 7) IN (
             '11-1011', -- Chief Executives
             '11-1021', -- General and Operations Managers
             '11-2011', -- Advertising and Promotions Managers
             '11-2021', -- Marketing Managers
             '11-2022', -- Sales Managers
             '11-2032', -- Public Relations Managers
             '11-3021', -- Computer and Information Systems Managers
             '11-3031', -- Financial Managers
             '11-3111', -- Compensation and Benefits Managers
             '11-3121', -- Human Resources Managers
             '11-3131', -- Training and Development Managers
             '11-9041', -- Architectural and Engineering Managers
             '11-9121', -- Natural Sciences Managers
             '11-9161', -- Emergency Management Directors
             '11-9198', -- Personal Service Managers (used by tech for program mgmt)
             '11-9199'  -- Managers, All Other
           )
           AND pw_wage_level = 'IV'
        THEN 'L7 - Executive'

      WHEN SUBSTR(soc_code, 1, 2) = '11'
           AND SUBSTR(soc_code, 1, 7) IN (
             '11-1011','11-1021','11-2011','11-2021','11-2022','11-2032',
             '11-3021','11-3031','11-3111','11-3121','11-3131',
             '11-9041','11-9121','11-9161','11-9198','11-9199'
           )
           AND pw_wage_level = 'III'
        THEN 'L6 - Senior Manager'

      WHEN SUBSTR(soc_code, 1, 2) = '11'
           AND SUBSTR(soc_code, 1, 7) IN (
             '11-1011','11-1021','11-2011','11-2021','11-2022','11-2032',
             '11-3021','11-3031','11-3111','11-3121','11-3131',
             '11-9041','11-9121','11-9161','11-9198','11-9199'
           )
           AND pw_wage_level IN ('I', 'II')
        THEN 'L5 - Manager'

      -- Non-tech management SOCs → Non-Tech bucket
      WHEN SUBSTR(soc_code, 1, 2) = '11'
        THEN 'Non-Tech'

      -- Non-tech SOC major groups → Non-Tech bucket
      -- 19: Social/Life Sciences, 21: Social Services, 31-53: trades/support
      WHEN SUBSTR(soc_code, 1, 2) IN ('19','21','31','33','35','37','39','43','45','47','49','51','53')
        THEN 'Non-Tech'

      -- Civil Engineering → Non-Tech
      WHEN SUBSTR(soc_code, 1, 7) = '17-2051'
        THEN 'Non-Tech'

      -- Tech roles: L1-L4 by wage level
      WHEN pw_wage_level = 'IV'
        THEN 'L4 - Expert'

      WHEN pw_wage_level = 'III'
        THEN 'L3 - Senior'

      WHEN pw_wage_level = 'II'
        THEN 'L2 - Mid-Level'

      WHEN pw_wage_level = 'I'
        THEN 'L1 - Entry-Level'

      WHEN (pw_wage_level IS NULL OR pw_wage_level = '')
           AND prevailing_wage > 0 THEN
        CASE
          WHEN wage_rate_of_pay_from <= prevailing_wage * 1.17
            THEN 'L1 - Entry-Level'
          WHEN wage_rate_of_pay_from <= prevailing_wage * 1.34
            THEN 'L2 - Mid-Level'
          WHEN wage_rate_of_pay_from <= prevailing_wage * 1.50
            THEN 'L3 - Senior'
          ELSE 'L4 - Expert'
        END

      ELSE 'Unclassified'
    END AS std_career_level

FROM h1b_wages
WHERE wage_rate_of_pay_from > 0;

CREATE INDEX idx_sc_career ON h1b_salary_compare(std_career_level);
CREATE INDEX idx_sc_employer ON h1b_salary_compare(employer_name);
CREATE INDEX idx_sc_soc ON h1b_salary_compare(soc_title);
CREATE INDEX idx_sc_employer_career ON h1b_salary_compare(employer_name, std_career_level);
CREATE INDEX idx_sc_soc_career ON h1b_salary_compare(soc_title, std_career_level);
CREATE INDEX idx_sc_soc_employer_career ON h1b_salary_compare(soc_title, employer_name, std_career_level);

-- ─────────────────────────────────────────────────────────────────────────────
-- Aggregated salary summary: avg wage per employer + career level
-- For dashboard salary comparison (pre-computed, no aggregation at query time)
-- ─────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS h1b_salary_summary;

CREATE TABLE h1b_salary_summary (
    employer_name TEXT,
    avg_wage REAL,
    std_career_level TEXT
);

INSERT INTO h1b_salary_summary (employer_name, avg_wage, std_career_level)
SELECT
    employer_name,
    ROUND(AVG(wage_rate_of_pay_from), 0) AS avg_wage,
    std_career_level
FROM h1b_salary_compare
GROUP BY employer_name, std_career_level;

CREATE INDEX idx_summary_employer ON h1b_salary_summary(employer_name);
CREATE INDEX idx_summary_level ON h1b_salary_summary(std_career_level);
CREATE INDEX idx_summary_employer_level ON h1b_salary_summary(employer_name, std_career_level);
