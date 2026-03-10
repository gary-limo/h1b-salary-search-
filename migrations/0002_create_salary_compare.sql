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

INSERT INTO h1b_salary_compare (employer_name, soc_title, wage_rate_of_pay_from, std_career_level)
SELECT
    employer_name,
    soc_title,
    wage_rate_of_pay_from,

    CASE
      WHEN SUBSTR(soc_code, 1, 2) = '11' AND pw_wage_level = 'IV'
        THEN 'L7 - Executive'

      WHEN SUBSTR(soc_code, 1, 2) = '11' AND pw_wage_level = 'III'
        THEN 'L6 - Senior Manager'

      WHEN SUBSTR(soc_code, 1, 2) = '11'
           AND pw_wage_level IN ('I', 'II')
        THEN 'L5 - Manager'

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
    AVG(wage_rate_of_pay_from) AS avg_wage,
    std_career_level
FROM h1b_salary_compare
GROUP BY employer_name, std_career_level;

CREATE INDEX idx_summary_employer ON h1b_salary_summary(employer_name);
CREATE INDEX idx_summary_level ON h1b_salary_summary(std_career_level);
CREATE INDEX idx_summary_employer_level ON h1b_salary_summary(employer_name, std_career_level);
