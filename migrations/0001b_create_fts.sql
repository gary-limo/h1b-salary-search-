-- Autocomplete suggest tables: distinct values for fast no-context prefix lookups.
-- PRIMARY KEY on each column creates an implicit B-tree index (prefix LIKE uses it).
-- When context fields are provided (employer/job/location), handleSuggest
-- falls back to h1b_wages with exact-match filters for fully-interlinked suggestions.
DROP TABLE IF EXISTS h1b_suggest_employers;
CREATE TABLE h1b_suggest_employers (employer_name TEXT PRIMARY KEY);
INSERT INTO h1b_suggest_employers
SELECT DISTINCT employer_name FROM h1b_wages WHERE trim(employer_name) != '';

DROP TABLE IF EXISTS h1b_suggest_jobs;
CREATE TABLE h1b_suggest_jobs (job_title TEXT PRIMARY KEY);
INSERT INTO h1b_suggest_jobs
SELECT DISTINCT job_title FROM h1b_wages WHERE trim(job_title) != '';

DROP TABLE IF EXISTS h1b_suggest_locations;
CREATE TABLE h1b_suggest_locations (worksite_city TEXT PRIMARY KEY);
INSERT INTO h1b_suggest_locations
SELECT DISTINCT worksite_city FROM h1b_wages WHERE trim(worksite_city) != '';
