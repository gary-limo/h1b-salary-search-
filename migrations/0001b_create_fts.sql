-- FTS5 virtual table for search (used by worker's handleSearch)
-- Must run AFTER h1b_wages schema exists and data is loaded.
-- For local: run after migrations/0001 + h1b_wages_export.sql
-- For prod: FTS was created manually in Cloudflare UI; this file allows local parity.

CREATE VIRTUAL TABLE IF NOT EXISTS h1b_wages_fts USING fts5(
  employer_name,
  job_title,
  worksite_city,
  worksite_state,
  content='h1b_wages',
  content_rowid='id'
);

-- Rebuild FTS index from h1b_wages (required after bulk load)
INSERT INTO h1b_wages_fts(h1b_wages_fts) VALUES('rebuild');
