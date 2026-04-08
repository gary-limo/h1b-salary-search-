-- Programmatic SEO: slug -> canonical employer_name (lowercase, matches h1b_wages)
-- Populated by scripts/build_employer_seo_table.py after h1b_wages load.

CREATE TABLE IF NOT EXISTS employer_seo (
	slug TEXT PRIMARY KEY NOT NULL,
	employer_name TEXT NOT NULL,
	filing_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_employer_seo_employer_name ON employer_seo(employer_name);
