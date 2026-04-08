#!/usr/bin/env python3
"""
Fill employer_seo from distinct employers in local h1b_wages (Wrangler D1 SQLite).

Slug rules:
  - Kebab-case from employer_name; collisions get a stable 8-char hex suffix (sha256 of name).

Run after migrations/0002_employer_seo.sql is applied. Invoked from create_db.py.
"""

from __future__ import annotations

import glob
import hashlib
import os
import re
import sqlite3
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
WRANGLER_D1_DIR = os.path.join(
    PROJECT_DIR, ".wrangler", "state", "v3", "d1", "miniflare-D1DatabaseObject"
)


def find_local_d1_db():
    pattern = os.path.join(WRANGLER_D1_DIR, "*.sqlite")
    matches = glob.glob(pattern)
    return matches[0] if matches else None


def base_slug(name: str) -> str:
    s = (name or "").lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s if s else "employer"


def assign_slugs(rows: list[tuple[str, int]]) -> list[tuple[str, str, int]]:
    """rows: (employer_name, filing_count) sorted by employer_name."""
    slug_to_employer: dict[str, str] = {}
    out: list[tuple[str, str, int]] = []

    for employer_name, filing_count in rows:
        en = employer_name or ""
        base = base_slug(en)
        slug = base
        if slug not in slug_to_employer:
            slug_to_employer[slug] = en
        elif slug_to_employer[slug] != en:
            h = hashlib.sha256(en.encode("utf-8")).hexdigest()[:8]
            slug = f"{base}-{h}"
            while slug in slug_to_employer and slug_to_employer[slug] != en:
                h = hashlib.sha256((en + slug).encode("utf-8")).hexdigest()[:8]
                slug = f"{base}-{h}"
            slug_to_employer[slug] = en
        out.append((slug, en, int(filing_count)))
    return out


def main() -> int:
    local_db = find_local_d1_db()
    if not local_db:
        print("ERROR: No local D1 SQLite under .wrangler/state/...", file=sys.stderr)
        return 1

    conn = sqlite3.connect(local_db)
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='employer_seo'"
        )
        if not cur.fetchone():
            print(
                "ERROR: employer_seo table missing. Apply migrations/0002_employer_seo.sql first.",
                file=sys.stderr,
            )
            return 1

        conn.execute("DELETE FROM employer_seo")
        rows = conn.execute(
            "SELECT employer_name, COUNT(*) AS c FROM h1b_wages "
            "GROUP BY employer_name ORDER BY employer_name"
        ).fetchall()

        assigned = assign_slugs(rows)
        conn.executemany(
            "INSERT INTO employer_seo (slug, employer_name, filing_count) VALUES (?, ?, ?)",
            assigned,
        )
        conn.commit()
        print(f"employer_seo: inserted {len(assigned):,} employer row(s)")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
