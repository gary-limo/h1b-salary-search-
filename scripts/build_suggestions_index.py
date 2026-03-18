#!/usr/bin/env python3
"""
Pre-build a JSON suggestion index from distinct_employer_job_pairs.txt.
Output: public/suggestions_index.json for Worker R2 + local fallback.
Run after distinct_employer_job_pairs.txt exists (same pipeline as to_parquet.py).
"""

import json
import os
import re
from collections import defaultdict
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
INPUT_FILE = os.path.join(PROJECT_DIR, "distinct_employer_job_pairs.txt")
OUTPUT_FILE = os.path.join(PROJECT_DIR, "public", "suggestions_index.json")
INDEX_HTML = os.path.join(PROJECT_DIR, "public", "index.html")
MAX_CONTEXT_PER_KEY = 200


def update_index_html_version(version: str) -> None:
    """Set suggestions_index.json?v= and suggestions-cache- in index.html."""
    if not os.path.exists(INDEX_HTML):
        return
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        html = f.read()
    html = re.sub(r"suggestions_index\.json\?v=\d{6}", f"suggestions_index.json?v={version}", html)
    html = re.sub(r"suggestions-cache-\d{6}", f"suggestions-cache-{version}", html)
    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(html)


def main() -> None:
    if not os.path.exists(INPUT_FILE):
        print(f"Missing {INPUT_FILE}. Run pipeline to create distinct pairs first.")
        raise SystemExit(1)

    employers = []
    jobs = []
    employer_to_jobs = defaultdict(list)
    job_to_employers = defaultdict(list)

    with open(INPUT_FILE, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if "|" not in line:
                continue
            emp, job = line.split("|", 1)
            emp, job = emp.strip(), job.strip()
            if not emp or not job:
                continue
            employers.append(emp)
            jobs.append(job)
            ek = emp.lower()
            jk = job.lower()
            if len(employer_to_jobs[ek]) < MAX_CONTEXT_PER_KEY:
                employer_to_jobs[ek].append(job)
            if len(job_to_employers[jk]) < MAX_CONTEXT_PER_KEY:
                job_to_employers[jk].append(emp)

    unique_employers = sorted(set(employers), key=str.casefold)
    unique_jobs = sorted(set(jobs), key=str.casefold)

    payload = {
        "employers": unique_employers,
        "jobs": unique_jobs,
        "employerToJobs": dict(employer_to_jobs),
        "jobToEmployers": dict(job_to_employers),
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    version = datetime.now().strftime("%y%m%d")
    update_index_html_version(version)

    size_mb = os.path.getsize(OUTPUT_FILE) / 1024 / 1024
    print(f"Rows: {len(employers):,}")
    print(f"Unique employers: {len(unique_employers):,}")
    print(f"Unique jobs: {len(unique_jobs):,}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Size: {size_mb:.2f} MB")


if __name__ == "__main__":
    main()
