#!/usr/bin/env python3
"""
Increment SEARCH_CACHE_VERSION in wrangler.jsonc so /api/search KV + edge keys change.

New keys are unused until you deploy the Worker (vars are baked at deploy).
Run from repo root, typically via run_pipeline.sh --prod.
"""

import re
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
WRANGLER = PROJECT_DIR / "wrangler.jsonc"


def main() -> int:
    if not WRANGLER.is_file():
        print(f"Missing {WRANGLER}", file=sys.stderr)
        return 1
    text = WRANGLER.read_text(encoding="utf-8")
    pattern = re.compile(r'("SEARCH_CACHE_VERSION"\s*:\s*")(\d+)(")')

    def repl(m: re.Match) -> str:
        n = int(m.group(2))
        return f"{m.group(1)}{n + 1}{m.group(3)}"

    new_text, n = pattern.subn(repl, text, count=1)
    if n != 1:
        print("Could not find SEARCH_CACHE_VERSION numeric entry in wrangler.jsonc", file=sys.stderr)
        return 1
    WRANGLER.write_text(new_text, encoding="utf-8")
    m = pattern.search(new_text)
    ver = m.group(2) if m else "?"
    print(f"SEARCH_CACHE_VERSION is now {ver} in wrangler.jsonc")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
