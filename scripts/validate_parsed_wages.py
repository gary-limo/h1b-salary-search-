#!/usr/bin/env python3
"""
After data_parsing.py: verify wage-related columns in parsed_output.csv.
Uses the same float parsing as create_db (wage_parse.parse_wage_float).

Rules:
  - WAGE_RATE_OF_PAY_FROM: required, must parse to a finite number > 0
  - PREVAILING_WAGE: required, must parse to a finite number > 0
  - WAGE_RATE_OF_PAY_TO: optional if blank; if non-blank must parse to finite number > 0
  - PW_WAGE_LEVEL: reported if blank (warning only; DOL sometimes omits)

Exit 1 if any hard rule fails (pipeline should stop).
"""

from __future__ import annotations

import csv
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from wage_parse import parse_wage_float

CSV_PATH = os.path.join(PROJECT_DIR, "parsed_output.csv")
MAX_SAMPLES = 35

# ANSI (bold red / yellow / cyan / reset)
R = "\033[1;31m"
Y = "\033[1;33m"
C = "\033[1;36m"
G = "\033[1;32m"
X = "\033[0m"


def is_tty():
    return sys.stdout.isatty()


def paint(s: str, color: str) -> str:
    if not is_tty():
        return s
    return f"{color}{s}{X}"


def finite_positive(n: float | None) -> bool:
    return n is not None and n == n and n > 0 and abs(n) != float("inf")


def main() -> int:
    if not os.path.isfile(CSV_PATH):
        print(paint(f"ERROR: {CSV_PATH} not found.", R), file=sys.stderr)
        return 1

    bad_from: list[tuple[str, str, str, str]] = []  # case, reason, raw, employer
    bad_prev: list[tuple[str, str, str, str]] = []
    bad_to: list[tuple[str, str, str, str]] = []
    pw_blank: list[tuple[str, str]] = []

    total = 0
    with open(CSV_PATH, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        required = {"CASE_NUMBER", "WAGE_RATE_OF_PAY_FROM", "PREVAILING_WAGE"}
        if not required.issubset(set(reader.fieldnames or [])):
            print(
                paint(f"ERROR: CSV missing columns. Need {sorted(required)}.", R),
                file=sys.stderr,
            )
            return 1

        for row in reader:
            total += 1
            cn = (row.get("CASE_NUMBER") or "").strip() or "(no case_number)"
            emp = (row.get("EMPLOYER_NAME") or "").strip()[:48]

            raw_from = row.get("WAGE_RATE_OF_PAY_FROM")
            raw_to = row.get("WAGE_RATE_OF_PAY_TO")
            raw_pw = row.get("PREVAILING_WAGE")
            raw_pwl = (row.get("PW_WAGE_LEVEL") or "").strip()

            fstrip = (raw_from or "").strip()
            if not fstrip:
                bad_from.append((cn, "blank", "", emp))
            else:
                nf = parse_wage_float(raw_from)
                if not finite_positive(nf):
                    bad_from.append((cn, "unparsed_or_non_positive", fstrip, emp))

            pwstrip = (raw_pw or "").strip()
            if not pwstrip:
                bad_prev.append((cn, "blank", "", emp))
            else:
                npw = parse_wage_float(raw_pw)
                if not finite_positive(npw):
                    bad_prev.append((cn, "unparsed_or_non_positive", pwstrip, emp))

            tostrip = (raw_to or "").strip()
            if tostrip:
                nt = parse_wage_float(raw_to)
                if not finite_positive(nt):
                    bad_to.append((cn, "unparsed_or_non_positive", tostrip, emp))

            if not raw_pwl:
                pw_blank.append((cn, emp))

    # ── Banner output (pipeline visibility) ─────────────────────────
    line = "█" * 72
    print("")
    print(paint(line, C))
    print(paint("  DATA QUALITY — WAGE FIELDS (parsed_output.csv)", C))
    print(paint(line, C))
    print(f"  Total data rows scanned: {total:,}")
    print(f"  {paint('WAGE_RATE_OF_PAY_FROM', Y)} invalid: {len(bad_from):,}")
    print(f"  {paint('PREVAILING_WAGE', Y)} invalid: {len(bad_prev):,}")
    print(f"  {paint('WAGE_RATE_OF_PAY_TO', Y)} invalid (non-empty only): {len(bad_to):,}")
    print(f"  {paint('PW_WAGE_LEVEL', Y)} blank (informational; not a failure): {len(pw_blank):,}")
    print(paint(line, C))

    def dump_samples(label: str, rows: list[tuple], show_raw: bool):
        if not rows:
            return
        print(paint(f"\n  ▶ {label} — sample identifiers (case_number | employer | detail)", R if "invalid" in label else Y))
        for tup in rows[:MAX_SAMPLES]:
            if len(tup) >= 4 and show_raw:
                cn, reason, raw, em = tup[:4]
                raw_disp = f" raw={raw!r}" if raw else ""
                print(f"      • {cn} | {em}{raw_disp} ({reason})")
            elif len(tup) >= 2:
                cn, em = tup[0], tup[1]
                print(f"      • {cn} | {em}")
        if len(rows) > MAX_SAMPLES:
            print(paint(f"      … and {len(rows) - MAX_SAMPLES:,} more", Y))

    dump_samples("WAGE_RATE_OF_PAY_FROM invalid", bad_from, True)
    dump_samples("PREVAILING_WAGE invalid", bad_prev, True)
    dump_samples("WAGE_RATE_OF_PAY_TO invalid", bad_to, True)
    if pw_blank and len(pw_blank) <= 15:
        dump_samples("PW_WAGE_LEVEL blank (informational)", pw_blank, False)
    elif pw_blank:
        print(
            paint(
                f"\n  (PW_WAGE_LEVEL blank: {len(pw_blank):,} rows — omitting sample list; common in source LCA data.)",
                Y,
            )
        )

    hard_fail = len(bad_from) + len(bad_prev) + len(bad_to)
    print("")
    if hard_fail:
        print(
            paint(
                f"  ✗ FAILED: {hard_fail:,} row(s) failed wage validation — fix parsing or source data before D1 load.",
                R,
            )
        )
        print(paint(line, R))
        print("")
        return 1

    print(
        paint(
            "  ✓ OK: required wage fields (FROM, PREVAILING) are present and parse as positive numbers;"
            " TO is either blank or valid.",
            G,
        )
    )
    if pw_blank:
        print(
            paint(
                f"  Note: {len(pw_blank):,} row(s) have blank PW_WAGE_LEVEL (DOL often omits; does not fail this check).",
                Y,
            )
        )
    print(paint(line, C))
    print("")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
