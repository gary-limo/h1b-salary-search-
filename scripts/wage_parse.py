"""Shared wage string → float parsing (CSV → SQL). Used by create_db and validate_parsed_wages."""

from __future__ import annotations

import re
from typing import Optional


def parse_wage_float(raw_value) -> Optional[float]:
    """Parse wage strings for SQL REAL. Handles commas; legacy CSV may use '144354 22' for decimals."""
    if raw_value is None:
        return None
    s = str(raw_value).strip()
    if not s:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        pass
    m = re.fullmatch(r"(\d+)\s+(\d{1,2})\s*", s)
    if m:
        try:
            return float(f"{m.group(1)}.{m.group(2)}")
        except ValueError:
            return None
    try:
        return float(re.sub(r"\s+", "", s))
    except ValueError:
        return None
