"""Normalize `application_difficulty_score` to an integer 1–5 for PostgreSQL.

Keeps a text→score map for legacy LLM strings; new prompts should return JSON integers only.
"""
from __future__ import annotations

import math
import re
from typing import Any, Optional

_SCORE_FROM_TEXT: dict[str, int] = {}
for _labels, _n in (
    (("very low",), 1),
    (("low", "low selectivity"), 2),
    (("low-moderate", "low moderate", "low-medium", "low medium"), 2),
    (("moderate", "competitive"), 3),
    (
        (
            "moderate-high",
            "moderate high",
            "moderately high",
            "selective",
            "high",
        ),
        4,
    ),
    (("very high", "extremely high"), 5),
):
    for _lab in _labels:
        _SCORE_FROM_TEXT[_lab.lower()] = _n

_FRAC_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*/\s*5\s*$", re.IGNORECASE)
_INT_RE = re.compile(r"^\s*(\d+)\s*$")


def normalize_application_difficulty(raw: Any) -> Optional[int]:
    """Return 1–5, or None if unknown / null."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        if 1 <= raw <= 5:
            return raw
        if raw < 1:
            return 1
        if raw > 5:
            return 5
        return None
    if isinstance(raw, float):
        if raw != raw:  # NaN
            return None
        r = int(math.floor(float(raw) + 0.5))
        return max(1, min(5, r))
    s = str(raw).strip()
    if not s:
        return None

    m = _INT_RE.match(s)
    if m:
        return normalize_application_difficulty(int(m.group(1)))

    m = _FRAC_RE.match(s)
    if m:
        x = float(m.group(1))
        r = int(math.floor(x + 0.5))
        return max(1, min(5, r))

    low = s.lower().strip()
    if low in _SCORE_FROM_TEXT:
        return _SCORE_FROM_TEXT[low]
    # Longest key first so e.g. "very high" beats "high"
    for key, val in sorted(_SCORE_FROM_TEXT.items(), key=lambda kv: -len(kv[0])):
        if key in low:
            return val
    return None
