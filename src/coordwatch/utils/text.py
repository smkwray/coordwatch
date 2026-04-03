
from __future__ import annotations

import re
from typing import Iterable

MONEY_RE = re.compile(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(?:billion|bn)", re.IGNORECASE)


def clean_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def parse_first_billion_amount(text: str) -> float | None:
    if not text:
        return None
    m = MONEY_RE.search(text)
    if not m:
        return None
    return float(m.group(1).replace(",", ""))


def extract_all_billion_amounts(text: str) -> list[float]:
    return [float(m.replace(",", "")) for m in MONEY_RE.findall(text or "")]


def contains_any(text: str, patterns: Iterable[str]) -> bool:
    lowered = (text or "").lower()
    return any(p.lower() in lowered for p in patterns)
