from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup

from coordwatch.utils.http import download_to_path, get_text
from coordwatch.utils.text import clean_whitespace

QUARTER_TO_END_MONTH = {
    "Q1": "march",
    "Q2": "june",
    "Q3": "september",
    "Q4": "december",
}

REFUNDING_KEYWORDS = [
    "quarterly refunding statement",
    "quarterly refunding",
    "borrowing estimates",
    "borrowing estimate",
    "tbac",
    "minutes",
    "press release",
]

BUYBACK_KEYWORDS = [
    "buyback",
    "buy-back",
    "tentative buyback schedule",
    "results",
]

STATEMENT_SIGNAL_PATTERNS = {
    "tbac_mention_flag": [
        "tbac",
        "treasury borrowing advisory committee",
    ],
    "market_function_mention_flag": [
        "market functioning",
        "well-functioning",
        "well functioning",
        "market liquidity",
        "liquidity conditions",
        "market capacity",
        "regular and predictable",
    ],
    "regular_predictable_mention_flag": [
        "regular and predictable",
    ],
    "bill_flexibility_mention_flag": [
        "bill issuance",
        "bill financing",
        "bill share",
        "short-dated issuance",
        "bills absorb",
        "bills continue to absorb",
        "bills remain elevated",
        "bills can be increased",
        "bills can be decreased",
        "bill sector",
    ],
    "cash_management_mention_flag": [
        "cash balance",
        "cash management",
        "treasury general account",
        "tga",
    ],
    "buyback_mention_flag": BUYBACK_KEYWORDS,
    "coupon_size_mention_flag": [
        "auction sizes",
        "auction size",
        "nominal coupon",
        "coupon auction",
        "coupon-bearing",
        "new issue 10-year",
        "new issue 30-year",
        "reopen the 10-year",
    ],
}


def sanitize_filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name or "index.html"
    if "." not in name:
        name = f"{name}.html"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)


def extract_links(url: str) -> list[dict[str, str]]:
    html = get_text(url)
    soup = BeautifulSoup(html, "lxml")
    out: list[dict[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        text = clean_whitespace(a.get_text(" ", strip=True))
        full_url = urljoin(url, href)
        out.append({"source_page": url, "text": text, "url": full_url})
    return out


def filter_links(links: list[dict[str, str]], keywords: list[str]) -> list[dict[str, str]]:
    out = []
    seen = set()
    for link in links:
        hay = f"{link.get('text', '')} {link.get('url', '')}".lower()
        if any(keyword.lower() in hay for keyword in keywords):
            key = (link["url"], link.get("text", ""))
            if key not in seen:
                seen.add(key)
                out.append(link)
    return out


def download_link_records(records: list[dict[str, str]], out_dir: Path, overwrite: bool = False) -> pd.DataFrame:
    rows = []
    for record in records:
        url = record["url"]
        filename = sanitize_filename_from_url(url)
        local_path = out_dir / filename
        try:
            download_to_path(url, local_path, overwrite=overwrite)
            status = "downloaded"
        except Exception as exc:  # noqa: BLE001
            status = f"error: {exc}"
        rows.append({**record, "local_path": str(local_path), "download_status": status})
    return pd.DataFrame(rows)


def _extract_primary_html_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for node in soup(["script", "style", "noscript"]):
        node.decompose()
    selectors = [
        "main",
        "article",
        '[role="main"]',
        ".main-content",
        ".region-content",
        ".field--name-body",
        ".node__content",
    ]
    best = ""
    for selector in selectors:
        for node in soup.select(selector):
            candidate = clean_whitespace(node.get_text(" ", strip=True))
            if len(candidate) > len(best):
                best = candidate
    if best:
        return best
    return clean_whitespace(soup.get_text(" ", strip=True))


def html_to_text(path: Path) -> str:
    html = path.read_text(encoding="utf-8", errors="ignore")
    return _extract_primary_html_text(html)


def pdf_to_text(path: Path) -> str:
    chunks: list[str] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            chunks.append(page.extract_text() or "")
    return clean_whitespace("\n".join(chunks))


def file_to_text(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        return pdf_to_text(path)
    if suffix in {".html", ".htm"}:
        return html_to_text(path)
    return clean_whitespace(path.read_text(encoding="utf-8", errors="ignore"))


def url_to_text(url: str) -> str:
    html = get_text(url)
    return _extract_primary_html_text(html)


def quarter_end_month(quarter: str | None) -> str | None:
    if not quarter or "Q" not in str(quarter):
        return None
    return QUARTER_TO_END_MONTH.get(str(quarter)[-2:].upper())


def _parse_float_token(token: str) -> float:
    return float(token.replace(",", ""))


def extract_cash_balance_assumption(text: str, quarter: str | None = None) -> float | None:
    cleaned = clean_whitespace(text or "")
    if not cleaned:
        return None

    target_month = quarter_end_month(quarter)
    number = r"([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)"

    paired_patterns = [
        re.compile(
            rf"end[- ]of[- ](march|june|september|december)(?:\s+\d{{4}})?\s+and\s+end[- ]of[- ]"
            rf"(march|june|september|december)(?:\s+\d{{4}})?\s+cash balances? of\s+\$?{number}\s*"
            rf"(?:billion|bn)\s+and\s+\$?{number}\s*(?:billion|bn)",
            flags=re.IGNORECASE,
        ),
        re.compile(
            rf"cash balances? of\s+\$?{number}\s*(?:billion|bn)\s+and\s+\$?{number}\s*(?:billion|bn)"
            rf"[^.{{}}]{{0,120}}end[- ]of[- ](march|june|september|december)(?:\s+\d{{4}})?[^.{{}}]{{0,80}}"
            rf"end[- ]of[- ](march|june|september|december)(?:\s+\d{{4}})?",
            flags=re.IGNORECASE,
        ),
    ]
    for pattern in paired_patterns:
        m = pattern.search(cleaned)
        if not m:
            continue
        groups = list(m.groups())
        if pattern is paired_patterns[0]:
            month1, month2, value1, value2 = groups
        else:
            value1, value2, month1, month2 = groups
        if target_month == month1.lower():
            return _parse_float_token(value1)
        if target_month == month2.lower():
            return _parse_float_token(value2)

    if target_month:
        month_patterns = [
            re.compile(
                rf"end[- ]of[- ]{target_month}(?:\s+\d{{4}})?[^.{{}}]{{0,120}}cash balance(?:s)?(?: of| at| assumption of)?"
                rf"[^$0-9]{{0,40}}\$?{number}\s*(?:billion|bn)",
                flags=re.IGNORECASE,
            ),
            re.compile(
                rf"cash balance(?:s)?(?: of| at| assumption of)?[^.{{}}]{{0,120}}end[- ]of[- ]{target_month}(?:\s+\d{{4}})?"
                rf"[^$0-9]{{0,40}}\$?{number}\s*(?:billion|bn)",
                flags=re.IGNORECASE,
            ),
            re.compile(
                rf"end[- ]of[- ]{target_month}(?:\s+\d{{4}})?[^$0-9]{{0,40}}\$?{number}\s*(?:billion|bn)\s+cash balance",
                flags=re.IGNORECASE,
            ),
        ]
        for pattern in month_patterns:
            m = pattern.search(cleaned)
            if m:
                return _parse_float_token(m.group(1))

    generic_patterns = [
        re.compile(rf"cash balance[^$0-9]{{0,160}}\$?{number}\s*(?:billion|bn)", flags=re.IGNORECASE),
        re.compile(rf"cash balance of[^$0-9]{{0,160}}\$?{number}\s*(?:billion|bn)", flags=re.IGNORECASE),
        re.compile(rf"assumes? an end[- ]of[- ]quarter cash balance of\s+\$?{number}\s*(?:billion|bn)", flags=re.IGNORECASE),
    ]
    for pattern in generic_patterns:
        m = pattern.search(cleaned)
        if m:
            return _parse_float_token(m.group(1))
    return None


def extract_refunding_numeric_hints(text: str, quarter: str | None = None) -> dict[str, float | int | None]:
    lowered = (text or "").lower()
    values = {
        "privately_held_net_marketable_borrowing_bn": None,
        "cash_balance_assumption_bn": extract_cash_balance_assumption(text, quarter=quarter),
        "debt_limit_flag": int("debt limit" in lowered or "debt ceiling" in lowered),
        "soma_explicit_mention_flag": int(
            "soma" in lowered or "federal reserve" in lowered or "reinvestment" in lowered or "redemption" in lowered
        ),
        "bills_shock_absorber_flag": int("shock absorber" in lowered and "bill" in lowered),
    }

    patterns = {
        "privately_held_net_marketable_borrowing_bn": [
            r"privately held net marketable borrowing[^$]{0,120}\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(?:billion|bn)",
            r"net marketable borrowing[^$]{0,120}\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(?:billion|bn)",
        ],
    }

    for key, regexes in patterns.items():
        for pattern in regexes:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                values[key] = float(m.group(1).replace(",", ""))
                break
    return values


def extract_statement_signal_hints(text: str) -> dict[str, float | int]:
    cleaned = clean_whitespace(text or "")
    lowered = cleaned.lower()
    values: dict[str, float | int] = {
        "statement_text_length": len(cleaned),
        "statement_word_count": len(cleaned.split()) if cleaned else 0,
    }
    for key, patterns in STATEMENT_SIGNAL_PATTERNS.items():
        values[key] = int(any(pattern.lower() in lowered for pattern in patterns))
    return values


def cached_statement_text(url: str, cache_dir: Path, source_dir: Path | None = None, overwrite: bool = False) -> tuple[str, Path]:
    filename = sanitize_filename_from_url(url)
    if source_dir is not None:
        source_path = source_dir / filename
        if source_path.exists():
            return file_to_text(source_path), source_path
    cache_path = cache_dir / filename
    download_to_path(url, cache_path, overwrite=overwrite)
    return file_to_text(cache_path), cache_path


def statement_metadata_from_path(path: Path) -> dict[str, str]:
    stem = path.stem.lower()
    title = stem.replace("_", " ").strip()
    return {"statement_title": title}
