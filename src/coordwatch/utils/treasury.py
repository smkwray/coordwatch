from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup

from coordwatch.utils.http import download_to_path, get_text
from coordwatch.utils.text import clean_whitespace

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


def html_to_text(path: Path) -> str:
    html = path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "lxml")
    return clean_whitespace(soup.get_text(" ", strip=True))


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


def extract_refunding_numeric_hints(text: str) -> dict[str, float | int | None]:
    lowered = (text or "").lower()
    values = {
        "privately_held_net_marketable_borrowing_bn": None,
        "cash_balance_assumption_bn": None,
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
        "cash_balance_assumption_bn": [
            r"cash balance[^$]{0,80}\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(?:billion|bn)",
            r"cash balance of[^$]{0,80}\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s*(?:billion|bn)",
        ],
    }

    for key, regexes in patterns.items():
        for pattern in regexes:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                values[key] = float(m.group(1).replace(",", ""))
                break
    return values


def statement_metadata_from_path(path: Path) -> dict[str, str]:
    stem = path.stem.lower()
    title = stem.replace("_", " ").strip()
    return {"statement_title": title}
