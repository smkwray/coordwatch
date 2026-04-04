#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import argparse

import pandas as pd

from coordwatch.config import load_source_manifest
from coordwatch.io import write_csv
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import RAW_DIR, ensure_repo_dirs
from coordwatch.utils.http import get_json

configure_logging()
logger = get_logger(__name__)


def fetch_paginated(base_url: str, filter_expr: str, page_size: int = 1000) -> pd.DataFrame:
    page = 1
    frames: list[pd.DataFrame] = []
    while True:
        query = urlencode(
            {
                "filter": filter_expr,
                "sort": "record_date",
                "page[number]": page,
                "page[size]": page_size,
            }
        )
        payload = get_json(f"{base_url}?{query}", timeout=90)
        data = payload.get("data", [])
        if not data:
            break
        frames.append(pd.DataFrame(data))
        meta = payload.get("meta", {})
        total_pages = int(meta.get("total-pages", page))
        if page >= total_pages:
            break
        page += 1
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download DTS operating cash balance and debt-to-the-penny daily series")
    parser.add_argument("--start-date", default="2023-01-01")
    args = parser.parse_args()

    ensure_repo_dirs()
    manifest = load_source_manifest()["treasury_optional"]
    out_dir = RAW_DIR / "downloads" / "treasury" / "daily_ops"
    out_dir.mkdir(parents=True, exist_ok=True)

    filter_expr = f"record_date:gte:{args.start_date}"
    datasets = [
        ("operating_cash_balance.csv", manifest["dts_operating_cash_balance_api"]),
        ("debt_to_penny.csv", manifest["debt_to_penny_api"]),
    ]
    for filename, url in datasets:
        df = fetch_paginated(url, filter_expr)
        write_csv(df, out_dir / filename)
        logger.info("Downloaded %s rows -> %s", len(df), out_dir / filename)


if __name__ == "__main__":
    main()
