
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from coordwatch.io import ensure_parent, write_json
from coordwatch.utils.http import get_json


def download_catalog(url: str, out_path: Path) -> Any:
    payload = get_json(url)
    write_json(out_path, payload)
    return payload


def latest_url(template: str, seriesbreak: str) -> str:
    return template.format(seriesbreak=seriesbreak)


def history_url(keyid: str) -> str:
    """Return the confirmed historical endpoint for a primary dealer keyid."""
    return f"https://markets.newyorkfed.org/api/pd/get/{keyid}.json"


def historical_url_candidates(seriesbreak: str) -> list[str]:
    """Return candidate URLs, with the confirmed /get/ endpoint first."""
    return [
        f"https://markets.newyorkfed.org/api/pd/get/{seriesbreak}.json",
        f"https://markets.newyorkfed.org/api/pd/list/{seriesbreak}.json",
        f"https://markets.newyorkfed.org/api/pd/list/{seriesbreak}/timeseries.json",
    ]


def try_download_json(url: str, out_path: Path) -> tuple[bool, str]:
    try:
        payload = get_json(url)
        write_json(out_path, payload)
        return True, "ok"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def flatten_series_payload(payload: Any) -> pd.DataFrame:
    # The endpoint format is not fully pinned down yet. This flattener handles a
    # few common JSON shapes and otherwise returns an empty DataFrame.
    if isinstance(payload, list):
        return pd.DataFrame(payload)
    if isinstance(payload, dict):
        for key in ["data", "observations", "timeseries", "series", "results"]:
            value = payload.get(key)
            if isinstance(value, list):
                return pd.DataFrame(value)
        # Some payloads may be nested one layer deeper.
        for value in payload.values():
            if isinstance(value, dict):
                nested = flatten_series_payload(value)
                if not nested.empty:
                    return nested
    return pd.DataFrame()
