
from __future__ import annotations

import os
from io import StringIO
from pathlib import Path

import pandas as pd

from coordwatch.io import write_csv
from coordwatch.utils.http import get


def _try_api_endpoint(series_id: str, out_path: Path) -> pd.DataFrame | None:
    """Try the official FRED API if FRED_API_KEY is set."""
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        return None
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={api_key}"
        f"&file_type=json&observation_start=1950-01-01"
    )
    response = get(url, timeout=90)
    payload = response.json()
    observations = payload.get("observations", [])
    if not observations:
        return None
    rows = [{"DATE": obs["date"], "VALUE": obs["value"]} for obs in observations]
    df = pd.DataFrame(rows)
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df["VALUE"] = pd.to_numeric(df["VALUE"].replace(".", pd.NA), errors="coerce")
    # Write as CSV for compatibility with the rest of the pipeline
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return df


def download_fred_series(series_id: str, base_url: str, out_path: Path) -> pd.DataFrame:
    # Try public CSV endpoint first
    try:
        url = base_url.format(series_id=series_id)
        response = get(url)
        text = response.text
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")
        df = pd.read_csv(out_path)
        rename = {c: c.strip() for c in df.columns}
        df = df.rename(columns=rename)
        if "observation_date" in df.columns and "DATE" not in df.columns:
            df = df.rename(columns={"observation_date": "DATE"})
        if "VALUE" not in df.columns:
            value_cols = [c for c in df.columns if c != "DATE"]
            if len(value_cols) == 1:
                df = df.rename(columns={value_cols[0]: "VALUE"})
        if "DATE" in df.columns and "VALUE" in df.columns:
            df = df[["DATE", "VALUE"]].copy()
        if "DATE" in df.columns:
            df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
        if "VALUE" in df.columns:
            df["VALUE"] = pd.to_numeric(df["VALUE"].replace(".", pd.NA), errors="coerce")
        df.to_csv(out_path, index=False)
        return df
    except Exception:
        # Fall back to FRED API if available
        result = _try_api_endpoint(series_id, out_path)
        if result is not None:
            return result
        raise


def series_to_wide(files: dict[str, Path]) -> pd.DataFrame:
    frames = []
    for series_id, path in files.items():
        if not path.exists():
            continue
        df = pd.read_csv(path)
        if "DATE" not in df.columns or "VALUE" not in df.columns:
            continue
        tmp = df[["DATE", "VALUE"]].copy()
        tmp["DATE"] = pd.to_datetime(tmp["DATE"], errors="coerce")
        tmp[series_id] = pd.to_numeric(tmp["VALUE"].replace(".", pd.NA), errors="coerce")
        frames.append(tmp[["DATE", series_id]])
    if not frames:
        return pd.DataFrame()
    out = frames[0]
    for frame in frames[1:]:
        out = out.merge(frame, on="DATE", how="outer")
    return out.sort_values("DATE").reset_index(drop=True)
