from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def timestamp_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _sanitize_for_json(obj: Any) -> Any:
    """Replace float NaN/Inf with None for valid JSON."""
    import math
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(v) for v in obj]
    return obj


def write_json(path: Path, obj: Any, indent: int = 2) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8") as f:
        json.dump(_sanitize_for_json(obj), f, indent=indent, default=str)


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_text(path: Path, text: str) -> None:
    ensure_parent(path)
    path.write_text(text, encoding="utf-8")


def read_csv_if_exists(path: Path, **kwargs: Any) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, **kwargs)


def write_csv(df: pd.DataFrame, path: Path, index: bool = False) -> None:
    ensure_parent(path)
    df.to_csv(path, index=index)


def write_parquet(df: pd.DataFrame, path: Path, index: bool = False) -> Path:
    ensure_parent(path)
    try:
        df.to_parquet(path, index=index)
        return path
    except Exception:  # noqa: BLE001
        fallback = path.with_suffix('.csv')
        df.to_csv(fallback, index=index)
        return fallback


def read_table(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.suffix == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported table type: {path}")


def read_best_table(path: Path) -> pd.DataFrame:
    candidates: list[Path]
    if path.suffix:
        candidates = [path]
        if path.suffix == '.parquet':
            candidates.append(path.with_suffix('.csv'))
        elif path.suffix == '.csv':
            candidates.append(path.with_suffix('.parquet'))
    else:
        candidates = [path.with_suffix('.parquet'), path.with_suffix('.csv')]
    for cand in candidates:
        if not cand.exists():
            continue
        try:
            return read_table(cand)
        except Exception:  # noqa: BLE001
            continue
    raise FileNotFoundError(f"Could not find readable table for {path}")


def write_status(path: Path, status: str, **payload: Any) -> None:
    obj = {"status": status, "created_at_utc": timestamp_utc(), **payload}
    write_json(path, obj)
