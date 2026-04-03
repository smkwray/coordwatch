from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from coordwatch.io import load_json_if_exists, write_json
from coordwatch.logging_utils import get_logger
from coordwatch.paths import RAW_DIR
from coordwatch.utils.http import get_json

LOGGER = get_logger(__name__)

SOMA_HOLDINGS_URL = "https://markets.newyorkfed.org/read"
TREASURY_HOLDING_TYPES = "bills,notesbonds,frn,tips"
DEFAULT_SOMA_CACHE_DIR = RAW_DIR / "downloads" / "nyfed" / "soma" / "holdings"


def soma_holdings_detail_url(as_of_date: pd.Timestamp | date | str) -> str:
    as_of = pd.Timestamp(as_of_date).strftime("%Y-%m-%d")
    return (
        f"{SOMA_HOLDINGS_URL}?productCode=30&startDt={as_of}&endDt={as_of}"
        f"&query=details&holdingTypes={TREASURY_HOLDING_TYPES}&format=json"
    )


def soma_holdings_cache_path(
    as_of_date: pd.Timestamp | date | str,
    cache_dir: Path | None = None,
) -> Path:
    as_of = pd.Timestamp(as_of_date).strftime("%Y-%m-%d")
    return (cache_dir or DEFAULT_SOMA_CACHE_DIR) / f"{as_of}.json"


def fetch_soma_holdings_payload(
    as_of_date: pd.Timestamp | date | str,
    cache_dir: Path | None = None,
    refresh: bool = False,
) -> dict:
    cache_path = soma_holdings_cache_path(as_of_date, cache_dir)
    if cache_path.exists() and not refresh:
        payload = load_json_if_exists(cache_path)
        if isinstance(payload, dict):
            return payload
    payload = get_json(soma_holdings_detail_url(as_of_date), timeout=60)
    write_json(cache_path, payload)
    return payload


def holdings_frame_from_payload(payload: dict) -> pd.DataFrame:
    holdings = payload.get("soma", {}).get("holdings", [])
    if not holdings:
        return pd.DataFrame()
    df = pd.DataFrame(holdings)
    numeric_cols = ["coupon", "parValue", "inflationCompensation", "changeFromPriorWeek"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].replace("", np.nan), errors="coerce")
    for col in ["asOfDate", "maturityDate"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def load_soma_holdings_frame(
    as_of_date: pd.Timestamp | date | str,
    cache_dir: Path | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    payload = fetch_soma_holdings_payload(as_of_date, cache_dir=cache_dir, refresh=refresh)
    return holdings_frame_from_payload(payload)


def prefetch_soma_holdings(
    as_of_dates: Iterable[pd.Timestamp | date | str],
    cache_dir: Path | None = None,
    refresh: bool = False,
    max_workers: int = 4,
) -> dict[str, int]:
    dates = sorted({pd.Timestamp(dt).strftime("%Y-%m-%d") for dt in as_of_dates if pd.notna(dt)})
    if not dates:
        return {"requested": 0, "downloaded": 0, "failed": 0}

    cache_root = cache_dir or DEFAULT_SOMA_CACHE_DIR
    downloaded = 0
    failed = 0
    to_fetch = [dt for dt in dates if refresh or not soma_holdings_cache_path(dt, cache_root).exists()]
    if not to_fetch:
        return {"requested": len(dates), "downloaded": 0, "failed": 0}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(fetch_soma_holdings_payload, dt, cache_root, refresh): dt
            for dt in to_fetch
        }
        for future in as_completed(futures):
            dt = futures[future]
            try:
                future.result()
                downloaded += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                LOGGER.warning("Failed to download SOMA holdings for %s: %s", dt, exc)

    return {"requested": len(dates), "downloaded": downloaded, "failed": failed}


def _interpolate_curve_yield(remaining_years: float, curve: dict[str, float]) -> float:
    points = [
        (2.0, curve.get("DGS2")),
        (5.0, curve.get("DGS5")),
        (10.0, curve.get("DGS10")),
        (20.0, curve.get("DGS20")),
        (30.0, curve.get("DGS30")),
    ]
    clean = [(term, float(rate) / 100.0) for term, rate in points if rate is not None and pd.notna(rate)]
    if not clean:
        return 0.04
    if remaining_years <= clean[0][0]:
        return clean[0][1]
    if remaining_years >= clean[-1][0]:
        return clean[-1][1]
    for (x0, y0), (x1, y1) in zip(clean[:-1], clean[1:]):
        if x0 <= remaining_years <= x1:
            weight = (remaining_years - x0) / (x1 - x0)
            return y0 + weight * (y1 - y0)
    return clean[-1][1]


def _coupon_cashflow_duration(years: float, coupon_rate: float, yield_rate: float) -> float:
    if years <= 0:
        return 0.0
    freq = 2
    periods = max(int(np.ceil(years * freq)), 1)
    per_yield = yield_rate / freq
    per_coupon = coupon_rate / freq
    times = np.arange(1, periods + 1) / freq
    cashflows = np.full(periods, per_coupon)
    cashflows[-1] += 1.0
    discounts = (1.0 + per_yield) ** np.arange(1, periods + 1)
    pv = cashflows / discounts
    price = float(pv.sum())
    if price <= 0:
        return max(years, 0.0)
    macaulay = float((times * pv).sum() / price)
    return macaulay / max(1.0 + per_yield, 1e-9)


def estimate_modified_duration(
    remaining_years: float,
    coupon_rate: float,
    security_type: str,
    curve: dict[str, float],
) -> float:
    years = max(float(remaining_years), 0.0)
    sec = (security_type or "").strip().lower()
    if years <= 0:
        return 0.0
    if "frn" in sec:
        return 0.25
    if "bill" in sec:
        yld = _interpolate_curve_yield(max(years, 0.25), curve)
        return years / max(1.0 + yld, 1e-9)
    yld = _interpolate_curve_yield(max(years, 0.25), curve)
    return _coupon_cashflow_duration(years, max(float(coupon_rate), 0.0) / 100.0, yld)


def estimate_runoff_duration_equivalent(
    prior_holdings: pd.DataFrame,
    current_holdings: pd.DataFrame,
    curve: dict[str, float],
    as_of_date: pd.Timestamp | date | str,
) -> float:
    if prior_holdings.empty:
        return 0.0

    as_of = pd.Timestamp(as_of_date)
    prior = prior_holdings.copy()
    current = current_holdings.copy()
    for frame in [prior, current]:
        for col in ["parValue", "inflationCompensation", "coupon"]:
            if col in frame.columns:
                frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0)
        if "maturityDate" in frame.columns:
            frame["maturityDate"] = pd.to_datetime(frame["maturityDate"], errors="coerce")
    prior["principal_bn"] = (prior.get("parValue", 0) + prior.get("inflationCompensation", 0)) / 1e9
    current["principal_bn"] = (current.get("parValue", 0) + current.get("inflationCompensation", 0)) / 1e9

    prior = prior.dropna(subset=["cusip"]).drop_duplicates(subset=["cusip"], keep="last")
    current = current.dropna(subset=["cusip"]).drop_duplicates(subset=["cusip"], keep="last")
    work = prior.merge(
        current[["cusip", "principal_bn"]],
        on="cusip",
        how="left",
        suffixes=("_prior", "_current"),
    )
    work["principal_bn_current"] = pd.to_numeric(work["principal_bn_current"], errors="coerce").fillna(0)
    work["runoff_bn"] = (work["principal_bn_prior"] - work["principal_bn_current"]).clip(lower=0)
    if float(work["runoff_bn"].sum()) <= 0:
        return 0.0

    work["duration_years"] = [
        estimate_modified_duration(years, coupon, sec_type, curve)
        for years, coupon, sec_type in zip(
            ((pd.to_datetime(work["maturityDate"], errors="coerce") - as_of).dt.days / 365.25).clip(lower=0),
            pd.to_numeric(work.get("coupon"), errors="coerce").fillna(0),
            work.get("securityType", pd.Series("", index=work.index)),
        )
    ]
    work["runoff_duration_equiv"] = work["runoff_bn"] * work["duration_years"]
    return round(float(work["runoff_duration_equiv"].sum()), 3)
