
from __future__ import annotations

import numpy as np
import pandas as pd

from coordwatch.config import load_model_specs


DEFAULT_LIQUIDITY_STATE_QUANTILE = 0.35


def liquidity_state_quantile() -> float:
    specs = load_model_specs()
    return float(specs.get("liquidity_state_quantile", DEFAULT_LIQUIDITY_STATE_QUANTILE))


def add_liquidity_state(
    df: pd.DataFrame,
    liquidity_col: str = "system_liquidity_bn",
    quantile: float | None = None,
) -> pd.DataFrame:
    out = df.copy()
    quantile = liquidity_state_quantile() if quantile is None else float(quantile)
    threshold = float(out[liquidity_col].quantile(quantile))
    out["low_liquidity"] = (out[liquidity_col] <= threshold).astype(int)
    out["low_liquidity_prev"] = out["low_liquidity"].shift(1).fillna(0).astype(int)
    return out


def add_liquidity_tightness_zscore(
    df: pd.DataFrame,
    liquidity_col: str = "system_liquidity_bn",
    output_col: str = "liquidity_tightness_z",
) -> pd.DataFrame:
    out = df.copy()
    values = pd.to_numeric(out[liquidity_col], errors="coerce")
    mean = float(values.mean()) if values.notna().any() else 0.0
    std = float(values.std(ddof=0)) if values.notna().any() else 0.0
    if std == 0 or np.isnan(std):
        out[output_col] = 0.0
        return out
    out[output_col] = (-(values - mean) / std).round(4)
    return out


def add_qt2_liquidity_state(
    df: pd.DataFrame,
    liquidity_col: str = "system_liquidity_bn",
    qt2_start: str = "2022-06-01",
) -> pd.DataFrame:
    """Add a QT2-specific low-liquidity flag using the QT2-subsample median."""
    out = df.copy()
    week_col = out["week"] if "week" in out.columns else out.index
    qt2_mask = pd.to_datetime(week_col, errors="coerce") >= pd.Timestamp(qt2_start)
    qt2_vals = out.loc[qt2_mask, liquidity_col].dropna()
    if qt2_vals.empty:
        out["qt2_low_liquidity"] = np.nan
        return out
    qt2_median = float(qt2_vals.median())
    out["qt2_low_liquidity"] = np.where(
        qt2_mask & out[liquidity_col].notna(),
        (out[liquidity_col] <= qt2_median).astype(int),
        np.nan,
    )
    return out


def add_repo_spreads(df: pd.DataFrame, tgcr_col: str = "tgcr_rate", on_rrp_col: str = "on_rrp_award_rate", iorb_col: str = "iorb_rate") -> pd.DataFrame:
    out = df.copy()
    if tgcr_col in out.columns and on_rrp_col in out.columns:
        out["repo_spread_bp"] = ((out[tgcr_col] - out[on_rrp_col]) * 100).round(2)
    if tgcr_col in out.columns and iorb_col in out.columns:
        out["repo_spread_iorb_bp"] = ((out[tgcr_col] - out[iorb_col]) * 100).round(2)
    return out


def compute_qt_runoff_proxy(soma_treasuries_bn: pd.Series) -> pd.Series:
    delta = -soma_treasuries_bn.diff().fillna(0)
    return delta.round(2)
