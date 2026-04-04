
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
