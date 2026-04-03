
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from coordwatch.io import read_csv_if_exists, write_csv, write_parquet
from coordwatch.logging_utils import get_logger
from coordwatch.paths import INTERIM_DIR, MANUAL_DIR, PROCESSED_DIR, RAW_DIR, REFERENCE_DIR, ensure_repo_dirs

LOGGER = get_logger(__name__)

COUPON_DURATION_MAP = {
    "delta_2y_bn": 1.9,
    "delta_3y_bn": 2.8,
    "delta_5y_bn": 4.5,
    "delta_7y_bn": 6.1,
    "delta_10y_bn": 8.5,
    "delta_20y_bn": 14.0,
    "delta_30y_bn": 18.0,
    "delta_frn_bn": 0.25,
}


def _coerce_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def manual_overrides_df() -> pd.DataFrame:
    path = MANUAL_DIR / "refunding_manual_overrides.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if df.empty:
        return df
    numeric_cols = [c for c in df.columns if c.endswith("_bn") or c.endswith("_flag") or c.endswith("_override")]
    return _coerce_numeric(df, numeric_cols)


def extracted_refunding_df() -> pd.DataFrame:
    candidates = [
        INTERIM_DIR / "refunding_statement_extracts.csv",
        INTERIM_DIR / "refunding_statement_extracts.parquet",
    ]
    for path in candidates:
        if path.exists():
            if path.suffix == ".csv":
                df = pd.read_csv(path)
            else:
                df = pd.read_parquet(path)
            return df
    return pd.DataFrame()


def demo_refunding_df() -> pd.DataFrame:
    candidates = [
        RAW_DIR / "demo" / "treasury" / "refunding_panel_demo.parquet",
        RAW_DIR / "demo" / "treasury" / "refunding_panel_demo.csv",
    ]
    for path in candidates:
        if path.exists():
            if path.suffix == ".parquet":
                return pd.read_parquet(path)
            return pd.read_csv(path)
    return pd.DataFrame()


def compute_coupon_dv01_from_deltas(df: pd.DataFrame) -> pd.Series:
    total = pd.Series(0.0, index=df.index)
    for col, duration in COUPON_DURATION_MAP.items():
        if col in df.columns:
            total = total.add(pd.to_numeric(df[col], errors="coerce").fillna(0) * duration, fill_value=0)
    return total


def _prep_real_refunding_base(extracted: pd.DataFrame, manual: pd.DataFrame) -> pd.DataFrame:
    # Manual overrides are the authoritative numeric layer.
    if manual.empty and extracted.empty:
        return pd.DataFrame()

    if manual.empty:
        base = extracted.copy()
    elif extracted.empty:
        base = manual.copy()
    else:
        if "quarter" in extracted.columns:
            key = "quarter"
        elif "refunding_date" in extracted.columns:
            key = "refunding_date"
        else:
            key = None
        if key is None:
            base = manual.copy()
        else:
            base = extracted.merge(manual, on=key, how="outer", suffixes=("", "_manual"))

    if "quarter" not in base.columns and "refunding_date" in base.columns:
        base["refunding_date"] = pd.to_datetime(base["refunding_date"], errors="coerce")
        base["quarter"] = base["refunding_date"].dt.to_period("Q").astype(str)

    # Prefer manual columns where available.
    for col in [
        "statement_url",
        "statement_title",
        "privately_held_net_marketable_borrowing_bn",
        "cash_balance_assumption_bn",
        "debt_limit_flag",
        "delta_2y_bn",
        "delta_3y_bn",
        "delta_5y_bn",
        "delta_7y_bn",
        "delta_10y_bn",
        "delta_20y_bn",
        "delta_30y_bn",
        "delta_frn_bn",
        "soma_explicit_mention_flag",
        "bills_shock_absorber_flag",
        "clean_sample_flag",
        "classification_prior",
        "verification_status",
        "reviewer_notes",
    ]:
        manual_col = f"{col}_manual"
        if manual_col in base.columns:
            base[col] = base[manual_col].combine_first(base.get(col))

    base["refunding_date"] = pd.to_datetime(base["refunding_date"], errors="coerce")
    base = base.sort_values(["refunding_date", "quarter"]).drop_duplicates(subset=["quarter"], keep="last")
    base["coupon_dv01_shock"] = pd.to_numeric(base.get("coupon_dv01_override"), errors="coerce")
    fallback_coupon = compute_coupon_dv01_from_deltas(base)
    base["coupon_dv01_shock"] = base["coupon_dv01_shock"].combine_first(fallback_coupon)
    base["bill_dv01_offset"] = pd.to_numeric(base.get("bill_dv01_offset_override"), errors="coerce").fillna(0)
    base["buyback_offset_dv01"] = pd.to_numeric(base.get("buyback_offset_dv01_override"), errors="coerce").fillna(0)
    base["expected_soma_redemptions_dv01"] = pd.to_numeric(base.get("expected_soma_redemptions_dv01_override"), errors="coerce").fillna(0)
    base["mix_shock_dv01"] = (base["coupon_dv01_shock"].fillna(0) - base["bill_dv01_offset"].fillna(0)).round(2)
    base["clean_sample_flag"] = pd.to_numeric(base.get("clean_sample_flag"), errors="coerce").fillna(1).astype(int)
    base["debt_limit_flag"] = pd.to_numeric(base.get("debt_limit_flag"), errors="coerce").fillna(0).astype(int)
    base["classification_prior"] = base.get("classification_prior", pd.Series(index=base.index, dtype=object)).fillna("review_required")
    return base


def build_refunding_panel(prefer_real: bool = True, output_dir: Path | None = None) -> pd.DataFrame:
    ensure_repo_dirs()
    out_dir = output_dir or PROCESSED_DIR
    extracted = extracted_refunding_df()
    manual = manual_overrides_df()
    panel = pd.DataFrame()
    source = ""

    if prefer_real:
        panel = _prep_real_refunding_base(extracted, manual)
        if not panel.empty:
            source = "real_or_manual"

    if panel.empty:
        panel = demo_refunding_df()
        source = "demo"

    if panel.empty:
        raise FileNotFoundError("No refunding panel inputs found. Run demo seed or populate manual overrides.")

    panel["refunding_date"] = pd.to_datetime(panel["refunding_date"], errors="coerce")
    panel = panel.sort_values("refunding_date").reset_index(drop=True)

    if "low_liquidity_prev" not in panel.columns:
        threshold = np.nanpercentile(panel.get("system_liquidity_q_bn", pd.Series([0] * len(panel))), 35) if "system_liquidity_q_bn" in panel.columns else 0
        low_liq = (panel.get("system_liquidity_q_bn", pd.Series([threshold + 1] * len(panel))) <= threshold).astype(int)
        panel["low_liquidity_prev"] = low_liq.shift(1).fillna(0).astype(int)

    panel["expected_soma_redemptions_x_low_liquidity"] = (
        pd.to_numeric(panel["expected_soma_redemptions_dv01"], errors="coerce").fillna(0)
        * pd.to_numeric(panel["low_liquidity_prev"], errors="coerce").fillna(0)
    ).round(2)

    output_cols = [
        "quarter",
        "refunding_date",
        "statement_url",
        "statement_title",
        "privately_held_net_marketable_borrowing_bn",
        "cash_balance_assumption_bn",
        "debt_limit_flag",
        "coupon_dv01_shock",
        "bill_dv01_offset",
        "buyback_offset_dv01",
        "mix_shock_dv01",
        "expected_soma_redemptions_dv01",
        "low_liquidity_prev",
        "expected_soma_redemptions_x_low_liquidity",
        "classification_prior",
        "clean_sample_flag",
    ]
    for col in output_cols:
        if col not in panel.columns:
            panel[col] = np.nan
    panel = panel[output_cols + [c for c in panel.columns if c not in output_cols]]
    panel["panel_source"] = source

    write_csv(panel, out_dir / "refunding_panel.csv")
    write_parquet(panel, out_dir / "refunding_panel.parquet")
    LOGGER.info("Built refunding panel with %s rows from %s", len(panel), source)
    return panel
