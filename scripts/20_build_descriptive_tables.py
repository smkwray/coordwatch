#!/usr/bin/env python
"""Build descriptive summary tables answering the 7 institutional questions.

Produces:
  - outputs/tables/regime_summary.csv: weekly panel averages by Fed regime
  - outputs/tables/episode_summary.csv: episode-level averages from expanded registry
  - outputs/tables/qt_comparison_summary.csv: side-by-side QT1 vs QT2 comparison
  - outputs/tables/quarterly_descriptive.csv: quarterly panel with net private duration supply
  - outputs/tables/correlation_matrix.csv: key variable correlations
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import numpy as np
import pandas as pd

from coordwatch.io import write_csv
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import MANUAL_DIR, PROCESSED_DIR, ensure_repo_dirs

configure_logging()
logger = get_logger(__name__)

TABLES_DIR = ROOT / "outputs" / "tables"


def assign_fed_regime(week: pd.Timestamp) -> str:
    """Assign Fed balance-sheet regime based on policy dates."""
    if week < pd.Timestamp("2008-11-25"):
        return "pre_qe"
    if week < pd.Timestamp("2014-10-29"):
        return "qe_expansion"
    if week < pd.Timestamp("2017-10-01"):
        return "reinvestment"
    if week < pd.Timestamp("2019-08-01"):
        return "qt1"
    if week < pd.Timestamp("2022-06-01"):
        return "qe_covid"
    if week < pd.Timestamp("2025-04-01"):
        return "qt2"
    return "qt2_taper"


def build_regime_summary(weekly: pd.DataFrame) -> pd.DataFrame:
    """Q1: Regime-level averages for all key variables."""
    weekly = weekly.copy()
    weekly["fed_regime"] = weekly["week"].apply(assign_fed_regime)

    agg_cols = {
        "soma_treasuries_bn": "mean",
        "reserves_bn": "mean",
        "tga_bn": "mean",
        "on_rrp_bn": "mean",
        "dealer_inventory_bn": "mean",
        "repo_spread_bp": "mean",
        "system_liquidity_bn": "mean",
        "qt_runoff_dv01": "mean",
    }
    # Only aggregate columns that exist and have data
    agg_cols = {k: v for k, v in agg_cols.items() if k in weekly.columns}

    summary = weekly.groupby("fed_regime").agg(
        n_weeks=("week", "count"),
        date_start=("week", "min"),
        date_end=("week", "max"),
        **{f"{k}_{v}": (k, v) for k, v in agg_cols.items()},
    )
    # Round numeric columns only
    num_cols = summary.select_dtypes(include="number").columns
    summary[num_cols] = summary[num_cols].round(2)

    # Order by date, ensure fed_regime is a column not index
    summary = summary.sort_values("date_start").reset_index()
    return summary


def build_episode_summary(weekly: pd.DataFrame) -> pd.DataFrame:
    """Episode-level averages from the expanded registry."""
    seed = pd.read_csv(MANUAL_DIR / "episode_registry_seed.csv")
    rows = []
    for _, ep in seed.iterrows():
        start = pd.Timestamp(ep["window_start"])
        end = pd.Timestamp(ep["window_end"])
        mask = (weekly["week"] >= start) & (weekly["week"] <= end)
        sub = weekly[mask]
        if sub.empty:
            continue
        row = {
            "episode_id": ep["episode_id"],
            "episode_name": ep["episode_name"],
            "window_start": str(start.date()),
            "window_end": str(end.date()),
            "n_weeks": len(sub),
            "soma_treasuries_bn": sub["soma_treasuries_bn"].mean(),
            "soma_change_bn": float(sub["soma_treasuries_bn"].dropna().iloc[-1] - sub["soma_treasuries_bn"].dropna().iloc[0]) if sub["soma_treasuries_bn"].notna().sum() > 1 else np.nan,
            "reserves_bn": sub["reserves_bn"].mean(),
            "tga_bn": sub["tga_bn"].mean(),
            "on_rrp_bn": sub["on_rrp_bn"].mean(),
            "dealer_inventory_bn": sub["dealer_inventory_bn"].mean(),
            "repo_spread_bp": sub["repo_spread_bp"].mean(),
            "system_liquidity_bn": sub["system_liquidity_bn"].mean() if "system_liquidity_bn" in sub.columns else np.nan,
        }
        rows.append(row)
    return pd.DataFrame(rows).round(2)


def build_quarterly_descriptive(quarter: pd.DataFrame, weekly: pd.DataFrame) -> pd.DataFrame:
    """Quarterly panel with net private duration supply and regime labels."""
    q = quarter.copy()
    # Net private duration supply = coupon DV01 + SOMA runoff DV01 - buyback DV01
    q["net_private_duration_dv01"] = (
        q["coupon_dv01_shock"].fillna(0)
        + q["expected_soma_redemptions_dv01"].fillna(0)
        - q["buyback_offset_dv01"].fillna(0)
    ).round(2)

    # ON RRP as share of system liquidity (quarterly average from weekly)
    weekly_q = weekly.copy()
    weekly_q["quarter"] = weekly_q["week"].dt.to_period("Q").astype(str)
    q_liq = weekly_q.groupby("quarter").agg(
        on_rrp_bn_q=("on_rrp_bn", "mean"),
        system_liquidity_bn_q=("system_liquidity_bn", "mean"),
        reserves_bn_q=("reserves_bn", "mean"),
        dealer_inventory_bn_q=("dealer_inventory_bn", "mean"),
        repo_spread_bp_q=("repo_spread_bp", "mean"),
    ).round(2)
    q_liq["on_rrp_share"] = (q_liq["on_rrp_bn_q"] / q_liq["system_liquidity_bn_q"]).round(4)
    q = q.merge(q_liq, on="quarter", how="left")

    # Assign regime
    q["refunding_date"] = pd.to_datetime(q["refunding_date"])
    q["fed_regime"] = q["refunding_date"].apply(assign_fed_regime)

    out_cols = [
        "quarter", "fed_regime",
        "coupon_dv01_shock", "bill_dv01_offset", "mix_shock_dv01",
        "expected_soma_redemptions_dv01", "buyback_offset_dv01",
        "net_private_duration_dv01",
        "privately_held_net_marketable_borrowing_bn",
        "on_rrp_bn_q", "on_rrp_share",
        "reserves_bn_q", "dealer_inventory_bn_q", "repo_spread_bp_q",
        "debt_limit_flag",
    ]
    out_cols = [c for c in out_cols if c in q.columns]
    return q[out_cols]


def build_qt_comparison_summary(weekly: pd.DataFrame, comparison_weeks: int = 52) -> pd.DataFrame:
    """Compare the first comparison_weeks of QT1 and QT2 on a normalized basis."""
    specs = [
        ("QT1", pd.Timestamp("2017-10-04"), pd.Timestamp("2019-07-31")),
        ("QT2", pd.Timestamp("2022-06-01"), pd.Timestamp("2025-03-31")),
    ]
    rows = []
    for label, start, end in specs:
        sub = weekly[(weekly["week"] >= start) & (weekly["week"] <= end)].sort_values("week").head(comparison_weeks).copy()
        if sub.empty:
            continue
        first = sub.iloc[0]
        last = sub.iloc[-1]
        rows.append({
            "regime": label,
            "comparison_weeks": len(sub),
            "date_start": str(first["week"].date()),
            "date_end": str(last["week"].date()),
            "soma_change_bn": float(last["soma_treasuries_bn"] - first["soma_treasuries_bn"]) if "soma_treasuries_bn" in sub.columns else np.nan,
            "reserves_change_bn": float(last["reserves_bn"] - first["reserves_bn"]) if "reserves_bn" in sub.columns else np.nan,
            "on_rrp_change_bn": float(last["on_rrp_bn"] - first["on_rrp_bn"]) if "on_rrp_bn" in sub.columns else np.nan,
            "dealer_inventory_change_bn": float(last["dealer_inventory_bn"] - first["dealer_inventory_bn"]) if "dealer_inventory_bn" in sub.columns else np.nan,
            "repo_spread_bp_mean": float(sub["repo_spread_bp"].mean()) if "repo_spread_bp" in sub.columns else np.nan,
            "repo_spread_bp_change": float(last["repo_spread_bp"] - first["repo_spread_bp"]) if "repo_spread_bp" in sub.columns else np.nan,
            "qt_runoff_dv01_mean": float(sub["qt_runoff_dv01"].mean()) if "qt_runoff_dv01" in sub.columns else np.nan,
            "qt_runoff_dv01_cum": float(sub["qt_runoff_dv01"].sum()) if "qt_runoff_dv01" in sub.columns else np.nan,
        })
    return pd.DataFrame(rows).round(2)


def build_correlation_matrix(weekly: pd.DataFrame) -> pd.DataFrame:
    """Key variable correlations for the QT2 period (2022+)."""
    qt2 = weekly[weekly["week"] >= pd.Timestamp("2022-06-01")].copy()
    corr_cols = [
        "soma_treasuries_bn", "reserves_bn", "tga_bn", "on_rrp_bn",
        "dealer_inventory_bn", "repo_spread_bp", "system_liquidity_bn",
    ]
    corr_cols = [c for c in corr_cols if c in qt2.columns]
    return qt2[corr_cols].corr().round(3)


def main() -> None:
    ensure_repo_dirs()
    TABLES_DIR.mkdir(parents=True, exist_ok=True)

    weekly = pd.read_parquet(PROCESSED_DIR / "master_weekly_panel.parquet")
    weekly["week"] = pd.to_datetime(weekly["week"])
    quarter = pd.read_parquet(PROCESSED_DIR / "refunding_panel.parquet")

    regime = build_regime_summary(weekly)
    write_csv(regime, TABLES_DIR / "regime_summary.csv")
    logger.info("Regime summary: %s regimes", len(regime))

    episodes = build_episode_summary(weekly)
    write_csv(episodes, TABLES_DIR / "episode_summary.csv")
    logger.info("Episode summary: %s episodes", len(episodes))

    qt_compare = build_qt_comparison_summary(weekly)
    write_csv(qt_compare, TABLES_DIR / "qt_comparison_summary.csv")
    logger.info("QT comparison summary: %s rows", len(qt_compare))

    q_desc = build_quarterly_descriptive(quarter, weekly)
    write_csv(q_desc, TABLES_DIR / "quarterly_descriptive.csv")
    logger.info("Quarterly descriptive: %s quarters", len(q_desc))

    corr = build_correlation_matrix(weekly)
    write_csv(corr, TABLES_DIR / "correlation_matrix.csv")
    logger.info("Correlation matrix: %sx%s (QT2 period)", *corr.shape)


if __name__ == "__main__":
    main()
