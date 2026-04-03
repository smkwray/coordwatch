#!/usr/bin/env python
"""Build publish artifacts for the static site.

Produces JSON data files in site/data/ and data/publish/ for the dynamic
frontend. No static figures — the site renders charts from data.
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from coordwatch.io import read_best_table, timestamp_utc, write_json
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import OUTPUTS_TABLES_DIR, PROCESSED_DIR, PUBLISH_DIR, ensure_repo_dirs
from coordwatch.publish.site import publish_table

configure_logging()
logger = get_logger(__name__)

TABLES_DIR = ROOT / "outputs" / "tables"


def _ts_to_str(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetime columns to ISO strings for JSON serialization."""
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d")
    return out


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    ensure_repo_dirs()

    # Load processed panels
    quarter = read_best_table(PROCESSED_DIR / "refunding_panel.parquet")
    weekly = read_best_table(PROCESSED_DIR / "master_weekly_panel.parquet")
    episodes = read_best_table(PROCESSED_DIR / "episode_registry.parquet")

    # Load econometric outputs
    reaction = pd.read_csv(OUTPUTS_TABLES_DIR / "reaction_function_main.csv")
    lp_dealer = pd.read_csv(OUTPUTS_TABLES_DIR / "main_lp_dealer.csv")
    lp_repo = pd.read_csv(OUTPUTS_TABLES_DIR / "main_lp_repo.csv")

    # Publish econometric tables (supporting evidence)
    publish_table(reaction, "reaction_function_main")
    publish_table(lp_dealer, "main_lp_dealer")
    publish_table(lp_repo, "main_lp_repo")

    # Publish episode registry
    publish_table(_ts_to_str(episodes), "episode_registry")

    # Publish full panels (for dynamic site charts)
    # Weekly: select key columns to keep JSON size manageable
    weekly_cols = [
        "week", "soma_treasuries_bn", "reserves_bn", "tga_bn", "on_rrp_bn",
        "dealer_inventory_bn", "repo_spread_bp", "system_liquidity_bn",
        "low_liquidity", "qt_runoff_dv01", "qt_runoff_source", "qt_runoff_proxy_bn",
        "coupon_dv01_shock", "bill_dv01_offset", "mix_shock_dv01",
        "buyback_offset_dv01", "expected_soma_redemptions_dv01",
        "duration_pressure_dv01", "fed_pressure_dv01",
        "quarter", "calendar_quarter", "input_source",
    ]
    weekly_cols = [c for c in weekly_cols if c in weekly.columns]
    weekly_pub = _ts_to_str(weekly[weekly_cols])
    # Only publish from 2003+ (when SOMA data starts being reliable)
    weekly_pub = weekly_pub[weekly_pub["week"] >= "2003-01-01"]
    publish_table(weekly_pub, "weekly_panel")

    # Quarterly panel: full
    publish_table(_ts_to_str(quarter), "quarterly_panel")

    # Publish descriptive tables (core evidence)
    for table_name in ["regime_summary", "episode_summary", "quarterly_descriptive", "correlation_matrix"]:
        path = TABLES_DIR / f"{table_name}.csv"
        if path.exists():
            df = pd.read_csv(path)
            publish_table(_ts_to_str(df), table_name)
    qt_compare_path = TABLES_DIR / "qt_comparison_summary.csv"
    if qt_compare_path.exists():
        publish_table(_ts_to_str(pd.read_csv(qt_compare_path)), "qt_comparison_summary")

    data_files = [
        "weekly_panel.json",
        "quarterly_panel.json",
        "episode_registry.json",
        "regime_summary.json",
        "episode_summary.json",
        "qt_comparison_summary.json",
        "quarterly_descriptive.json",
        "correlation_matrix.json",
        "reaction_function_main.json",
        "main_lp_dealer.json",
        "main_lp_repo.json",
    ]
    artifact_hashes = []
    for name in data_files:
        site_path = ROOT / "site" / "data" / name
        artifact_hashes.append({
            "file": name,
            "sha256": _sha256(site_path),
            "size_bytes": site_path.stat().st_size,
        })

    # Summary metadata
    summary = {
        "project": "CoordWatch",
        "generated_at_utc": timestamp_utc(),
        "quarter_rows": int(len(quarter)),
        "weekly_rows": int(len(weekly_pub)),
        "runoff_source_counts": weekly_pub.get("qt_runoff_source", pd.Series(dtype=object)).fillna("unknown").value_counts().to_dict(),
        "episode_rows": int(len(episodes)),
        "reaction_n_obs": int(reaction["n_obs"].iloc[0]) if not reaction.empty else None,
        "weekly_frequency": "W-WED",
        "site_window_start": "2003-01-01",
        "headline_reaction_terms": reaction[["term", "coef", "p_value"]].to_dict(orient="records"),
        "measurement_notes": [
            {
                "title": "Frequency alignment",
                "detail": "The public weekly panel uses Wednesday observations. Daily money-market inputs are kept separate before weekly aggregation."
            },
            {
                "title": "Runoff measure",
                "detail": "Weekly qt_runoff_dv01 compares consecutive New York Fed SOMA holdings snapshots at the CUSIP level, then weights holdings declines by remaining maturity and coupon-duration proxies. When holdings detail is unavailable, the series falls back to a quarterly allocation; qt_runoff_proxy_bn remains a separate level-change monitor."
            },
            {
                "title": "Runoff coverage",
                "detail": "Most public-window weeks use holdings-detail runoff. A small number of early-window and holiday weeks are marked as holdings gaps when the New York Fed payload does not carry usable security identifiers."
            },
            {
                "title": "Liquidity state",
                "detail": "Quarterly low_liquidity_prev is derived from average weekly system liquidity, defined as reserves plus ON RRP."
            },
            {
                "title": "Manual review",
                "detail": "Episode windows and selected refunding inputs use manual review files. Debt-limit periods are treated as confounded windows rather than clean policy comparisons."
            },
            {
                "title": "Interpretation",
                "detail": "Balance-sheet identities and descriptive comparisons are primary. Correlations and regressions are supplementary checks, not stand-alone proof of intent."
            }
        ],
        "data_files": data_files,
        "artifact_hashes": artifact_hashes,
    }
    write_json(PUBLISH_DIR / "summary.json", summary)
    write_json(ROOT / "site" / "data" / "summary.json", summary)
    logger.info("Built publish artifacts: %s data files", len(summary["data_files"]))


if __name__ == "__main__":
    main()
