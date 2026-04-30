#!/usr/bin/env python
"""Build a compact project-neutral research snapshot.

The snapshot collects high-signal facts from the processed panels and model
tables. It is intentionally not paper-specific: downstream writing projects can
quote or adapt these outputs without changing CoordWatch's public framing.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from coordwatch.io import write_csv, write_json
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import OUTPUTS_TABLES_DIR, PROCESSED_DIR, ensure_repo_dirs

configure_logging()
logger = get_logger(__name__)

QT1_START = pd.Timestamp("2017-10-04")
QT1_END = pd.Timestamp("2019-07-31")
QT2_START = pd.Timestamp("2022-06-01")
QT2_TAPER = pd.Timestamp("2025-04-01")


def _load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _num(value: Any) -> float | None:
    if pd.isna(value):
        return None
    return round(float(value), 4)


def _series_change(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> dict[str, Any]:
    if df.empty or date_col not in df.columns or value_col not in df.columns:
        return {"start": None, "end": None, "change": None, "n_obs": 0}
    work = df[[date_col, value_col]].copy()
    work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")
    work = work[(work[date_col] >= start) & (work[date_col] <= end)].dropna(subset=[date_col, value_col])
    work = work.sort_values(date_col)
    if work.empty:
        return {"start": None, "end": None, "change": None, "n_obs": 0}
    first = work.iloc[0]
    last = work.iloc[-1]
    return {
        "start_date": first[date_col].strftime("%Y-%m-%d"),
        "end_date": last[date_col].strftime("%Y-%m-%d"),
        "start": _num(first[value_col]),
        "end": _num(last[value_col]),
        "change": _num(last[value_col] - first[value_col]),
        "n_obs": int(len(work)),
    }


def _add_metric(
    rows: list[dict[str, Any]],
    metric_id: str,
    label: str,
    value: Any,
    unit: str,
    source: str,
    window: str = "",
    notes: str = "",
) -> None:
    rows.append(
        {
            "metric_id": metric_id,
            "label": label,
            "value": value,
            "unit": unit,
            "window": window,
            "source_artifact": source,
            "notes": notes,
        }
    )


def build_snapshot() -> dict[str, pd.DataFrame]:
    weekly = pd.read_parquet(PROCESSED_DIR / "master_weekly_panel.parquet")
    quarter = pd.read_parquet(PROCESSED_DIR / "refunding_panel.parquet")
    weekly["week"] = pd.to_datetime(weekly["week"], errors="coerce")
    quarter["refunding_date"] = pd.to_datetime(quarter["refunding_date"], errors="coerce")

    qdesc = _load_csv(OUTPUTS_TABLES_DIR / "quarterly_descriptive.csv")
    qt_compare = _load_csv(OUTPUTS_TABLES_DIR / "qt_comparison_summary.csv")
    reaction = _load_csv(OUTPUTS_TABLES_DIR / "reaction_function_main.csv")
    reaction_no_debt = _load_csv(OUTPUTS_TABLES_DIR / "reaction_function_no_debt_limit.csv")
    lp_repo = _load_csv(OUTPUTS_TABLES_DIR / "main_lp_repo.csv")
    lp_dealer = _load_csv(OUTPUTS_TABLES_DIR / "main_lp_dealer.csv")
    lp_mechanism = _load_csv(OUTPUTS_TABLES_DIR / "appendix_lp_repo_mechanism.csv")

    metrics: list[dict[str, Any]] = []
    _add_metric(metrics, "quarter_rows", "Quarterly refunding observations", int(len(quarter)), "quarters", "refunding_panel")
    _add_metric(metrics, "quarter_start", "First refunding quarter", str(quarter["quarter"].min()), "quarter", "refunding_panel")
    _add_metric(metrics, "quarter_end", "Last refunding quarter", str(quarter["quarter"].max()), "quarter", "refunding_panel")
    _add_metric(metrics, "weekly_rows", "Weekly observations", int(len(weekly)), "weeks", "master_weekly_panel")
    _add_metric(metrics, "weekly_date_start", "First weekly observation", weekly["week"].min().strftime("%Y-%m-%d"), "date", "master_weekly_panel")
    _add_metric(metrics, "weekly_date_end", "Last weekly observation", weekly["week"].max().strftime("%Y-%m-%d"), "date", "master_weekly_panel")

    for window_name, start, end in [("QT1", QT1_START, QT1_END), ("QT2", QT2_START, QT2_TAPER)]:
        for col, label in [
            ("soma_treasuries_bn", "SOMA Treasury holdings"),
            ("reserves_bn", "Reserve balances"),
            ("on_rrp_bn", "ON RRP usage"),
            ("tga_bn", "Treasury General Account"),
            ("dealer_inventory_bn", "Primary dealer Treasury inventory"),
            ("repo_spread_bp", "TGCR less ON RRP award rate"),
        ]:
            change = _series_change(weekly, "week", col, start, end)
            _add_metric(
                metrics,
                f"{window_name.lower()}_{col}_change",
                f"{label} change",
                change["change"],
                "bp" if col.endswith("_bp") else "bn",
                "master_weekly_panel",
                window=f"{change.get('start_date')} to {change.get('end_date')}",
                notes=f"{change['n_obs']} non-null weekly observations in window.",
            )

    if not qdesc.empty:
        qt2_q = qdesc[qdesc["quarter"].astype(str) >= "2022Q2"].copy()
        for col, label in [
            ("expected_soma_redemptions_dv01", "Fed runoff duration pressure"),
            ("coupon_dv01_shock", "Coupon issuance duration shock"),
            ("bill_dv01_offset", "Bill issuance offset"),
            ("buyback_offset_dv01", "Buyback duration offset"),
            ("net_private_duration_dv01", "Net private duration supply"),
        ]:
            if col in qt2_q.columns:
                _add_metric(
                    metrics,
                    f"qt2_{col}_sum",
                    f"QT2 cumulative {label}",
                    _num(pd.to_numeric(qt2_q[col], errors="coerce").sum()),
                    "dv01_proxy",
                    "quarterly_descriptive",
                    window=f"{qt2_q['quarter'].min()} to {qt2_q['quarter'].max()}",
                )

    if not qt_compare.empty:
        for _, row in qt_compare.iterrows():
            regime = str(row.get("regime", "")).lower()
            for col in ["soma_change_bn", "reserves_change_bn", "on_rrp_change_bn", "qt_runoff_dv01_cum"]:
                if col in row:
                    _add_metric(
                        metrics,
                        f"{regime}_{col}",
                        f"{row.get('regime')} {col.replace('_', ' ')}",
                        _num(row[col]),
                        "bn" if col.endswith("_bn") else "dv01_proxy",
                        "qt_comparison_summary",
                        window=f"{row.get('date_start')} to {row.get('date_end')}",
                    )

    model_frames = [
        ("reaction_main", reaction),
        ("reaction_no_debt_limit", reaction_no_debt),
        ("lp_repo", lp_repo),
        ("lp_dealer", lp_dealer),
        ("lp_repo_mechanism", lp_mechanism),
    ]
    model_rows: list[dict[str, Any]] = []
    for model_id, df in model_frames:
        if df.empty or "term" not in df.columns:
            continue
        keep = df.copy()
        if "horizon" in keep.columns:
            keep = keep[keep["horizon"].isin([0, 4, 8])]
        for _, row in keep.iterrows():
            model_rows.append(
                {
                    "model_id": model_id,
                    "outcome": row.get("outcome", ""),
                    "horizon": row.get("horizon", ""),
                    "term": row.get("term", ""),
                    "coef": _num(row.get("coef")),
                    "std_err": _num(row.get("std_err")),
                    "p_value": _num(row.get("p_value")),
                    "ci_lower_95": _num(row.get("ci_lower_95")),
                    "ci_upper_95": _num(row.get("ci_upper_95")),
                    "n_obs": int(row["n_obs"]) if "n_obs" in row and pd.notna(row["n_obs"]) else None,
                    "r_squared": _num(row.get("r_squared")),
                }
            )

    return {"metrics": pd.DataFrame(metrics), "model_terms": pd.DataFrame(model_rows)}


def main() -> None:
    ensure_repo_dirs()
    snapshot = build_snapshot()
    metrics = snapshot["metrics"]
    model_terms = snapshot["model_terms"]
    write_csv(metrics, OUTPUTS_TABLES_DIR / "research_snapshot_metrics.csv")
    write_csv(model_terms, OUTPUTS_TABLES_DIR / "research_snapshot_model_terms.csv")
    write_json(
        OUTPUTS_TABLES_DIR / "research_snapshot.json",
        {
            "metadata": {
                "description": "Project-neutral compact summary of CoordWatch processed panels and model outputs.",
                "source_outputs": [
                    "data/processed/refunding_panel.parquet",
                    "data/processed/master_weekly_panel.parquet",
                    "outputs/tables/*.csv",
                ],
            },
            "metrics": metrics.to_dict(orient="records"),
            "model_terms": model_terms.to_dict(orient="records"),
        },
    )
    logger.info("Wrote research snapshot with %s metrics and %s model terms", len(metrics), len(model_terms))


if __name__ == "__main__":
    main()
