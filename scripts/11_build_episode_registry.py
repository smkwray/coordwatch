#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from coordwatch.io import read_best_table, write_csv, write_parquet
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import MANUAL_DIR, PROCESSED_DIR, ensure_repo_dirs

configure_logging()
logger = get_logger(__name__)


def classify_alignment(expected_soma: float, mix_shock: float, debt_limit_flag: int) -> str:
    if debt_limit_flag == 1:
        return "confounded_window"
    if expected_soma > 0 and mix_shock < 0:
        return "offsetting_interaction"
    if expected_soma > 0 and mix_shock > 0:
        return "amplifying_interaction"
    return "neutral_window"


def main() -> None:
    ensure_repo_dirs()
    quarter = read_best_table(PROCESSED_DIR / "refunding_panel.parquet")
    seed = pd.read_csv(MANUAL_DIR / "episode_registry_seed.csv")
    quarter["alignment_state_model"] = [
        classify_alignment(es, ms, int(dl) if pd.notna(dl) else 0)
        for es, ms, dl in zip(quarter["expected_soma_redemptions_dv01"], quarter["mix_shock_dv01"], quarter["debt_limit_flag"])
    ]

    episode_rows = []
    for _, row in seed.iterrows():
        start = pd.Timestamp(row["window_start"])
        end = pd.Timestamp(row["window_end"])
        sub = quarter[(pd.to_datetime(quarter["refunding_date"]) >= start) & (pd.to_datetime(quarter["refunding_date"]) <= end)].copy()
        episode_rows.append({
            **row.to_dict(),
            "n_quarters": int(len(sub)),
            "avg_mix_shock_dv01": round(float(sub["mix_shock_dv01"].mean()), 3) if len(sub) else None,
            "avg_expected_soma_redemptions_dv01": round(float(sub["expected_soma_redemptions_dv01"].mean()), 3) if len(sub) else None,
            "avg_buyback_offset_dv01": round(float(sub["buyback_offset_dv01"].mean()), 3) if len(sub) else None,
            "model_majority_state": sub["alignment_state_model"].mode().iat[0] if len(sub) and not sub["alignment_state_model"].mode().empty else None,
        })
    episode_registry = pd.DataFrame(episode_rows)
    write_csv(quarter, PROCESSED_DIR / "quarter_alignment_assignments.csv")
    write_parquet(quarter, PROCESSED_DIR / "quarter_alignment_assignments.parquet")
    write_csv(episode_registry, PROCESSED_DIR / "episode_registry.csv")
    write_parquet(episode_registry, PROCESSED_DIR / "episode_registry.parquet")
    logger.info("Built episode registry with %s episodes", len(episode_registry))


if __name__ == "__main__":
    main()
