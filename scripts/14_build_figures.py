#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from coordwatch.io import read_best_table
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import OUTPUTS_FIGURES_DIR, OUTPUTS_TABLES_DIR, PROCESSED_DIR
from coordwatch.publish.charts import plot_alignment_timeline, plot_episode_quadrants, plot_irf

configure_logging()
logger = get_logger(__name__)


def main() -> None:
    quarter = read_best_table(PROCESSED_DIR / "refunding_panel.parquet")
    weekly = read_best_table(PROCESSED_DIR / "master_weekly_panel.parquet")
    lp_dealer = pd.read_csv(OUTPUTS_TABLES_DIR / "main_lp_dealer.csv")
    lp_repo = pd.read_csv(OUTPUTS_TABLES_DIR / "main_lp_repo.csv")

    plot_alignment_timeline(weekly, quarter, OUTPUTS_FIGURES_DIR / "alignment_timeline.png")
    plot_episode_quadrants(quarter, OUTPUTS_FIGURES_DIR / "episode_quadrants.png")
    plot_irf(lp_dealer, term="fed_pressure_dv01", title="Dealer inventory response to Fed pressure", out_path=OUTPUTS_FIGURES_DIR / "irf_dealer_inventory.png")
    plot_irf(lp_repo, term="fed_pressure_dv01", title="Repo-spread response to Fed pressure", out_path=OUTPUTS_FIGURES_DIR / "irf_repo_spread.png")
    plot_irf(lp_dealer, term="fed_pressure_x_low_liquidity", title="Dealer inventory interaction response", out_path=OUTPUTS_FIGURES_DIR / "irf_dealer_interaction.png")
    plot_irf(lp_repo, term="fed_pressure_x_low_liquidity", title="Repo-spread interaction response", out_path=OUTPUTS_FIGURES_DIR / "irf_repo_interaction.png")
    logger.info("Built figures")


if __name__ == "__main__":
    main()
