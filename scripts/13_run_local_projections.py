#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from coordwatch.econometrics.local_projections import run_local_projections
from coordwatch.io import read_best_table, write_csv
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import OUTPUTS_TABLES_DIR, PROCESSED_DIR

configure_logging()
logger = get_logger(__name__)


def main() -> None:
    weekly = read_best_table(PROCESSED_DIR / "master_weekly_panel.parquet")
    dealer = run_local_projections(weekly, outcome="dealer_inventory_bn")
    repo = run_local_projections(weekly, outcome="repo_spread_bp")
    write_csv(dealer.table, OUTPUTS_TABLES_DIR / "main_lp_dealer.csv")
    write_csv(repo.table, OUTPUTS_TABLES_DIR / "main_lp_repo.csv")
    logger.info("Wrote local-projection output tables")


if __name__ == "__main__":
    main()
