#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from coordwatch.econometrics.local_projections import run_local_projections, run_named_local_projection
from coordwatch.io import read_best_table, write_csv
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import OUTPUTS_TABLES_DIR, PROCESSED_DIR

configure_logging()
logger = get_logger(__name__)

APPENDIX_OUTPUTS = [
    ("repo_iorb", "appendix_lp_repo_iorb"),
    ("repo_mechanism", "appendix_lp_repo_mechanism"),
    ("repo_no_debt_limit", "appendix_lp_repo_no_debt_limit"),
    ("repo_continuous_liquidity", "appendix_lp_repo_continuous_liquidity"),
    ("repo_refunding_event", "appendix_lp_repo_refunding_event"),
    ("repo_refunding_placebo", "appendix_lp_repo_refunding_placebo"),
]


def main() -> None:
    weekly = read_best_table(PROCESSED_DIR / "master_weekly_panel.parquet")
    dealer = run_local_projections(weekly, outcome="dealer_inventory_bn")
    repo = run_local_projections(weekly, outcome="repo_spread_bp")
    write_csv(dealer.table, OUTPUTS_TABLES_DIR / "main_lp_dealer.csv")
    write_csv(repo.table, OUTPUTS_TABLES_DIR / "main_lp_repo.csv")
    for spec_name, stem in APPENDIX_OUTPUTS:
        bundle = run_named_local_projection(weekly, spec_name)
        write_csv(bundle.table, OUTPUTS_TABLES_DIR / f"{stem}.csv")
    logger.info("Wrote local-projection output tables including %s appendix specs", len(APPENDIX_OUTPUTS))


if __name__ == "__main__":
    main()
