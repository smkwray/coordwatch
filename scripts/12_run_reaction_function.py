#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from coordwatch.econometrics.reaction import run_reaction_function
from coordwatch.io import read_best_table, write_csv, write_text
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import OUTPUTS_TABLES_DIR, PROCESSED_DIR

configure_logging()
logger = get_logger(__name__)

REACTION_OUTPUTS = [
    ("reaction_function", "reaction_function_main"),
    ("continuous_liquidity", "reaction_function_continuous_liquidity"),
    ("no_debt_limit", "reaction_function_no_debt_limit"),
]


def main() -> None:
    quarter = read_best_table(PROCESSED_DIR / "refunding_panel.parquet")
    for spec_name, stem in REACTION_OUTPUTS:
        bundle = run_reaction_function(quarter, spec_name=spec_name)
        write_csv(bundle.coefficients, OUTPUTS_TABLES_DIR / f"{stem}.csv")
        write_csv(bundle.fitted, OUTPUTS_TABLES_DIR / f"{stem}_fitted.csv")
        write_text(OUTPUTS_TABLES_DIR / f"{stem}_summary.txt", bundle.summary_text)
    logger.info("Wrote %s reaction-function output tables", len(REACTION_OUTPUTS))


if __name__ == "__main__":
    main()
