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


def main() -> None:
    quarter = read_best_table(PROCESSED_DIR / "refunding_panel.parquet")
    bundle = run_reaction_function(quarter)
    write_csv(bundle.coefficients, OUTPUTS_TABLES_DIR / "reaction_function_main.csv")
    write_csv(bundle.fitted, OUTPUTS_TABLES_DIR / "reaction_function_fitted.csv")
    write_text(OUTPUTS_TABLES_DIR / "reaction_function_summary.txt", bundle.summary_text)
    logger.info("Wrote reaction-function outputs")


if __name__ == "__main__":
    main()
