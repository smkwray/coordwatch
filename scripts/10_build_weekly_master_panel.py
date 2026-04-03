#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from coordwatch.construct.panels import build_weekly_master_panel
from coordwatch.io import read_best_table
from coordwatch.logging_utils import configure_logging
from coordwatch.paths import PROCESSED_DIR

configure_logging()


def main() -> None:
    quarter = read_best_table(PROCESSED_DIR / "refunding_panel.parquet")
    build_weekly_master_panel(quarter)


if __name__ == "__main__":
    main()
