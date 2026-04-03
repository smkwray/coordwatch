
#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import argparse

from coordwatch.demo import build_demo_seed
from coordwatch.logging_utils import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build synthetic demo inputs for offline CoordWatch runs")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    summary = build_demo_seed(seed=args.seed)
    logger.info("Built demo seed: %s", summary)


if __name__ == "__main__":
    main()
