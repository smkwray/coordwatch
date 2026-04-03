#!/usr/bin/env python
"""Build site bundle manifest listing all JSON data files available."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from coordwatch.io import write_json
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import SITE_DATA_DIR, ensure_repo_dirs

configure_logging()
logger = get_logger(__name__)


def main() -> None:
    ensure_repo_dirs()
    data_files = sorted([p.name for p in SITE_DATA_DIR.glob("*.json") if p.name != "site_manifest.json"])
    manifest = {
        "data_files": data_files,
    }
    write_json(SITE_DATA_DIR / "site_manifest.json", manifest)
    logger.info("Built site manifest with %s data files", len(data_files))


if __name__ == "__main__":
    main()
