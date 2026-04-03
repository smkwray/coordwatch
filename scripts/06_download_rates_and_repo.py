
#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import time

from coordwatch.config import load_source_manifest
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import RAW_DIR, ensure_repo_dirs
from coordwatch.utils.fred import download_fred_series

SERIES = ["RRPONTSYAWARD", "TGCR", "IORB", "DGS2", "DGS5", "DGS10", "DGS20", "DGS30"]

configure_logging()
logger = get_logger(__name__)


def main() -> None:
    ensure_repo_dirs()
    manifest = load_source_manifest()
    base_url = manifest["fred"]["base_graph_csv"]
    out_dir = RAW_DIR / "downloads" / "fred"
    out_dir.mkdir(parents=True, exist_ok=True)
    for i, sid in enumerate(SERIES):
        try:
            download_fred_series(sid, base_url, out_dir / f"{sid}.csv")
            logger.info("Downloaded %s", sid)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to download %s: %s", sid, exc)
        if i < len(SERIES) - 1:
            time.sleep(2)


if __name__ == "__main__":
    main()
