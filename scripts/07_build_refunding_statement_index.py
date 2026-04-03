
#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from coordwatch.io import write_csv
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import INTERIM_DIR, RAW_DIR, ensure_repo_dirs

configure_logging()
logger = get_logger(__name__)


def main() -> None:
    ensure_repo_dirs()
    base = RAW_DIR / "downloads" / "treasury"
    manifests = []
    for rel in ["refunding/download_manifest.csv", "financing/download_manifest.csv"]:
        path = base / rel
        if path.exists():
            df = pd.read_csv(path)
            df["manifest_source"] = rel
            manifests.append(df)
    if not manifests:
        demo_path = RAW_DIR / "demo" / "treasury" / "refunding_panel_demo.csv"
        if demo_path.exists():
            demo = pd.read_csv(demo_path)
            demo["local_path"] = str(demo_path)
            demo["source_page"] = "demo"
            demo["text"] = demo.get("statement_title", "demo")
            demo["url"] = demo.get("statement_url", "demo://refunding_panel_demo.csv")
            demo["download_status"] = "demo"
            idx = demo[["quarter", "refunding_date", "statement_title", "statement_url", "local_path", "source_page", "text", "url", "download_status"]].copy()
            write_csv(idx, INTERIM_DIR / "refunding_statement_index.csv")
            logger.info("Wrote demo refunding statement index with %s rows", len(idx))
            return
        raise FileNotFoundError("No Treasury download manifests or demo refunding panel found")
    out = pd.concat(manifests, ignore_index=True).drop_duplicates(subset=["url", "local_path"])
    write_csv(out, INTERIM_DIR / "refunding_statement_index.csv")
    logger.info("Wrote refunding statement index with %s rows", len(out))


if __name__ == "__main__":
    main()
