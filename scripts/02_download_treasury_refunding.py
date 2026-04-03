
#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import argparse

import pandas as pd

from coordwatch.config import load_source_manifest
from coordwatch.io import write_csv
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import RAW_DIR, ensure_repo_dirs
from coordwatch.utils.treasury import REFUNDING_KEYWORDS, download_link_records, extract_links, filter_links

configure_logging()
logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download Treasury quarterly refunding pages and linked files")
    parser.add_argument("--download-files", action="store_true", help="Also download linked artifacts")
    args = parser.parse_args()

    ensure_repo_dirs()
    manifest = load_source_manifest()["treasury"]
    pages = [manifest["refunding_landing"], manifest["refunding_most_recent"], manifest["refunding_archives"], manifest["refunding_process"]]
    all_links = []
    for page in pages:
        try:
            all_links.extend(extract_links(page))
            logger.info("Parsed links from %s", page)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Could not parse %s: %s", page, exc)
    filtered = filter_links(all_links, REFUNDING_KEYWORDS)
    filtered_df = pd.DataFrame(filtered).drop_duplicates()
    out_dir = RAW_DIR / "downloads" / "treasury" / "refunding"
    out_dir.mkdir(parents=True, exist_ok=True)
    write_csv(filtered_df, out_dir / "link_index.csv")
    logger.info("Wrote refunding link index with %s rows", len(filtered_df))
    if args.download_files:
        downloaded = download_link_records(filtered_df.to_dict(orient="records"), out_dir / "files")
        write_csv(downloaded, out_dir / "download_manifest.csv")
        logger.info("Wrote download manifest with %s rows", len(downloaded))


if __name__ == "__main__":
    main()
