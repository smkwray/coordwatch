
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
from coordwatch.io import write_csv, write_text
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import RAW_DIR, ensure_repo_dirs
from coordwatch.utils.http import get_text
from coordwatch.utils.treasury import BUYBACK_KEYWORDS, download_link_records, extract_links, filter_links

configure_logging()
logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download Treasury buyback reference materials")
    parser.add_argument("--download-files", action="store_true")
    args = parser.parse_args()

    ensure_repo_dirs()
    manifest = load_source_manifest()["treasury"]
    pages = [manifest["buybacks_results"], manifest["buybacks_faq"], manifest["buybacks_rules"]]
    out_dir = RAW_DIR / "downloads" / "treasury" / "buybacks"
    out_dir.mkdir(parents=True, exist_ok=True)
    all_links = []
    for page in pages:
        try:
            html = get_text(page)
            stem = Path(page.rstrip('/')).name or "page"
            write_text(out_dir / f"{stem}.html", html)
            all_links.extend(extract_links(page))
        except Exception as exc:  # noqa: BLE001
            logger.exception("Could not fetch %s: %s", page, exc)
    filtered = filter_links(all_links, BUYBACK_KEYWORDS)
    filtered_df = pd.DataFrame(filtered).drop_duplicates()
    write_csv(filtered_df, out_dir / "link_index.csv")
    logger.info("Wrote buyback link index with %s rows", len(filtered_df))
    if args.download_files:
        downloaded = download_link_records(filtered_df.to_dict(orient="records"), out_dir / "files")
        write_csv(downloaded, out_dir / "download_manifest.csv")
        logger.info("Wrote buyback download manifest with %s rows", len(downloaded))


if __name__ == "__main__":
    main()
