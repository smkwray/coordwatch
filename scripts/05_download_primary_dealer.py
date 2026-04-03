
#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import argparse

from coordwatch.config import load_source_manifest
from coordwatch.io import write_json, write_status
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import RAW_DIR, ensure_repo_dirs
from coordwatch.utils.nyfed import download_catalog, historical_url_candidates, latest_url, try_download_json

configure_logging()
logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download NY Fed primary dealer statistics")
    parser.add_argument("--series", nargs="+", required=True, help="Seriesbreak IDs such as PDPOSGST-TOT")
    args = parser.parse_args()

    ensure_repo_dirs()
    manifest = load_source_manifest()["nyfed"]
    out_dir = RAW_DIR / "downloads" / "nyfed"
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        download_catalog(manifest["primary_dealer_catalog_json"], out_dir / "catalog.json")
        logger.info("Downloaded catalog")
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to download catalog: %s", exc)
        write_status(out_dir / "catalog_status.json", "error", error=str(exc))

    latest_template = manifest["primary_dealer_latest_template"]
    status_records = []
    for seriesbreak in args.series:
        latest_path = out_dir / f"{seriesbreak}_latest.json"
        ok, msg = try_download_json(latest_url(latest_template, seriesbreak), latest_path)
        status_records.append({"seriesbreak": seriesbreak, "endpoint_type": "latest", "success": ok, "message": msg, "path": str(latest_path)})
        if ok:
            logger.info("Downloaded latest release for %s", seriesbreak)
        historical_success = False
        for idx, url in enumerate(historical_url_candidates(seriesbreak), start=1):
            hist_path = out_dir / f"{seriesbreak}_history_candidate_{idx}.json"
            ok, msg = try_download_json(url, hist_path)
            status_records.append({"seriesbreak": seriesbreak, "endpoint_type": f"history_candidate_{idx}", "success": ok, "message": msg, "path": str(hist_path), "url": url})
            if ok:
                historical_success = True
                logger.info("Historical candidate worked for %s: %s", seriesbreak, url)
                break
        if not historical_success:
            logger.warning("No confirmed historical endpoint worked for %s; use manual fallback if needed", seriesbreak)
    write_json(out_dir / "download_status.json", status_records)


if __name__ == "__main__":
    main()
