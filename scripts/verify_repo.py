#!/usr/bin/env python
"""Verify that all expected pipeline artifacts exist."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from coordwatch.logging_utils import configure_logging
from coordwatch.paths import OUTPUTS_TABLES_DIR, PROCESSED_DIR, PUBLISH_DIR, SITE_DATA_DIR

configure_logging()

TABLES_DIR = ROOT / "outputs" / "tables"


def first_existing(*paths: Path) -> Path:
    for path in paths:
        if path.exists():
            return path
    return paths[0]


def check_exists(path: Path) -> dict:
    return {"path": str(path.relative_to(ROOT)), "exists": path.exists(), "size_bytes": path.stat().st_size if path.exists() else 0}


def main() -> None:
    expected = [
        # Core processed panels
        first_existing(PROCESSED_DIR / "refunding_panel.parquet", PROCESSED_DIR / "refunding_panel.csv"),
        first_existing(PROCESSED_DIR / "master_weekly_panel.parquet", PROCESSED_DIR / "master_weekly_panel.csv"),
        first_existing(PROCESSED_DIR / "episode_registry.parquet", PROCESSED_DIR / "episode_registry.csv"),
        # Econometric outputs
        OUTPUTS_TABLES_DIR / "reaction_function_main.csv",
        OUTPUTS_TABLES_DIR / "main_lp_dealer.csv",
        OUTPUTS_TABLES_DIR / "main_lp_repo.csv",
        # Descriptive tables
        TABLES_DIR / "regime_summary.csv",
        TABLES_DIR / "episode_summary.csv",
        TABLES_DIR / "qt_comparison_summary.csv",
        TABLES_DIR / "quarterly_descriptive.csv",
        TABLES_DIR / "correlation_matrix.csv",
        # Publish artifacts
        PUBLISH_DIR / "summary.json",
        # Site data
        SITE_DATA_DIR / "site_manifest.json",
        SITE_DATA_DIR / "weekly_panel.json",
        SITE_DATA_DIR / "quarterly_panel.json",
        SITE_DATA_DIR / "episode_registry.json",
        SITE_DATA_DIR / "qt_comparison_summary.json",
    ]
    report = [check_exists(path) for path in expected]
    all_ok = all(r["exists"] for r in report)
    summary_ok = False
    summary_path = PUBLISH_DIR / "summary.json"
    if summary_path.exists():
        summary = json.loads(summary_path.read_text())
        hashes = summary.get("artifact_hashes", [])
        data_files = summary.get("data_files", [])
        summary_ok = bool(summary.get("generated_at_utc")) and len(hashes) == len(data_files) and len(data_files) > 0
    report.append({"path": "data/publish/summary.json metadata", "exists": summary_ok, "size_bytes": 0})
    all_ok = all_ok and summary_ok
    print(json.dumps(report, indent=2))
    if not all_ok:
        missing = [r["path"] for r in report if not r["exists"]]
        print(f"\nMISSING: {missing}")
        sys.exit(1)
    else:
        print(f"\nAll {len(report)} artifacts present.")


if __name__ == "__main__":
    main()
