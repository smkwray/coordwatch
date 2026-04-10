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
PUBLIC_TEXT_GUARD_PATHS = [
    ROOT / "README.md",
    ROOT / "DATA_SOURCES.md",
    ROOT / "site",
]
PUBLIC_TEXT_GUARD_PATTERNS = [
    "codex",
    "claude",
    "clod",
    "do/",
    "handoff.md",
    "todo.md",
    "dontdo.md",
    "changes.md",
    "project-context.md",
    "memory-system.md",
    "orca.md",
    "mako.md",
    "dairy.md",
    "tandy.md",
]


def first_existing(*paths: Path) -> Path:
    for path in paths:
        if path.exists():
            return path
    return paths[0]


def check_exists(path: Path) -> dict:
    return {"path": str(path.relative_to(ROOT)), "exists": path.exists(), "size_bytes": path.stat().st_size if path.exists() else 0}


def scan_public_text_guard() -> list[dict]:
    findings: list[dict] = []
    for root_path in PUBLIC_TEXT_GUARD_PATHS:
        if root_path.is_dir():
            paths = [path for path in root_path.rglob("*") if path.is_file()]
        elif root_path.is_file():
            paths = [root_path]
        else:
            paths = []
        for path in paths:
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            text_lower = text.lower()
            for pattern in PUBLIC_TEXT_GUARD_PATTERNS:
                if pattern in text_lower:
                    findings.append({"path": str(path.relative_to(ROOT)), "pattern": pattern})
    return findings


def main() -> None:
    expected = [
        # Core processed panels
        first_existing(PROCESSED_DIR / "refunding_panel.parquet", PROCESSED_DIR / "refunding_panel.csv"),
        first_existing(PROCESSED_DIR / "master_weekly_panel.parquet", PROCESSED_DIR / "master_weekly_panel.csv"),
        first_existing(PROCESSED_DIR / "episode_registry.parquet", PROCESSED_DIR / "episode_registry.csv"),
        # Econometric outputs
        OUTPUTS_TABLES_DIR / "reaction_function_main.csv",
        OUTPUTS_TABLES_DIR / "reaction_function_continuous_liquidity.csv",
        OUTPUTS_TABLES_DIR / "reaction_function_no_debt_limit.csv",
        OUTPUTS_TABLES_DIR / "main_lp_dealer.csv",
        OUTPUTS_TABLES_DIR / "main_lp_repo.csv",
        OUTPUTS_TABLES_DIR / "appendix_lp_repo_iorb.csv",
        OUTPUTS_TABLES_DIR / "appendix_lp_repo_mechanism.csv",
        OUTPUTS_TABLES_DIR / "appendix_lp_repo_no_debt_limit.csv",
        OUTPUTS_TABLES_DIR / "appendix_lp_repo_continuous_liquidity.csv",
        OUTPUTS_TABLES_DIR / "appendix_lp_repo_refunding_event.csv",
        OUTPUTS_TABLES_DIR / "appendix_lp_repo_refunding_placebo.csv",
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
        SITE_DATA_DIR / "reaction_function_main.json",
        SITE_DATA_DIR / "reaction_function_continuous_liquidity.json",
        SITE_DATA_DIR / "reaction_function_no_debt_limit.json",
        SITE_DATA_DIR / "main_lp_dealer.json",
        SITE_DATA_DIR / "main_lp_repo.json",
        SITE_DATA_DIR / "appendix_lp_repo_iorb.json",
        SITE_DATA_DIR / "appendix_lp_repo_mechanism.json",
        SITE_DATA_DIR / "appendix_lp_repo_no_debt_limit.json",
        SITE_DATA_DIR / "appendix_lp_repo_continuous_liquidity.json",
        SITE_DATA_DIR / "appendix_lp_repo_refunding_event.json",
        SITE_DATA_DIR / "appendix_lp_repo_refunding_placebo.json",
        SITE_DATA_DIR / "qt_comparison_summary.json",
        SITE_DATA_DIR / "treasury_statement_signals.json",
        SITE_DATA_DIR / "manual_input_audit.json",
        SITE_DATA_DIR / "daily_mechanics_appendix.json",
        SITE_DATA_DIR / "daily_validation_appendix.json",
        SITE_DATA_DIR / "sectoral_absorbers_appendix.json",
        SITE_DATA_DIR / "auction_mix_appendix.json",
        SITE_DATA_DIR / "event_windows_appendix.json",
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
    guard_findings = scan_public_text_guard()
    guard_ok = not guard_findings
    report.append({"path": "public text guard", "exists": guard_ok, "size_bytes": 0})
    all_ok = all_ok and summary_ok and guard_ok
    print(json.dumps(report, indent=2))
    if not all_ok:
        missing = [r["path"] for r in report if not r["exists"]]
        print(f"\nMISSING: {missing}")
        if guard_findings:
            print("\nTRACKED TEXT GUARD FINDINGS:")
            for finding in guard_findings:
                print(f"- {finding['path']}: {finding['pattern']}")
        sys.exit(1)
    else:
        print(f"\nAll {len(report)} artifacts present.")


if __name__ == "__main__":
    main()
