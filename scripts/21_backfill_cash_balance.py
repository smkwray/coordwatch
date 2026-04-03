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

from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.utils.treasury import extract_cash_balance_assumption, file_to_text, url_to_text

configure_logging()
logger = get_logger(__name__)

IMPORT_PATH = ROOT / "data" / "raw" / "imports" / "qrawatch" / "official_quarterly_refunding_capture.csv"
MANUAL_PATH = ROOT / "data" / "manual" / "refunding_manual_overrides.csv"
REPORT_PATH = ROOT / "data" / "interim" / "cash_balance_backfill_report.csv"


def _existing_local_paths(field: str) -> list[Path]:
    paths = []
    for part in str(field or "").split("|"):
        p = Path(part.strip())
        if part.strip() and p.exists():
            paths.append(p)
    return paths


def _split_urls(field: str) -> list[str]:
    return [part.strip() for part in str(field or "").split("|") if part.strip()]


def load_source_text(row: pd.Series) -> tuple[str | None, str]:
    local_fields = [
        row.get("financing_source_doc_local"),
        row.get("refunding_statement_source_doc_local"),
        row.get("source_doc_local"),
    ]
    for field in local_fields:
        for path in _existing_local_paths(field):
            try:
                return file_to_text(path), str(path)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to read local source %s: %s", path, exc)

    url_fields = [
        row.get("financing_source_url"),
        row.get("refunding_statement_source_url"),
    ]
    for field in url_fields:
        for url in _split_urls(field):
            try:
                return url_to_text(url), url
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to fetch %s: %s", url, exc)
    return None, ""


def is_estimated_pre2016(quarter: str, current_value: float | int | None) -> bool:
    if not isinstance(quarter, str) or quarter > "2015Q4":
        return False
    if pd.isna(current_value):
        return True
    return float(current_value) in {200.0, 350.0}


def build_report() -> pd.DataFrame:
    qra = pd.read_csv(IMPORT_PATH)
    manual = pd.read_csv(MANUAL_PATH)
    merged = manual.merge(
        qra[
            [
                "quarter",
                "financing_source_url",
                "financing_source_doc_local",
                "refunding_statement_source_url",
                "refunding_statement_source_doc_local",
            ]
        ],
        on="quarter",
        how="left",
    )

    rows = []
    for _, row in merged.iterrows():
        quarter = row["quarter"]
        current = row.get("cash_balance_assumption_bn")
        if not is_estimated_pre2016(quarter, current):
            continue
        text, source = load_source_text(row)
        extracted = extract_cash_balance_assumption(text or "", quarter=quarter) if text else None
        rows.append(
            {
                "quarter": quarter,
                "current_cash_balance_assumption_bn": current,
                "extracted_cash_balance_assumption_bn": extracted,
                "source_used": source,
                "updated": pd.notna(extracted) and float(extracted) != float(current),
            }
        )
    report = pd.DataFrame(rows).sort_values("quarter").reset_index(drop=True)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(REPORT_PATH, index=False)
    return report


def apply_backfill(report: pd.DataFrame) -> tuple[int, pd.DataFrame]:
    manual = pd.read_csv(MANUAL_PATH)
    updates = report.dropna(subset=["extracted_cash_balance_assumption_bn"]).copy()
    updates["quarter"] = updates["quarter"].astype(str)
    count = 0
    for _, row in updates.iterrows():
        mask = manual["quarter"].astype(str) == row["quarter"]
        if not mask.any():
            continue
        current = float(manual.loc[mask, "cash_balance_assumption_bn"].iloc[0])
        new_value = float(row["extracted_cash_balance_assumption_bn"])
        if current == new_value:
            continue
        manual.loc[mask, "cash_balance_assumption_bn"] = round(new_value, 2)
        if "reviewer_notes" in manual.columns:
            manual.loc[mask, "reviewer_notes"] = (
                manual.loc[mask, "reviewer_notes"].fillna("").astype(str)
                + " Cash balance sourced from official financing/refunding release."
            ).str.strip()
        count += 1
    if count:
        manual.to_csv(MANUAL_PATH, index=False)
    return count, manual


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill pre-2016 cash balance assumptions from official Treasury release text")
    parser.add_argument("--apply", action="store_true", help="Write extracted values back to data/manual/refunding_manual_overrides.csv")
    args = parser.parse_args()

    report = build_report()
    found = int(report["extracted_cash_balance_assumption_bn"].notna().sum()) if not report.empty else 0
    changed = int(report["updated"].fillna(False).sum()) if not report.empty else 0
    logger.info("Cash-balance report rows=%s extracted=%s changed=%s", len(report), found, changed)
    if args.apply:
        applied, _ = apply_backfill(report)
        logger.info("Applied %s cash-balance updates to %s", applied, MANUAL_PATH)
    print(report.to_string(index=False))


if __name__ == "__main__":
    main()
