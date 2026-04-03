
#!/usr/bin/env python
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pathlib import Path

import pandas as pd

from coordwatch.io import write_csv
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import INTERIM_DIR, RAW_DIR, ensure_repo_dirs
from coordwatch.utils.treasury import extract_refunding_numeric_hints, file_to_text, statement_metadata_from_path

configure_logging()
logger = get_logger(__name__)


def main() -> None:
    ensure_repo_dirs()
    idx_path = INTERIM_DIR / "refunding_statement_index.csv"
    if not idx_path.exists():
        raise FileNotFoundError("Run scripts/07_build_refunding_statement_index.py first")
    idx = pd.read_csv(idx_path)

    records = []
    extracts = []
    if "quarter" in idx.columns and "demo" in idx.get("source_page", pd.Series(dtype=object)).astype(str).str.lower().unique().tolist():
        demo = pd.read_csv(RAW_DIR / "demo" / "treasury" / "refunding_panel_demo.csv")
        demo_extracts = demo[[
            "quarter",
            "refunding_date",
            "statement_url",
            "statement_title",
            "privately_held_net_marketable_borrowing_bn",
            "cash_balance_assumption_bn",
            "debt_limit_flag",
            "coupon_dv01_shock",
            "bill_dv01_offset",
            "buyback_offset_dv01",
            "mix_shock_dv01",
            "expected_soma_redemptions_dv01",
            "classification_prior",
            "clean_sample_flag",
        ]].copy()
        write_csv(demo_extracts, INTERIM_DIR / "refunding_statement_extracts.csv")
        logger.info("Copied demo refunding panel into interim extracts")
        return

    for _, row in idx.iterrows():
        local = row.get("local_path")
        if not isinstance(local, str):
            continue
        path = Path(local)
        if not path.exists():
            continue
        try:
            text = file_to_text(path)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Could not extract text from %s: %s", path, exc)
            continue
        meta = statement_metadata_from_path(path)
        numeric = extract_refunding_numeric_hints(text)
        rec = {**row.to_dict(), **meta, "text": text[:25000]}
        ext = {**row.to_dict(), **meta, **numeric}
        if "refunding_date" in ext:
            dt = pd.to_datetime(ext["refunding_date"], errors="coerce")
            ext["quarter"] = str(dt.to_period("Q")) if pd.notna(dt) else None
        records.append(rec)
        extracts.append(ext)
    write_csv(pd.DataFrame(records), INTERIM_DIR / "refunding_statement_texts.csv")
    write_csv(pd.DataFrame(extracts), INTERIM_DIR / "refunding_statement_extracts.csv")
    logger.info("Wrote %s text records and %s extract records", len(records), len(extracts))


if __name__ == "__main__":
    main()
