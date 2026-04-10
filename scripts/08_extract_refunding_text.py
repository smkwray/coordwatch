
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
from coordwatch.paths import INTERIM_DIR, MANUAL_DIR, RAW_DIR, ensure_repo_dirs
from coordwatch.utils.treasury import (
    cached_statement_text,
    extract_refunding_numeric_hints,
    extract_statement_signal_hints,
    file_to_text,
    statement_metadata_from_path,
)

configure_logging()
logger = get_logger(__name__)


def main() -> None:
    ensure_repo_dirs()
    idx_path = INTERIM_DIR / "refunding_statement_index.csv"
    records = []
    extracts = []
    if idx_path.exists():
        idx = pd.read_csv(idx_path)
    else:
        idx = pd.DataFrame()

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

    manual_path = MANUAL_DIR / "refunding_manual_overrides.csv"
    if manual_path.exists():
        manual = pd.read_csv(manual_path)
    else:
        manual = pd.DataFrame()

    if not manual.empty and manual.get("statement_url", pd.Series(dtype=object)).notna().any():
        cache_dir = RAW_DIR / "downloads" / "treasury" / "refunding" / "statement_cache"
        source_dir = RAW_DIR / "downloads" / "treasury" / "refunding" / "files"
        for _, row in manual.sort_values("refunding_date").iterrows():
            url = row.get("statement_url")
            if not isinstance(url, str) or not url.strip():
                continue
            try:
                text, path = cached_statement_text(url, cache_dir=cache_dir, source_dir=source_dir)
                status = "cached_local" if path.parent == source_dir else "downloaded_or_cached"
            except Exception as exc:  # noqa: BLE001
                logger.exception("Could not fetch Treasury statement from %s: %s", url, exc)
                text = ""
                path = cache_dir / "missing.html"
                status = f"error: {exc}"
            meta = statement_metadata_from_path(path)
            numeric = extract_refunding_numeric_hints(text, quarter=row.get("quarter"))
            signals = extract_statement_signal_hints(text)
            base = {
                "quarter": row.get("quarter"),
                "refunding_date": row.get("refunding_date"),
                "statement_url": url,
                "statement_title": row.get("statement_title") or meta.get("statement_title"),
                "local_path": str(path),
                "download_status": status,
                "source_page": url,
                "url": url,
                "text": text[:25000],
            }
            records.append(base)
            extracts.append(
                {
                    **base,
                    **meta,
                    **numeric,
                    **signals,
                    "statement_text_source": "manual_url_cache",
                }
            )
        records_df = pd.DataFrame(records)
        extracts_df = pd.DataFrame(extracts)
        write_csv(records_df, INTERIM_DIR / "refunding_statement_texts.csv")
        write_csv(extracts_df.drop(columns=["text"], errors="ignore"), INTERIM_DIR / "refunding_statement_extracts.csv")
        write_csv(extracts_df.drop(columns=["text"], errors="ignore"), INTERIM_DIR / "treasury_statement_signals.csv")
        logger.info("Wrote %s quarter-complete Treasury statement extracts from manual URLs", len(extracts_df))
        return

    if idx.empty:
        raise FileNotFoundError("Run scripts/07_build_refunding_statement_index.py first or populate manual overrides.")

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
        numeric = extract_refunding_numeric_hints(text, quarter=row.get("quarter"))
        signals = extract_statement_signal_hints(text)
        rec = {**row.to_dict(), **meta, "text": text[:25000]}
        ext = {**row.to_dict(), **meta, **numeric, **signals, "statement_text_source": "download_manifest"}
        if "refunding_date" in ext:
            dt = pd.to_datetime(ext["refunding_date"], errors="coerce")
            ext["quarter"] = str(dt.to_period("Q")) if pd.notna(dt) else None
        records.append(rec)
        extracts.append(ext)
    write_csv(pd.DataFrame(records), INTERIM_DIR / "refunding_statement_texts.csv")
    write_csv(pd.DataFrame(extracts), INTERIM_DIR / "refunding_statement_extracts.csv")
    write_csv(pd.DataFrame(extracts), INTERIM_DIR / "treasury_statement_signals.csv")
    logger.info("Wrote %s text records and %s extract records", len(records), len(extracts))


if __name__ == "__main__":
    main()
