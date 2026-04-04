#!/usr/bin/env python
"""Build publish artifacts for the static site.

Produces JSON data files in site/data/ and data/publish/ for the dynamic
frontend. No static figures — the site renders charts from data.
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import pandas as pd

from coordwatch.config import load_variables
from coordwatch.io import read_best_table, timestamp_utc, write_json
from coordwatch.logging_utils import configure_logging, get_logger
from coordwatch.paths import OUTPUTS_TABLES_DIR, PROCESSED_DIR, PUBLISH_DIR, RAW_DIR, ensure_repo_dirs
from coordwatch.publish.site import publish_table
from coordwatch.utils.fred import series_to_wide

configure_logging()
logger = get_logger(__name__)

TABLES_DIR = ROOT / "outputs" / "tables"
MANUAL_DIR = ROOT / "data" / "manual"
QRAWATCH_IMPORTS_DIR = ROOT / "data" / "raw" / "imports" / "qrawatch"
TREASURY_DAILY_OPS_DIR = ROOT / "data" / "raw" / "downloads" / "treasury" / "daily_ops"
SECTOR_ORDER = [
    "rest_of_world",
    "households_nonprofits",
    "us_chartered_depositories",
    "money_market_funds",
    "mutual_funds",
    "broker_dealers",
    "other_private_sectors",
]
AUCTION_TENOR_BUCKETS = {
    "coupon_2y_bn": (0, 2.5),
    "coupon_3y_bn": (2.5, 4.0),
    "coupon_5y_bn": (4.0, 6.0),
    "coupon_7y_bn": (6.0, 8.0),
    "coupon_10y_bn": (8.0, 12.0),
    "coupon_20y_bn": (12.0, 25.0),
    "coupon_30y_bn": (25.0, 100.0),
}


def _ts_to_str(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetime columns to ISO strings for JSON serialization."""
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d")
    return out


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _first_non_null_week(df: pd.DataFrame, value_col: str, min_week: str | None = None) -> str | None:
    if value_col not in df.columns or "week" not in df.columns:
        return None
    work = df.copy()
    if min_week is not None:
        work = work[work["week"] >= min_week]
    valid = work.loc[work[value_col].notna(), "week"]
    if valid.empty:
        return None
    first = pd.to_datetime(valid.iloc[0], errors="coerce")
    return first.strftime("%Y-%m-%d") if pd.notna(first) else None


def _load_manual_workflow_notes(readme_path: Path) -> list[str]:
    if not readme_path.exists():
        return []
    notes = []
    for line in readme_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            notes.append(stripped[2:].strip())
    return notes


def _build_manual_input_audit(manual: pd.DataFrame) -> dict:
    manual = manual.copy()
    manual["refunding_date"] = pd.to_datetime(manual["refunding_date"], errors="coerce")
    manual["cash_balance_statement_sourced"] = manual["reviewer_notes"].fillna("").str.contains(
        "Cash balance sourced from official financing/refunding release", case=False, regex=False
    )
    manual["has_reviewer_notes"] = manual["reviewer_notes"].fillna("").str.strip().ne("")
    manual_rows = manual[
        [
            "quarter",
            "refunding_date",
            "statement_title",
            "statement_url",
            "verification_status",
            "debt_limit_flag",
            "clean_sample_flag",
            "classification_prior",
            "cash_balance_statement_sourced",
            "reviewer_notes",
        ]
    ].copy()
    manual_rows["refunding_date"] = manual_rows["refunding_date"].dt.strftime("%Y-%m-%d")
    manual_rows = manual_rows.sort_values("quarter")
    summary = {
        "manual_quarter_rows": int(len(manual_rows)),
        "verified_rows": int(manual["verification_status"].fillna("").eq("verified").sum()),
        "debt_limit_rows": int(pd.to_numeric(manual["debt_limit_flag"], errors="coerce").fillna(0).astype(int).sum()),
        "clean_rows": int(pd.to_numeric(manual["clean_sample_flag"], errors="coerce").fillna(0).astype(int).sum()),
        "cash_balance_statement_sourced_rows": int(manual["cash_balance_statement_sourced"].sum()),
        "rows_with_reviewer_notes": int(manual["has_reviewer_notes"].sum()),
        "workflow_notes": _load_manual_workflow_notes(MANUAL_DIR / "README.md"),
    }
    return {
        "summary": summary,
        "rows": manual_rows.to_dict(orient="records"),
    }


def _build_daily_mechanics_appendix(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame()
    work = daily.copy()
    work["week"] = pd.to_datetime(work["week"], errors="coerce")
    work = work.sort_values("week").dropna(subset=["week"]).reset_index(drop=True)
    keep_cols = [
        "reserves_bn",
        "tga_bn",
        "on_rrp_bn",
        "system_liquidity_bn",
        "soma_treasuries_bn",
    ]
    existing = [col for col in keep_cols if col in work.columns]
    work[existing] = work[existing].ffill()
    windows = [
        ("debt_ceiling_2023", pd.Timestamp("2023-01-01"), pd.Timestamp("2023-07-15")),
        ("debt_ceiling_2025", pd.Timestamp("2025-01-01"), pd.Timestamp("2025-07-15")),
    ]
    frames = []
    for window_id, start, end in windows:
        sub = work[(work["week"] >= start) & (work["week"] <= end)].copy()
        if sub.empty:
            continue
        sub["window_id"] = window_id
        sub["day_index"] = range(len(sub))
        frames.append(sub[["window_id", "day_index", "week"] + existing])
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    return _ts_to_str(out)


def _build_daily_validation_appendix(daily: pd.DataFrame) -> dict:
    cash_path = TREASURY_DAILY_OPS_DIR / "operating_cash_balance.csv"
    debt_path = TREASURY_DAILY_OPS_DIR / "debt_to_penny.csv"
    base = {
        "metadata": {
            "available": False,
            "notes": [
                "Validation compares Wednesday H.4.1 TGA observations to Daily Treasury Statement TGA closing balances on matching dates.",
                "Debt-to-the-penny totals are included as a daily debt context check for the debt-ceiling windows.",
            ],
        },
        "matched_rows": [],
        "summary": [],
    }
    if not cash_path.exists() or not debt_path.exists() or daily.empty:
        base["metadata"]["notes"].append("Raw DTS/debt-to-the-penny downloads are not present, so the validation appendix is empty.")
        return base

    cash = pd.read_csv(cash_path, low_memory=False)
    debt = pd.read_csv(debt_path, low_memory=False)
    if cash.empty or debt.empty:
        base["metadata"]["notes"].append("Raw DTS/debt-to-the-penny files were present but empty.")
        return base

    cash["record_date"] = pd.to_datetime(cash["record_date"], errors="coerce")
    cash["open_today_bal"] = pd.to_numeric(cash["open_today_bal"].replace("null", pd.NA), errors="coerce") / 1000.0
    cash = cash[cash["account_type"] == "Treasury General Account (TGA) Closing Balance"].copy()
    cash = cash.rename(columns={"record_date": "week", "open_today_bal": "dts_tga_bn"})
    debt["record_date"] = pd.to_datetime(debt["record_date"], errors="coerce")
    debt["tot_pub_debt_out_amt"] = pd.to_numeric(debt["tot_pub_debt_out_amt"], errors="coerce") / 1e9
    debt = debt.rename(columns={"record_date": "week", "tot_pub_debt_out_amt": "debt_to_penny_bn"})
    debt = debt[["week", "debt_to_penny_bn"]].dropna(subset=["week"])

    work = daily.copy()
    work["week"] = pd.to_datetime(work["week"], errors="coerce")
    work = work[["week", "tga_bn"]].dropna(subset=["week", "tga_bn"])
    matched = work.merge(cash[["week", "dts_tga_bn"]], on="week", how="inner")
    matched = matched.merge(debt, on="week", how="left")
    matched["gap_bn"] = matched["tga_bn"] - matched["dts_tga_bn"]
    matched = matched.sort_values("week").reset_index(drop=True)

    windows = [
        ("debt_ceiling_2023", pd.Timestamp("2023-01-01"), pd.Timestamp("2023-07-15")),
        ("debt_ceiling_2025", pd.Timestamp("2025-01-01"), pd.Timestamp("2025-07-15")),
    ]
    summary_rows = []
    for window_id, start, end in windows:
        sub = matched[(matched["week"] >= start) & (matched["week"] <= end)].copy()
        debt_sub = debt[(debt["week"] >= start) & (debt["week"] <= end)].dropna(subset=["debt_to_penny_bn"]).copy()
        if sub.empty:
            continue
        first = sub.iloc[0]
        last = sub.iloc[-1]
        debt_change = None
        if not debt_sub.empty:
            debt_change = float(debt_sub.iloc[-1]["debt_to_penny_bn"] - debt_sub.iloc[0]["debt_to_penny_bn"])
        summary_rows.append(
            {
                "window": window_id,
                "matched_days": int(len(sub)),
                "h41_start_tga_bn": float(first["tga_bn"]),
                "dts_start_tga_bn": float(first["dts_tga_bn"]),
                "start_gap_bn": float(first["gap_bn"]),
                "h41_end_tga_bn": float(last["tga_bn"]),
                "dts_end_tga_bn": float(last["dts_tga_bn"]),
                "end_gap_bn": float(last["gap_bn"]),
                "mean_abs_gap_bn": float(sub["gap_bn"].abs().mean()),
                "max_abs_gap_bn": float(sub["gap_bn"].abs().max()),
                "debt_change_bn": debt_change,
            }
        )

    base["metadata"].update(
        {
            "available": True,
            "coverage_start": matched["week"].min().strftime("%Y-%m-%d") if not matched.empty else None,
            "coverage_end": matched["week"].max().strftime("%Y-%m-%d") if not matched.empty else None,
        }
    )
    base["matched_rows"] = _ts_to_str(matched).to_dict(orient="records")
    base["summary"] = summary_rows
    return base


def _load_sectoral_fred_frame() -> pd.DataFrame:
    variables = load_variables()
    sector_specs = variables.get("fred_sectoral_series", {})
    if not sector_specs:
        return pd.DataFrame()
    files: dict[str, Path] = {}
    for spec in sector_specs.values():
        sid = spec["series_id"]
        for root in (RAW_DIR / "downloads" / "fred", RAW_DIR / "demo" / "fred"):
            path = root / f"{sid}.csv"
            if path.exists():
                files[sid] = path
                break
    if not files:
        return pd.DataFrame()
    return series_to_wide(files)


def _build_sectoral_absorbers_appendix(quarter: pd.DataFrame) -> dict | None:
    variables = load_variables()
    sector_specs = variables.get("fred_sectoral_series", {})
    wide = _load_sectoral_fred_frame()
    if wide.empty or "DATE" not in wide.columns or not sector_specs:
        return None

    rename_map = {spec["series_id"]: key for key, spec in sector_specs.items()}
    work = wide.rename(columns=rename_map).copy()
    work["DATE"] = pd.to_datetime(work["DATE"], errors="coerce")
    work = work.dropna(subset=["DATE"]).sort_values("DATE")
    work["quarter"] = work["DATE"].dt.to_period("Q").astype(str)
    keep_cols = ["quarter"] + list(rename_map.values())
    work = work[keep_cols].drop_duplicates(subset=["quarter"], keep="last")
    work = work.sort_values("quarter").reset_index(drop=True)

    quarter_start = str(quarter["quarter"].iloc[0]) if "quarter" in quarter.columns and not quarter.empty else None
    if quarter_start is not None:
        work = work[work["quarter"] >= quarter_start].copy()
    if work.empty or "all_private_treasuries" not in work.columns:
        return None

    level_cols = [col for col in work.columns if col != "quarter"]
    for col in level_cols:
        work[col] = pd.to_numeric(work[col], errors="coerce") / 1000.0

    selected_cols = [col for col in level_cols if col != "all_private_treasuries"]
    work["identified_private_sectors_bn"] = work[selected_cols].sum(axis=1, min_count=1)
    work["other_private_sectors"] = work["all_private_treasuries"] - work["identified_private_sectors_bn"]
    work["other_private_sectors"] = work["other_private_sectors"].where(work["other_private_sectors"].abs() > 1e-9, 0.0)
    work["other_private_sectors"] = work["other_private_sectors"].clip(lower=0)

    for col in selected_cols + ["other_private_sectors"]:
        work[f"{col}_share"] = work[col] / work["all_private_treasuries"]

    qdesc_path = TABLES_DIR / "quarterly_descriptive.csv"
    if qdesc_path.exists():
        qdesc = pd.read_csv(qdesc_path)
        qdesc_cols = [col for col in ["quarter", "net_private_duration_dv01", "buyback_offset_dv01"] if col in qdesc.columns]
        if qdesc_cols:
            work = work.merge(qdesc[qdesc_cols], on="quarter", how="left")

    rows = _ts_to_str(work).to_dict(orient="records")
    if not rows:
        return None

    qt2_rows = work[work["quarter"] >= "2022Q2"].copy()
    start = qt2_rows.iloc[0] if not qt2_rows.empty else work.iloc[0]
    latest = work.iloc[-1]
    summary_rows = []
    for key in SECTOR_ORDER:
        label = "Other private sectors" if key == "other_private_sectors" else sector_specs[key]["label"]
        summary_rows.append(
            {
                "sector": label,
                "sector_key": key,
                "latest_level_bn": None if pd.isna(latest.get(key)) else float(latest[key]),
                "latest_share": None if pd.isna(latest.get(f"{key}_share")) else float(latest[f"{key}_share"]),
                "change_since_qt2_bn": None if pd.isna(latest.get(key)) or pd.isna(start.get(key)) else float(latest[key] - start[key]),
            }
        )

    return {
        "metadata": {
            "source": "FRED Z.1 Financial Accounts quarterly Treasury-holder series",
            "units": "billions of dollars",
            "coverage_start": rows[0]["quarter"],
            "latest_quarter": rows[-1]["quarter"],
            "qt2_start_quarter": str(start["quarter"]),
            "notes": [
                "Levels are quarterly Z.1 holdings series from FRED converted from millions to billions of dollars.",
                "Rest of world combines foreign official and private holders in the published Z.1 sector aggregate.",
                "Other private sectors is the published total minus the sectors shown explicitly in this appendix.",
            ],
        },
        "sectors": [
            {"key": key, "label": "Other private sectors" if key == "other_private_sectors" else sector_specs[key]["label"]}
            for key in SECTOR_ORDER
        ],
        "series": rows,
        "summary": summary_rows,
    }


def _parse_term_years(term: str) -> float | None:
    if not isinstance(term, str) or not term.strip():
        return None
    years = 0.0
    matched = False
    y_match = pd.Series([term]).str.extract(r"(\d+)-Year", expand=False).iloc[0]
    m_match = pd.Series([term]).str.extract(r"(\d+)-Month", expand=False).iloc[0]
    d_match = pd.Series([term]).str.extract(r"(\d+)-Day", expand=False).iloc[0]
    w_match = pd.Series([term]).str.extract(r"(\d+)-Week", expand=False).iloc[0]
    if pd.notna(y_match):
        years += float(y_match)
        matched = True
    if pd.notna(m_match):
        years += float(m_match) / 12.0
        matched = True
    if pd.notna(w_match):
        years += float(w_match) / 52.0
        matched = True
    if pd.notna(d_match):
        years += float(d_match) / 365.0
        matched = True
    return years if matched else None


def _bucket_coupon_tenor(years: float | None) -> str | None:
    if years is None or pd.isna(years):
        return None
    for bucket, (lo, hi) in AUCTION_TENOR_BUCKETS.items():
        if lo < float(years) <= hi:
            return bucket
    return None


def _build_auction_mix_appendix(quarter: pd.DataFrame) -> dict | None:
    path = QRAWATCH_IMPORTS_DIR / "auctions_query.csv"
    if not path.exists():
        return None
    auctions = pd.read_csv(path, low_memory=False)
    if auctions.empty:
        return None

    auctions["issue_date"] = pd.to_datetime(auctions["issue_date"], errors="coerce")
    auctions["offering_bn"] = pd.to_numeric(auctions["offering_amt"], errors="coerce") / 1e9
    auctions = auctions.dropna(subset=["issue_date", "offering_bn", "security_type"]).copy()
    auctions["quarter"] = auctions["issue_date"].dt.to_period("Q").astype(str)
    if "quarter" in quarter.columns and not quarter.empty:
        quarter_min = str(quarter["quarter"].iloc[0])
        quarter_max = str(quarter["quarter"].iloc[-1])
        auctions = auctions[(auctions["quarter"] >= quarter_min) & (auctions["quarter"] <= quarter_max)].copy()
    if auctions.empty:
        return None

    auctions["floating_rate_flag"] = auctions["floating_rate"].fillna("No").astype(str).str.lower().eq("yes")
    auctions["is_bill"] = auctions["security_type"].astype(str).eq("Bill")
    auctions["is_frn"] = auctions["floating_rate_flag"]
    auctions["is_coupon"] = auctions["security_type"].astype(str).isin(["Note", "Bond"]) & ~auctions["is_frn"]
    auctions["term_years"] = auctions["security_term"].apply(_parse_term_years)
    auctions["coupon_bucket"] = auctions["term_years"].apply(_bucket_coupon_tenor)

    q = auctions.groupby("quarter", as_index=False).agg(
        total_offering_bn=("offering_bn", "sum"),
        bill_offering_bn=("offering_bn", lambda s: s[auctions.loc[s.index, "is_bill"]].sum()),
        frn_offering_bn=("offering_bn", lambda s: s[auctions.loc[s.index, "is_frn"]].sum()),
        coupon_offering_bn=("offering_bn", lambda s: s[auctions.loc[s.index, "is_coupon"]].sum()),
        coupon_wam_years=("offering_bn", lambda s: (s[auctions.loc[s.index, "is_coupon"]] * auctions.loc[s.index, "term_years"]).sum() / s[auctions.loc[s.index, "is_coupon"]].sum() if s[auctions.loc[s.index, "is_coupon"]].sum() else pd.NA),
        total_wam_years=("offering_bn", lambda s: (s * auctions.loc[s.index, "term_years"].fillna(0)).sum() / s.sum() if s.sum() else pd.NA),
        cmb_bill_offering_bn=("offering_bn", lambda s: s[(auctions.loc[s.index, "is_bill"]) & (auctions.loc[s.index, "cash_management_bill_cmb"].fillna("No").astype(str).str.lower().eq("yes"))].sum()),
    ).sort_values("quarter")

    coupon_tenor = (
        auctions.loc[auctions["is_coupon"] & auctions["coupon_bucket"].notna(), ["quarter", "coupon_bucket", "offering_bn"]]
        .groupby(["quarter", "coupon_bucket"])["offering_bn"]
        .sum()
        .unstack(fill_value=0)
        .reset_index()
    )
    q = q.merge(coupon_tenor, on="quarter", how="left")
    for bucket in AUCTION_TENOR_BUCKETS:
        if bucket not in q.columns:
            q[bucket] = 0.0

    q["bill_share"] = q["bill_offering_bn"] / q["total_offering_bn"]
    q["frn_share"] = q["frn_offering_bn"] / q["total_offering_bn"]
    q["coupon_share"] = q["coupon_offering_bn"] / q["total_offering_bn"]
    q["cmb_bill_share"] = q["cmb_bill_offering_bn"] / q["bill_offering_bn"]
    for bucket in AUCTION_TENOR_BUCKETS:
        q[f"{bucket}_share"] = q[bucket] / q["coupon_offering_bn"]

    rows = q.to_dict(orient="records")
    if not rows:
        return None
    qt2 = q[q["quarter"] >= "2022Q2"].copy()
    start = qt2.iloc[0] if not qt2.empty else q.iloc[0]
    latest = q.iloc[-1]
    summary = [
        {
            "metric": "Bills",
            "latest_amount_bn": float(latest["bill_offering_bn"]),
            "latest_share": float(latest["bill_share"]),
            "change_since_qt2_share_pp": float((latest["bill_share"] - start["bill_share"]) * 100.0),
        },
        {
            "metric": "Coupons",
            "latest_amount_bn": float(latest["coupon_offering_bn"]),
            "latest_share": float(latest["coupon_share"]),
            "change_since_qt2_share_pp": float((latest["coupon_share"] - start["coupon_share"]) * 100.0),
        },
        {
            "metric": "FRNs",
            "latest_amount_bn": float(latest["frn_offering_bn"]),
            "latest_share": float(latest["frn_share"]),
            "change_since_qt2_share_pp": float((latest["frn_share"] - start["frn_share"]) * 100.0),
        },
        {
            "metric": "Coupon WAM",
            "latest_amount_bn": float(latest["coupon_wam_years"]) if pd.notna(latest["coupon_wam_years"]) else None,
            "latest_share": None,
            "change_since_qt2_share_pp": float(latest["coupon_wam_years"] - start["coupon_wam_years"]) if pd.notna(latest["coupon_wam_years"]) and pd.notna(start["coupon_wam_years"]) else None,
        },
    ]
    tenor_summary = []
    for bucket in AUCTION_TENOR_BUCKETS:
        tenor_summary.append(
            {
                "tenor": bucket.replace("coupon_", "").replace("_bn", ""),
                "latest_amount_bn": float(latest[bucket]),
                "latest_share": float(latest[f"{bucket}_share"]) if pd.notna(latest[f"{bucket}_share"]) else None,
            }
        )

    return {
        "metadata": {
            "source": "Treasury Fiscal Data auction results imported via qrawatch",
            "coverage_start": rows[0]["quarter"],
            "latest_quarter": rows[-1]["quarter"],
            "qt2_start_quarter": str(start["quarter"]),
            "notes": [
                "Quarters are grouped by issue date, so the appendix follows when securities actually settled into the market.",
                "Bills include cash-management bills; FRNs are separated from fixed-rate coupons.",
                "Coupon tenor shares are reported as shares of fixed-rate coupon issuance, not shares of total issuance.",
            ],
        },
        "series": rows,
        "summary": summary,
        "tenor_summary": tenor_summary,
    }


def main() -> None:
    ensure_repo_dirs()

    # Load processed panels
    quarter = read_best_table(PROCESSED_DIR / "refunding_panel.parquet")
    weekly = read_best_table(PROCESSED_DIR / "master_weekly_panel.parquet")
    daily = read_best_table(PROCESSED_DIR / "master_daily_panel.parquet")
    episodes = read_best_table(PROCESSED_DIR / "episode_registry.parquet")

    # Load econometric outputs
    reaction = pd.read_csv(OUTPUTS_TABLES_DIR / "reaction_function_main.csv")
    lp_dealer = pd.read_csv(OUTPUTS_TABLES_DIR / "main_lp_dealer.csv")
    lp_repo = pd.read_csv(OUTPUTS_TABLES_DIR / "main_lp_repo.csv")

    # Publish econometric tables (supporting evidence)
    publish_table(reaction, "reaction_function_main")
    publish_table(lp_dealer, "main_lp_dealer")
    publish_table(lp_repo, "main_lp_repo")

    # Publish episode registry
    publish_table(_ts_to_str(episodes), "episode_registry")

    # Publish full panels (for dynamic site charts)
    # Weekly: select key columns to keep JSON size manageable
    weekly_cols = [
        "week", "soma_treasuries_bn", "reserves_bn", "tga_bn", "on_rrp_bn",
        "dealer_inventory_bn", "repo_spread_bp", "system_liquidity_bn",
        "low_liquidity", "qt_runoff_dv01", "qt_runoff_source", "qt_runoff_proxy_bn",
        "coupon_dv01_shock", "bill_dv01_offset", "mix_shock_dv01",
        "buyback_offset_dv01", "expected_soma_redemptions_dv01",
        "duration_pressure_dv01", "fed_pressure_dv01",
        "quarter", "calendar_quarter", "input_source",
    ]
    weekly_cols = [c for c in weekly_cols if c in weekly.columns]
    weekly_pub = _ts_to_str(weekly[weekly_cols])
    # Only publish from 2003+ (when SOMA data starts being reliable)
    weekly_pub = weekly_pub[weekly_pub["week"] >= "2003-01-01"]
    publish_table(weekly_pub, "weekly_panel")

    daily_appendix = _build_daily_mechanics_appendix(daily)
    if not daily_appendix.empty:
        publish_table(daily_appendix, "daily_mechanics_appendix")
    daily_validation = _build_daily_validation_appendix(daily)
    write_json(PUBLISH_DIR / "daily_validation_appendix.json", daily_validation)
    write_json(ROOT / "site" / "data" / "daily_validation_appendix.json", daily_validation)

    sectoral_appendix = _build_sectoral_absorbers_appendix(quarter)
    if sectoral_appendix is not None:
        write_json(PUBLISH_DIR / "sectoral_absorbers_appendix.json", sectoral_appendix)
        write_json(ROOT / "site" / "data" / "sectoral_absorbers_appendix.json", sectoral_appendix)

    auction_mix_appendix = _build_auction_mix_appendix(quarter)
    if auction_mix_appendix is not None:
        write_json(PUBLISH_DIR / "auction_mix_appendix.json", auction_mix_appendix)
        write_json(ROOT / "site" / "data" / "auction_mix_appendix.json", auction_mix_appendix)

    # Quarterly panel: full
    publish_table(_ts_to_str(quarter), "quarterly_panel")

    # Publish descriptive tables (core evidence)
    for table_name in ["regime_summary", "episode_summary", "quarterly_descriptive", "correlation_matrix"]:
        path = TABLES_DIR / f"{table_name}.csv"
        if path.exists():
            df = pd.read_csv(path)
            publish_table(_ts_to_str(df), table_name)
    qt_compare_path = TABLES_DIR / "qt_comparison_summary.csv"
    if qt_compare_path.exists():
        publish_table(_ts_to_str(pd.read_csv(qt_compare_path)), "qt_comparison_summary")

    manual_overrides = pd.read_csv(MANUAL_DIR / "refunding_manual_overrides.csv")
    manual_input_audit = _build_manual_input_audit(manual_overrides)
    write_json(PUBLISH_DIR / "manual_input_audit.json", manual_input_audit)
    write_json(ROOT / "site" / "data" / "manual_input_audit.json", manual_input_audit)

    data_files = [
        "weekly_panel.json",
        "quarterly_panel.json",
        "episode_registry.json",
        "regime_summary.json",
        "episode_summary.json",
        "qt_comparison_summary.json",
        "quarterly_descriptive.json",
        "correlation_matrix.json",
        "reaction_function_main.json",
        "main_lp_dealer.json",
        "main_lp_repo.json",
        "manual_input_audit.json",
        "daily_mechanics_appendix.json",
        "daily_validation_appendix.json",
        "sectoral_absorbers_appendix.json",
        "auction_mix_appendix.json",
    ]
    artifact_hashes = []
    for name in data_files:
        site_path = ROOT / "site" / "data" / name
        artifact_hashes.append({
            "file": name,
            "sha256": _sha256(site_path),
            "size_bytes": site_path.stat().st_size,
        })

    quarterly_window_start = None
    if "quarter" in quarter.columns and not quarter.empty:
        quarterly_window_start = str(quarter["quarter"].iloc[0])
    dealer_window_start = _first_non_null_week(weekly, "dealer_inventory_bn", min_week="2003-01-01")
    repo_window_start = _first_non_null_week(weekly, "repo_spread_bp", min_week="2003-01-01")

    # Summary metadata
    summary = {
        "project": "CoordWatch",
        "generated_at_utc": timestamp_utc(),
        "quarter_rows": int(len(quarter)),
        "weekly_rows": int(len(weekly_pub)),
        "runoff_source_counts": weekly_pub.get("qt_runoff_source", pd.Series(dtype=object)).fillna("unknown").value_counts().to_dict(),
        "episode_rows": int(len(episodes)),
        "reaction_n_obs": int(reaction["n_obs"].iloc[0]) if not reaction.empty else None,
        "weekly_frequency": "W-WED",
        "site_window_start": "2003-01-01",
        "quarterly_window_start": quarterly_window_start,
        "dealer_window_start": dealer_window_start,
        "repo_window_start": repo_window_start,
        "headline_reaction_terms": reaction[["term", "coef", "p_value"]].to_dict(orient="records"),
        "measurement_notes": [
            {
                "title": "Frequency alignment",
                "detail": "The public weekly panel uses Wednesday observations. Daily money-market inputs are kept separate before weekly aggregation."
            },
            {
                "title": "Runoff measure",
                "detail": "Weekly qt_runoff_dv01 compares consecutive New York Fed SOMA holdings snapshots at the CUSIP level, then weights holdings declines by remaining maturity and coupon-duration proxies. When holdings detail is unavailable, the series falls back to a quarterly allocation; qt_runoff_proxy_bn remains a separate level-change monitor."
            },
            {
                "title": "Runoff coverage",
                "detail": "Most public-window weeks use holdings-detail runoff. A small number of early-window and holiday weeks are marked as holdings gaps when the New York Fed payload does not carry usable security identifiers."
            },
            {
                "title": "Liquidity state",
                "detail": "Quarterly low_liquidity_prev is derived from average weekly system liquidity, defined as reserves plus ON RRP."
            },
            {
                "title": "Manual review",
                "detail": "Episode windows and selected refunding inputs use manual review files. Debt-limit periods are treated as confounded windows rather than clean policy comparisons."
            },
            {
                "title": "Sample coverage",
                "detail": f"The public weekly site window starts in 2003, the quarterly refunding panel starts in {quarterly_window_start or 'the first available quarter'}, dealer series begin in {dealer_window_start or 'the first available week'}, and repo spread coverage begins in {repo_window_start or 'the first available week'}."
            },
            {
                "title": "Daily cash appendix",
                "detail": "The debt-ceiling appendix uses the processed daily panel with ON RRP at daily frequency and H.4.1 balance-sheet fields carried forward between release dates."
            },
            {
                "title": "DTS cross-check appendix",
                "detail": "The cash appendix now includes a cross-check layer that compares Wednesday H.4.1 TGA observations to Daily Treasury Statement TGA closing balances on matching dates and adds debt-to-the-penny totals for window-level debt changes."
            },
            {
                "title": "Sectoral absorber appendix",
                "detail": "The sector appendix uses quarterly FRED Z.1 Treasury-holder series, converted from millions to billions, to track published holdings across households, banks, funds, dealers, rest of world, and residual private sectors."
            },
            {
                "title": "Auction mix appendix",
                "detail": "The auction appendix groups Treasury auction results by issue date and reports realized bill, coupon, and FRN shares plus fixed-rate coupon tenor shares."
            },
            {
                "title": "Interpretation",
                "detail": "Balance-sheet identities and descriptive comparisons are primary. Correlations and regressions are supplementary checks, not stand-alone proof of intent."
            }
        ],
        "data_files": data_files,
        "artifact_hashes": artifact_hashes,
    }
    write_json(PUBLISH_DIR / "summary.json", summary)
    write_json(ROOT / "site" / "data" / "summary.json", summary)
    logger.info("Built publish artifacts: %s data files", len(summary["data_files"]))


if __name__ == "__main__":
    main()
