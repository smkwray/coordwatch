
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from coordwatch.construct.liquidity import add_liquidity_state, add_qt2_liquidity_state, add_repo_spreads, compute_qt_runoff_proxy
from coordwatch.construct.refunding import attach_quarterly_liquidity_state
from coordwatch.io import write_csv, write_parquet
from coordwatch.logging_utils import get_logger
from coordwatch.paths import PROCESSED_DIR, RAW_DIR, ensure_repo_dirs
from coordwatch.utils.soma import estimate_runoff_duration_equivalent, load_soma_holdings_frame, prefetch_soma_holdings

LOGGER = get_logger(__name__)
SOMA_RELIABILITY_START = pd.Timestamp("2003-01-01")


def _load_demo_weekly() -> pd.DataFrame:
    candidates = [
        RAW_DIR / "demo" / "weekly_master_demo.parquet",
        RAW_DIR / "demo" / "weekly_master_demo.csv",
    ]
    for path in candidates:
        if path.exists():
            if path.suffix == ".parquet":
                return pd.read_parquet(path)
            return pd.read_csv(path)
    return pd.DataFrame()


def _load_fred_raw() -> pd.DataFrame:
    mapping = {
        "WSHOTSL": "soma_treasuries_bn",
        "WRESBAL": "reserves_bn",
        "WTREGEN": "tga_bn",
        "RRPONTSYD": "on_rrp_bn",
        "RRPONTSYAWARD": "on_rrp_award_rate",
        "TGCR": "tgcr_rate",
        "IORB": "iorb_rate",
        "DGS2": "DGS2",
        "DGS5": "DGS5",
        "DGS10": "DGS10",
        "DGS20": "DGS20",
        "DGS30": "DGS30",
    }
    # FRED series in millions that need conversion to billions
    _millions_to_bn = {"WSHOTSL", "WRESBAL", "WTREGEN"}
    frames = []
    for sid, name in mapping.items():
        path = RAW_DIR / "downloads" / "fred" / f"{sid}.csv"
        if not path.exists():
            continue
        df = pd.read_csv(path)
        if {"DATE", "VALUE"}.issubset(df.columns):
            tmp = df[["DATE", "VALUE"]].copy()
            tmp["DATE"] = pd.to_datetime(tmp["DATE"], errors="coerce")
            tmp[name] = pd.to_numeric(tmp["VALUE"].replace(".", np.nan), errors="coerce")
            if sid in _millions_to_bn:
                tmp[name] = tmp[name] / 1000.0
            frames.append(tmp[["DATE", name]])
    if not frames:
        return pd.DataFrame()
    out = frames[0]
    for frame in frames[1:]:
        out = out.merge(frame, on="DATE", how="outer")
    out = out.sort_values("DATE").rename(columns={"DATE": "week"}).reset_index(drop=True)
    return out


def _load_primary_dealer_raw() -> pd.DataFrame:
    import json

    # Try NY Fed API JSON files first (from script 05)
    nyfed_dir = RAW_DIR / "downloads" / "nyfed"
    json_candidates = sorted(nyfed_dir.glob("*_history_candidate_*.json")) if nyfed_dir.exists() else []
    if json_candidates:
        parts = []
        for path in json_candidates:
            data = json.loads(path.read_text())
            obs = data.get("pd", {}).get("timeseries", [])
            if not obs:
                obs = data.get("pd", {}).get("observations", [])
            if obs:
                df = pd.DataFrame(obs)
                if {"asofdate", "keyid", "value"}.issubset(df.columns):
                    keyid = df["keyid"].iloc[0]
                    tmp = df[["asofdate", "value"]].copy()
                    tmp = tmp.rename(columns={"asofdate": "date"})
                    tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce")
                    # Values are in millions; convert to billions
                    tmp[keyid] = pd.to_numeric(tmp["value"], errors="coerce") / 1000.0
                    parts.append(tmp[["date", keyid]])
        if parts:
            out = parts[0]
            for part in parts[1:]:
                out = out.merge(part, on="date", how="outer")
            return out.sort_values("date")

    # Fallback: demo CSV
    manual_demo = RAW_DIR / "demo" / "nyfed" / "primary_dealer_demo.csv"
    if manual_demo.exists():
        df = pd.read_csv(manual_demo)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return df

    # Fallback: manual CSV directory
    manual_dir = nyfed_dir / "manual" if nyfed_dir.exists() else None
    csv_candidates = list(manual_dir.glob("*.csv")) if manual_dir and manual_dir.exists() else []
    if csv_candidates:
        parts = []
        for path in csv_candidates:
            df = pd.read_csv(path)
            lower = {c.lower(): c for c in df.columns}
            date_col = lower.get("date") or lower.get("week") or lower.get("asofdate")
            value_cols = [c for c in df.columns if c != date_col]
            if date_col and value_cols:
                tmp = df[[date_col] + value_cols].copy().rename(columns={date_col: "date"})
                tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce")
                parts.append(tmp)
        if parts:
            out = parts[0]
            for part in parts[1:]:
                out = out.merge(part, on="date", how="outer")
            return out.sort_values("date")
    return pd.DataFrame()


def _build_raw_daily_panel() -> pd.DataFrame:
    fred = _load_fred_raw()
    if fred.empty:
        return pd.DataFrame()

    daily = fred.copy()
    pd_raw = _load_primary_dealer_raw()
    if not pd_raw.empty:
        rename_map = {
            "PDPOSGST-TOT": "dealer_inventory_bn",
            "PDSORA-UTSETTOT": "repo_agreements_tsy_bn",
            "PDSIRRA-UTSETTOT": "reverse_repo_agreements_tsy_bn",
        }
        pd_raw = pd_raw.rename(columns=rename_map)
        daily = daily.merge(pd_raw, left_on="week", right_on="date", how="left")
        daily = daily.drop(columns=[c for c in ["date"] if c in daily.columns])

    if "system_liquidity_bn" not in daily.columns and {"reserves_bn", "on_rrp_bn"}.issubset(daily.columns):
        daily["system_liquidity_bn"] = daily["reserves_bn"] + daily["on_rrp_bn"]
    daily = add_repo_spreads(daily)
    daily["calendar_quarter"] = daily["week"].dt.to_period("Q").astype(str)
    daily["input_source"] = "raw"
    return daily.sort_values("week").reset_index(drop=True)


def _derive_true_weekly_panel(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return daily

    daily = daily.copy().sort_values("week").drop_duplicates(subset=["week"], keep="last")
    calendar = pd.DataFrame({
        "week": pd.date_range(daily["week"].min().normalize(), daily["week"].max().normalize(), freq="D"),
    })
    full = calendar.merge(daily, on="week", how="left").sort_values("week").reset_index(drop=True)
    fill_cols = [c for c in full.columns if c != "week"]
    full[fill_cols] = full[fill_cols].ffill()
    weekly = full[full["week"].dt.weekday == 2].copy().reset_index(drop=True)
    weekly["calendar_quarter"] = weekly["week"].dt.to_period("Q").astype(str)
    return weekly


def _attach_quarterly_shocks(weekly: pd.DataFrame, refunding_panel: pd.DataFrame) -> pd.DataFrame:
    if weekly.empty:
        return weekly

    refunding_panel = refunding_panel.copy().sort_values("refunding_date")
    refunding_panel["refunding_date"] = pd.to_datetime(refunding_panel["refunding_date"], errors="coerce")
    attach_cols = [
        "quarter",
        "refunding_date",
        "coupon_dv01_shock",
        "bill_dv01_offset",
        "mix_shock_dv01",
        "buyback_offset_dv01",
        "expected_soma_redemptions_dv01",
        "classification_prior",
        "clean_sample_flag",
    ]
    attach = refunding_panel[attach_cols].copy()
    out = pd.merge_asof(
        weekly.sort_values("week"),
        attach,
        left_on="week",
        right_on="refunding_date",
        direction="backward",
        suffixes=("", "_attach"),
    )
    out["quarter"] = out["quarter"].fillna(out["calendar_quarter"])
    return out


def _allocate_weekly_runoff_dv01(weekly: pd.DataFrame) -> pd.Series:
    if "expected_soma_redemptions_dv01" not in weekly.columns:
        return pd.Series(0.0, index=weekly.index)
    obs_per_quarter = weekly.groupby("quarter")["week"].transform("count").replace(0, np.nan)
    runoff = pd.to_numeric(weekly["expected_soma_redemptions_dv01"], errors="coerce").fillna(0) / obs_per_quarter
    return runoff.fillna(0).round(2)


def _compute_holdings_based_weekly_runoff(weekly: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    if weekly.empty or "week" not in weekly.columns:
        return pd.Series(0.0, index=weekly.index), pd.Series("quarterly_allocation", index=weekly.index)

    weekly_dates = (
        weekly.loc[weekly["week"].notna() & (weekly["week"] >= SOMA_RELIABILITY_START), "week"]
        .drop_duplicates()
        .sort_values()
    )
    if not weekly_dates.empty:
        stats = prefetch_soma_holdings(weekly_dates, max_workers=4)
        LOGGER.info(
            "SOMA holdings prefetch requested=%s downloaded=%s failed=%s",
            stats["requested"],
            stats["downloaded"],
            stats["failed"],
        )

    fallback = _allocate_weekly_runoff_dv01(weekly)
    values = pd.Series(np.nan, index=weekly.index, dtype=float)
    sources = pd.Series("quarterly_allocation", index=weekly.index, dtype=object)
    curve_cols = ["DGS2", "DGS5", "DGS10", "DGS20", "DGS30"]
    weekly_sorted = weekly.sort_values("week").copy()
    last_valid_week = pd.NaT
    last_valid_holdings = pd.DataFrame()
    for idx, row in weekly_sorted.iterrows():
        week = row.get("week", pd.NaT)
        if pd.isna(week):
            values.loc[idx] = 0.0
            continue
        if week < SOMA_RELIABILITY_START:
            values.loc[idx] = round(float(fallback.loc[idx]), 3)
            continue
        try:
            current_holdings = load_soma_holdings_frame(week)
            if current_holdings.empty or "cusip" not in current_holdings.columns or current_holdings["cusip"].dropna().empty:
                values.loc[idx] = 0.0
                sources.loc[idx] = "holdings_gap"
                continue
            if last_valid_holdings.empty or pd.isna(last_valid_week):
                values.loc[idx] = 0.0
                sources.loc[idx] = "holdings_detail"
                last_valid_holdings = current_holdings
                last_valid_week = week
                continue
            curve = {col: row.get(col, np.nan) for col in curve_cols}
            runoff = estimate_runoff_duration_equivalent(last_valid_holdings, current_holdings, curve, last_valid_week)
            values.loc[idx] = round(runoff, 3)
            sources.loc[idx] = "holdings_detail"
            last_valid_holdings = current_holdings
            last_valid_week = week
            continue
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Falling back to quarterly runoff allocation for %s: %s", week.date(), exc)
        values.loc[idx] = round(float(fallback.loc[idx]), 3)
    return values.fillna(0.0), sources


def build_weekly_master_panel(refunding_panel: pd.DataFrame, prefer_real: bool = True, output_dir: Path | None = None) -> pd.DataFrame:
    ensure_repo_dirs()
    out_dir = output_dir or PROCESSED_DIR
    daily = _build_raw_daily_panel() if prefer_real else pd.DataFrame()
    if not daily.empty:
        weekly = _derive_true_weekly_panel(daily)
        source = "raw"
        write_csv(daily, out_dir / "master_daily_panel.csv")
        write_parquet(daily, out_dir / "master_daily_panel.parquet")
    else:
        demo = _load_demo_weekly()
        if not demo.empty:
            weekly = demo.copy()
            source = "demo"
        else:
            raise FileNotFoundError("No weekly raw inputs found. Run demo seed or download FRED/NY Fed data.")

    weekly["week"] = pd.to_datetime(weekly["week"], errors="coerce")
    weekly = weekly.sort_values("week").reset_index(drop=True)
    weekly["calendar_quarter"] = weekly.get("calendar_quarter", weekly["week"].dt.to_period("Q").astype(str))
    if "system_liquidity_bn" not in weekly.columns and {"reserves_bn", "on_rrp_bn"}.issubset(weekly.columns):
        weekly["system_liquidity_bn"] = weekly["reserves_bn"] + weekly["on_rrp_bn"]
    if "repo_spread_bp" not in weekly.columns:
        weekly = add_repo_spreads(weekly)
    if "dealer_inventory_bn" not in weekly.columns:
        weekly["dealer_inventory_bn"] = np.nan
    weekly = _attach_quarterly_shocks(weekly, refunding_panel)
    weekly["qt_runoff_proxy_bn"] = compute_qt_runoff_proxy(weekly["soma_treasuries_bn"]) if "soma_treasuries_bn" in weekly.columns else 0.0
    if source == "raw":
        runoff_values, runoff_source = _compute_holdings_based_weekly_runoff(weekly)
        weekly["qt_runoff_dv01"] = runoff_values.round(3)
        weekly["qt_runoff_source"] = runoff_source
    else:
        weekly["qt_runoff_dv01"] = weekly.get("qt_runoff_dv01", _allocate_weekly_runoff_dv01(weekly)).fillna(0).round(3)
        weekly["qt_runoff_source"] = weekly.get(
            "qt_runoff_source",
            pd.Series("demo_seed", index=weekly.index, dtype=object),
        )
    weekly["coupon_dv01_shock"] = weekly.get("coupon_dv01_shock", pd.Series(index=weekly.index, dtype=float)).fillna(0)
    weekly["bill_dv01_offset"] = weekly.get("bill_dv01_offset", pd.Series(index=weekly.index, dtype=float)).fillna(0)
    weekly["mix_shock_dv01"] = weekly.get("mix_shock_dv01", pd.Series(index=weekly.index, dtype=float)).fillna(0)
    weekly["buyback_offset_dv01"] = weekly.get("buyback_offset_dv01", pd.Series(index=weekly.index, dtype=float)).fillna(0)
    weekly["duration_pressure_dv01"] = (
        weekly["qt_runoff_dv01"].fillna(0) + weekly["coupon_dv01_shock"] - weekly["buyback_offset_dv01"]
    ).round(2)
    weekly["fed_pressure_dv01"] = (
        weekly["duration_pressure_dv01"] - weekly["bill_dv01_offset"]
    ).round(2)
    if "system_liquidity_bn" not in weekly.columns and {"reserves_bn", "on_rrp_bn"}.issubset(weekly.columns):
        weekly["system_liquidity_bn"] = (weekly["reserves_bn"] + weekly["on_rrp_bn"]).round(2)
    if "low_liquidity" not in weekly.columns and "system_liquidity_bn" in weekly.columns:
        weekly = add_liquidity_state(weekly)
    if "low_liquidity_prev" not in weekly.columns:
        weekly["low_liquidity_prev"] = weekly["low_liquidity"].shift(1).fillna(0).astype(int)
    if "qt2_low_liquidity" not in weekly.columns and "system_liquidity_bn" in weekly.columns:
        weekly = add_qt2_liquidity_state(weekly)
    weekly["fed_pressure_x_low_liquidity"] = (weekly["fed_pressure_dv01"] * weekly["low_liquidity_prev"]).round(2)
    weekly["dealer_inventory_lag1"] = weekly["dealer_inventory_bn"].shift(1)
    weekly["repo_spread_lag1"] = weekly["repo_spread_bp"].shift(1)
    weekly["tga_change_bn"] = weekly.get("tga_bn", pd.Series(index=weekly.index, dtype=float)).diff().fillna(0)
    quarter_end = weekly.groupby("calendar_quarter")["week"].transform("max") == weekly["week"]
    weekly["quarter_end_flag"] = quarter_end.astype(int)
    weekly["input_source"] = source

    refunding_enriched = attach_quarterly_liquidity_state(refunding_panel, weekly)
    write_csv(refunding_enriched, out_dir / "refunding_panel.csv")
    write_parquet(refunding_enriched, out_dir / "refunding_panel.parquet")

    write_csv(weekly, out_dir / "master_weekly_panel.csv")
    write_parquet(weekly, out_dir / "master_weekly_panel.parquet")
    LOGGER.info("Built weekly master panel with %s rows from %s inputs", len(weekly), source)
    return weekly
