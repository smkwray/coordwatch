
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from coordwatch.construct.liquidity import add_liquidity_state, add_repo_spreads, compute_qt_runoff_proxy
from coordwatch.io import write_csv, write_parquet
from coordwatch.logging_utils import get_logger
from coordwatch.paths import PROCESSED_DIR, RAW_DIR, ensure_repo_dirs

LOGGER = get_logger(__name__)


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


def build_weekly_master_panel(refunding_panel: pd.DataFrame, prefer_real: bool = True, output_dir: Path | None = None) -> pd.DataFrame:
    ensure_repo_dirs()
    out_dir = output_dir or PROCESSED_DIR
    # Prefer real FRED data over demo
    fred = _load_fred_raw() if prefer_real else pd.DataFrame()
    if not fred.empty:
        pd_raw = _load_primary_dealer_raw()
        weekly = fred.copy()
        if not pd_raw.empty:
            rename_map = {
                "PDPOSGST-TOT": "dealer_inventory_bn",
                "PDSORA-UTSETTOT": "repo_agreements_tsy_bn",
                "PDSIRRA-UTSETTOT": "reverse_repo_agreements_tsy_bn",
            }
            pd_raw = pd_raw.rename(columns=rename_map)
            weekly = weekly.merge(pd_raw, left_on="week", right_on="date", how="left")
            weekly = weekly.drop(columns=[c for c in ["date"] if c in weekly.columns])
        source = "raw"

        if "system_liquidity_bn" not in weekly.columns and {"reserves_bn", "on_rrp_bn"}.issubset(weekly.columns):
            weekly["system_liquidity_bn"] = weekly["reserves_bn"] + weekly["on_rrp_bn"]
        weekly = add_liquidity_state(weekly)
        weekly = add_repo_spreads(weekly)
        if "qt_runoff_dv01" not in weekly.columns and "soma_treasuries_bn" in weekly.columns:
            weekly["qt_runoff_dv01"] = compute_qt_runoff_proxy(weekly["soma_treasuries_bn"])
        if "dealer_inventory_bn" not in weekly.columns:
            weekly["dealer_inventory_bn"] = np.nan
    else:
        demo = _load_demo_weekly()
        if not demo.empty:
            weekly = demo.copy()
            source = "demo"
        else:
            raise FileNotFoundError("No weekly raw inputs found. Run demo seed or download FRED/NY Fed data.")

        if "system_liquidity_bn" not in weekly.columns and {"reserves_bn", "on_rrp_bn"}.issubset(weekly.columns):
            weekly["system_liquidity_bn"] = weekly["reserves_bn"] + weekly["on_rrp_bn"]
        weekly = add_liquidity_state(weekly)
        weekly = add_repo_spreads(weekly)
        if "qt_runoff_dv01" not in weekly.columns and "soma_treasuries_bn" in weekly.columns:
            weekly["qt_runoff_dv01"] = compute_qt_runoff_proxy(weekly["soma_treasuries_bn"])
        if "dealer_inventory_bn" not in weekly.columns:
            weekly["dealer_inventory_bn"] = np.nan
        weekly["dealer_inventory_lag1"] = weekly["dealer_inventory_bn"].shift(1)
        weekly["repo_spread_lag1"] = weekly.get("repo_spread_bp", pd.Series(index=weekly.index, dtype=float)).shift(1)
        weekly["tga_change_bn"] = weekly.get("tga_bn", pd.Series(index=weekly.index, dtype=float)).diff().fillna(0)
        weekly["quarter_end_flag"] = weekly["week"].dt.is_quarter_end.astype(int)

    weekly["week"] = pd.to_datetime(weekly["week"], errors="coerce")
    weekly = weekly.sort_values("week").reset_index(drop=True)
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
    weekly = pd.merge_asof(
        weekly,
        attach,
        left_on="week",
        right_on="refunding_date",
        direction="backward",
        suffixes=("", "_attach"),
    )
    for col in [
        "coupon_dv01_shock",
        "bill_dv01_offset",
        "mix_shock_dv01",
        "buyback_offset_dv01",
        "expected_soma_redemptions_dv01",
        "classification_prior",
        "clean_sample_flag",
    ]:
        attach_col = f"{col}_attach"
        if col not in weekly.columns and attach_col in weekly.columns:
            weekly[col] = weekly[attach_col]
        elif attach_col in weekly.columns:
            weekly[col] = weekly[col].combine_first(weekly[attach_col])
            weekly = weekly.drop(columns=[attach_col])
    weekly["quarter"] = weekly["week"].dt.to_period("Q").astype(str)
    if "qt_runoff_dv01" not in weekly.columns:
        weekly["qt_runoff_dv01"] = 0.0
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
    weekly["fed_pressure_x_low_liquidity"] = (weekly["fed_pressure_dv01"] * weekly["low_liquidity_prev"]).round(2)
    weekly["dealer_inventory_lag1"] = weekly["dealer_inventory_bn"].shift(1)
    weekly["repo_spread_lag1"] = weekly["repo_spread_bp"].shift(1)
    weekly["tga_change_bn"] = weekly.get("tga_change_bn", pd.Series(index=weekly.index, dtype=float)).fillna(0)
    weekly["quarter_end_flag"] = weekly.get("quarter_end_flag", pd.Series(index=weekly.index, dtype=float)).fillna(0).astype(int)
    weekly["input_source"] = source

    write_csv(weekly, out_dir / "master_weekly_panel.csv")
    write_parquet(weekly, out_dir / "master_weekly_panel.parquet")
    LOGGER.info("Built weekly master panel with %s rows from %s inputs", len(weekly), source)
    return weekly
