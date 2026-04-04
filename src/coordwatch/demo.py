
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from coordwatch.io import write_csv, write_json, write_parquet
from coordwatch.paths import INTERIM_DIR, RAW_DIR, REFERENCE_DIR, ensure_repo_dirs
from coordwatch.utils.dates import refunding_date_for_quarter


def _quarterly_demo(seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    quarters = pd.period_range("2016Q1", "2026Q4", freq="Q")
    df = pd.DataFrame({"quarter": quarters.astype(str)})
    df["refunding_date"] = [refunding_date_for_quarter(q) for q in quarters]
    df["year"] = quarters.year
    df["qnum"] = quarters.quarter

    borrowing = 420 + 18 * np.arange(len(df)) / 4 + rng.normal(0, 25, len(df))
    cash = 550 + rng.normal(0, 35, len(df))
    debt_limit = np.zeros(len(df), dtype=int)
    buyback = np.zeros(len(df))
    expected_soma = np.zeros(len(df))

    for i, q in enumerate(quarters.astype(str)):
        if "2020Q" in q:
            borrowing[i] += 1200 if q in {"2020Q2", "2020Q3"} else 700
            cash[i] += 250
        if q in {"2023Q2", "2023Q3", "2025Q3"}:
            debt_limit[i] = 1
        if q >= "2024Q2":
            buyback[i] = max(0, rng.normal(2.5, 0.6))

        if "2017Q4" <= q <= "2018Q4":
            expected_soma[i] = 14 + 2 * (i % 4) + rng.normal(0, 1.0)
        elif "2020Q2" <= q <= "2021Q1":
            expected_soma[i] = -18 + rng.normal(0, 1.2)
        elif "2022Q3" <= q <= "2024Q1":
            expected_soma[i] = 28 + rng.normal(0, 1.5)
        elif "2024Q2" <= q <= "2025Q2":
            expected_soma[i] = 18 + rng.normal(0, 1.2)
        elif "2025Q3" <= q <= "2025Q4":
            expected_soma[i] = 9 + rng.normal(0, 1.0)
        elif q >= "2026Q1":
            expected_soma[i] = -6 + rng.normal(0, 0.7)
        else:
            expected_soma[i] = rng.normal(0, 0.5)

    df["privately_held_net_marketable_borrowing_bn"] = borrowing.round(2)
    df["cash_balance_assumption_bn"] = cash.round(2)
    df["debt_limit_flag"] = debt_limit
    df["buyback_offset_dv01"] = buyback.round(2)
    df["expected_soma_redemptions_dv01"] = expected_soma.round(2)

    # Proxy system liquidity at quarterly frequency to determine the lagged low-liquidity state.
    sys_liq_q = []
    value = 3300.0
    for q, es in zip(df["quarter"], df["expected_soma_redemptions_dv01"]):
        if "2020Q2" <= q <= "2021Q2":
            value += 180 + rng.normal(0, 25)
        elif es > 0:
            value -= 80 + 2.3 * es + rng.normal(0, 15)
        else:
            value += 35 + rng.normal(0, 10)
        if q == "2025Q3":
            value -= 160
        sys_liq_q.append(value)
    df["system_liquidity_q_bn"] = np.round(sys_liq_q, 2)
    threshold = np.nanpercentile(df["system_liquidity_q_bn"], 35)
    df["low_liquidity"] = (df["system_liquidity_q_bn"] <= threshold).astype(int)
    df["low_liquidity_prev"] = df["low_liquidity"].shift(1).fillna(0).astype(int)

    coupon = 0.33 * df["expected_soma_redemptions_dv01"] + 0.04 * (df["privately_held_net_marketable_borrowing_bn"] - 500)
    coupon += rng.normal(0, 2.2, len(df))

    bill_offset = 1.4 + 0.12 * np.maximum(df["expected_soma_redemptions_dv01"], 0)
    bill_offset += 4.3 * df["low_liquidity_prev"]
    bill_offset += rng.normal(0, 1.0, len(df))
    bill_offset = np.maximum(bill_offset, 0)

    # Force a documented negative-control style episode in mid-2022.
    mask_negative_control = df["quarter"].isin(["2022Q2", "2022Q3"])
    coupon.loc[mask_negative_control] -= 10
    bill_offset.loc[mask_negative_control] += 2

    # QT1 modern positive case.
    mask_qt1 = (df["quarter"] >= "2017Q4") & (df["quarter"] <= "2018Q4")
    coupon.loc[mask_qt1] += 4

    # Offsetting in late 2025 when liquidity is thin.
    mask_2025 = df["quarter"].isin(["2025Q3", "2025Q4"])
    bill_offset.loc[mask_2025] += 5

    df["coupon_dv01_shock"] = coupon.round(2)
    df["bill_dv01_offset"] = bill_offset.round(2)
    df["mix_shock_dv01"] = (df["coupon_dv01_shock"] - df["bill_dv01_offset"]).round(2)
    df["expected_soma_redemptions_x_low_liquidity"] = (df["expected_soma_redemptions_dv01"] * df["low_liquidity_prev"]).round(2)

    def classify(row):
        if row["debt_limit_flag"] == 1 or row["quarter"].startswith("2020"):
            return "confounded_window"
        if row["expected_soma_redemptions_dv01"] > 0 and row["mix_shock_dv01"] < 0:
            return "offsetting_interaction"
        if row["expected_soma_redemptions_dv01"] > 0 and row["mix_shock_dv01"] > 0:
            return "amplifying_interaction"
        return "neutral_window"

    df["classification_prior"] = df.apply(classify, axis=1)
    df["clean_sample_flag"] = (~df["classification_prior"].eq("confounded_window")).astype(int)
    df["statement_title"] = "Demo refunding statement"
    df["statement_url"] = "demo://refunding_panel_demo.csv"
    return df


def _weekly_demo(quarter_df: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 1)
    weeks = pd.date_range("2016-01-06", "2026-12-30", freq="W-WED")
    week_df = pd.DataFrame({"week": weeks})

    qmap = quarter_df[["quarter", "refunding_date", "mix_shock_dv01", "buyback_offset_dv01", "expected_soma_redemptions_dv01"]].copy()
    qmap["refunding_date"] = pd.to_datetime(qmap["refunding_date"])
    week_df = pd.merge_asof(
        week_df.sort_values("week"),
        qmap.sort_values("refunding_date"),
        left_on="week",
        right_on="refunding_date",
        direction="backward",
    )
    week_df["quarter"] = week_df["week"].dt.to_period("Q").astype(str)
    week_df[["mix_shock_dv01", "buyback_offset_dv01", "expected_soma_redemptions_dv01"]] = week_df[[
        "mix_shock_dv01",
        "buyback_offset_dv01",
        "expected_soma_redemptions_dv01",
    ]].fillna(0)

    qt_runoff = week_df["expected_soma_redemptions_dv01"] / 13 + rng.normal(0, 0.4, len(week_df))
    qt_runoff = np.round(qt_runoff, 2)
    week_df["qt_runoff_dv01"] = qt_runoff
    week_df["fed_pressure_dv01"] = (week_df["qt_runoff_dv01"] + week_df["mix_shock_dv01"] - week_df["buyback_offset_dv01"]).round(2)

    # System liquidity process.
    reserves = []
    on_rrp = []
    soma = []
    tga = []
    tgcr = []
    on_rrp_rate = []
    iorb = []
    y2 = []
    y5 = []
    y10 = []
    y20 = []
    y30 = []

    reserves_level = 2200.0
    on_rrp_level = 50.0
    soma_level = 2400.0
    tga_level = 350.0
    policy_rate = 0.25

    for i, row in week_df.iterrows():
        q = row["quarter"]
        pressure = row["fed_pressure_dv01"]
        runoff = row["qt_runoff_dv01"]

        if "2020Q2" <= q <= "2021Q2":
            soma_level += 35 + rng.normal(0, 4)
            reserves_level += 28 + rng.normal(0, 6)
            on_rrp_level = max(0, on_rrp_level - 6 + rng.normal(0, 1.5))
            policy_rate = 0.08
        else:
            soma_level += -2.6 * runoff + rng.normal(0, 1.5)
            reserves_level += -4.8 * max(runoff, 0) + rng.normal(0, 5)
            if q >= "2022Q1":
                on_rrp_level += 8 - 1.9 * max(runoff, 0) + rng.normal(0, 4)
            else:
                on_rrp_level += rng.normal(0, 2)
            if q >= "2022Q1":
                policy_rate = min(5.4, policy_rate + 0.03)
            if q >= "2024Q3":
                on_rrp_level += -10 + rng.normal(0, 4)

        if q == "2025Q3":
            tga_level += 28 + rng.normal(0, 10)
        else:
            tga_level += rng.normal(0, 12)
        tga_level = max(80, tga_level)
        on_rrp_level = max(5, on_rrp_level)
        reserves_level = max(900, reserves_level)
        soma_level = max(3500 if q >= "2021Q1" else 1800, soma_level)

        system_liq = reserves_level + on_rrp_level
        repo_spread = 2.0 + 0.55 * max(pressure, 0) + (1 if system_liq < 2100 else 0) * 6 + 0.02 * (tga_level - 350) + rng.normal(0, 1.3)
        tgcr_rate = policy_rate + repo_spread / 100
        on_rrp_award = max(0.0, policy_rate - 0.05 + rng.normal(0, 0.03))
        iorb_rate = max(0.0, policy_rate - 0.1 + rng.normal(0, 0.03))

        curve_shift = 0.05 * pressure + 0.002 * (tga_level - 350)
        y2_rate = policy_rate + 0.4 + curve_shift + rng.normal(0, 0.07)
        y5_rate = y2_rate + 0.35 + 0.05 * max(pressure, 0) + rng.normal(0, 0.05)
        y10_rate = y5_rate + 0.25 + 0.06 * max(pressure, 0) + rng.normal(0, 0.05)
        y20_rate = y10_rate + 0.18 + rng.normal(0, 0.04)
        y30_rate = y20_rate + 0.12 + rng.normal(0, 0.04)

        reserves.append(reserves_level)
        on_rrp.append(on_rrp_level)
        soma.append(soma_level)
        tga.append(tga_level)
        tgcr.append(tgcr_rate)
        on_rrp_rate.append(on_rrp_award)
        iorb.append(iorb_rate)
        y2.append(y2_rate)
        y5.append(y5_rate)
        y10.append(y10_rate)
        y20.append(y20_rate)
        y30.append(y30_rate)

    week_df["soma_treasuries_bn"] = np.round(soma, 2)
    week_df["reserves_bn"] = np.round(reserves, 2)
    week_df["on_rrp_bn"] = np.round(on_rrp, 2)
    week_df["tga_bn"] = np.round(tga, 2)
    week_df["system_liquidity_bn"] = (week_df["reserves_bn"] + week_df["on_rrp_bn"]).round(2)
    threshold = np.nanpercentile(week_df["system_liquidity_bn"], 30)
    week_df["low_liquidity"] = (week_df["system_liquidity_bn"] <= threshold).astype(int)
    week_df["low_liquidity_prev"] = week_df["low_liquidity"].shift(1).fillna(0).astype(int)
    week_df["fed_pressure_x_low_liquidity"] = (week_df["fed_pressure_dv01"] * week_df["low_liquidity_prev"]).round(2)

    inventory = []
    level = 85.0
    for _, row in week_df.iterrows():
        level = 0.88 * level + 6 + 0.85 * row["fed_pressure_dv01"] + 7.5 * row["low_liquidity_prev"] + rng.normal(0, 3)
        inventory.append(level)
    week_df["dealer_inventory_bn"] = np.round(inventory, 2)

    week_df["tgcr_rate"] = np.round(tgcr, 4)
    week_df["on_rrp_award_rate"] = np.round(on_rrp_rate, 4)
    week_df["iorb_rate"] = np.round(iorb, 4)
    week_df["repo_spread_bp"] = ((week_df["tgcr_rate"] - week_df["on_rrp_award_rate"]) * 100).round(2)
    week_df["repo_spread_iorb_bp"] = ((week_df["tgcr_rate"] - week_df["iorb_rate"]) * 100).round(2)
    week_df["tga_change_bn"] = week_df["tga_bn"].diff().fillna(0).round(2)
    week_df["quarter_end_flag"] = week_df["week"].dt.is_quarter_end.astype(int)
    week_df["dealer_inventory_lag1"] = week_df["dealer_inventory_bn"].shift(1)
    week_df["repo_spread_lag1"] = week_df["repo_spread_bp"].shift(1)
    week_df["DGS2"] = np.round(y2, 3)
    week_df["DGS5"] = np.round(y5, 3)
    week_df["DGS10"] = np.round(y10, 3)
    week_df["DGS20"] = np.round(y20, 3)
    week_df["DGS30"] = np.round(y30, 3)

    return week_df


def _sectoral_demo(quarter_df: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 2)
    q = quarter_df.copy()
    q["date"] = pd.PeriodIndex(q["quarter"], freq="Q").start_time
    n = len(q)
    trend = np.linspace(0, 1, n)
    qt2_flag = (q["quarter"] >= "2022Q2").astype(float)
    debt_limit = q["debt_limit_flag"].astype(float)

    total_bn = 14500 + 8200 * trend + 180 * debt_limit + rng.normal(0, 110, n)
    households_share = 0.27 - 0.02 * trend + rng.normal(0, 0.004, n)
    row_share = 0.23 + 0.05 * trend + rng.normal(0, 0.005, n)
    banks_share = 0.14 - 0.02 * trend + rng.normal(0, 0.003, n)
    mmf_share = 0.03 + 0.05 * qt2_flag + 0.015 * trend + rng.normal(0, 0.003, n)
    mutual_share = 0.11 + 0.01 * trend + rng.normal(0, 0.003, n)
    dealers_share = 0.025 + 0.004 * qt2_flag + rng.normal(0, 0.001, n)
    shares = np.vstack([households_share, row_share, banks_share, mmf_share, mutual_share, dealers_share]).T
    shares = np.clip(shares, 0.01, None)
    selected_total = shares.sum(axis=1)
    scale = np.minimum(0.82 / selected_total, 1.0)
    shares = shares * scale[:, None]

    values_bn = {
        "BOGZ1FL893061105Q": total_bn,
        "BOGZ1LM153061105Q": total_bn * shares[:, 0],
        "BOGZ1LM263061105Q": total_bn * shares[:, 1],
        "BOGZ1LM763061100Q": total_bn * shares[:, 2],
        "BOGZ1FL633061105Q": total_bn * shares[:, 3],
        "BOGZ1LM653061105Q": total_bn * shares[:, 4],
        "BOGZ1FL663061105Q": total_bn * shares[:, 5],
    }
    return pd.DataFrame({"DATE": q["date"], **{sid: np.round(vals * 1000, 0) for sid, vals in values_bn.items()}})


def build_demo_seed(seed: int = 42) -> dict[str, int | str]:
    ensure_repo_dirs()
    base = RAW_DIR / "demo"
    (base / "fred").mkdir(parents=True, exist_ok=True)
    (base / "treasury").mkdir(parents=True, exist_ok=True)
    (base / "nyfed").mkdir(parents=True, exist_ok=True)

    quarter_df = _quarterly_demo(seed=seed)
    week_df = _weekly_demo(quarter_df, seed=seed)
    sector_df = _sectoral_demo(quarter_df, seed=seed)

    write_csv(quarter_df, base / "treasury" / "refunding_panel_demo.csv")
    write_parquet(quarter_df, base / "treasury" / "refunding_panel_demo.parquet")
    write_csv(week_df, base / "weekly_master_demo.csv")
    write_parquet(week_df, base / "weekly_master_demo.parquet")

    # Demo FRED-like files.
    fred_map = {
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
    for sid, col in fred_map.items():
        out = week_df[["week", col]].rename(columns={"week": "DATE", col: "VALUE"}).copy()
        write_csv(out, base / "fred" / f"{sid}.csv")

    for sid in [col for col in sector_df.columns if col != "DATE"]:
        out = sector_df[["DATE", sid]].rename(columns={sid: "VALUE"}).copy()
        write_csv(out, base / "fred" / f"{sid}.csv")

    pd_demo = week_df[["week", "dealer_inventory_bn"]].rename(columns={"week": "date", "dealer_inventory_bn": "PDPOSGST-TOT"}).copy()
    pd_demo["PDSORA-UTSETTOT"] = np.round(600 + 2.5 * week_df["fed_pressure_dv01"].to_numpy() + seed, 2)
    pd_demo["PDSIRRA-UTSETTOT"] = np.round(520 - 1.8 * week_df["fed_pressure_dv01"].to_numpy() + seed, 2)
    write_csv(pd_demo, base / "nyfed" / "primary_dealer_demo.csv")
    write_json(base / "nyfed" / "timeseries_demo.json", {
        "series": [
            {"seriesbreak": "PDPOSGST-TOT", "description": "Net Treasury position excluding TIPS"},
            {"seriesbreak": "PDSORA-UTSETTOT", "description": "Repo agreements Treasuries ex-TIPS"},
            {"seriesbreak": "PDSIRRA-UTSETTOT", "description": "Reverse repo agreements Treasuries ex-TIPS"},
        ]
    })

    summary = {
        "seed": seed,
        "quarter_rows": int(len(quarter_df)),
        "weekly_rows": int(len(week_df)),
        "demo_paths": {
            "quarter": str(base / "treasury" / "refunding_panel_demo.csv"),
            "weekly": str(base / "weekly_master_demo.parquet"),
        },
    }
    write_json(INTERIM_DIR / "demo_seed_summary.json", summary)
    return summary
