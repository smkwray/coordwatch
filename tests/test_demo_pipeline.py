
from __future__ import annotations

from pathlib import Path

import pandas as pd

from coordwatch.construct.panels import build_weekly_master_panel
from coordwatch.construct.refunding import build_refunding_panel
from coordwatch.demo import build_demo_seed
from coordwatch.econometrics.local_projections import run_local_projections
from coordwatch.econometrics.reaction import run_reaction_function


def test_demo_seed_builds_panels(tmp_path: Path) -> None:
    summary = build_demo_seed(seed=123)
    assert summary["quarter_rows"] > 20
    quarter = build_refunding_panel(prefer_real=False, output_dir=tmp_path)
    weekly = build_weekly_master_panel(quarter, prefer_real=False, output_dir=tmp_path)
    assert not quarter.empty
    assert not weekly.empty
    assert {"mix_shock_dv01", "expected_soma_redemptions_dv01"}.issubset(quarter.columns)
    assert {"fed_pressure_dv01", "dealer_inventory_bn", "repo_spread_bp"}.issubset(weekly.columns)


def test_demo_estimators_run(tmp_path: Path) -> None:
    build_demo_seed(seed=321)
    quarter = build_refunding_panel(prefer_real=False, output_dir=tmp_path)
    weekly = build_weekly_master_panel(quarter, prefer_real=False, output_dir=tmp_path)
    reaction = run_reaction_function(quarter)
    dealer_lp = run_local_projections(weekly, outcome="dealer_inventory_bn")
    assert not reaction.coefficients.empty
    assert not dealer_lp.table.empty
    shock_rows = dealer_lp.table[(dealer_lp.table["term"] == "fed_pressure_dv01") & (dealer_lp.table["horizon"] == 0)]
    assert not shock_rows.empty
    coef = float(shock_rows["coef"].iloc[0])
    assert pd.notna(coef)
