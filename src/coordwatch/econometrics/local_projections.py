from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
import statsmodels.api as sm

from coordwatch.config import load_model_specs


@dataclass
class LPBundle:
    table: pd.DataFrame
    design_sample: pd.DataFrame


def _expand_shock_terms(spec: dict[str, Any]) -> list[str]:
    if spec.get("shock_terms"):
        return list(spec["shock_terms"])
    return [spec["shock"], spec["interaction"]]


def _apply_lp_sample_filters(df: pd.DataFrame, spec: dict[str, Any]) -> pd.DataFrame:
    work = df.copy()
    if spec.get("exclude_debt_limit", False) and "debt_limit_flag" in work.columns:
        work = work.loc[work["debt_limit_flag"].fillna(0).astype(int) == 0].copy()
    return work


def _empty_rows(outcome: str, horizon: int, shock_terms: list[str]) -> list[dict]:
    return [
        {
            "horizon": horizon,
            "outcome": outcome,
            "term": term,
            "coef": float("nan"),
            "std_err": float("nan"),
            "ci_lower_95": float("nan"),
            "ci_upper_95": float("nan"),
            "p_value": float("nan"),
            "n_obs": 0,
            "r_squared": float("nan"),
            "dropped_for_no_variation": False,
        }
        for term in shock_terms
    ]


def _estimate_single_horizon(
    df: pd.DataFrame,
    outcome: str,
    horizon: int,
    shock_terms: list[str],
    controls: list[str],
    cov_type: str,
    hac_lags: int,
) -> list[dict]:
    work = df.copy()
    work["week"] = pd.to_datetime(work["week"], errors="coerce")
    work = work.sort_values("week").drop_duplicates(subset=["week"]).reset_index(drop=True)

    future = work[["week", outcome]].copy().rename(columns={outcome: f"{outcome}_future"})
    future["week"] = future["week"] - pd.to_timedelta(horizon * 7, unit="D")

    lagged = work[["week", outcome]].copy().rename(columns={outcome: f"{outcome}_lag1"})
    lagged["week"] = lagged["week"] + pd.to_timedelta(7, unit="D")

    work = work.merge(future, on="week", how="left").merge(lagged, on="week", how="left")
    work[f"lhs_h{horizon}"] = work[f"{outcome}_future"] - work[f"{outcome}_lag1"]
    cols = [f"lhs_h{horizon}"] + shock_terms + controls
    for col in cols:
        if col not in work.columns:
            work[col] = pd.NA
    sub = work[cols].copy().dropna()
    if sub.empty:
        return _empty_rows(outcome, horizon, shock_terms)
    y = sub[f"lhs_h{horizon}"]
    design = sub[shock_terms + controls].copy()
    dropped_terms = set()
    for col in shock_terms:
        if col in design.columns and design[col].nunique(dropna=True) <= 1:
            design = design.drop(columns=[col])
            dropped_terms.add(col)
    if design.empty:
        return _empty_rows(outcome, horizon, shock_terms)
    X = sm.add_constant(design)
    result = sm.OLS(y, X).fit(cov_type=cov_type, cov_kwds={"maxlags": hac_lags} if cov_type.upper() == "HAC" else None)
    rows = []
    for term in shock_terms:
        coef = float("nan") if term in dropped_terms else float(result.params.get(term, float("nan")))
        se = float("nan") if term in dropped_terms else float(result.bse.get(term, float("nan")))
        pv = float("nan") if term in dropped_terms else float(result.pvalues.get(term, float("nan")))
        rows.append(
            {
                "horizon": horizon,
                "outcome": outcome,
                "term": term,
                "coef": coef,
                "std_err": se,
                "ci_lower_95": coef - 1.96 * se if pd.notna(se) else float("nan"),
                "ci_upper_95": coef + 1.96 * se if pd.notna(se) else float("nan"),
                "p_value": pv,
                "n_obs": int(result.nobs),
                "r_squared": float(result.rsquared),
                "dropped_for_no_variation": term in dropped_terms,
            }
        )
    return rows


def run_local_projection_spec(df: pd.DataFrame, spec: dict[str, Any], outcome: str | None = None) -> LPBundle:
    base_spec = load_model_specs().get("local_projections", {})
    work = _apply_lp_sample_filters(df, spec)
    work["week"] = pd.to_datetime(work["week"], errors="coerce")
    work = work.sort_values("week").drop_duplicates(subset=["week"]).reset_index(drop=True)
    if len(work) > 1:
        deltas = work["week"].diff().dropna()
        is_weekly = bool(((deltas.dt.days % 7) == 0).all())
        if not is_weekly:
            raise ValueError("Local projections require a weekly panel whose gaps stay on a 7-day grid.")
    horizons = spec.get("horizons", base_spec["horizons"])
    shock_terms = _expand_shock_terms(spec)
    resolved_outcome = outcome or spec["outcome"]
    controls = [c for c in spec.get("controls", []) if c in work.columns]
    cov_type = spec.get("cov_type", base_spec.get("cov_type", "HAC"))
    hac_lags = int(spec.get("hac_lags", base_spec.get("hac_lags", 4)))

    rows = []
    for h in horizons:
        rows.extend(
            _estimate_single_horizon(
                work,
                outcome=resolved_outcome,
                horizon=h,
                shock_terms=shock_terms,
                controls=controls,
                cov_type=cov_type,
                hac_lags=hac_lags,
            )
        )
    table = pd.DataFrame(rows)
    return LPBundle(table=table, design_sample=work.copy())


def run_named_local_projection(df: pd.DataFrame, spec_name: str) -> LPBundle:
    specs = load_model_specs().get("local_projection_appendices", {})
    spec = specs.get(spec_name)
    if spec is None:
        raise KeyError(f"Unknown local-projection spec: {spec_name}")
    return run_local_projection_spec(df, spec)


def run_local_projections(df: pd.DataFrame, outcome: str) -> LPBundle:
    spec = dict(load_model_specs()["local_projections"])
    spec["outcome"] = outcome
    return run_local_projection_spec(df, spec, outcome=outcome)
