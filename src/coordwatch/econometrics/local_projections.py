
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import statsmodels.api as sm

from coordwatch.config import load_model_specs


@dataclass
class LPBundle:
    table: pd.DataFrame
    design_sample: pd.DataFrame


def _estimate_single_horizon(df: pd.DataFrame, outcome: str, horizon: int, shock: str, interaction: str, controls: list[str], cov_type: str, hac_lags: int) -> dict:
    work = df.copy()
    work["week"] = pd.to_datetime(work["week"], errors="coerce")
    work = work.sort_values("week").drop_duplicates(subset=["week"]).reset_index(drop=True)

    future = work[["week", outcome]].copy().rename(columns={outcome: f"{outcome}_future"})
    future["week"] = future["week"] - pd.to_timedelta(horizon * 7, unit="D")

    lagged = work[["week", outcome]].copy().rename(columns={outcome: f"{outcome}_lag1"})
    lagged["week"] = lagged["week"] + pd.to_timedelta(7, unit="D")

    work = work.merge(future, on="week", how="left").merge(lagged, on="week", how="left")
    work[f"lhs_h{horizon}"] = work[f"{outcome}_future"] - work[f"{outcome}_lag1"]
    cols = [f"lhs_h{horizon}", shock, interaction] + controls
    sub = work[cols].copy()
    sub = sub.dropna()
    if sub.empty:
        return {
            "horizon": horizon,
            "outcome": outcome,
            "term": shock,
            "coef": float("nan"),
            "std_err": float("nan"),
            "p_value": float("nan"),
            "n_obs": 0,
        }
    y = sub[f"lhs_h{horizon}"]
    design = sub[[shock, interaction] + controls].copy()
    dropped_terms = set()
    for col in [shock, interaction]:
        if col in design.columns and design[col].nunique(dropna=True) <= 1:
            design = design.drop(columns=[col])
            dropped_terms.add(col)
    X = sm.add_constant(design)
    result = sm.OLS(y, X).fit(cov_type=cov_type, cov_kwds={"maxlags": hac_lags} if cov_type.upper() == "HAC" else None)
    rows = []
    for term in [shock, interaction]:
        coef = float("nan") if term in dropped_terms else float(result.params.get(term, float("nan")))
        se = float("nan") if term in dropped_terms else float(result.bse.get(term, float("nan")))
        pv = float("nan") if term in dropped_terms else float(result.pvalues.get(term, float("nan")))
        rows.append({
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
        })
    return rows


def run_local_projections(df: pd.DataFrame, outcome: str) -> LPBundle:
    spec = load_model_specs()["local_projections"]
    work = df.copy()
    work["week"] = pd.to_datetime(work["week"], errors="coerce")
    work = work.sort_values("week").drop_duplicates(subset=["week"]).reset_index(drop=True)
    if len(work) > 1:
        deltas = work["week"].diff().dropna()
        is_weekly = bool((deltas == pd.Timedelta(days=7)).all())
        if not is_weekly:
            raise ValueError("Local projections require a true weekly panel with 7-day spacing.")
    horizons = spec["horizons"]
    shock = spec["shock"]
    interaction = spec["interaction"]
    controls = [c for c in spec.get("controls", []) if c in work.columns]
    cov_type = spec.get("cov_type", "HAC")
    hac_lags = int(spec.get("hac_lags", 4))

    rows = []
    for h in horizons:
        est = _estimate_single_horizon(work, outcome=outcome, horizon=h, shock=shock, interaction=interaction, controls=controls, cov_type=cov_type, hac_lags=hac_lags)
        if isinstance(est, list):
            rows.extend(est)
        else:
            rows.append(est)
    table = pd.DataFrame(rows)
    return LPBundle(table=table, design_sample=work.copy())
