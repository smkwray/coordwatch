
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
    work = df.copy().reset_index(drop=True)
    work[f"lhs_h{horizon}"] = work[outcome].shift(-horizon) - work[outcome].shift(1)
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
    X = sm.add_constant(sub[[shock, interaction] + controls])
    result = sm.OLS(y, X).fit(cov_type=cov_type, cov_kwds={"maxlags": hac_lags} if cov_type.upper() == "HAC" else None)
    rows = []
    for term in [shock, interaction]:
        coef = float(result.params.get(term, float("nan")))
        se = float(result.bse.get(term, float("nan")))
        pv = float(result.pvalues.get(term, float("nan")))
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
        })
    return rows


def run_local_projections(df: pd.DataFrame, outcome: str) -> LPBundle:
    spec = load_model_specs()["local_projections"]
    horizons = spec["horizons"]
    shock = spec["shock"]
    interaction = spec["interaction"]
    controls = [c for c in spec.get("controls", []) if c in df.columns]
    cov_type = spec.get("cov_type", "HAC")
    hac_lags = int(spec.get("hac_lags", 4))

    rows = []
    for h in horizons:
        est = _estimate_single_horizon(df, outcome=outcome, horizon=h, shock=shock, interaction=interaction, controls=controls, cov_type=cov_type, hac_lags=hac_lags)
        if isinstance(est, list):
            rows.extend(est)
        else:
            rows.append(est)
    table = pd.DataFrame(rows)
    return LPBundle(table=table, design_sample=df.copy())
