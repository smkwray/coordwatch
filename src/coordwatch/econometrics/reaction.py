
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import statsmodels.api as sm

from coordwatch.config import load_model_specs


@dataclass
class RegressionResultBundle:
    coefficients: pd.DataFrame
    fitted: pd.DataFrame
    summary_text: str


def _get_reaction_spec(spec_name: str) -> dict[str, Any]:
    specs = load_model_specs()
    if spec_name == "reaction_function":
        return dict(specs["reaction_function"])
    appendix = specs.get("reaction_function_appendices", {}).get(spec_name)
    if appendix is None:
        raise KeyError(f"Unknown reaction-function spec: {spec_name}")
    return dict(appendix)


def _apply_sample_filters(df: pd.DataFrame, spec: dict[str, Any]) -> pd.DataFrame:
    work = df.copy()
    if spec.get("clean_sample_only", True) and "clean_sample_flag" in work.columns:
        work = work.loc[work["clean_sample_flag"].fillna(1).astype(int) == 1].copy()
    if spec.get("exclude_debt_limit", False) and "debt_limit_flag" in work.columns:
        work = work.loc[work["debt_limit_flag"].fillna(0).astype(int) == 0].copy()
    return work


def run_reaction_function(df: pd.DataFrame, spec_name: str = "reaction_function") -> RegressionResultBundle:
    spec = _get_reaction_spec(spec_name)
    dep = spec["dependent"]
    regressors = spec["regressors"]
    cov = spec.get("robust_cov", "HC3")

    work = _apply_sample_filters(df, spec)
    cols = [dep] + regressors
    for col in cols:
        if col not in work.columns:
            work[col] = np.nan
    meta_cols = [c for c in ["quarter"] if c in work.columns]
    work = work[cols + meta_cols].dropna(subset=[dep])
    X = work[regressors].apply(pd.to_numeric, errors="coerce")
    y = pd.to_numeric(work[dep], errors="coerce")
    valid = X.notna().all(axis=1) & y.notna()
    X = X.loc[valid]
    y = y.loc[valid]
    meta = work.loc[valid, meta_cols].reset_index(drop=True)
    X = sm.add_constant(X)
    result = sm.OLS(y, X).fit(cov_type=cov)

    coef = pd.DataFrame({
        "term": result.params.index,
        "coef": result.params.values,
        "std_err": result.bse.values,
        "t_value": result.tvalues.values,
        "p_value": result.pvalues.values,
    })
    coef["ci_lower_95"] = coef["coef"] - 1.96 * coef["std_err"]
    coef["ci_upper_95"] = coef["coef"] + 1.96 * coef["std_err"]
    coef["n_obs"] = int(result.nobs)
    coef["r_squared"] = float(result.rsquared)

    fitted = meta.copy()
    fitted["actual"] = y.to_numpy()
    fitted["fitted"] = result.fittedvalues.to_numpy()
    fitted["residual"] = result.resid.to_numpy()

    return RegressionResultBundle(coefficients=coef, fitted=fitted, summary_text=result.summary().as_text())
