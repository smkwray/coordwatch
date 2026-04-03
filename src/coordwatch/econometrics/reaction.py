
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import statsmodels.api as sm

from coordwatch.config import load_model_specs


@dataclass
class RegressionResultBundle:
    coefficients: pd.DataFrame
    fitted: pd.DataFrame
    summary_text: str


def run_reaction_function(df: pd.DataFrame) -> RegressionResultBundle:
    spec = load_model_specs()["reaction_function"]
    dep = spec["dependent"]
    regressors = spec["regressors"]
    cov = spec.get("robust_cov", "HC3")

    work = df.copy()
    work = work.loc[work.get("clean_sample_flag", 1).fillna(1).astype(int) == 1].copy()
    cols = [dep] + regressors
    work = work[cols + [c for c in ["quarter", "classification_prior"] if c in work.columns]].dropna(subset=[dep])
    X = work[regressors].apply(pd.to_numeric, errors="coerce")
    y = pd.to_numeric(work[dep], errors="coerce")
    valid = X.notna().all(axis=1) & y.notna()
    X = X.loc[valid]
    y = y.loc[valid]
    meta = work.loc[valid, [c for c in ["quarter", "classification_prior"] if c in work.columns]].reset_index(drop=True)
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
