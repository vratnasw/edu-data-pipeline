"""Housing processor — incl. housing_cost_burden_teachers derived metric."""
from __future__ import annotations

import logging

import pandas as pd

from processors._base import (
    filter_california, save_processed, standardize_columns,
)
from processors.generic_proc import process as _generic

log = logging.getLogger(__name__)


def process(source: str, year=None) -> dict:
    return _generic(source, year)


def housing_cost_burden_teachers(zillow_zori: pd.DataFrame,
                                       cde_teacher_salary: pd.DataFrame) -> pd.DataFrame:
    """rent / teacher salary ratio. Keys on county_code; outputs one row
    per (county_code, year)."""
    z = zillow_zori.copy()
    if "county_code" not in z.columns or "median_rent" not in z.columns:
        return pd.DataFrame(columns=["county_code", "year", "housing_cost_burden_teachers"])
    t = cde_teacher_salary.copy()
    out = z.merge(t, on=["county_code", "year"], how="left")
    out["housing_cost_burden_teachers"] = (
        out["median_rent"] * 12 / out.get("median_teacher_salary", 1.0))
    return out[["county_code", "year", "housing_cost_burden_teachers"]]
