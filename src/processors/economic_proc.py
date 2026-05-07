"""Economic processor — cleans BEA / BLS / Census ACS / SAIPE / FHFA / Zillow / etc."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
import config.r2_client as r2  # noqa: E402

from processors._base import (  # noqa: E402
    fips_pad, save_processed, standardize_columns,
    validate_numeric, filter_california, fiscal_to_yyyy_yy,
)

log = logging.getLogger(__name__)


def process(source: str, year=None) -> dict:
    """Generic economic processor — selects per-source cleaning function."""
    raw_key = f"raw/{source}/{year or 'latest'}.parquet"
    info = r2.exists(raw_key)
    if info is None:
        return {"ok": False, "skipped": True, "reason": "raw missing", "key": raw_key}
    df = r2.download(raw_key)
    df = standardize_columns(df)
    if source == "census_acs5":
        df = _process_acs(df)
    elif source == "bls_unemployment":
        df = _process_bls(df)
    elif source == "bea_gdp_personal_income":
        df = _process_bea(df)
    elif source == "fhfa_hpi":
        df = _process_fhfa(df)
    elif source == "zillow_zori":
        df = _process_zori(df)
    elif source == "census_saipe_districts":
        df = _process_saipe(df)
    else:
        df = filter_california(df)
    out_key = save_processed(df, source, year)
    return {"ok": True, "key": out_key, "rows": int(len(df)),
              "cols": int(df.shape[1])}


def _process_acs(df):
    df = df.copy()
    if "state" in df.columns and "county" in df.columns:
        df["county_code"] = (fips_pad(df["state"], 2)
                                + fips_pad(df["county"], 3))
    for c in ("b19013_001e", "b17001_002e", "b15003_022e",
                "b25070_010e", "b25003_002e"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    validate_numeric(df, "b19013_001e", 0, 250000)
    return df


def _process_bls(df):
    df = df.copy()
    if "seriesid" in df.columns:
        df["county_code"] = df["seriesid"].astype(str).str[5:10]
    if "value" in df.columns:
        df["unemployment_rate"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def _process_bea(df):
    df = df.copy()
    if "geofips" in df.columns:
        df["county_code"] = fips_pad(df["geofips"], 5)
    if "datavalue" in df.columns:
        df["value"] = pd.to_numeric(df["datavalue"], errors="coerce")
    return df


def _process_fhfa(df):
    df = filter_california(df)
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df


def _process_zori(df):
    """Zillow wide → long; CA metros only."""
    if "RegionName" in df.columns:
        df = df[df["StateName"].str.upper() == "CA"].copy() \
            if "StateName" in df.columns else df
    return df


def _process_saipe(df):
    df = df.copy()
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df
