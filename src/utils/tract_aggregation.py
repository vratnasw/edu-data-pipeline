"""Aggregate tract-level data to county-level.

Tract GEOIDs in CA are 11-digit FIPS strings: `06` + `<county_3>` + `<tract_6>`.
For population weighting, callers pass a `population` column; when missing,
we fall back to unweighted mean.

NOTE: aggregating to county rather than to school district is a deliberate
simplification because the Census 2020 SD→tract relationship file isn't
published. District-level joins are downstream via county_code → district
in the master panel. To upgrade to true district-level later: download TIGER
unified-SD polygons (tl_2020_06_unsd.zip), do a spatial join from tract
centroids, and replace this helper.
"""
from __future__ import annotations

import logging
from typing import Iterable

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

CA_STATE_FIPS = "06"


def normalize_tract_fips(s: pd.Series) -> pd.Series:
    """Convert numeric/string tract GEOIDs to 11-digit zero-padded strings."""
    return (s.astype(str).str.replace(".0", "", regex=False)
              .str.replace(r"\D", "", regex=True)
              .str.zfill(11))


def tract_to_county(tract_fips: pd.Series) -> pd.Series:
    """First 5 digits of an 11-digit tract GEOID = state+county FIPS."""
    return normalize_tract_fips(tract_fips).str.slice(0, 5)


def aggregate_to_county(
    df: pd.DataFrame, *,
    tract_col: str,
    value_cols: Iterable[str],
    pop_col: str | None = None,
    state_filter: str | None = CA_STATE_FIPS,
) -> pd.DataFrame:
    """Aggregate tract rows to county-level (population-weighted if pop_col given).

    Returns one row per `county_code` (5-digit FIPS) with one column per
    value_col and a `n_tracts` count column.
    """
    df = df.copy()
    df["_county"] = tract_to_county(df[tract_col])
    df["_state"] = df["_county"].str.slice(0, 2)
    if state_filter is not None:
        df = df[df["_state"] == state_filter]
    if df.empty:
        log.warning("[tract_agg] no rows after state filter %s", state_filter)
        return pd.DataFrame(columns=["county_code", "n_tracts", *value_cols])

    # Drop rows where ALL value cols are NaN (no signal)
    keep_mask = ~df[list(value_cols)].isna().all(axis=1)
    df = df[keep_mask]

    if pop_col and pop_col in df.columns:
        df["_w"] = pd.to_numeric(df[pop_col], errors="coerce").fillna(0.0)
    else:
        df["_w"] = 1.0

    out_rows = []
    for cc, g in df.groupby("_county"):
        rec: dict = {"county_code": cc, "n_tracts": int(len(g))}
        w = g["_w"].to_numpy()
        for c in value_cols:
            v = pd.to_numeric(g[c], errors="coerce").to_numpy()
            ok = ~np.isnan(v) & (w > 0)
            if ok.sum() == 0:
                rec[c] = np.nan
            else:
                rec[c] = float(np.average(v[ok], weights=w[ok]))
        out_rows.append(rec)
    out = pd.DataFrame(out_rows)
    log.info("[tract_agg] aggregated %d tracts → %d counties, value_cols=%s",
              len(df), len(out), list(value_cols))
    return out
