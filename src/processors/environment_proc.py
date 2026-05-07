"""Environment processor — incl. school-level air quality interpolation
via inverse distance weighting from EPA monitor coordinates."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
import config.r2_client as r2  # noqa: E402

from processors._base import (  # noqa: E402
    filter_california, save_processed, standardize_columns,
)

log = logging.getLogger(__name__)
EARTH_R_KM = 6371.0088


def process(source: str, year=None) -> dict:
    if source == "epa_aqs":
        return _process_aqs(year)
    if source in ("calenviroscreen", "epa_tri", "fema_nfhl", "noaa_ghcn"):
        from processors.generic_proc import process as gp
        return gp(source, year)
    return {"ok": False, "skipped": True, "reason": f"unknown env source {source}"}


def _process_aqs(year) -> dict:
    raw_key = f"raw/epa_aqs/{year or 'latest'}.parquet"
    if r2.exists(raw_key) is None:
        return {"ok": False, "skipped": True, "reason": "raw missing"}
    df = r2.download(raw_key)
    df = standardize_columns(df)
    df = filter_california(df)
    # Keep only PM2.5 (Local Conditions, 88101) and ozone (44201)
    if "parameter_code" in df.columns:
        df = df[df["parameter_code"].isin([88101, 44201])].copy()
    out_key = save_processed(df, "epa_aqs", year)
    return {"ok": True, "key": out_key, "rows": int(len(df))}


def school_aqi_from_monitors(schools: pd.DataFrame, monitors: pd.DataFrame,
                                 radius_km: float = 50.0) -> pd.DataFrame:
    """Compute a school-level PM25 AQI by inverse-distance weighted interpolation
    from monitor readings within `radius_km`. Coordinates: schools[lat,lon],
    monitors[latitude,longitude,arithmetic_mean]."""
    if schools.empty or monitors.empty:
        return pd.DataFrame(columns=["cds", "aqi_pm25"])
    s = schools.dropna(subset=["lat", "lon"])
    m = monitors.dropna(subset=["latitude", "longitude", "arithmetic_mean"])
    out = []
    for _, school in s.iterrows():
        d = _haversine(school["lat"], school["lon"],
                          m["latitude"].to_numpy(), m["longitude"].to_numpy())
        within = d < radius_km
        if not within.any():
            out.append({"cds": school.get("cds"), "aqi_pm25": np.nan})
            continue
        w = 1.0 / (d[within] ** 2 + 1e-9)
        v = m["arithmetic_mean"].to_numpy()[within]
        out.append({"cds": school.get("cds"),
                      "aqi_pm25": float((w * v).sum() / w.sum())})
    return pd.DataFrame(out)


def _haversine(lat1, lon1, lat2, lon2) -> np.ndarray:
    rlat1, rlat2 = np.radians(lat1), np.radians(lat2)
    dlat = rlat2 - rlat1
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat / 2) ** 2 + np.cos(rlat1) * np.cos(rlat2) * np.sin(dlon / 2) ** 2
    return 2 * EARTH_R_KM * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
