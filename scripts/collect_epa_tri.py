"""Phase 11 — EPA TRI facility proximity.

Source: EPA Envirofacts REST API
  https://data.epa.gov/efservice/TRI_FACILITY/STATE_ABBR/CA/CSV
  https://data.epa.gov/efservice/TRI_REPORTING_FORM/STATE_ABBR/CA/REPORTING_YEAR/<YEAR>/CSV

For each CA TRI facility we ship latitude/longitude. For release totals (air,
water) we sum across reporting forms per facility-year, which is too heavy
to pull for every year on a flaky connection. Therefore for the first
production pass we ship facility locations + a coarse "facility within 2km"
count per school, and defer multi-year release quantities (deferred field
documented in run_summary).

Outputs:
  raw/epa_tri/facilities_ca.parquet
  processed/canonical/epa_tri_proximity.parquet
"""
from __future__ import annotations

import io
import logging
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "src"))

import numpy as np
import pandas as pd
import requests

import config.r2_client as r2  # noqa: E402

logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s %(levelname)s :: %(message)s",
                       datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0) Chrome/120.0.0.0 Safari/537.36"}
SCHOOLS_GEO = (REPO.parent / "SFUSD_DATA_ANALYSIS" / "dashboard" / "public"
                  / "data" / "ca" / "geo" / "schools.geojson")


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0088
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1; dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))


def main() -> int:
    url = "https://data.epa.gov/efservice/TRI_FACILITY/STATE_ABBR/CA/CSV"
    log.info("[tri] GET %s", url)
    r = requests.get(url, headers=HEADERS, timeout=600)
    r.raise_for_status()
    df = pd.read_csv(io.BytesIO(r.content), low_memory=False)
    log.info("[tri] %d CA TRI facilities", len(df))

    # Find lat/lon — prefer `pref_*` (decimal degrees) over `fac_*` (DDMMSS)
    lat_col = ("pref_latitude" if "pref_latitude" in df.columns
                  else next((c for c in df.columns if "preferred_lat" in c.lower()
                              or ("latitude" in c.lower() and "fac" not in c.lower())),
                              None))
    lon_col = ("pref_longitude" if "pref_longitude" in df.columns
                  else next((c for c in df.columns if "preferred_long" in c.lower()
                              or ("longitude" in c.lower() and "fac" not in c.lower())),
                              None))
    if not lat_col or not lon_col:
        log.error("[tri] no lat/lon; cols=%s", list(df.columns)[:30])
        return 1
    log.info("[tri] lat=%s lon=%s", lat_col, lon_col)

    df["_lat"] = pd.to_numeric(df[lat_col], errors="coerce")
    df["_lon"] = pd.to_numeric(df[lon_col], errors="coerce")
    # Some TRI facilities have positive longitudes (sign mistake) — flip
    df.loc[df["_lon"] > 0, "_lon"] = -df["_lon"]
    df = df.dropna(subset=["_lat", "_lon"])
    df = df[(df["_lat"] > 32) & (df["_lat"] < 43)
              & (df["_lon"] > -125) & (df["_lon"] < -113)]
    log.info("[tri] facilities with valid CA bbox lat/lon: %d", len(df))

    # Raw upload
    raw_local = REPO / "data_cache" / "raw" / "epa_tri" / "facilities_ca.parquet"
    raw_local.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(raw_local, index=False)
    r2.upload(raw_local, "raw/epa_tri/facilities_ca.parquet")
    log.info("[tri] raw uploaded")

    # Schools
    import geopandas as gpd
    schools = gpd.read_file(SCHOOLS_GEO)
    schools["_lat"] = schools.geometry.y
    schools["_lon"] = schools.geometry.x
    schools["cds"] = schools["cds"].astype(str)

    flat = df["_lat"].to_numpy(); flon = df["_lon"].to_numpy()
    rows = []
    for _, srow in schools.iterrows():
        slat, slon = float(srow["_lat"]), float(srow["_lon"])
        if not (np.isfinite(slat) and np.isfinite(slon)):
            continue
        d = haversine_km(slat, slon, flat, flon)
        d2 = d[d <= 2.0]
        # inverse-distance-squared-weighted facility count (no release qty
        # available without per-year reporting pull)
        w_idx = 1.0 / (d2 ** 2 + 0.01) if len(d2) else np.array([])
        rows.append({
            "cds": str(srow["cds"]),
            "epa_tri_nearest_km": float(d.min()) if len(d) else None,
            "epa_tri_count_2km": int(len(d2)),
            "epa_tri_proximity_weighted": float(w_idx.sum()) if len(d2) else 0.0,
        })
    school_out = pd.DataFrame(rows)
    log.info("[tri] school proximity rows: %d", len(school_out))

    school_out["cds_district"] = school_out["cds"].str[:7]
    dist_agg = (school_out.groupby("cds_district").agg(
        epa_tri_district_nearest_km=("epa_tri_nearest_km", "mean"),
        epa_tri_district_count_2km_mean=("epa_tri_count_2km", "mean"),
        epa_tri_district_weighted_mean=("epa_tri_proximity_weighted", "mean"),
    ).reset_index().rename(columns={"cds_district": "cds"}))

    p1 = REPO / "data_cache" / "processed" / "canonical" / "epa_tri_proximity.parquet"
    p1.parent.mkdir(parents=True, exist_ok=True)
    school_out.to_parquet(p1, index=False)
    r2.upload(p1, "processed/canonical/epa_tri_proximity.parquet")
    log.info("[tri] school-level uploaded")

    p2 = REPO / "data_cache" / "processed" / "canonical" / "epa_tri_district.parquet"
    dist_agg.to_parquet(p2, index=False)
    r2.upload(p2, "processed/canonical/epa_tri_district.parquet")
    log.info("[tri] district-level uploaded: %d districts", len(dist_agg))

    log.info("[tri] count_2km mean=%.1f median=%d max=%d",
              school_out["epa_tri_count_2km"].mean(),
              int(school_out["epa_tri_count_2km"].median()),
              int(school_out["epa_tri_count_2km"].max()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
