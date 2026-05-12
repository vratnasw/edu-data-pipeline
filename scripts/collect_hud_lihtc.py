"""Phase 7 — HUD LIHTC project proximity.

Source: HUD LIHTC Public Database zip at
  https://www.huduser.gov/lihtc/lihtcpub.zip   (~18 MB; contains an Access
  database + CSV exports)

Outputs:
  raw/hud_lihtc/lihtcpub_2024.parquet         — full CA project list
  processed/canonical/hud_lihtc_proximity.parquet
                                              — per-school proximity features
  processed/canonical/hud_lihtc_district.parquet
                                              — district-level aggregates

Per-school features:
  hud_lihtc_nearest_km           — Haversine distance to nearest project
  hud_lihtc_count_1km            — count within 1 km
  hud_lihtc_count_2km            — count within 2 km
  hud_lihtc_units_2km            — sum of low-income units within 2 km
"""
from __future__ import annotations

import io
import logging
import sys
import zipfile
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

URL = "https://www.huduser.gov/lihtc/lihtcpub.zip"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0) Chrome/120.0.0.0 Safari/537.36"}
SCHOOLS_GEO = (REPO.parent / "SFUSD_DATA_ANALYSIS" / "dashboard" / "public"
                  / "data" / "ca" / "geo" / "schools.geojson")


def haversine_km(lat1, lon1, lat2, lon2):
    """Vectorized Haversine. lat/lon in degrees, returns km."""
    R = 6371.0088
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1; dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))


def main() -> int:
    log.info("[lihtc] downloading %s (~18 MB)", URL)
    r = requests.get(URL, headers=HEADERS, timeout=600)
    r.raise_for_status()
    log.info("[lihtc] %.1f MB received", len(r.content) / 1e6)

    zf = zipfile.ZipFile(io.BytesIO(r.content))
    log.info("[lihtc] zip contents: %s", zf.namelist())

    # Find the CSV / xlsx with the project records
    csv_names = [n for n in zf.namelist()
                    if n.lower().endswith((".csv", ".xlsx", ".xls"))]
    if not csv_names:
        log.error("[lihtc] no CSV/xlsx in zip"); return 1
    name = csv_names[0]
    log.info("[lihtc] reading %s", name)
    raw_bytes = zf.read(name)
    if name.lower().endswith(".csv"):
        df = pd.read_csv(io.BytesIO(raw_bytes), low_memory=False, encoding="latin-1")
    else:
        df = pd.read_excel(io.BytesIO(raw_bytes))
    log.info("[lihtc] %d total rows, %d cols", len(df), df.shape[1])

    # Find state column — may be abbreviation, FIPS code, or full name
    state_col = next((c for c in df.columns if "state" in str(c).lower()
                          and "name" not in str(c).lower()), None)
    if state_col is None:
        log.error("[lihtc] no state column; cols=%s", list(df.columns)[:30])
        return 1
    log.info("[lihtc] state col=%s, sample values=%s",
              state_col, df[state_col].head(5).tolist())
    s = df[state_col].astype(str).str.upper().str.strip()
    # HUD LIHTC state_id is formatted as "XX-YY-ZZZZZ" — match prefix or whole
    mask = ((s == "CA") | (s == "06") | (s == "6") | (s == "CALIFORNIA")
              | s.str.startswith("CA-") | s.str.startswith("CA "))
    df_ca = df[mask].copy()
    log.info("[lihtc] CA projects: %d", len(df_ca))

    # Find lat/lon columns
    lat_col = next((c for c in df.columns if "latitude" in str(c).lower()
                      or str(c).lower() == "lat"), None)
    lon_col = next((c for c in df.columns if "longitude" in str(c).lower()
                      or str(c).lower() == "lon" or str(c).lower() == "long"), None)
    if not lat_col or not lon_col:
        log.error("[lihtc] no lat/lon cols; have %s", list(df.columns)[:30])
        return 1
    log.info("[lihtc] lat=%s lon=%s", lat_col, lon_col)

    # Find unit count
    units_col = next((c for c in df.columns
                          if "li_unit" in str(c).lower() or "low_income"
                          in str(c).lower() or "n_li" in str(c).lower()),
                        None)
    log.info("[lihtc] units col: %s", units_col)

    df_ca["_lat"] = pd.to_numeric(df_ca[lat_col], errors="coerce")
    df_ca["_lon"] = pd.to_numeric(df_ca[lon_col], errors="coerce")
    df_ca["_units"] = pd.to_numeric(df_ca[units_col], errors="coerce") if units_col else 0
    df_ca = df_ca.dropna(subset=["_lat", "_lon"])
    log.info("[lihtc] CA projects with valid lat/lon: %d", len(df_ca))

    # Raw upload
    raw_local = REPO / "data_cache" / "raw" / "hud_lihtc" / "lihtcpub_ca.parquet"
    raw_local.parent.mkdir(parents=True, exist_ok=True)
    df_ca.to_parquet(raw_local, index=False)
    r2.upload(raw_local, "raw/hud_lihtc/lihtcpub_ca.parquet")
    log.info("[lihtc] raw uploaded")

    # Schools
    import geopandas as gpd
    schools = gpd.read_file(SCHOOLS_GEO)
    schools["_lat"] = schools.geometry.y
    schools["_lon"] = schools.geometry.x
    schools["cds"] = schools["cds"].astype(str)
    log.info("[lihtc] schools loaded: %d", len(schools))

    # Project arrays
    plat = df_ca["_lat"].to_numpy()
    plon = df_ca["_lon"].to_numpy()
    punits = df_ca["_units"].fillna(0).to_numpy()

    # Per-school proximity features
    out_rows = []
    for _, srow in schools.iterrows():
        slat = float(srow["_lat"]); slon = float(srow["_lon"])
        if not (np.isfinite(slat) and np.isfinite(slon)):
            continue
        d = haversine_km(slat, slon, plat, plon)
        if len(d) == 0:
            continue
        out_rows.append({
            "cds": str(srow["cds"]),
            "hud_lihtc_nearest_km": float(d.min()),
            "hud_lihtc_count_1km": int((d <= 1.0).sum()),
            "hud_lihtc_count_2km": int((d <= 2.0).sum()),
            "hud_lihtc_units_2km": float(punits[d <= 2.0].sum()),
        })
    school_out = pd.DataFrame(out_rows)
    log.info("[lihtc] school proximity rows: %d", len(school_out))

    # District-level aggregate (mean across schools in same district)
    school_out["cds_district"] = school_out["cds"].str[:7]
    dist_agg = (school_out.groupby("cds_district").agg(
        hud_lihtc_district_nearest_km=("hud_lihtc_nearest_km", "mean"),
        hud_lihtc_district_count_2km_mean=("hud_lihtc_count_2km", "mean"),
        hud_lihtc_district_units_2km_mean=("hud_lihtc_units_2km", "mean"),
        n_schools=("cds", "count"),
    ).reset_index().rename(columns={"cds_district": "cds"}))

    # Save
    p1 = REPO / "data_cache" / "processed" / "canonical" / "hud_lihtc_proximity.parquet"
    p1.parent.mkdir(parents=True, exist_ok=True)
    school_out.to_parquet(p1, index=False)
    r2.upload(p1, "processed/canonical/hud_lihtc_proximity.parquet")
    log.info("[lihtc] school-level uploaded: %d schools", len(school_out))

    p2 = REPO / "data_cache" / "processed" / "canonical" / "hud_lihtc_district.parquet"
    dist_agg.to_parquet(p2, index=False)
    r2.upload(p2, "processed/canonical/hud_lihtc_district.parquet")
    log.info("[lihtc] district-level uploaded: %d districts", len(dist_agg))

    # Coverage
    log.info("[lihtc] nearest_km range: %.2f - %.2f",
              school_out["hud_lihtc_nearest_km"].min(),
              school_out["hud_lihtc_nearest_km"].max())
    log.info("[lihtc] count_2km mean=%.1f median=%d max=%d",
              school_out["hud_lihtc_count_2km"].mean(),
              int(school_out["hud_lihtc_count_2km"].median()),
              int(school_out["hud_lihtc_count_2km"].max()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
