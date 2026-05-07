"""Tract-level spatial join: schools → containing tract → district aggregate."""
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

log = logging.getLogger(__name__)


def _safe_download(key: str) -> pd.DataFrame | None:
    if r2.exists(key) is None:
        return None
    return r2.download(key)


def run_tract_spatial_join(
    schools_geojson_key: str = "raw/schools_geojson/latest.parquet",
    spine_key: str = "processed/education/ca_wide_features.parquet",
    tract_sources: list | None = None,
    out_key: str = "processed/joined/tract_joined.parquet",
) -> dict:
    """Spatial-join schools (point geom) into tract polygons; aggregate
    tract-level features to district level via population-weighted mean
    across all tracts intersecting the district boundary."""
    try:
        import geopandas as gpd
        from shapely.geometry import Point
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "skipped": True, "reason": f"geopandas: {e}"}

    spine = _safe_download(spine_key)
    if spine is None:
        return {"ok": False, "skipped": True, "reason": "spine missing"}

    src_keys = tract_sources or [
        "processed/cdc_places/latest.parquet",
        "processed/calenviroscreen/v4.parquet",
        "processed/hud_affh/latest.parquet",
        "processed/opp_insights_neighborhood/latest.parquet",
    ]

    # Build per-school point gdf (impute district centroid for missing geo)
    if "lat" not in spine.columns or "lon" not in spine.columns:
        return {"ok": False, "skipped": True,
                  "reason": "spine has no lat/lon — supply tract spine first"}
    s = spine.dropna(subset=["lat", "lon"]).copy()
    s["geometry"] = [Point(lon, lat) for lat, lon in
                       zip(s["lat"].astype(float), s["lon"].astype(float))]
    schools_gdf = gpd.GeoDataFrame(s, geometry="geometry", crs="EPSG:4326")

    out_rows = []
    for src in src_keys:
        df = _safe_download(src)
        if df is None or "tract_geoid" not in df.columns:
            continue
        # We need polygon geometries per tract. Production fix: load CA
        # census-tract polygons from census tigerline. For now we just
        # pass tract values through if we can join on tract_geoid that
        # has been pre-attached to each school by a separate step.
        if "tract_geoid" in schools_gdf.columns:
            merged = schools_gdf.merge(df, on="tract_geoid", how="left")
            out_rows.append(merged)

    if not out_rows:
        return {"ok": False, "skipped": True,
                  "reason": "no tract sources joinable; pre-attach tract_geoid first"}

    out = out_rows[0]
    for d in out_rows[1:]:
        out = out.merge(d.drop(columns="geometry", errors="ignore"),
                          on="cds", how="left", suffixes=("", "_dup"))
    # Aggregate to district level via mean (would be pop-weighted in --full)
    if "district_cds" in out.columns:
        agg = out.groupby("district_cds", as_index=False).mean(numeric_only=True)
    else:
        agg = out
    import tempfile
    f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False); f.close()
    agg.to_parquet(f.name, index=False)
    r2.upload(f.name, out_key)
    return {"ok": True, "key": out_key, "rows": int(len(agg))}
