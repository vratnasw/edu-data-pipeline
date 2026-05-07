"""Point-source proximity joiner — distance to nearest TRI / LIHTC facility,
counts within radii, IDW-weighted toxic release. Uses CA Albers (EPSG:3310)."""
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


def run_proximity_join(
    spine_key: str = "processed/education/ca_wide_features.parquet",
    tri_key: str = "processed/epa_tri/2022.parquet",
    lihtc_key: str = "processed/hud_lihtc/latest.parquet",
    out_key: str = "processed/joined/proximity_joined.parquet",
) -> dict:
    try:
        import geopandas as gpd
        from shapely.geometry import Point
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "skipped": True, "reason": f"geopandas: {e}"}

    spine = _safe_download(spine_key)
    if spine is None:
        return {"ok": False, "skipped": True, "reason": "spine missing"}
    s = spine.dropna(subset=["lat", "lon"]).copy()
    s["geometry"] = [Point(lon, lat) for lat, lon in
                       zip(s["lat"].astype(float), s["lon"].astype(float))]
    schools = gpd.GeoDataFrame(s, geometry="geometry", crs="EPSG:4326").to_crs("EPSG:3310")

    tri = _safe_download(tri_key)
    lihtc = _safe_download(lihtc_key)

    rows = pd.DataFrame({"cds": s["cds"]})

    def _nearest_stats(facilities_df: pd.DataFrame, lat_col: str, lon_col: str,
                          weight_col: str | None = None,
                          name: str = "fac") -> pd.DataFrame:
        if facilities_df is None or facilities_df.empty:
            return pd.DataFrame({
                "cds": rows["cds"],
                f"{name}_dist_nearest_km": np.nan,
                f"{name}_count_500m": 0,
                f"{name}_count_1km": 0,
                f"{name}_count_2km": 0,
            })
        f = facilities_df.dropna(subset=[lat_col, lon_col]).copy()
        if f.empty:
            return _nearest_stats(None, lat_col, lon_col, weight_col, name)
        f["geometry"] = [Point(lon, lat)
                          for lat, lon in zip(f[lat_col].astype(float),
                                                  f[lon_col].astype(float))]
        f_gdf = gpd.GeoDataFrame(f, geometry="geometry", crs="EPSG:4326").to_crs("EPSG:3310")
        # Nearest distance via spatial join
        sj = gpd.sjoin_nearest(schools, f_gdf, how="left",
                                  distance_col=f"{name}_dist_m")
        # Count facilities within radii — bbox-based fast count
        coords_s = np.array([[g.x, g.y] for g in schools.geometry])
        coords_f = np.array([[g.x, g.y] for g in f_gdf.geometry])
        # naive O(N*M) — acceptable for ~10k schools × ~10k facilities
        dists = np.linalg.norm(coords_s[:, None, :] - coords_f[None, :, :], axis=-1)
        c500 = (dists <= 500).sum(axis=1)
        c1k = (dists <= 1000).sum(axis=1)
        c2k = (dists <= 2000).sum(axis=1)
        out_df = pd.DataFrame({
            "cds": rows["cds"].values,
            f"{name}_dist_nearest_km": sj.groupby(level=0)[f"{name}_dist_m"].first().values / 1000.0
                if f"{name}_dist_m" in sj.columns else np.nan,
            f"{name}_count_500m": c500,
            f"{name}_count_1km": c1k,
            f"{name}_count_2km": c2k,
        })
        if weight_col and weight_col in f.columns:
            w = f[weight_col].fillna(0).to_numpy()
            inside_2k = dists <= 2000
            inv_d2 = 1.0 / (dists ** 2 + 1e-9)
            score = (inv_d2 * inside_2k * w[None, :]).sum(axis=1)
            out_df[f"{name}_idw_within_2km"] = score
        return out_df

    tri_stats = _nearest_stats(
        tri, "latitude" if tri is not None and "latitude" in (tri.columns if tri is not None else []) else "lat",
        "longitude" if tri is not None and "longitude" in (tri.columns if tri is not None else []) else "lon",
        weight_col="total_releases_lbs" if tri is not None and "total_releases_lbs" in (tri.columns if tri is not None else []) else None,
        name="tri",
    )
    lihtc_stats = _nearest_stats(
        lihtc, "latitude" if lihtc is not None and "latitude" in (lihtc.columns if lihtc is not None else []) else "lat",
        "longitude" if lihtc is not None and "longitude" in (lihtc.columns if lihtc is not None else []) else "lon",
        name="lihtc",
    )
    out = rows.merge(tri_stats, on="cds", how="left").merge(lihtc_stats, on="cds", how="left")
    import tempfile
    f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False); f.close()
    out.to_parquet(f.name, index=False)
    r2.upload(f.name, out_key)
    return {"ok": True, "key": out_key, "rows": int(len(out))}
