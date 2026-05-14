"""Phase 15 — SF Environmental Justice + Air Pollutant Exposure Zone overlays.

SF-only data from data.sfgov.org SODA endpoints:
  - y6ci-vpnb : SF Environmental Justice Communities Map (tract-level score)
  - t65d-x6p8 : Air Pollutant Exposure Zone (single boundary polygon)

For each CA school we compute (point-in-polygon):
  sf_in_ejc_zone           : bool — school is inside an EJC tract
  sf_ejc_score             : int  — EJC score of containing tract (NaN otherwise)
  sf_in_air_pollutant_exposure_zone : bool

Non-SF schools get False / NaN. Useful as school-level GNN node features.
"""
from __future__ import annotations

import io
import logging
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "src"))

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import shape

import config.r2_client as r2  # noqa: E402

logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s %(levelname)s :: %(message)s",
                       datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0) Chrome/120.0.0.0 Safari/537.36"}
SCHOOLS_GEO = (REPO.parent / "SFUSD_DATA_ANALYSIS" / "dashboard" / "public"
                  / "data" / "ca" / "geo" / "schools.geojson")

EJC_ID = "y6ci-vpnb"
APEZ_ID = "t65d-x6p8"


def fetch_all_features(dataset_id: str) -> gpd.GeoDataFrame:
    """SODA endpoints cap at $limit=200 default. Paginate to pull everything."""
    out = []
    offset = 0
    page = 1000
    while True:
        url = f"https://data.sfgov.org/resource/{dataset_id}.geojson"
        r = requests.get(url, headers=HEADERS,
                          params={"$limit": page, "$offset": offset}, timeout=120)
        r.raise_for_status()
        d = r.json()
        feats = d.get("features", [])
        if not feats: break
        for f in feats:
            if f.get("geometry") is None: continue
            props = dict(f.get("properties", {}))
            props["geometry"] = shape(f["geometry"])
            out.append(props)
        if len(feats) < page: break
        offset += page
    return gpd.GeoDataFrame(out, geometry="geometry", crs="EPSG:4326")


def main() -> int:
    log.info("[sf] fetching EJC dataset %s", EJC_ID)
    ejc = fetch_all_features(EJC_ID)
    log.info("[sf] EJC: %d polygons", len(ejc))
    if "score" in ejc.columns:
        ejc["score"] = pd.to_numeric(ejc["score"], errors="coerce")

    log.info("[sf] fetching APEZ dataset %s", APEZ_ID)
    apez = fetch_all_features(APEZ_ID)
    log.info("[sf] APEZ: %d polygons", len(apez))

    # Upload raw GeoJSON-as-parquet for both
    ejc_local = REPO / "data_cache" / "raw" / "sf_overlays" / "ejc_zones.parquet"
    ejc_local.parent.mkdir(parents=True, exist_ok=True)
    ejc.to_parquet(ejc_local, index=False)
    r2.upload(ejc_local, "raw/sf_overlays/ejc_zones.parquet")
    apez_local = REPO / "data_cache" / "raw" / "sf_overlays" / "apez.parquet"
    apez.to_parquet(apez_local, index=False)
    r2.upload(apez_local, "raw/sf_overlays/apez.parquet")

    # Schools point-in-polygon
    schools = gpd.read_file(SCHOOLS_GEO)
    schools["cds"] = schools["cds"].astype(str)
    log.info("[sf] CA schools loaded: %d", len(schools))
    if schools.crs != ejc.crs:
        schools = schools.to_crs(ejc.crs)

    # EJC spatial join
    ejc_join = gpd.sjoin(schools[["cds", "geometry"]], ejc[["score", "geometry"]],
                              how="left", predicate="within")
    ejc_join = ejc_join.drop_duplicates("cds", keep="first")
    ejc_join["sf_in_ejc_zone"] = ejc_join["score"].notna()
    ejc_join["sf_ejc_score"] = ejc_join["score"]
    log.info("[sf] schools in EJC zone: %d / %d",
              int(ejc_join["sf_in_ejc_zone"].sum()), len(schools))

    # APEZ spatial join (any polygon in apez)
    apez_join = gpd.sjoin(schools[["cds", "geometry"]], apez[["geometry"]],
                                how="left", predicate="within")
    apez_join = apez_join.drop_duplicates("cds", keep="first")
    apez_join["sf_in_air_pollutant_exposure_zone"] = apez_join["index_right"].notna()
    log.info("[sf] schools in APEZ: %d / %d",
              int(apez_join["sf_in_air_pollutant_exposure_zone"].sum()), len(schools))

    # Merge to one school-level frame
    out = (schools[["cds"]].merge(
              ejc_join[["cds", "sf_in_ejc_zone", "sf_ejc_score"]],
              on="cds", how="left")
              .merge(apez_join[["cds", "sf_in_air_pollutant_exposure_zone"]],
                       on="cds", how="left"))
    # Type cleanup: pad NaN/False on bool cols
    out["sf_in_ejc_zone"] = out["sf_in_ejc_zone"].fillna(False).astype(bool)
    out["sf_in_air_pollutant_exposure_zone"] = (
        out["sf_in_air_pollutant_exposure_zone"].fillna(False).astype(bool))

    # Canonical school-level + district-level aggregate
    sch_local = REPO / "data_cache" / "processed" / "canonical" / "sf_overlays_school.parquet"
    sch_local.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(sch_local, index=False)
    r2.upload(sch_local, "processed/canonical/sf_overlays_school.parquet")

    # District-level aggregate: fraction of schools per district inside each zone
    out["cds_district"] = out["cds"].astype(str).str.replace("-", "").str[:6]
    dist = (out.groupby("cds_district").agg(
        sf_district_ejc_share=("sf_in_ejc_zone", "mean"),
        sf_district_ejc_score_mean=("sf_ejc_score", "mean"),
        sf_district_apez_share=("sf_in_air_pollutant_exposure_zone", "mean"),
        n_schools=("cds", "count"),
    ).reset_index().rename(columns={"cds_district": "cds"}))
    dist_local = REPO / "data_cache" / "processed" / "canonical" / "sf_overlays_district.parquet"
    dist.to_parquet(dist_local, index=False)
    r2.upload(dist_local, "processed/canonical/sf_overlays_district.parquet")

    # Summary
    log.info("[sf] school-level uploaded (%d rows)", len(out))
    log.info("[sf] district-level uploaded (%d districts)", len(dist))
    n_ejc_d = (dist["sf_district_ejc_share"] > 0).sum()
    n_apez_d = (dist["sf_district_apez_share"] > 0).sum()
    log.info("[sf] districts touching EJC: %d, touching APEZ: %d", n_ejc_d, n_apez_d)
    return 0


if __name__ == "__main__":
    sys.exit(main())
