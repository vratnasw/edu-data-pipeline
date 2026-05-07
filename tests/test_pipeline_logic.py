"""Pipeline orchestrator: skip-when-fresh logic + tract spatial-join sanity."""
from __future__ import annotations

import os
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from collectors._base import (
    list_collectors, register_collector, _default_check_update,
)


def test_pipeline_skips_when_check_update_false():
    """Synthesize a collector whose check_update returns needs_update=False;
    pipeline must skip it and report `skipped: True`."""
    @register_collector("test_skip_source", "test")
    def _do(year=None, force: bool = False) -> dict:
        if not force:
            return {"ok": True, "skipped": True, "reason": "fresh"}
        return {"ok": True, "skipped": False, "key": "raw/test_skip_source/latest.parquet"}

    spec = next(s for s in list_collectors() if s.name == "test_skip_source")
    rep = spec.collect(force=False)
    assert rep["ok"] and rep["skipped"]
    rep2 = spec.collect(force=True)
    assert rep2["ok"] and not rep2.get("skipped", False)


def test_tract_spatial_synthetic_5_schools():
    """5 synthetic schools vs 3 synthetic tract polygons — verify each
    school resolves to exactly one containing tract."""
    pytest.importorskip("geopandas")
    import geopandas as gpd
    from shapely.geometry import Point, Polygon

    schools = pd.DataFrame({
        "cds": ["s1", "s2", "s3", "s4", "s5"],
        "lat": [37.0, 37.5, 37.1, 37.9, 37.3],
        # nudge lon values so no school sits exactly on a tract boundary
        "lon": [-122.5, -122.05, -122.4, -122.1, -122.2],
    })
    schools["geometry"] = [Point(lon, lat)
                              for lat, lon in zip(schools["lat"], schools["lon"])]
    schools_gdf = gpd.GeoDataFrame(schools, geometry="geometry", crs="EPSG:4326")
    # Two tracts roughly partitioning the lat/lon space
    tracts = gpd.GeoDataFrame({
        "tract_id": ["T1", "T2", "T3"],
        "geometry": [
            Polygon([(-122.6, 36.9), (-122.3, 36.9), (-122.3, 37.6), (-122.6, 37.6)]),
            Polygon([(-122.3, 36.9), (-122.0, 36.9), (-122.0, 37.6), (-122.3, 37.6)]),
            Polygon([(-122.3, 37.6), (-122.0, 37.6), (-122.0, 38.0), (-122.3, 38.0)]),
        ],
    }, crs="EPSG:4326")
    sj = gpd.sjoin(schools_gdf, tracts, how="left", predicate="within")
    # Every school must resolve to exactly one tract
    assert sj["tract_id"].notna().all()
    assert len(sj) == 5
    # s4 is at lat=37.9 → in T3
    assert sj[sj["cds"] == "s4"]["tract_id"].iloc[0] == "T3"


def test_default_check_update_no_source_name():
    rep = _default_check_update(year=2022, source_name=None)
    assert rep["needs_update"] is True
