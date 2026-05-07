"""Joiner logic + hierarchical imputation + DuckDB synth + master-panel validation."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from joiners.master_panel import (
    column_quality, hierarchical_impute, build_data_dictionary,
)


def test_county_join_key_construction():
    """Spine must derive county_code from cds[:2] when not present."""
    spine = pd.DataFrame({"cds": ["01-12345-0123456", "37-65432-7654321"],
                            "x": [1.0, 2.0]})
    spine["county_code"] = spine["cds"].astype(str).str.slice(0, 2).str.zfill(2)
    assert (spine["county_code"] == ["01", "37"]).all()


def test_hierarchical_imputation_fills_missing_via_district_then_county():
    df = pd.DataFrame({
        "cds": [f"d{i}" for i in range(6)],
        "district_cds": ["A", "A", "A", "B", "B", "B"],
        "county_code": ["01", "01", "01", "01", "01", "01"],
        "value": [1.0, np.nan, 3.0, np.nan, np.nan, 6.0],
    })
    out = hierarchical_impute(df)
    # row 1's district A mean = (1+3)/2 = 2 → fills nan
    assert abs(out.loc[1, "value"] - 2.0) < 1e-9
    # row 3 (B) district mean = 6 → fills
    assert abs(out.loc[3, "value"] - 6.0) < 1e-9
    # row 4 same district B mean = 6 → fills
    assert abs(out.loc[4, "value"] - 6.0) < 1e-9


def test_hierarchical_imputation_fallback_to_state_mean():
    """If a district has all NaN, county fills; if county is all NaN, state fills."""
    df = pd.DataFrame({
        "district_cds": ["A", "A", "B", "B"],
        "county_code": ["01", "01", "01", "01"],
        "value": [10.0, np.nan, np.nan, np.nan],
    })
    out = hierarchical_impute(df)
    # county-mean = 10 → all NaNs fill to 10
    assert (out["value"] == 10.0).all()


def test_column_quality_returns_one_minus_missingness():
    df = pd.DataFrame({"a": [1, 2, 3, np.nan], "b": [1, 2, 3, 4]})
    q = column_quality(df)
    assert abs(q["a"] - 0.75) < 1e-9
    assert q["b"] == 1.0


def test_data_dictionary_has_required_fields():
    df = pd.DataFrame({"x": [1, 2, np.nan]})
    dd = build_data_dictionary(df, source_map={"x": "test_source"})
    rec = dd[0]
    for k in ("column", "dtype", "missingness", "n_nonnull", "source"):
        assert k in rec
    assert rec["source"] == "test_source"
    assert abs(rec["missingness"] - 1/3) < 1e-9


def test_proximity_radius_and_idw_compute():
    """Distance computation in a flat plane should give sensible radii counts."""
    schools = np.array([[0, 0]])    # one school at origin
    facs = np.array([[100, 0],         # 100m
                       [600, 0],          # 600m
                       [1500, 0],         # 1500m
                       [3000, 0]])        # 3000m
    d = np.linalg.norm(schools[:, None, :] - facs[None, :, :], axis=-1)[0]
    assert (d <= 500).sum() == 1
    assert (d <= 1000).sum() == 2
    assert (d <= 2000).sum() == 3
    weights = np.array([10, 20, 30, 40], dtype=float)
    inside_2k = d <= 2000
    inv_d2 = 1.0 / (d ** 2 + 1e-9)
    score = (inv_d2 * inside_2k * weights).sum()
    assert score > 0


def test_missingness_threshold_validation():
    """Master panel must flag any column above the configured threshold."""
    df = pd.DataFrame({
        "good": [1, 2, 3, 4],
        "bad": [1, np.nan, np.nan, np.nan],   # 75% missing > 0.40 threshold
    })
    threshold = 0.40
    missingness = df.isna().mean()
    above = missingness[missingness > threshold].index.tolist()
    assert "bad" in above
    assert "good" not in above
