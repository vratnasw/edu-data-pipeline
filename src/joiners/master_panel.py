"""Master panel — left-joins everything onto the CDS spine + hierarchical
imputation + per-column quality scoring + public ACL + data dictionary."""
from __future__ import annotations

import datetime as dt
import json
import logging
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
import config.r2_client as r2  # noqa: E402

log = logging.getLogger(__name__)


def _safe_download(key: str) -> pd.DataFrame | None:
    if r2.exists(key) is None: return None
    return r2.download(key)


def hierarchical_impute(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing numeric values: school → district → county → state mean."""
    out = df.copy()
    numeric = out.select_dtypes(include=[np.number]).columns.tolist()
    if not numeric:
        return out
    if "district_cds" in out.columns:
        district_means = out.groupby("district_cds")[numeric].transform("mean")
        out[numeric] = out[numeric].fillna(district_means)
    if "county_code" in out.columns:
        county_means = out.groupby("county_code")[numeric].transform("mean")
        out[numeric] = out[numeric].fillna(county_means)
    state_means = out[numeric].mean(numeric_only=True)
    out[numeric] = out[numeric].fillna(state_means)
    return out


def column_quality(df: pd.DataFrame) -> dict:
    return {c: 1.0 - float(df[c].isna().mean()) for c in df.columns}


def build_data_dictionary(df: pd.DataFrame, source_map: dict) -> list[dict]:
    rows = []
    for c in df.columns:
        rows.append({
            "column": c,
            "dtype": str(df[c].dtype),
            "missingness": float(df[c].isna().mean()),
            "n_nonnull": int(df[c].notna().sum()),
            "source": source_map.get(c, "unknown"),
            "units": "",  # filled in by hand for the paper data dictionary
        })
    return rows


def run_master_panel(
    spine_key: str = "processed/education/ca_wide_features.parquet",
    county_key: str = "processed/joined/county_panel.parquet",
    tract_key: str = "processed/joined/tract_joined.parquet",
    proximity_key: str = "processed/joined/proximity_joined.parquet",
    edu_keys: list | None = None,
    out_key: str = "processed/joined/master_panel.parquet",
    public_key: str = "public/master_panel.parquet",
) -> dict:
    spine = _safe_download(spine_key)
    if spine is None:
        return {"ok": False, "skipped": True, "reason": "spine missing"}

    n0 = len(spine); panel = spine.copy()
    layers = []
    for key, source_label in (
        (county_key, "county_panel"),
        (tract_key, "tract_joined"),
        (proximity_key, "proximity_joined"),
    ):
        d = _safe_download(key)
        if d is None: continue
        join_keys = [k for k in ("cds", "year") if k in panel.columns and k in d.columns]
        if not join_keys:
            join_keys = ["cds"] if "cds" in panel.columns and "cds" in d.columns else []
        if not join_keys: continue
        panel = panel.merge(d, on=join_keys, how="left", suffixes=("", f"_{source_label}"))
        layers.append(source_label)

    if edu_keys:
        for key in edu_keys:
            d = _safe_download(key)
            if d is not None and "cds" in d.columns:
                panel = panel.merge(d, on=["cds"] +
                                            (["year"] if "year" in d.columns else []),
                                            how="left",
                                            suffixes=("", "_edu"))
                layers.append(key)

    n1 = len(panel)
    retention = n1 / max(1, n0)
    if retention < 0.95:
        log.warning("master panel retention %.2f < 0.95 — investigate", retention)

    # Hierarchical impute + quality
    imputed = hierarchical_impute(panel)
    qual = column_quality(imputed)
    over_threshold = {c: q for c, q in qual.items() if 1 - q > 0.40}

    # Persist
    f1 = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False); f1.close()
    imputed.to_parquet(f1.name, index=False)
    r2.upload(f1.name, out_key)
    r2.upload(f1.name, public_key)
    r2.make_public(public_key)

    # Data dictionary
    src_map = {c: "spine" for c in spine.columns}
    for s in layers: src_map.update({c: s for c in imputed.columns if c not in src_map})
    dd = build_data_dictionary(imputed, src_map)
    dd_path = _REPO / "logs" / "data_dictionary.json"
    dd_path.parent.mkdir(parents=True, exist_ok=True)
    dd_path.write_text(json.dumps({
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "n_rows": n1,
        "n_cols": int(imputed.shape[1]),
        "retention": retention,
        "n_columns_above_40pct_missing": len(over_threshold),
        "columns": dd,
    }, indent=2), encoding="utf-8")
    f2 = tempfile.NamedTemporaryFile(suffix=".json", delete=False); f2.close()
    Path(f2.name).write_text(dd_path.read_text(encoding="utf-8"), encoding="utf-8")
    r2.upload(f2.name, "public/data_dictionary.json")
    r2.make_public("public/data_dictionary.json")

    return {
        "ok": True,
        "out_key": out_key,
        "public_key": public_key,
        "rows": n1,
        "cols": int(imputed.shape[1]),
        "retention": retention,
        "n_columns_above_40pct_missing": len(over_threshold),
        "layers_joined": layers,
        "public_url": r2.public_url(public_key),
    }
