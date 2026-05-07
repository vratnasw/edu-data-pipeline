"""County-level joiner — joins all county-keyed processed sources to the CDS spine."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

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


def run_county_join(spine_key: str = "processed/education/ca_wide_features.parquet",
                       county_sources: list | None = None,
                       out_key: str = "processed/joined/county_panel.parquet") -> dict:
    spine = _safe_download(spine_key)
    if spine is None:
        return {"ok": False, "skipped": True, "reason": f"spine missing: {spine_key}"}
    if "county_code" not in spine.columns and "cds" in spine.columns:
        spine["county_code"] = spine["cds"].astype(str).str.slice(0, 2).str.zfill(2)
    src_keys = county_sources or [
        "processed/bea_gdp_personal_income/latest.parquet",
        "processed/bls_unemployment/latest.parquet",
        "processed/census_acs5/2022.parquet",
        "processed/zillow_zori/latest.parquet",
        "processed/fhfa_hpi/latest.parquet",
        "processed/ca_doj_openjustice/2022.parquet",
    ]
    out = spine.copy()
    n0 = len(out)
    joined: list[str] = []
    for key in src_keys:
        d = _safe_download(key)
        if d is None or "county_code" not in d.columns: continue
        d2 = d.groupby("county_code", as_index=False).first()
        before = len(out)
        out = out.merge(d2, on="county_code", how="left", suffixes=("", "_dup"))
        joined.append(key)
        log.info("county_join: merged %s (rows %d → %d)", key, before, len(out))
    n1 = len(out)
    retention = n1 / max(1, n0)
    if retention < 0.95:
        log.warning("county_join retention %.2f < 0.95", retention)
    import tempfile
    f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False); f.close()
    out.to_parquet(f.name, index=False)
    rep = r2.upload(f.name, out_key)
    return {"ok": True, "key": out_key, "rows": n1, "cols": int(out.shape[1]),
              "retention": retention, "joined": joined, **rep}
