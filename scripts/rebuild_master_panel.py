"""Track 1.5 step 3: rebuild master_panel.parquet from clean canonical sources.

Replaces the broken county_panel self-join with proper county-level merges.
Key insight: spine has 1010 districts × 10 years = 10100 rows; county-level
sources have 58 counties × N years. The right merge is `(county_code, year)`,
which broadcasts each county-year value to ALL districts in that county.

Year-only sources (Zillow CA-aggregate) are broadcast to every county-year.
Single-year sources (Census ACS 2022) are merged only on county_code so the
2022 value applies to every year (the cleanest option for static cross-section
data; downstream code should treat it as a fixed effect, not panel-time-varying).
"""
from __future__ import annotations

import json
import logging
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO)); sys.path.insert(0, str(REPO / "src"))
from utils.config_loader import load_dotenv  # noqa: E402
import config.r2_client as r2  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def _safe_download(key: str) -> pd.DataFrame | None:
    if r2.exists(key) is None:
        return None
    return r2.download(key)


def _normalize_county_code(s: pd.Series) -> pd.Series:
    return s.astype(str).str.zfill(5)


def main() -> int:
    load_dotenv()
    if not r2.smoke_check()["ok"]:
        log.error("R2 not configured"); return 1

    # ---- Spine ---- #
    spine = _safe_download("processed/education/ca_wide_features.parquet")
    if spine is None:
        log.error("spine missing on R2"); return 1
    # CRITICAL: CA's CDS county codes (01-58 alphabetical) are NOT FIPS.
    # FIPS county codes for CA use the federal 1979 sequence (odd numbers).
    # Conversion: FIPS = 2 * CDS_county_num - 1 (verified for Alameda 01→001,
    # Los Angeles 19→037, Orange 30→059, Yuba 58→115).
    cds_county_num = pd.to_numeric(
        spine["cds"].astype(str).str.slice(0, 2), errors="coerce")
    spine["county_code"] = ("06" +
                                  (2 * cds_county_num - 1).fillna(0).astype(int)
                                  .astype(str).str.zfill(3))
    log.info("spine: shape=%s, %d districts × %d years",
              spine.shape, spine["cds"].nunique(), spine["year_num"].nunique())
    log.info("  county_code sample: %s", spine["county_code"].drop_duplicates().head(5).tolist())

    panel = spine.copy()
    n0 = len(panel)
    layers_joined: list[dict] = []

    # ---- Canonical (county, year)-keyed sources ---- #
    for src_key, key_cols, source_label in [
        ("processed/canonical/bea.parquet",                ["county_code", "year"], "bea"),
        ("processed/canonical/bls_unemployment.parquet",  ["county_code", "year"], "bls"),
        ("processed/canonical/epa_aqs.parquet",            ["county_code", "year"], "epa_aqs"),
    ]:
        d = _safe_download(src_key)
        if d is None: continue
        d["county_code"] = _normalize_county_code(d["county_code"])
        d["year"] = pd.to_numeric(d["year"], errors="coerce").astype("Int64")
        # rename year → year_num for spine compatibility
        d = d.rename(columns={"year": "year_num"})
        before = panel.shape
        panel = panel.merge(d, on=["county_code", "year_num"], how="left")
        layers_joined.append({"source": source_label, "before": before,
                                "after": panel.shape, "key_cols": key_cols,
                                "added_cols": [c for c in d.columns
                                                if c not in ("county_code", "year_num")]})
        log.info("merged %s: %s → %s", source_label, before, panel.shape)

    # ---- County-only (single-year) sources: ACS 2022 ---- #
    acs = _safe_download("processed/canonical/census_acs5.parquet")
    if acs is not None:
        acs["county_code"] = _normalize_county_code(acs["county_code"])
        # Drop year — broadcast as time-invariant
        acs_static = acs.drop(columns=[c for c in acs.columns
                                                if c == "year"]).drop_duplicates("county_code")
        before = panel.shape
        panel = panel.merge(acs_static, on="county_code", how="left")
        layers_joined.append({"source": "census_acs5", "before": before,
                                "after": panel.shape, "key_cols": ["county_code"],
                                "note": "single-year (2022) broadcast to all years"})
        log.info("merged census_acs5: %s → %s", before, panel.shape)

    # ---- Year-only sources: Zillow CA-aggregate ---- #
    zil = _safe_download("processed/canonical/zillow.parquet")
    if zil is not None:
        zil["year"] = pd.to_numeric(zil["year"], errors="coerce").astype("Int64")
        zil = zil.rename(columns={"year": "year_num"})
        before = panel.shape
        panel = panel.merge(zil, on="year_num", how="left")
        layers_joined.append({"source": "zillow", "before": before,
                                "after": panel.shape, "key_cols": ["year_num"],
                                "note": "year-only (CA aggregate) broadcast to all districts"})
        log.info("merged zillow: %s → %s", before, panel.shape)

    n1 = len(panel)
    if n1 != n0:
        log.error("ROW COUNT CHANGED %d → %d. Aborting (join introduced cardinality bug).",
                    n0, n1); return 2

    # ---- Validate exactly 1010 districts × 10 years ---- #
    expected = spine["cds"].nunique() * spine["year_num"].nunique()
    if n1 != expected:
        log.error("rows %d != expected %d", n1, expected); return 2
    log.info("✓ row count = %d (= %d districts × %d years)",
              n1, spine["cds"].nunique(), spine["year_num"].nunique())

    # ---- Missingness per economic variable ---- #
    canonical_vars = ["bea_gdp_total", "bls_unemployment_rate",
                          "epa_pm25_annual_mean",
                          "census_acs5_median_household_income",
                          "zillow_median_rent"]
    missingness = {}
    for v in canonical_vars:
        if v in panel.columns:
            missingness[v] = round(float(panel[v].isna().mean()), 4)
        else:
            missingness[v] = None
    log.info("\nMISSINGNESS PER ECONOMIC VARIABLE:")
    for v, m in missingness.items():
        log.info("  %-45s  %s", v,
                  f"{m:.2%}" if m is not None else "ABSENT")

    # ---- Persist ---- #
    f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False); f.close()
    panel.to_parquet(f.name, index=False)
    rep = r2.upload(f.name, "processed/joined/master_panel.parquet")
    rep_pub = r2.upload(f.name, "public/master_panel.parquet")

    # ---- Data dictionary ---- #
    dd = []
    for col in panel.columns:
        dd.append({
            "column": col,
            "dtype": str(panel[col].dtype),
            "missingness": float(panel[col].isna().mean()),
            "n_nonnull": int(panel[col].notna().sum()),
        })
    dd_payload = {"generated_at": datetime.now(timezone.utc).isoformat(),
                    "n_rows": int(len(panel)),
                    "n_cols": int(panel.shape[1]),
                    "layers_joined": layers_joined,
                    "columns": dd}
    dd_path = REPO / "data_cache" / "data_dictionary.json"
    dd_path.write_text(json.dumps(dd_payload, indent=2, default=str), encoding="utf-8")
    r2.upload(dd_path, "public/data_dictionary.json")

    # ---- Final summary ---- #
    log.info("\n=== REBUILT MASTER PANEL ===")
    log.info("  rows: %d  cols: %d", len(panel), panel.shape[1])
    log.info("  R2: processed/joined/master_panel.parquet (%d B)", rep["size_bytes"])
    log.info("  R2: public/master_panel.parquet")
    log.info("  R2: public/data_dictionary.json")
    log.info("  layers_joined: %d", len(layers_joined))
    log.info("  columns ending in _county_panel: %d (must be 0)",
              sum(1 for c in panel.columns if c.endswith("_county_panel")))

    out = REPO / "logs" / "track1_5_rebuild_report.json"
    out.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows": int(len(panel)), "cols": int(panel.shape[1]),
        "expected_rows": int(expected),
        "row_count_correct": int(len(panel)) == int(expected),
        "missingness_per_economic_variable": missingness,
        "layers_joined": layers_joined,
        "n_columns_ending_in_county_panel_suffix": int(sum(1 for c in panel.columns
                                                                if c.endswith("_county_panel"))),
    }, indent=2, default=str), encoding="utf-8")
    log.info("report: %s", out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
