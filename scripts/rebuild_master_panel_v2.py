"""Track 2: rebuild master_panel.parquet with Track 1.5 sources PLUS new sources.

Same join scaffolding as the Track 1.5 rebuild_master_panel.py, but adds:
  - Census SAIPE district poverty (district-level, multi-year)
  - CDC PLACES (county-level, vintage 2023 broadcast)
  - CalEnviroScreen (county-level, vintage 2021 broadcast)
  - OI neighborhood mobility (county-level, broadcast)
  - HUD LIHTC district-level proximity aggregates
  - EPA TRI district-level proximity aggregates

Deferred (Track 3, stubbed): CA DOJ OpenJustice, CHKS, HUD AFFH, FEMA flood.
See stubs/deferred_sources.json on R2.
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


def _safe_download(key: str):
    if r2.exists(key) is None:
        log.warning("[panel] absent on R2: %s — skipping", key)
        return None
    return r2.download(key)


def _norm_cc(s):
    return s.astype(str).str.replace(r"\D", "", regex=True).str.zfill(5)


def main() -> int:
    load_dotenv()
    if not r2.smoke_check()["ok"]:
        log.error("R2 not configured"); return 1

    spine = _safe_download("processed/education/ca_wide_features.parquet")
    if spine is None:
        log.error("spine missing on R2"); return 1
    cds_num = pd.to_numeric(spine["cds"].astype(str).str.slice(0, 2),
                                errors="coerce")
    spine["county_code"] = ("06" + (2 * cds_num - 1).fillna(0).astype(int)
                                  .astype(str).str.zfill(3))
    log.info("spine: %d districts × %d years = %d rows",
              spine["cds"].nunique(), spine["year_num"].nunique(), len(spine))

    panel = spine.copy()
    n_expected = len(panel)
    layers: list[dict] = []
    new_columns: list[str] = []

    def _merge_county_year(src_key: str, label: str, year_col: str = "year_num"):
        nonlocal panel
        d = _safe_download(src_key)
        if d is None: return
        d["county_code"] = _norm_cc(d["county_code"])
        if year_col in d.columns and year_col != "year_num":
            d = d.rename(columns={year_col: "year_num"})
        if "year" in d.columns and "year_num" not in d.columns:
            d = d.rename(columns={"year": "year_num"})
        d["year_num"] = pd.to_numeric(d["year_num"], errors="coerce").astype("Int64")
        before = panel.shape
        panel = panel.merge(d, on=["county_code", "year_num"], how="left")
        added = [c for c in d.columns if c not in ("county_code", "year_num")]
        new_columns.extend(added)
        layers.append({"source": label, "before": before, "after": panel.shape,
                          "key": "county+year", "added": added})
        log.info("merged %s: %s → %s (added %d cols)", label, before, panel.shape, len(added))

    def _merge_county_only(src_key: str, label: str):
        """For static / single-vintage county-level sources — broadcast to all years."""
        nonlocal panel
        d = _safe_download(src_key)
        if d is None: return
        d["county_code"] = _norm_cc(d["county_code"])
        # Drop any year-like or count-like helper col so we don't shadow others
        drop_cols = [c for c in d.columns if c in ("year", "year_num", "n_tracts")]
        d = d.drop(columns=drop_cols, errors="ignore").drop_duplicates("county_code")
        before = panel.shape
        panel = panel.merge(d, on="county_code", how="left")
        added = [c for c in d.columns if c not in ("county_code",)]
        new_columns.extend(added)
        layers.append({"source": label, "before": before, "after": panel.shape,
                          "key": "county_code (broadcast)", "added": added})
        log.info("merged %s: %s → %s (added %d cols, broadcast)", label, before, panel.shape, len(added))

    def _merge_district_year(src_key: str, label: str):
        """For sources keyed on (district_cds, year_num).

        SAIPE's cds is from districts.geojson where district_name → CDS lookup
        yields the 7-digit district CDS (no dash). Spine cds is 14-digit with
        dashes. Both reduce to 6 digits-only for matching.
        """
        nonlocal panel
        d = _safe_download(src_key)
        if d is None: return
        if "cds" not in d.columns:
            log.warning("[panel] %s has no cds column — skipping", label)
            return
        d = d.copy()
        d["_join"] = d["cds"].astype(str).str.replace("-", "").str[:6]
        if "year_num" not in d.columns and "year" in d.columns:
            d = d.rename(columns={"year": "year_num"})
        d["year_num"] = pd.to_numeric(d["year_num"], errors="coerce").astype("Int64")
        d = d.dropna(subset=["_join"]).drop_duplicates(["_join", "year_num"])
        panel["_join"] = panel["cds"].astype(str).str.replace("-", "").str[:6]
        before = panel.shape
        # Don't carry source cds + district_name through (would shadow others)
        d_keep = d.drop(columns=["cds", "district_id", "district_name"],
                            errors="ignore")
        panel = panel.merge(d_keep, on=["_join", "year_num"], how="left")
        panel = panel.drop(columns=["_join"], errors="ignore")
        added = [c for c in d_keep.columns if c not in ("_join", "year_num")]
        new_columns.extend(added)
        layers.append({"source": label, "before": before, "after": panel.shape,
                          "key": "district_cds6+year", "added": added})
        log.info("merged %s: %s → %s (added %d cols)", label, before, panel.shape, len(added))

    def _merge_district_static(src_key: str, label: str):
        """For district-level proximity aggregates (no year axis).

        Handles two CDS encodings:
          spine:        '01-10017-0000000' (14 digits + dashes)
          LIHTC / TRI:  '01-1001'           (XX-YYYY = 6 digits w/ dash)
        Both reduce to '011001' (digits-only first 6) for the join.

        We use 6 digits because the LIHTC/TRI source cds is exactly that wide
        — the 7th digit of the spine's district-cds is essentially always 0
        for unified districts.
        """
        nonlocal panel
        d = _safe_download(src_key)
        if d is None: return
        if "cds" not in d.columns:
            return
        d = d.rename(columns={"cds": "cds_district"})
        d["_join"] = d["cds_district"].astype(str).str.replace("-", "").str[:6]
        d = d.drop_duplicates("_join")
        panel["_join"] = panel["cds"].astype(str).str.replace("-", "").str[:6]
        before = panel.shape
        panel = panel.merge(d.drop(columns=["cds_district"], errors="ignore"),
                                  on="_join", how="left")
        panel = panel.drop(columns=["_join"], errors="ignore")
        added = [c for c in d.columns
                    if c not in ("cds_district", "_join", "n_schools")]
        new_columns.extend(added)
        layers.append({"source": label, "before": before, "after": panel.shape,
                          "key": "district_cds6 (static)", "added": added})
        log.info("merged %s: %s → %s (added %d cols, static)", label, before, panel.shape, len(added))

    # ---- Track 1.5 county-year sources ---- #
    _merge_county_year("processed/canonical/bea.parquet", "bea")
    _merge_county_year("processed/canonical/bls_unemployment.parquet", "bls")
    _merge_county_year("processed/canonical/epa_aqs.parquet", "epa_aqs")

    # ---- Track 1.5 single-year + year-only ---- #
    acs = _safe_download("processed/canonical/census_acs5.parquet")
    if acs is not None:
        acs["county_code"] = _norm_cc(acs["county_code"])
        acs_static = acs.drop(columns=[c for c in acs.columns
                                                if c == "year"]).drop_duplicates("county_code")
        before = panel.shape
        panel = panel.merge(acs_static, on="county_code", how="left")
        added = [c for c in acs_static.columns if c != "county_code"]
        new_columns.extend(added)
        layers.append({"source": "census_acs5", "before": before,
                          "after": panel.shape, "key": "county_code (broadcast)",
                          "added": added})
        log.info("merged census_acs5: %s → %s", before, panel.shape)

    zil = _safe_download("processed/canonical/zillow.parquet")
    if zil is not None:
        zil["year"] = pd.to_numeric(zil["year"], errors="coerce").astype("Int64")
        zil = zil.rename(columns={"year": "year_num"})
        before = panel.shape
        panel = panel.merge(zil, on="year_num", how="left")
        added = [c for c in zil.columns if c != "year_num"]
        new_columns.extend(added)
        layers.append({"source": "zillow", "before": before, "after": panel.shape,
                          "key": "year_num (broadcast)", "added": added})
        log.info("merged zillow: %s → %s", before, panel.shape)

    # ---- Track 2: NEW SOURCES ---- #
    _merge_county_only("processed/canonical/cdc_places.parquet", "cdc_places")
    _merge_county_only("processed/canonical/calenviroscreen.parquet", "calenviroscreen")
    _merge_county_only("processed/canonical/opportunity_insights_mobility.parquet", "oi_mobility")
    _merge_district_year("processed/canonical/census_saipe.parquet", "census_saipe")
    _merge_district_static("processed/canonical/hud_lihtc_district.parquet", "hud_lihtc")
    _merge_district_static("processed/canonical/epa_tri_district.parquet", "epa_tri")

    # ---- Validate ---- #
    if len(panel) != n_expected:
        log.error("ROW COUNT CHANGED %d → %d", n_expected, len(panel)); return 2
    log.info("✓ row count = %d (preserved through %d joins)", len(panel), len(layers))

    # ---- Hierarchical imputation (district → county mean → state mean) ---- #
    state_means = {c: panel[c].mean() for c in new_columns
                      if c in panel.columns and pd.api.types.is_numeric_dtype(panel[c])}
    for c in list(state_means.keys()):
        # County mean (broadcast)
        county_means = panel.groupby("county_code")[c].transform("mean")
        panel[c] = panel[c].fillna(county_means)
        # State mean (broadcast)
        panel[c] = panel[c].fillna(state_means[c])

    # ---- Missingness report ---- #
    miss_report = {c: float(panel[c].isna().mean()) for c in new_columns
                      if c in panel.columns}
    high_miss = {c: m for c, m in miss_report.items() if m > 0.40}
    log.info("\n=== MISSINGNESS REPORT (after imputation) ===")
    for c, m in sorted(miss_report.items(), key=lambda kv: kv[1], reverse=True)[:30]:
        flag = " ⚠ HIGH" if m > 0.40 else ""
        log.info("  %-55s %.1f%%%s", c, 100 * m, flag)
    if high_miss:
        log.warning("[panel] %d cols with missingness >40%% — flagged as unreliable",
                      len(high_miss))

    # ---- Persist ---- #
    f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False); f.close()
    panel.to_parquet(f.name, index=False)
    rep = r2.upload(f.name, "processed/joined/master_panel.parquet")
    r2.upload(f.name, "public/master_panel.parquet")
    log.info("uploaded master_panel.parquet: %d rows, %d cols, %d KB",
              len(panel), panel.shape[1], rep["size_bytes"] // 1024)

    # ---- Data dictionary ---- #
    dd = []
    for col in panel.columns:
        dd.append({"column": col, "dtype": str(panel[col].dtype),
                      "missingness": float(panel[col].isna().mean()),
                      "is_track2_new": col in new_columns,
                      "high_missingness_flag": col in high_miss})
    dd_payload = {"generated_at": datetime.now(timezone.utc).isoformat(),
                    "n_rows": int(len(panel)), "n_cols": int(panel.shape[1]),
                    "track2_sources_integrated": [l["source"] for l in layers],
                    "track2_high_missingness_columns": list(high_miss.keys()),
                    "track3_deferred_sources": [
                        "ca_doj_openjustice", "ca_chks", "hud_affh", "fema_flood",
                    ],
                    "layers_joined": layers,
                    "columns": dd}
    dd_path = REPO / "data_cache" / "data_dictionary.json"
    dd_path.write_text(json.dumps(dd_payload, indent=2, default=str), encoding="utf-8")
    r2.upload(dd_path, "public/data_dictionary.json")
    log.info("data dictionary uploaded")

    return 0


if __name__ == "__main__":
    sys.exit(main())
