"""Track 1.5: produce processed/canonical/<source>.parquet for every source
with real (non-stub) data, using consistent county_code + year keys and
canonical snake_case column names per the user-supplied convention.

Output per-source contracts:
  bea               county_code, year, bea_gdp_total, bea_personal_income_per_capita
  bls_unemployment  county_code, year, bls_unemployment_rate
  census_acs5       county_code, year, census_acs5_median_household_income
  zillow            year, zillow_median_rent  (state-level CA aggregate;
                      no metro→county crosswalk available in Track 1.5)
  epa_aqs           county_code, year, epa_pm25_annual_mean
  noaa              SKIPPED — station-level data, no county aggregator yet
  cdc_places        SKIPPED — tract-level health, not in the economic set

Stub-only sources (saipe, fhfa, calenviroscreen, ca_controller, hud_lihtc, etc.)
are noted as deferred — they require a per-source HTML extractor pass before
they can yield real columns.
"""
from __future__ import annotations

import json
import logging
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO)); sys.path.insert(0, str(REPO / "src"))
from utils.config_loader import load_dotenv  # noqa: E402
import config.r2_client as r2  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

CACHE = REPO / "data_cache" / "canonical"
CACHE.mkdir(parents=True, exist_ok=True)


def _save_canonical(df: pd.DataFrame, name: str) -> dict:
    p = CACHE / f"{name}.parquet"
    df.to_parquet(p, index=False)
    rep = r2.upload(p, f"processed/canonical/{name}.parquet")
    log.info("[%s] shape=%s cols=%s", name, df.shape, list(df.columns))
    rep.update({"name": name, "rows": len(df), "cols": list(df.columns)})
    return rep


# --------------------------------------------------------------------------- #
# BEA — county GDP + per-capita personal income
# --------------------------------------------------------------------------- #

def proc_bea() -> dict:
    df = r2.download("raw/bea_gdp_personal_income/latest.parquet")
    df = df[["geofips", "timeperiod", "datavalue", "metric"]].copy()
    df["county_code"] = df["geofips"].astype(str).str.zfill(5)
    df["year"] = pd.to_numeric(df["timeperiod"], errors="coerce").astype("Int64")
    df["datavalue"] = pd.to_numeric(df["datavalue"], errors="coerce")
    df = df.dropna(subset=["county_code", "year"])
    # Pivot: one row per (county_code, year) with two value cols
    wide = df.pivot_table(index=["county_code", "year"],
                              columns="metric", values="datavalue",
                              aggfunc="mean").reset_index()
    wide.columns.name = None
    wide = wide.rename(columns={"gdp": "bea_gdp_total",
                                      "personal_income": "bea_personal_income_per_capita"})
    keep = ["county_code", "year"] + [c for c in
                ("bea_gdp_total", "bea_personal_income_per_capita") if c in wide.columns]
    wide = wide[keep]
    wide["year"] = wide["year"].astype("Int64")
    return _save_canonical(wide, "bea")


# --------------------------------------------------------------------------- #
# BLS — annual unemployment rate per county
# --------------------------------------------------------------------------- #

def proc_bls() -> dict:
    df = r2.download("raw/bls_unemployment/latest.parquet")
    df = df[["year", "period", "value", "seriesID"]].copy()
    # seriesID format: LAUCN<2-state><3-county>0000000003 — first 5 digits after
    # 'LAUCN' are state+county FIPS.
    df["county_code"] = df["seriesID"].astype(str).str[5:10]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    # M01..M12 are monthly; M13 is annual avg. Prefer M13 if present, else mean
    annual = df[df["period"] == "M13"][["county_code", "year", "value"]].copy()
    if annual.empty:
        annual = (df[df["period"].str.startswith("M")]
                       .groupby(["county_code", "year"], as_index=False)["value"].mean())
    annual = annual.rename(columns={"value": "bls_unemployment_rate"})
    annual = annual.dropna(subset=["county_code", "year"])
    return _save_canonical(annual, "bls_unemployment")


# --------------------------------------------------------------------------- #
# Census ACS5 — county median household income
# --------------------------------------------------------------------------- #

def proc_census_acs5() -> dict:
    df = r2.download("raw/census_acs5/2022.parquet")
    df["county_code"] = df["state"].astype(str).str.zfill(2) + \
                              df["county"].astype(str).str.zfill(3)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["census_acs5_median_household_income"] = pd.to_numeric(
        df["B19013_001E"], errors="coerce")
    df = df[["county_code", "year",
                "census_acs5_median_household_income"]].dropna(subset=["county_code", "year"])
    return _save_canonical(df, "census_acs5")


# --------------------------------------------------------------------------- #
# Zillow ZORI — CA-state-level annual median (no metro→county map)
# --------------------------------------------------------------------------- #

def proc_zillow() -> dict:
    df = r2.download("raw/zillow_zori/latest.parquet")
    # Filter to CA metros
    df = df[(df["StateName"] == "CA") & (df["RegionType"].str.lower() == "msa")].copy()
    if df.empty:
        return _save_canonical(pd.DataFrame(columns=["year", "zillow_median_rent"]),
                                  "zillow")
    date_cols = [c for c in df.columns if "-" in c and len(c) == 10]
    melted = df.melt(id_vars=["RegionName"],
                          value_vars=date_cols,
                          var_name="date", value_name="rent")
    melted["year"] = pd.to_datetime(melted["date"], errors="coerce").dt.year
    melted["rent"] = pd.to_numeric(melted["rent"], errors="coerce")
    annual = (melted.dropna(subset=["year", "rent"])
                       .groupby("year", as_index=False)["rent"].median()
                       .rename(columns={"rent": "zillow_median_rent"}))
    annual["year"] = annual["year"].astype("Int64")
    return _save_canonical(annual, "zillow")


# --------------------------------------------------------------------------- #
# EPA AQS — county PM2.5 annual mean
# --------------------------------------------------------------------------- #

def proc_epa_aqs() -> dict:
    df = r2.download("raw/epa_aqs/2022.parquet")
    # Filter to PM2.5 (parameter_code 88101) and CA (state code 6)
    sc = df["State Code"].astype(int) if "State Code" in df.columns else None
    pc = df["Parameter Code"].astype(int) if "Parameter Code" in df.columns else None
    if sc is None or pc is None:
        return _save_canonical(
            pd.DataFrame(columns=["county_code", "year", "epa_pm25_annual_mean"]),
            "epa_aqs")
    df = df[(sc == 6) & (pc == 88101)].copy()
    if df.empty:
        return _save_canonical(
            pd.DataFrame(columns=["county_code", "year", "epa_pm25_annual_mean"]),
            "epa_aqs")
    df["county_code"] = (df["State Code"].astype(int).astype(str).str.zfill(2)
                              + df["County Code"].astype(int).astype(str).str.zfill(3))
    df["year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
    val_col = next((c for c in df.columns
                          if c.lower() in ("arithmetic mean", "arithmetic_mean", "mean")),
                       None)
    if val_col is None:
        return _save_canonical(
            pd.DataFrame(columns=["county_code", "year", "epa_pm25_annual_mean"]),
            "epa_aqs")
    df["val"] = pd.to_numeric(df[val_col], errors="coerce")
    out = (df.dropna(subset=["county_code", "year", "val"])
                .groupby(["county_code", "year"], as_index=False)["val"].mean()
                .rename(columns={"val": "epa_pm25_annual_mean"}))
    return _save_canonical(out, "epa_aqs")


# --------------------------------------------------------------------------- #
# Top-level: run all + report
# --------------------------------------------------------------------------- #

def main() -> int:
    load_dotenv()
    if not r2.smoke_check()["ok"]:
        log.error("R2 not configured"); return 1

    reports = []
    for fn, name in [(proc_bea, "bea"), (proc_bls, "bls_unemployment"),
                        (proc_census_acs5, "census_acs5"), (proc_zillow, "zillow"),
                        (proc_epa_aqs, "epa_aqs")]:
        try:
            reports.append(fn())
        except Exception as e:  # noqa: BLE001
            log.error("[%s] FAILED: %s", name, e)
            reports.append({"name": name, "ok": False, "error": str(e)})

    deferred = {
        "saipe": "Stub-only on R2; child poverty rate requires HTML extractor",
        "fhfa": "Stub-only on R2; HPI requires CMS link extractor",
        "calenviroscreen": "Stub-only on R2; OEHHA Excel link shifted",
        "ca_sco_local_finance": "Stub-only; needs ByTheNumbers API extractor",
        "ca_edd_lmi": "Stub-only; SSL cert + portal link extractor",
        "noaa": "Real data on R2 but station→county aggregator not yet wired",
        "irs_soi": "Stub-only",
    }

    summary = {"canonical_sources": reports, "deferred": deferred}
    out = REPO / "logs" / "canonical_build_report.json"
    out.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    log.info("\nreport: %s", out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
