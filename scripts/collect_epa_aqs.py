"""Phase 6 — EPA AQS multi-year annual concentration (PM2.5 FRM + Ozone).

Source: EPA AirData annual_conc_by_monitor zips at
  https://aqs.epa.gov/aqsweb/airdata/annual_conc_by_monitor_<YEAR>.zip

For each year 2010..2023, downloads the zip, filters to CA monitors (state
code 06) and parameters PM2.5 FRM (88101) + Ozone (44201), then groups by
county and takes the unweighted mean across monitors. (The annual_conc_by
file already has a County Code column, so no lat/lon spatial join is needed.)

Output canonical:
  processed/canonical/epa_aqs.parquet — one row per (county_code, year_num)
  with columns epa_pm25_annual_mean (μg/m³) + epa_ozone_annual_mean (ppm).
"""
from __future__ import annotations

import io
import logging
import sys
import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "src"))

import pandas as pd
import requests

import config.r2_client as r2  # noqa: E402

logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s %(levelname)s :: %(message)s",
                       datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 edu-data-pipeline"}
YEARS = list(range(2010, 2024))
PARAM_PM25_FRM = 88101
PARAM_OZONE = 44201
PARAMS_OF_INTEREST = {PARAM_PM25_FRM: "epa_pm25_annual_mean",
                          PARAM_OZONE: "epa_ozone_annual_mean"}


def fetch_year(year: int) -> pd.DataFrame:
    url = f"https://aqs.epa.gov/aqsweb/airdata/annual_conc_by_monitor_{year}.zip"
    log.info("[aqs %d] GET %s", year, url)
    r = requests.get(url, headers=HEADERS, timeout=600)
    r.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    name = zf.namelist()[0]
    df = pd.read_csv(zf.open(name), low_memory=False)
    # Filter to CA + relevant parameters
    df = df[df["State Code"].astype(str).str.zfill(2) == "06"]
    df = df[df["Parameter Code"].isin(PARAMS_OF_INTEREST.keys())]
    # Annual primary metric: PM2.5 = Arithmetic Mean, Ozone = 4th max 8-hr (DV)
    # For panel features, use Arithmetic Mean (interpretable across years)
    # Some years use different metric names — handle both
    df["annual_value"] = df["Arithmetic Mean"]
    df["year_num"] = year
    df["county_code"] = (df["State Code"].astype(str).str.zfill(2)
                            + df["County Code"].astype(str).str.zfill(3))
    return df[["year_num", "county_code", "Parameter Code", "annual_value",
                  "Local Site Name", "Site Num", "Latitude", "Longitude"]]


def main() -> int:
    all_dfs = []
    for y in YEARS:
        try:
            df = fetch_year(y)
            log.info("[aqs %d] %d monitor-rows CA after filter", y, len(df))
            r2.upload(_dump_temp(df), f"raw/epa_aqs/{y}.parquet")
            all_dfs.append(df)
        except Exception as e:  # noqa: BLE001
            log.warning("[aqs %d] FAILED: %s", y, e)
            continue

    if not all_dfs:
        log.error("no years collected"); return 1

    panel = pd.concat(all_dfs, ignore_index=True)
    log.info("[aqs] total monitor-rows: %d", len(panel))

    # County-year-parameter mean across monitors
    grouped = (panel.groupby(["county_code", "year_num", "Parameter Code"])
                  ["annual_value"].mean().unstack("Parameter Code")
                  .reset_index())
    grouped = grouped.rename(columns={PARAM_PM25_FRM: "epa_pm25_annual_mean",
                                            PARAM_OZONE: "epa_ozone_annual_mean"})

    local = REPO / "data_cache" / "processed" / "canonical" / "epa_aqs.parquet"
    local.parent.mkdir(parents=True, exist_ok=True)
    grouped.to_parquet(local, index=False)
    r2.upload(local, "processed/canonical/epa_aqs.parquet")
    log.info("[aqs] canonical uploaded: %d rows (%d county-years)",
              len(grouped), len(grouped))

    # Coverage report
    for c in ("epa_pm25_annual_mean", "epa_ozone_annual_mean"):
        if c in grouped.columns:
            miss = grouped[c].isna().sum()
            log.info("  %-30s missingness: %d/%d (%.1f%%)  range: %.2f-%.2f",
                      c, miss, len(grouped), 100 * miss / len(grouped),
                      grouped[c].min(), grouped[c].max())
    # County-year coverage
    n_county_years = len(grouped)
    log.info("[aqs] county-years: %d (max possible: 58 counties × 14 yrs = 812)",
              n_county_years)
    return 0


def _dump_temp(df: pd.DataFrame) -> Path:
    import tempfile
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp.close()
    df.to_parquet(tmp.name, index=False)
    return Path(tmp.name)


if __name__ == "__main__":
    sys.exit(main())
