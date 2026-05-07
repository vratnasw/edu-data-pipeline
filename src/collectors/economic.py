"""Economic data collectors.

All sources are open / public-domain federal or CA-state data. Each
collector falls back to a clear error message if its required API key
or URL is unreachable, so the orchestrator can record `skip` rather
than crash.
"""
from __future__ import annotations

import io
import logging
import os
from pathlib import Path

import pandas as pd

from collectors._base import (
    cache_path, has_api_key, list_collectors, make_raw_key,
    register_collector, upload_dataframe, _default_check_update,
)
from utils.http_client import download_to, get_with_backoff, head_last_modified

log = logging.getLogger(__name__)

CA_FIPS = "06"
# 58 California county FIPS suffixes (3-digit)
CA_COUNTY_FIPS_SUFFIXES = [
    "001", "003", "005", "007", "009", "011", "013", "015", "017", "019",
    "021", "023", "025", "027", "029", "031", "033", "035", "037", "039",
    "041", "043", "045", "047", "049", "051", "053", "055", "057", "059",
    "061", "063", "065", "067", "069", "071", "073", "075", "077", "079",
    "081", "083", "085", "087", "089", "091", "093", "095", "097", "099",
    "101", "103", "105", "107", "109", "111", "113", "115",
]


# --------------------------------------------------------------------------- #
# Default check_update wrapper
# --------------------------------------------------------------------------- #

def _default_check_update(year=None, source_name=None):  # noqa: F811
    from collectors._base import _default_check_update as _d
    return _d(year=year, source_name=source_name)


# --------------------------------------------------------------------------- #
# BEA: county-level GDP + personal income
# --------------------------------------------------------------------------- #

@register_collector("bea_gdp_personal_income", "economic",
                       api_key_env="BEA_API_KEY")
def bea_gdp_personal_income(year=None, force: bool = False) -> dict:
    if not has_api_key("BEA_API_KEY"):
        return {"ok": False, "skipped": True, "reason": "BEA_API_KEY missing"}
    key_year = year or "latest"
    out_key = make_raw_key("bea_gdp_personal_income", key_year)
    if not force and not bea_gdp_personal_income_check_update(year)["needs_update"]:
        return {"ok": True, "skipped": True, "key": out_key, "reason": "fresh"}

    base = "https://apps.bea.gov/api/data"
    api_key = os.environ["BEA_API_KEY"]
    geofips = ",".join([f"{CA_FIPS}{s}" for s in CA_COUNTY_FIPS_SUFFIXES])
    rows = []
    for line_code, label in [(1, "gdp"), (3, "personal_income")]:
        params = {
            "UserID": api_key, "method": "GetData",
            "DatasetName": "Regional",
            "TableName": "CAGDP1" if line_code == 1 else "CAINC1",
            "LineCode": str(line_code),
            "GeoFips": geofips,
            "Year": str(year) if year else "ALL",
            "ResultFormat": "JSON",
        }
        r = get_with_backoff(base, params=params)
        d = r.json()
        data = (((d.get("BEAAPI") or {}).get("Results") or {}).get("Data") or [])
        for rec in data:
            rec["metric"] = label
            rows.append(rec)
    if not rows:
        return {"ok": False, "skipped": True, "reason": "no rows returned"}
    df = pd.DataFrame(rows)
    df.columns = [c.lower() for c in df.columns]
    rep = upload_dataframe(df, out_key)
    return {"ok": True, "key": out_key, **rep}


def bea_gdp_personal_income_check_update(year=None):
    return _default_check_update(year=year,
                                          source_name="bea_gdp_personal_income")


# --------------------------------------------------------------------------- #
# BLS: county unemployment (LAUS series)
# --------------------------------------------------------------------------- #

@register_collector("bls_unemployment", "economic", api_key_env="BLS_API_KEY")
def bls_unemployment(year=None, force: bool = False) -> dict:
    if not has_api_key("BLS_API_KEY"):
        return {"ok": False, "skipped": True, "reason": "BLS_API_KEY missing"}
    key_year = year or "latest"
    out_key = make_raw_key("bls_unemployment", key_year)
    if not force and not bls_unemployment_check_update(year)["needs_update"]:
        return {"ok": True, "skipped": True, "key": out_key, "reason": "fresh"}
    api_key = os.environ["BLS_API_KEY"]
    series = [f"LAUCN{CA_FIPS}{s}0000000003" for s in CA_COUNTY_FIPS_SUFFIXES]
    # BLS API limits 50 series per call
    rows = []
    for batch_start in range(0, len(series), 50):
        batch = series[batch_start:batch_start + 50]
        body = {
            "seriesid": batch,
            "startyear": str(year) if year else "2010",
            "endyear": str(year) if year else "2023",
            "registrationkey": api_key,
        }
        r = get_with_backoff("https://api.bls.gov/publicAPI/v2/timeseries/data",
                                  params=None, headers={"Content-Type": "application/json"})
        # BLS uses POST for batched series; fall through to direct POST:
        import requests
        resp = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/",
                                json=body, timeout=120)
        if resp.status_code != 200:
            log.warning("bls batch %d failed http=%d", batch_start, resp.status_code)
            continue
        for s in (resp.json().get("Results") or {}).get("series", []):
            for d in s.get("data", []):
                d["seriesID"] = s["seriesID"]; rows.append(d)
    if not rows:
        return {"ok": False, "skipped": True, "reason": "empty BLS response"}
    df = pd.DataFrame(rows)
    rep = upload_dataframe(df, out_key)
    return {"ok": True, "key": out_key, **rep}


def bls_unemployment_check_update(year=None):
    return _default_check_update(year=year, source_name="bls_unemployment")


# --------------------------------------------------------------------------- #
# Census ACS5
# --------------------------------------------------------------------------- #

@register_collector("census_acs5", "economic", api_key_env="CENSUS_API_KEY")
def census_acs5(year=None, force: bool = False) -> dict:
    if not has_api_key("CENSUS_API_KEY"):
        return {"ok": False, "skipped": True, "reason": "CENSUS_API_KEY missing"}
    yr = int(year or 2022)
    out_key = make_raw_key("census_acs5", yr)
    if not force and not census_acs5_check_update(yr)["needs_update"]:
        return {"ok": True, "skipped": True, "key": out_key, "reason": "fresh"}
    api_key = os.environ["CENSUS_API_KEY"]
    vars_needed = ",".join([
        "B19013_001E", "B17001_002E", "B15003_022E",
        "B25070_010E", "B25003_002E", "NAME",
    ])
    url = f"https://api.census.gov/data/{yr}/acs/acs5"
    params = {"get": vars_needed, "for": "county:*", "in": f"state:{CA_FIPS}",
                "key": api_key}
    r = get_with_backoff(url, params=params)
    rows = r.json()
    if not rows or len(rows) < 2:
        return {"ok": False, "skipped": True, "reason": "empty census response"}
    df = pd.DataFrame(rows[1:], columns=rows[0])
    df["year"] = yr
    rep = upload_dataframe(df, out_key)
    return {"ok": True, "key": out_key, **rep}


def census_acs5_check_update(year=None):
    return _default_check_update(year=year, source_name="census_acs5")


# --------------------------------------------------------------------------- #
# Census SAIPE school district poverty (direct download)
# --------------------------------------------------------------------------- #

@register_collector("census_saipe_districts", "economic")
def census_saipe_districts(year=None, force: bool = False) -> dict:
    yr = int(year or 2022)
    out_key = make_raw_key("census_saipe_districts", yr)
    if not force and not census_saipe_districts_check_update(yr)["needs_update"]:
        return {"ok": True, "skipped": True, "key": out_key, "reason": "fresh"}
    # SAIPE provides per-year text files; layout has shifted over years
    # so we use the school-district file naming convention:
    url = (f"https://www2.census.gov/programs-surveys/saipe/datasets/{yr}"
              f"/{yr}-school-districts/sd{str(yr)[-2:]}.txt")
    p = cache_path("census_saipe_districts", yr, "txt")
    try:
        download_to(url, p)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "skipped": True, "reason": f"saipe download failed: {e}"}
    # Layout: columns are fixed-width; load all then filter to CA
    try:
        df = pd.read_csv(p, sep="\t", encoding="latin-1", low_memory=False)
    except Exception:
        df = pd.read_csv(p, sep=None, engine="python", encoding="latin-1")
    # CA = state FIPS 06 — column name varies by year; scan
    state_col = next((c for c in df.columns
                          if "state" in c.lower() and "fips" in c.lower()), None)
    if state_col:
        df = df[df[state_col].astype(str).str.zfill(2) == CA_FIPS].copy()
    df["year"] = yr
    rep = upload_dataframe(df, out_key)
    return {"ok": True, "key": out_key, **rep}


def census_saipe_districts_check_update(year=None):
    return _default_check_update(year=year, source_name="census_saipe_districts")


# --------------------------------------------------------------------------- #
# FHFA HPI
# --------------------------------------------------------------------------- #

_FHFA_FILES = [
    ("HPI_AT_metro.csv", "https://www.fhfa.gov/hpi/download/quarterly_datasets/HPI_AT_metro.csv"),
    ("HPI_AT_nonmetro.csv", "https://www.fhfa.gov/hpi/download/quarterly_datasets/HPI_AT_nonmetro.csv"),
]


@register_collector("fhfa_hpi", "economic")
def fhfa_hpi(year=None, force: bool = False) -> dict:
    out_key = make_raw_key("fhfa_hpi", "latest")
    if not force and not fhfa_hpi_check_update()["needs_update"]:
        return {"ok": True, "skipped": True, "key": out_key, "reason": "fresh"}
    rows = []
    for name, url in _FHFA_FILES:
        try:
            p = cache_path("fhfa_hpi", name.replace(".csv", ""), "csv")
            download_to(url, p)
            d = pd.read_csv(p, low_memory=False)
            d["__source"] = name; rows.append(d)
        except Exception as e:  # noqa: BLE001
            log.warning("fhfa %s failed: %s", name, e)
    if not rows:
        return {"ok": False, "skipped": True, "reason": "all FHFA files failed"}
    df = pd.concat(rows, ignore_index=True)
    rep = upload_dataframe(df, out_key)
    return {"ok": True, "key": out_key, **rep}


def fhfa_hpi_check_update(year=None):
    return _default_check_update(year=year, source_name="fhfa_hpi")


# --------------------------------------------------------------------------- #
# Zillow ZORI
# --------------------------------------------------------------------------- #

@register_collector("zillow_zori", "economic")
def zillow_zori(year=None, force: bool = False) -> dict:
    out_key = make_raw_key("zillow_zori", "latest")
    if not force and not zillow_zori_check_update()["needs_update"]:
        return {"ok": True, "skipped": True, "key": out_key, "reason": "fresh"}
    url = ("https://files.zillowstatic.com/research/public_csvs/zori/"
              "Metro_zori_uc_sfrcondo_sm_month.csv")
    p = cache_path("zillow_zori", "metro", "csv")
    try:
        download_to(url, p)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "skipped": True, "reason": f"zori download: {e}"}
    df = pd.read_csv(p)
    rep = upload_dataframe(df, out_key)
    return {"ok": True, "key": out_key, **rep}


def zillow_zori_check_update(year=None):
    return _default_check_update(year=year, source_name="zillow_zori")


# --------------------------------------------------------------------------- #
# Stub-style collectors (HTML scrape sources): IRS, CA-SCO, CA-BOE, CA-EDD
# --------------------------------------------------------------------------- #

def _stub_html_source(name: str, url: str) -> dict:
    """Sources that require an HTML scrape pass to find the latest CSV link.
    We download the landing-page HTML so a future maintainer can extend the
    extractor; we don't try to reverse-engineer brittle links here."""
    out_key = make_raw_key(name, "landing", "html")
    try:
        r = get_with_backoff(url)
        p = cache_path(name, "landing", "html")
        p.write_bytes(r.content)
        # Wrap as a single-row dataframe with the HTML content for traceability
        df = pd.DataFrame([{"url": url, "content_bytes": len(r.content),
                              "fetched_at": pd.Timestamp.utcnow().isoformat()}])
        rep = upload_dataframe(df, make_raw_key(name, "latest_index"))
        return {"ok": True, "key": rep["key"],
                  "note": "landing page only; extractor TODO", **rep}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "skipped": True, "reason": f"{type(e).__name__}: {e}"}


@register_collector("irs_soi", "economic")
def irs_soi(year=None, force: bool = False) -> dict:
    return _stub_html_source("irs_soi",
                                  "https://www.irs.gov/statistics/soi-tax-stats-county-data")


def irs_soi_check_update(year=None):
    return _default_check_update(year=year, source_name="irs_soi")


@register_collector("ca_sco_local_finance", "economic")
def ca_sco_local_finance(year=None, force: bool = False) -> dict:
    return _stub_html_source("ca_sco_local_finance",
                                  "https://bythenumbers.sco.ca.gov/")


def ca_sco_local_finance_check_update(year=None):
    return _default_check_update(year=year, source_name="ca_sco_local_finance")


@register_collector("ca_boe_property_tax", "economic")
def ca_boe_property_tax(year=None, force: bool = False) -> dict:
    return _stub_html_source("ca_boe_property_tax",
                                  "https://www.boe.ca.gov/dataportal/")


def ca_boe_property_tax_check_update(year=None):
    return _default_check_update(year=year, source_name="ca_boe_property_tax")


@register_collector("ca_edd_lmi", "economic")
def ca_edd_lmi(year=None, force: bool = False) -> dict:
    return _stub_html_source("ca_edd_lmi",
                                  "https://labormarketinfo.edd.ca.gov/")


def ca_edd_lmi_check_update(year=None):
    return _default_check_update(year=year, source_name="ca_edd_lmi")
