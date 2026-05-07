"""Environment collectors: CalEnviroScreen, EPA AQS, EPA TRI, NOAA GHCN, FEMA NFHL."""
from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd

from collectors._base import (
    cache_path, has_api_key, make_raw_key, register_collector,
    upload_dataframe, _default_check_update,
)
from utils.http_client import download_to, get_with_backoff

log = logging.getLogger(__name__)


@register_collector("calenviroscreen", "environment")
def calenviroscreen(year=None, force: bool = False) -> dict:
    """CalEnviroScreen 4.0 — direct Excel download."""
    out_key = make_raw_key("calenviroscreen", "v4")
    if not force and not calenviroscreen_check_update()["needs_update"]:
        return {"ok": True, "skipped": True, "key": out_key, "reason": "fresh"}
    # OEHHA hosts the report on this landing page; the file URL is stable:
    url = ("https://oehha.ca.gov/sites/default/files/media/downloads/"
              "calenviroscreen/document/calenviroscreen40resultsdatadictionary.xlsx")
    p = cache_path("calenviroscreen", "v4", "xlsx")
    try:
        download_to(url, p)
        df = pd.read_excel(p, sheet_name=0)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "skipped": True, "reason": f"calenv download: {e}"}
    rep = upload_dataframe(df, out_key)
    return {"ok": True, "key": out_key, **rep}


def calenviroscreen_check_update(year=None):
    return _default_check_update(year=year, source_name="calenviroscreen")


@register_collector("epa_aqs", "environment")
def epa_aqs(year=None, force: bool = False) -> dict:
    """EPA AQS annual conc by monitor — pulls PM25 + ozone for the requested year."""
    yr = int(year or 2022)
    out_key = make_raw_key("epa_aqs", yr)
    if not force and not epa_aqs_check_update(yr)["needs_update"]:
        return {"ok": True, "skipped": True, "key": out_key, "reason": "fresh"}
    base = "https://aqs.epa.gov/aqsweb/airdata"
    files = [f"annual_conc_by_monitor_{yr}.zip"]
    rows = []
    for fname in files:
        url = f"{base}/{fname}"
        zp = cache_path("epa_aqs", f"{yr}_zip", "zip")
        try:
            download_to(url, zp)
            import zipfile
            with zipfile.ZipFile(zp) as zf:
                csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
                for n in csv_names:
                    with zf.open(n) as f:
                        d = pd.read_csv(f, low_memory=False)
                    d = d[d.get("State Code", "").astype(str).str.zfill(2) == "06"] \
                        if "State Code" in d.columns else d
                    rows.append(d)
        except Exception as e:  # noqa: BLE001
            log.warning("epa_aqs %s failed: %s", fname, e)
    if not rows:
        return {"ok": False, "skipped": True, "reason": "no AQS data"}
    df = pd.concat(rows, ignore_index=True)
    rep = upload_dataframe(df, out_key)
    return {"ok": True, "key": out_key, **rep}


def epa_aqs_check_update(year=None):
    return _default_check_update(year=year, source_name="epa_aqs")


@register_collector("epa_tri", "environment")
def epa_tri(year=None, force: bool = False) -> dict:
    """EPA TRI Basic Data Files — California facilities."""
    yr = int(year or 2022)
    out_key = make_raw_key("epa_tri", yr)
    if not force and not epa_tri_check_update(yr)["needs_update"]:
        return {"ok": True, "skipped": True, "key": out_key, "reason": "fresh"}
    url = (f"https://data.epa.gov/efservice/tri_facility_form_R/year/=/{yr}/"
              "state_abbr/=/CA/csv")
    p = cache_path("epa_tri", yr, "csv")
    try:
        download_to(url, p)
        df = pd.read_csv(p, low_memory=False)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "skipped": True, "reason": f"tri: {e}"}
    rep = upload_dataframe(df, out_key)
    return {"ok": True, "key": out_key, **rep}


def epa_tri_check_update(year=None):
    return _default_check_update(year=year, source_name="epa_tri")


@register_collector("noaa_ghcn", "environment", api_key_env="NOAA_TOKEN")
def noaa_ghcn(year=None, force: bool = False) -> dict:
    """NOAA GHCN-Daily (token-gated). Aggregates to county level here is
    deferred to the processor — collector just stores raw station data."""
    if not has_api_key("NOAA_TOKEN"):
        return {"ok": False, "skipped": True, "reason": "NOAA_TOKEN missing"}
    yr = int(year or 2022)
    out_key = make_raw_key("noaa_ghcn", yr)
    if not force and not noaa_ghcn_check_update(yr)["needs_update"]:
        return {"ok": True, "skipped": True, "key": out_key, "reason": "fresh"}
    headers = {"token": os.environ["NOAA_TOKEN"]}
    base = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
    rows = []
    for datatype in ("TMAX", "TMIN", "PRCP", "SNOW"):
        params = {"datasetid": "GHCND", "locationid": "FIPS:06",
                    "startdate": f"{yr}-01-01", "enddate": f"{yr}-12-31",
                    "datatypeid": datatype, "limit": "1000", "units": "metric"}
        try:
            r = get_with_backoff(base, params=params, headers=headers)
            rows.extend(r.json().get("results") or [])
        except Exception as e:  # noqa: BLE001
            log.warning("noaa %s failed: %s", datatype, e)
    if not rows:
        return {"ok": False, "skipped": True, "reason": "no NOAA rows"}
    df = pd.DataFrame(rows)
    rep = upload_dataframe(df, out_key)
    return {"ok": True, "key": out_key, **rep}


def noaa_ghcn_check_update(year=None):
    return _default_check_update(year=year, source_name="noaa_ghcn")


@register_collector("fema_nfhl", "environment")
def fema_nfhl(year=None, force: bool = False) -> dict:
    """FEMA flood hazard layer — landing page only (file is multi-GB shapefile)."""
    from collectors.economic import _stub_html_source
    return _stub_html_source("fema_nfhl", "https://msc.fema.gov/portal/")


def fema_nfhl_check_update(year=None):
    return _default_check_update(year=year, source_name="fema_nfhl")
