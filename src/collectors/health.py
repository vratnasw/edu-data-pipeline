"""Health collectors: CDC PLACES, CHKS, CDPH."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from collectors._base import (
    cache_path, make_raw_key, register_collector, upload_dataframe,
    _default_check_update,
)
from utils.http_client import download_to, get_with_backoff

log = logging.getLogger(__name__)


@register_collector("cdc_places", "health")
def cdc_places(year=None, force: bool = False) -> dict:
    """CDC PLACES census-tract estimates (Socrata API)."""
    out_key = make_raw_key("cdc_places", year or "latest")
    if not force and not cdc_places_check_update(year)["needs_update"]:
        return {"ok": True, "skipped": True, "key": out_key, "reason": "fresh"}
    url = "https://data.cdc.gov/resource/cwsq-ngmh.json"
    measures = ("DEPRESSION", "CASTHMA", "OBESITY", "LPA")
    rows = []
    for measure in measures:
        params = {"$where": f"stateabbr='CA' AND measureid='{measure}'",
                    "$limit": "200000"}
        try:
            r = get_with_backoff(url, params=params)
            rows.extend(r.json())
        except Exception as e:  # noqa: BLE001
            log.warning("cdc_places %s failed: %s", measure, e)
    if not rows:
        return {"ok": False, "skipped": True, "reason": "no rows"}
    df = pd.DataFrame(rows)
    rep = upload_dataframe(df, out_key)
    return {"ok": True, "key": out_key, **rep}


def cdc_places_check_update(year=None):
    return _default_check_update(year=year, source_name="cdc_places")


@register_collector("ca_chks", "health")
def ca_chks(year=None, force: bool = False) -> dict:
    """California Healthy Kids Survey aggregate district-level data.
    The actual download links are gated behind a publication index page;
    we fetch the landing page for traceability."""
    from collectors.economic import _stub_html_source
    return _stub_html_source("ca_chks", "https://chks.wested.org/resources/")


def ca_chks_check_update(year=None):
    return _default_check_update(year=year, source_name="ca_chks")


@register_collector("ca_cdph_county_indicators", "health")
def ca_cdph_county_indicators(year=None, force: bool = False) -> dict:
    from collectors.economic import _stub_html_source
    return _stub_html_source("ca_cdph_county_indicators",
        "https://www.cdph.ca.gov/Programs/CCDPHP/DCDIC/CDSRB/Pages/ReportandDataRequest.aspx")


def ca_cdph_county_indicators_check_update(year=None):
    return _default_check_update(year=year,
                                          source_name="ca_cdph_county_indicators")
