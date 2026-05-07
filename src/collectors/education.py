"""Education collectors: SEDA, NCES CCD, EdFacts, OI college mobility."""
from __future__ import annotations

import logging

import pandas as pd

from collectors._base import (
    cache_path, make_raw_key, register_collector, upload_dataframe,
    _default_check_update,
)
from utils.http_client import download_to

log = logging.getLogger(__name__)


@register_collector("seda", "education")
def seda(year=None, force: bool = False) -> dict:
    """SEDA Stanford Education Data Archive — public district scores."""
    from collectors.economic import _stub_html_source
    return _stub_html_source("seda", "https://edopportunity.org/getdata/")


def seda_check_update(year=None):
    return _default_check_update(year=year, source_name="seda")


@register_collector("nces_ccd", "education")
def nces_ccd(year=None, force: bool = False) -> dict:
    """NCES Common Core of Data — district + school-level data files."""
    from collectors.economic import _stub_html_source
    return _stub_html_source("nces_ccd", "https://nces.ed.gov/ccd/files.asp")


def nces_ccd_check_update(year=None):
    return _default_check_update(year=year, source_name="nces_ccd")


@register_collector("edfacts_assessments", "education")
def edfacts_assessments(year=None, force: bool = False) -> dict:
    from collectors.economic import _stub_html_source
    return _stub_html_source("edfacts_assessments",
                                  "https://www.ed.gov/about/ed-overview/EDFacts")


def edfacts_assessments_check_update(year=None):
    return _default_check_update(year=year, source_name="edfacts_assessments")


@register_collector("opp_insights_college_mobility", "education")
def opp_insights_college_mobility(year=None, force: bool = False) -> dict:
    """Opportunity Insights mrc_table2.csv — high-school college mobility."""
    out_key = make_raw_key("opp_insights_college_mobility", "latest")
    if not force and not opp_insights_college_mobility_check_update()["needs_update"]:
        return {"ok": True, "skipped": True, "key": out_key, "reason": "fresh"}
    url = "https://opportunityinsights.org/wp-content/uploads/2018/03/mrc_table2.csv"
    p = cache_path("opp_insights_college_mobility", "mrc_table2", "csv")
    try:
        download_to(url, p)
        df = pd.read_csv(p, low_memory=False)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "skipped": True, "reason": f"oi-mrc: {e}"}
    rep = upload_dataframe(df, out_key)
    return {"ok": True, "key": out_key, **rep}


def opp_insights_college_mobility_check_update(year=None):
    return _default_check_update(year=year,
                                          source_name="opp_insights_college_mobility")
