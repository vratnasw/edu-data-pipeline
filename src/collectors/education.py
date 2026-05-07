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
    """SEDA Stanford Education Data Archive — public district scores.
    URL drift fix 2026-05: edopportunity.org/getdata/ moved to
    /get-the-data/. Production datasets are gated behind a Box link
    on that landing page; stub captures the landing for a future
    release-cycle extractor."""
    from collectors.economic import _stub_html_source
    return _stub_html_source("seda", "https://edopportunity.org/get-the-data/")


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
    """EdFacts state-assessment data.
    URL drift fix 2026-05: ed.gov/about/ed-overview/EDFacts → /data."""
    from collectors.economic import _stub_html_source
    return _stub_html_source("edfacts_assessments", "https://www.ed.gov/data")


def edfacts_assessments_check_update(year=None):
    return _default_check_update(year=year, source_name="edfacts_assessments")


@register_collector("opp_insights_college_mobility", "education")
def opp_insights_college_mobility(year=None, force: bool = False) -> dict:
    """Opportunity Insights mrc_table2.csv — high-school college mobility.
    URL drift fix 2026-05: the 2018/03/mrc_table2.csv direct link returns
    404; the file is now distributed via opportunityatlas.org/data and
    requires a button-click to download. Falling back to landing page
    stub until a stable replacement URL is identified."""
    from collectors.economic import _stub_html_source
    return _stub_html_source("opp_insights_college_mobility",
                                  "https://opportunityinsights.org/data/")


def opp_insights_college_mobility_check_update(year=None):
    return _default_check_update(year=year,
                                          source_name="opp_insights_college_mobility")
