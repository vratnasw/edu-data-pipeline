"""Political collectors: CA SOS elections, LAO budget, FollowTheMoney."""
from __future__ import annotations

import logging
import os

import pandas as pd

from collectors._base import (
    has_api_key, make_raw_key, register_collector, upload_dataframe,
    _default_check_update,
)
from utils.http_client import get_with_backoff

log = logging.getLogger(__name__)


@register_collector("ca_sos_elections", "political")
def ca_sos_elections(year=None, force: bool = False) -> dict:
    from collectors.economic import _stub_html_source
    return _stub_html_source("ca_sos_elections",
        "https://www.sos.ca.gov/elections/prior-elections/statewide-election-results")


def ca_sos_elections_check_update(year=None):
    return _default_check_update(year=year, source_name="ca_sos_elections")


@register_collector("ca_lao_budget", "political")
def ca_lao_budget(year=None, force: bool = False) -> dict:
    from collectors.economic import _stub_html_source
    return _stub_html_source("ca_lao_budget", "https://lao.ca.gov/Publications")


def ca_lao_budget_check_update(year=None):
    return _default_check_update(year=year, source_name="ca_lao_budget")


@register_collector("ftm_education_finance", "political", api_key_env="FTM_API_KEY")
def ftm_education_finance(year=None, force: bool = False) -> dict:
    """OpenSecrets / FollowTheMoney API was PERMANENTLY DISCONTINUED 2025-04-15.
    The API endpoint is dead; no replacement is offered.
    Track 1 marks this source as `deferred: True`. Future replacement
    candidates: ca_sos_elections (already wired) + FEC bulk for federal,
    plus CA Fair Political Practices Commission filings."""
    return {"ok": False, "skipped": True,
              "reason": "API permanently discontinued 2025-04-15 (OpenSecrets/FTM)",
              "deferred": True,
              "replacement": "ca_sos_elections + FEC bulk (no direct CA education-finance feed available)"}


def ftm_education_finance_check_update(year=None):
    return _default_check_update(year=year, source_name="ftm_education_finance")
