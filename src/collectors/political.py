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
    if not has_api_key("FTM_API_KEY"):
        return {"ok": False, "skipped": True, "reason": "FTM_API_KEY missing"}
    out_key = make_raw_key("ftm_education_finance", year or "latest")
    url = "https://api.followthemoney.org"
    params = {"key": os.environ["FTM_API_KEY"], "s": "CA",
                "y": year if year else None}
    try:
        r = get_with_backoff(url, params=params)
        rows = r.json()
        if isinstance(rows, dict): rows = rows.get("records", [])
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "skipped": True, "reason": f"ftm: {e}"}
    if not rows:
        return {"ok": False, "skipped": True, "reason": "no ftm rows"}
    rep = upload_dataframe(pd.DataFrame(rows), out_key)
    return {"ok": True, "key": out_key, **rep}


def ftm_education_finance_check_update(year=None):
    return _default_check_update(year=year, source_name="ftm_education_finance")
