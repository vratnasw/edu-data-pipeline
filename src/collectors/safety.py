"""Safety collectors: CA DOJ OpenJustice, OJJDP juvenile justice."""
from __future__ import annotations

import logging

import pandas as pd

from collectors._base import (
    make_raw_key, register_collector, upload_dataframe, _default_check_update,
)
from utils.http_client import get_with_backoff

log = logging.getLogger(__name__)


@register_collector("ca_doj_openjustice", "safety")
def ca_doj_openjustice(year=None, force: bool = False) -> dict:
    yr = int(year or 2022)
    out_key = make_raw_key("ca_doj_openjustice", yr)
    if not force and not ca_doj_openjustice_check_update(yr)["needs_update"]:
        return {"ok": True, "skipped": True, "key": out_key, "reason": "fresh"}
    url = "https://openjustice.doj.ca.gov/api/v1/crimes"
    params = {"year": yr}
    try:
        r = get_with_backoff(url, params=params)
        d = r.json()
        rows = d if isinstance(d, list) else (d.get("data") or d.get("results") or [])
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "skipped": True, "reason": f"oj api: {e}"}
    if not rows:
        return {"ok": False, "skipped": True, "reason": "empty oj response"}
    df = pd.DataFrame(rows); df["year"] = yr
    rep = upload_dataframe(df, out_key)
    return {"ok": True, "key": out_key, **rep}


def ca_doj_openjustice_check_update(year=None):
    return _default_check_update(year=year, source_name="ca_doj_openjustice")


@register_collector("ojjdp_county", "safety")
def ojjdp_county(year=None, force: bool = False) -> dict:
    from collectors.economic import _stub_html_source
    return _stub_html_source("ojjdp_county",
                                  "https://www.ojjdp.gov/ojstatbb/ezacjrp/")


def ojjdp_county_check_update(year=None):
    return _default_check_update(year=year, source_name="ojjdp_county")
