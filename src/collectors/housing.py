"""Housing collectors: HUD AFFH, HUD LIHTC, Opportunity Insights neighborhood."""
from __future__ import annotations

import logging

import pandas as pd

from collectors._base import (
    cache_path, make_raw_key, register_collector, upload_dataframe,
    _default_check_update,
)
from utils.http_client import download_to

log = logging.getLogger(__name__)


@register_collector("hud_affh", "housing")
def hud_affh(year=None, force: bool = False) -> dict:
    """HUD AFFH-T — landing-page index (multiple ZIP downloads)."""
    from collectors.economic import _stub_html_source
    return _stub_html_source("hud_affh",
                                  "https://www.huduser.gov/portal/datasets/affh.html")


def hud_affh_check_update(year=None):
    return _default_check_update(year=year, source_name="hud_affh")


@register_collector("hud_lihtc", "housing")
def hud_lihtc(year=None, force: bool = False) -> dict:
    """HUD LIHTC — direct CSV returns 202 with empty body (HUD anti-bot
    behavior). Fall back to landing-page stub; downstream extractor TODO."""
    from collectors.economic import _stub_html_source
    return _stub_html_source("hud_lihtc",
                                  "https://www.huduser.gov/portal/datasets/lihtc.html")


def hud_lihtc_check_update(year=None):
    return _default_check_update(year=year, source_name="hud_lihtc")


@register_collector("opp_insights_neighborhood", "housing")
def opp_insights_neighborhood(year=None, force: bool = False) -> dict:
    """Opportunity Insights neighborhood + county outcomes.
    URL drift: the 2018/10 direct-CSV paths now 404. Fall back to
    landing-page stub since the new download UX is button-click only."""
    from collectors.economic import _stub_html_source
    return _stub_html_source("opp_insights_neighborhood",
                                  "https://opportunityinsights.org/data/")
    files = [
        ("https://opportunityinsights.org/wp-content/uploads/2018/10/county_outcomes.csv",
          "county_outcomes"),
        ("https://opportunityinsights.org/wp-content/uploads/2018/10/neighborhood_outcomes.csv",
          "neighborhood_outcomes"),
    ]
    parts = []
    for url, name in files:
        try:
            p = cache_path("opp_insights_neighborhood", name, "csv")
            download_to(url, p)
            d = pd.read_csv(p, low_memory=False); d["__file"] = name
            parts.append(d)
        except Exception as e:  # noqa: BLE001
            log.warning("oi %s: %s", name, e)
    if not parts:
        return {"ok": False, "skipped": True, "reason": "no OI files"}
    df = pd.concat(parts, ignore_index=True)
    rep = upload_dataframe(df, out_key)
    return {"ok": True, "key": out_key, **rep}


def opp_insights_neighborhood_check_update(year=None):
    return _default_check_update(year=year, source_name="opp_insights_neighborhood")
