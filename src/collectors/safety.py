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
    """CA DOJ OpenJustice — the v1 /api/v1/crimes endpoint returns HTML
    (deprecated). Their data is now downloaded as CSV per dataset from the
    portal. Stub the landing page; downstream extractor TODO."""
    from collectors.economic import _stub_html_source
    return _stub_html_source("ca_doj_openjustice",
                                  "https://openjustice.doj.ca.gov/")


def ca_doj_openjustice_check_update(year=None):
    return _default_check_update(year=year, source_name="ca_doj_openjustice")


@register_collector("ojjdp_county", "safety")
def ojjdp_county(year=None, force: bool = False) -> dict:
    from collectors.economic import _stub_html_source
    return _stub_html_source("ojjdp_county",
                                  "https://www.ojjdp.gov/ojstatbb/ezacjrp/")


def ojjdp_county_check_update(year=None):
    return _default_check_update(year=year, source_name="ojjdp_county")
