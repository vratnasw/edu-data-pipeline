"""Infrastructure collectors: FCC broadband, NCES facilities, USAC E-Rate."""
from __future__ import annotations

from collectors._base import register_collector, _default_check_update


@register_collector("fcc_broadband", "infrastructure")
def fcc_broadband(year=None, force: bool = False) -> dict:
    """FCC broadband — original /general URL times out intermittently.
    Use parent FCC home as a stable stub target."""
    from collectors.economic import _stub_html_source
    return _stub_html_source("fcc_broadband", "https://www.fcc.gov/")


def fcc_broadband_check_update(year=None):
    return _default_check_update(year=year, source_name="fcc_broadband")


@register_collector("nces_facilities", "infrastructure")
def nces_facilities(year=None, force: bool = False) -> dict:
    """NCES FRSS — /publications/20190 is 404. Use FRSS landing page."""
    from collectors.economic import _stub_html_source
    return _stub_html_source("nces_facilities",
        "https://nces.ed.gov/surveys/frss/")


def nces_facilities_check_update(year=None):
    return _default_check_update(year=year, source_name="nces_facilities")


@register_collector("usac_erate", "infrastructure")
def usac_erate(year=None, force: bool = False) -> dict:
    """USAC E-Rate — /E-Rate is 404 (case sensitive). Use root portal."""
    from collectors.economic import _stub_html_source
    return _stub_html_source("usac_erate", "https://opendata.usac.org/")


def usac_erate_check_update(year=None):
    return _default_check_update(year=year, source_name="usac_erate")
