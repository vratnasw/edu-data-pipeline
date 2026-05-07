"""Infrastructure collectors: FCC broadband, NCES facilities, USAC E-Rate."""
from __future__ import annotations

from collectors._base import register_collector, _default_check_update


@register_collector("fcc_broadband", "infrastructure")
def fcc_broadband(year=None, force: bool = False) -> dict:
    from collectors.economic import _stub_html_source
    return _stub_html_source("fcc_broadband",
        "https://www.fcc.gov/general/broadband-deployment-data")


def fcc_broadband_check_update(year=None):
    return _default_check_update(year=year, source_name="fcc_broadband")


@register_collector("nces_facilities", "infrastructure")
def nces_facilities(year=None, force: bool = False) -> dict:
    from collectors.economic import _stub_html_source
    return _stub_html_source("nces_facilities",
        "https://nces.ed.gov/surveys/frss/publications/20190")


def nces_facilities_check_update(year=None):
    return _default_check_update(year=year, source_name="nces_facilities")


@register_collector("usac_erate", "infrastructure")
def usac_erate(year=None, force: bool = False) -> dict:
    from collectors.economic import _stub_html_source
    return _stub_html_source("usac_erate", "https://opendata.usac.org/E-Rate/")


def usac_erate_check_update(year=None):
    return _default_check_update(year=year, source_name="usac_erate")
