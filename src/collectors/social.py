"""Social collectors: CA DHCS Medi-Cal, CA CDSS CalFresh/CalWORKs."""
from __future__ import annotations

from collectors._base import register_collector, _default_check_update


@register_collector("ca_dhcs_medical", "social")
def ca_dhcs_medical(year=None, force: bool = False) -> dict:
    from collectors.economic import _stub_html_source
    return _stub_html_source("ca_dhcs_medical",
                                  "https://www.dhcs.ca.gov/dataandstats/statistics/Pages/default.aspx")


def ca_dhcs_medical_check_update(year=None):
    return _default_check_update(year=year, source_name="ca_dhcs_medical")


@register_collector("ca_cdss_caseloads", "social")
def ca_cdss_caseloads(year=None, force: bool = False) -> dict:
    from collectors.economic import _stub_html_source
    return _stub_html_source("ca_cdss_caseloads",
                                  "https://www.cdss.ca.gov/inforesources/Data-Portal")


def ca_cdss_caseloads_check_update(year=None):
    return _default_check_update(year=year, source_name="ca_cdss_caseloads")
