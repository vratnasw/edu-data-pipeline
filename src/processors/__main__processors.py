"""Dispatcher: source_name → processor.process(source_name, year)."""
from __future__ import annotations

from processors import economic_proc, environment_proc, generic_proc, housing_proc

ECONOMIC = {
    "bea_gdp_personal_income", "bls_unemployment", "census_acs5",
    "census_saipe_districts", "fhfa_hpi", "zillow_zori", "irs_soi",
    "ca_sco_local_finance", "ca_boe_property_tax", "ca_edd_lmi",
}
ENVIRONMENT = {"calenviroscreen", "epa_aqs", "epa_tri", "noaa_ghcn", "fema_nfhl"}
HOUSING = {"hud_affh", "hud_lihtc", "opp_insights_neighborhood"}


def dispatch(source: str, year=None) -> dict:
    if source in ECONOMIC:
        return economic_proc.process(source, year)
    if source in ENVIRONMENT:
        return environment_proc.process(source, year)
    if source in HOUSING:
        return housing_proc.process(source, year)
    return generic_proc.process(source, year)
