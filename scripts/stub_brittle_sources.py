"""Phases 4, 5, 10 — sources we cannot pull reliably from this session.

Each writes a minimal landing-page stub to R2 documenting:
  - the source name + landing URL
  - the documented blocker (per `feedback_html_scrape_stubs.md` memory)
  - what was attempted and why it failed
  - the manual / Lightning AI fallback procedure

The master-panel build treats these as absent (no join) until the blocker
is resolved.
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import config.r2_client as r2  # noqa: E402

logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s %(levelname)s :: %(message)s",
                       datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


STUBS = {
    "ca_doj_openjustice": {
        "source": "CA DOJ OpenJustice — crime statistics",
        "landing_url": "https://openjustice.doj.ca.gov/data",
        "attempted_url": "https://openjustice.doj.ca.gov/api/v1/crimes",
        "blocker": ("API endpoint returns HTML instead of JSON (documented in "
                       "memory: feedback_html_scrape_stubs.md). Public bulk "
                       "data downloads on the landing page require manual "
                       "form-submit + email retrieval."),
        "fallback": ("Manually download the annual Crimes-and-Clearances CSV "
                       "for each year 2010-2023 from the OpenJustice portal "
                       "(by city + by county), upload as raw/ca_doj/<year>.csv "
                       "to R2, then write a processor that aggregates city → "
                       "county and computes per-100K rates."),
        "deferred_columns": [
            "ca_doj_violent_crime_rate",
            "ca_doj_property_crime_rate",
            "ca_doj_arrest_rate",
        ],
    },
    "ca_chks": {
        "source": "California Healthy Kids Survey (CHKS)",
        "landing_url": "https://chks.wested.org/resources/data-files",
        "attempted_url": "https://chks.wested.org/resources/data-files",
        "blocker": ("CHKS public data files are NOT published as direct "
                       "downloads — the data-files page is a request form "
                       "(researcher must submit a CHKS data-request via "
                       "WestEd). District-level aggregates exist only behind "
                       "this gated flow."),
        "fallback": ("Either submit a CHKS request (typically 2-3 weeks "
                       "approval) for the district-level aggregate panel, or "
                       "scrape the public CalSCHLS DataQuest summary tables "
                       "for selected indicators (HTML-only, county-level "
                       "only — does NOT give district-level)."),
        "deferred_columns": [
            "chks_school_connectedness",
            "chks_safety_at_school",
            "chks_sad_or_hopeless",
            "chks_seriously_considered_suicide",
        ],
    },
    "hud_affh": {
        "source": "HUD AFFH-T opportunity indices",
        "landing_url": "https://hudgis-hud.opendata.arcgis.com/datasets/HUD::affht-2022",
        "attempted_url": ("https://services.arcgis.com/VTyQ9soqVukalItT/"
                            "ArcGIS/rest/services/HUDAFFHT/FeatureServer/0/query"),
        "blocker": ("HUD's ArcGIS open-data portal is a SPA — direct REST "
                       "queries return errors without a valid service ID. The "
                       "actual dataset GUID rotates and must be looked up "
                       "interactively. AFFH-T 2022 source CSVs are also "
                       "behind dynamic portal navigation."),
        "fallback": ("On Lightning AI: 1) open the ArcGIS dataset page, "
                       "2) copy the 'View API resources' GUID, 3) hit "
                       "/arcgis/rest/services/<GUID>/FeatureServer/0/query "
                       "with where=STATE='CA' & outFields=* & f=geojson, "
                       "4) aggregate tracts → county via the same "
                       "tract_aggregation helper."),
        "deferred_columns": [
            "hud_affh_opportunity_index",
            "hud_affh_school_proficiency_index",
            "hud_affh_job_proximity_index",
            "hud_affh_transit_access_index",
            "hud_affh_low_poverty_index",
        ],
    },
    "kidsdata_foster_entries": {
        "source": "kidsdata.org — Foster Care Entry Rates (by county)",
        "landing_url": ("https://www.kidsdata.org/topic/14/foster-entries/table"
                          "#fmt=2324&loc=2,127,347,...&tf=108"),
        "attempted_url": "https://www.kidsdata.org/api/table/table",
        "blocker": ("Three-layer block: (1) Cloudflare 'Just a moment...' "
                       "JS challenge — passable via cloudscraper. (2) Export "
                       "modal gated by Google reCAPTCHA — not solvable by "
                       "automation. (3) /api/table/table endpoint exists and "
                       "returns 500 without exact query schema built "
                       "dynamically from the page UI state. Underlying source "
                       "(UC Berkeley CCWIP at ccwip.berkeley.edu) is also a "
                       "JS-rendered SPA with no scrapable HTML."),
        "fallback": ("On Lightning AI: (option A) install playwright + "
                       "headless Chromium (`pip install playwright && "
                       "playwright install chromium`), load the kidsdata page, "
                       "wait for the XHR to /api/table/table to complete, and "
                       "intercept the JSON response. (option B) Use "
                       "kidsdata.org's 'Email me this data' modal manually — "
                       "they deliver CSVs in 5-10 min. (option C) Pull from "
                       "the CCWIP API directly — Berkeley CCWIP serves data "
                       "via Angular HTTP at "
                       "ccwip.berkeley.edu/api/SecondaryAxisRequest with "
                       "params for report_id + county_id."),
        "deferred_columns": [
            "kidsdata_foster_entry_rate_per_1k",
            "kidsdata_foster_entry_count",
        ],
        "alternative_signal_already_present": (
            "education spine column `foster_count` (from CDE DataQuest "
            "chart_type 32) gives the point-in-time foster caseload by school. "
            "This is a related but distinct signal — `foster_count` measures "
            "current caseload, kidsdata_foster_entry_rate measures annual "
            "turnover/inflow. Both are useful but the spine signal alone is "
            "non-zero coverage for the master panel."),
    },
    "fema_flood": {
        "source": "FEMA National Flood Hazard Layer (NFHL)",
        "landing_url": "https://msc.fema.gov/portal/advanceSearch",
        "attempted_url": ("https://msc.fema.gov/portal/downloadProduct?"
                            "productTypeID=NFHL&productSubTypeID=NFHL_ST&"
                            "productKey=06"),
        "blocker": ("FEMA MSC portal requires session state from the UI; "
                       "direct downloads return HTML, not the geodatabase. "
                       "The CA state geodatabase is ~500MB and needs the "
                       "GDAL/FileGDB or OpenFileGDB driver to read."),
        "fallback": ("On Lightning AI: 1) install fiona + gdal-bin, 2) "
                       "download the CA NFHL geodatabase manually through the "
                       "portal, 3) extract S_FLD_HAZ_AR polygons where "
                       "FLD_ZONE in {'A','AE','AH','AO','V','VE'}, 4) for "
                       "each CA school point compute point-in-polygon + "
                       "haversine distance to nearest flood zone boundary."),
        "deferred_columns": [
            "fema_flood_in_zone",
            "fema_flood_distance_km",
        ],
    },
}


def main() -> int:
    out_path = REPO / "data_cache" / "stubs" / "deferred_sources.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(STUBS, indent=2), encoding="utf-8")
    log.info("[stubs] wrote local manifest: %s", out_path)

    # Upload manifest to R2 under each source's prefix
    for name, stub in STUBS.items():
        per_source = REPO / "data_cache" / "stubs" / f"{name}_stub.json"
        per_source.write_text(json.dumps(stub, indent=2), encoding="utf-8")
        r2.upload(per_source, f"raw/{name}/blocker.json")
        log.info("[stubs] %s -> raw/%s/blocker.json", stub["source"], name)

    # Also publish a top-level deferred manifest
    r2.upload(out_path, "stubs/deferred_sources.json")
    log.info("[stubs] manifest uploaded: stubs/deferred_sources.json")
    log.warning("DEFERRED: %s — see stubs/deferred_sources.json on R2",
                  list(STUBS.keys()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
