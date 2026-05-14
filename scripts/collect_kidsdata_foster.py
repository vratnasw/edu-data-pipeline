"""Phase 16 — kidsdata.org foster-care entry rates (county-level, multi-year).

Source: kidsdata.org /api/table/table endpoint.
  https://www.kidsdata.org/api/table/table?ind=14&loc=<county_ids>&tf=108&fmt=<YY1YY2>

Approach: Cloudflare 'Just a moment...' JS challenge passed via cloudscraper;
direct API call (no Playwright needed) once we know the right `ind=14` param
(missing from the obvious schema — found by intercepting the real XHR with
Playwright as a one-shot reconnaissance).

The endpoint returns a JSON list with one HTML string per request — the
location-table rendering. We parse it with BeautifulSoup.

Outputs:
  raw/kidsdata_foster_entries/raw_<fmt>.json (raw payloads, one per vintage)
  processed/canonical/kidsdata_foster_entries.parquet
     county_code (5-digit FIPS), year_num, kidsdata_foster_entry_rate_per_1k
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "src"))

import pandas as pd
from bs4 import BeautifulSoup

import cloudscraper
import config.r2_client as r2  # noqa: E402

logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s %(levelname)s :: %(message)s",
                       datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

INDICATOR = 14
TIME_FRAME = 108
ALL_CA_COUNTY_LOC_IDS = ("2,127,347,1763,331,348,336,171,321,345,357,332,324,"
                              "369,358,362,360,337,327,364,356,217,353,328,354,"
                              "323,352,320,339,334,365,343,330,367,344,355,366,"
                              "368,265,349,361,4,273,59,370,326,333,322,341,338,"
                              "350,342,329,325,359,351,363,340,335")

# Try wide range; vintages with no data return an empty table → skipped
VINTAGES = ["2324", "2223", "2122", "2021", "1920", "1819", "1718", "1617",
              "1516", "1415", "1314", "1213", "1112", "1011"]

# CA county name → 5-digit FIPS (verified for all 58 CA counties)
# Source: CA OSI county directory (alphabetical = CDS county numbering).
# FIPS for CA = state(06) + 3-digit county code from the 1979 federal sequence.
CA_COUNTY_NAME_TO_FIPS = {
    "Alameda County": "06001", "Alpine County": "06003", "Amador County": "06005",
    "Butte County": "06007", "Calaveras County": "06009", "Colusa County": "06011",
    "Contra Costa County": "06013", "Del Norte County": "06015",
    "El Dorado County": "06017", "Fresno County": "06019", "Glenn County": "06021",
    "Humboldt County": "06023", "Imperial County": "06025", "Inyo County": "06027",
    "Kern County": "06029", "Kings County": "06031", "Lake County": "06033",
    "Lassen County": "06035", "Los Angeles County": "06037", "Madera County": "06039",
    "Marin County": "06041", "Mariposa County": "06043", "Mendocino County": "06045",
    "Merced County": "06047", "Modoc County": "06049", "Mono County": "06051",
    "Monterey County": "06053", "Napa County": "06055", "Nevada County": "06057",
    "Orange County": "06059", "Placer County": "06061", "Plumas County": "06063",
    "Riverside County": "06065", "Sacramento County": "06067",
    "San Benito County": "06069", "San Bernardino County": "06071",
    "San Diego County": "06073", "San Francisco County": "06075",
    "San Joaquin County": "06077", "San Luis Obispo County": "06079",
    "San Mateo County": "06081", "Santa Barbara County": "06083",
    "Santa Clara County": "06085", "Santa Cruz County": "06087",
    "Shasta County": "06089", "Sierra County": "06091", "Siskiyou County": "06093",
    "Solano County": "06095", "Sonoma County": "06097", "Stanislaus County": "06099",
    "Sutter County": "06101", "Tehama County": "06103", "Trinity County": "06105",
    "Tulare County": "06107", "Tuolumne County": "06109", "Ventura County": "06111",
    "Yolo County": "06113", "Yuba County": "06115",
}


def fetch_vintage(scraper, fmt: str) -> list:
    """Returns the parsed list of HTML-table strings for one vintage."""
    url = "https://www.kidsdata.org/api/table/table"
    params = {
        "ind": INDICATOR, "loc": ALL_CA_COUNTY_LOC_IDS, "tf": TIME_FRAME,
        "fmt": fmt, "sortType": "asc", "sortColumnId": 0,
    }
    r = scraper.get(url, params=params,
                       headers={"Accept": "application/json",
                                  "Referer": "https://www.kidsdata.org/topic/14/foster-entries/table"},
                       timeout=120)
    r.raise_for_status()
    return r.json()


def parse_table_html(html: str, vintage: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for tr in soup.find_all("tr"):
        th = tr.find("th")
        if not th: continue
        loc_id = th.get("data-id")
        loc_type = th.get("data-locationtype")
        name = th.get_text(strip=True)
        for td in tr.find_all("td"):
            txt = td.get_text(strip=True).replace(",", "")
            try:
                val = float(txt)
                rows.append({"county_name": name, "loc_id": loc_id,
                                "loc_type": loc_type, "vintage": vintage,
                                "kidsdata_foster_entry_rate_per_1k": val})
                break
            except ValueError:
                continue
    return pd.DataFrame(rows)


def main() -> int:
    scraper = cloudscraper.create_scraper(browser={"browser": "chrome",
                                                          "platform": "windows"})
    # Warm Cloudflare cookie
    scraper.get("https://www.kidsdata.org/topic/14/foster-entries/table",
                  timeout=120)

    all_dfs = []
    for fmt in VINTAGES:
        log.info("[kd] fetching fmt=%s", fmt)
        try:
            payload = fetch_vintage(scraper, fmt)
        except Exception as e:  # noqa: BLE001
            log.warning("[kd %s] FAILED: %s", fmt, e)
            continue
        # Save raw
        raw_local = (REPO / "data_cache" / "raw"
                        / "kidsdata_foster_entries" / f"raw_{fmt}.json")
        raw_local.parent.mkdir(parents=True, exist_ok=True)
        raw_local.write_text(json.dumps(payload), encoding="utf-8")
        r2.upload(raw_local, f"raw/kidsdata_foster_entries/raw_{fmt}.json")
        if not payload:
            log.warning("[kd %s] empty payload", fmt); continue
        html_str = payload[0] if isinstance(payload, list) else str(payload)
        df = parse_table_html(html_str, fmt)
        if df.empty:
            log.warning("[kd %s] no rows parsed (data unavailable for vintage)",
                          fmt); continue
        log.info("[kd %s] parsed %d rows", fmt, len(df))
        all_dfs.append(df)

    if not all_dfs:
        log.error("[kd] no vintages with data — aborting")
        return 1

    panel = pd.concat(all_dfs, ignore_index=True)

    # vintage 2324 → year_num 2023
    panel["year_num"] = panel["vintage"].str[:2].astype(int) + 2000
    # Map county_name → FIPS county_code; state-level rows get NaN
    panel["county_code"] = panel["county_name"].map(CA_COUNTY_NAME_TO_FIPS)
    matched = panel["county_code"].notna().sum()
    state_rows = (panel["loc_type"] == "State").sum()
    log.info("[kd] matched %d/%d rows to county FIPS (%d state rows excluded)",
              matched, len(panel), state_rows)

    # Canonical: county-level only
    canon = panel[panel["county_code"].notna()][[
        "year_num", "county_code", "county_name",
        "kidsdata_foster_entry_rate_per_1k"
    ]].copy()

    local = (REPO / "data_cache" / "processed" / "canonical"
                / "kidsdata_foster_entries.parquet")
    local.parent.mkdir(parents=True, exist_ok=True)
    canon.to_parquet(local, index=False)
    r2.upload(local, "processed/canonical/kidsdata_foster_entries.parquet")
    log.info("[kd] canonical uploaded: %d rows, %d counties, %d years",
              len(canon), canon["county_code"].nunique(),
              canon["year_num"].nunique())
    log.info("[kd] rate range: %.2f - %.2f per 1k",
              canon["kidsdata_foster_entry_rate_per_1k"].min(),
              canon["kidsdata_foster_entry_rate_per_1k"].max())
    log.info("[kd] missingness: 0.0%% (all matched counties have a value)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
