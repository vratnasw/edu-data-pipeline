"""Discover AECF state location IDs from indicator-selection-locations endpoint.

Writes data_cache/aecf_state_ids.json with {state_abbr: {state_id, state_name}}.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import cloudscraper

REPO = Path(__file__).resolve().parents[1]
CACHE = REPO / "data_cache"
CACHE.mkdir(parents=True, exist_ok=True)
OUT = CACHE / "aecf_state_ids.json"

US_STATE_NAMES = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA",
    "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN",
    "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA",
    "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI",
    "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO", "Montana": "MT",
    "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ",
    "New Mexico": "NM", "New York": "NY", "North Carolina": "NC",
    "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR",
    "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
}

HDR = {"Accept": "application/json", "Referer": "https://datacenter.aecf.org/"}


def main():
    s = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows"})
    s.get("https://datacenter.aecf.org/data?location=AL", timeout=120)
    time.sleep(1)

    r = s.get("https://datacenter.aecf.org/api/indicator-selection-locations",
              headers=HDR, timeout=60)
    r.raise_for_status()
    items = r.json()
    assert isinstance(items, list), f"unexpected response type: {type(items).__name__}"

    out = {}
    for entry in items:
        name = entry.get("name")
        if name in US_STATE_NAMES:
            abbr = US_STATE_NAMES[name]
            out[abbr] = {"state_id": int(entry["id"]), "state_name": name}

    out = dict(sorted(out.items()))
    OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote {len(out)} entries to {OUT}")

    missing = [a for a in US_STATE_NAMES.values() if a not in out]
    if missing:
        print(f"MISSING: {missing}")
        return 1
    print("All 51 jurisdictions resolved.")
    for abbr, info in out.items():
        print(f"  {abbr}  id={info['state_id']:>3}  {info['state_name']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
