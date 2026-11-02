"""Probe AECF endpoint shapes to understand how to extract state IDs."""
from __future__ import annotations

import json
import time
from pathlib import Path

import cloudscraper

HDR = {"Accept": "application/json", "Referer": "https://datacenter.aecf.org/"}


def main():
    s = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows"})
    s.get("https://datacenter.aecf.org/data?location=AL", timeout=120)
    time.sleep(1)

    # Endpoint 1: indicator-selection-locations (returned a list of 17628 bytes)
    r = s.get("https://datacenter.aecf.org/api/indicator-selection-locations",
              headers=HDR, timeout=60)
    print("=== indicator-selection-locations ===")
    print(f"status {r.status_code}, size {len(r.content)}")
    j = r.json()
    print(f"type: {type(j).__name__}, len: {len(j) if hasattr(j, '__len__') else '?'}")
    if isinstance(j, list) and j:
        print(f"first item type: {type(j[0]).__name__}")
        print(f"first item: {json.dumps(j[0], indent=2)[:1500]}")
        if len(j) > 1:
            print(f"second item: {json.dumps(j[1], indent=2)[:1500]}")
        if len(j) > 10:
            print(f"item 10: {json.dumps(j[10], indent=2)[:800]}")

    Path("c:/Users/visha/Downloads/r_and_d/edu-data-pipeline/data_cache/aecf_isl_raw.json").write_text(
        json.dumps(j, indent=2), encoding="utf-8")

    # Endpoint 2: customreportlocation?id=2 (known AL)
    print("\n=== customreportlocation?id=2 ===")
    r = s.get("https://datacenter.aecf.org/api/customreportlocation?id=2",
              headers=HDR, timeout=60)
    print(f"status {r.status_code}, size {len(r.content)}")
    if r.status_code == 200:
        try:
            j2 = r.json()
            print(f"keys: {list(j2.keys()) if isinstance(j2, dict) else '?'}")
            # Show top-level fields
            if isinstance(j2, dict):
                for k, v in j2.items():
                    if isinstance(v, (str, int, float, bool, type(None))):
                        print(f"  {k} = {v}")
                    else:
                        print(f"  {k}: {type(v).__name__}, len={len(v) if hasattr(v, '__len__') else '?'}")
        except Exception as e:
            print(f"json err: {e}")
            print(r.text[:800])


if __name__ == "__main__":
    main()
