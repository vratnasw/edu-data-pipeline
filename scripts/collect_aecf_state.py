"""AECF KIDS COUNT state-level collector.

Pulls all indicators for one state from the AECF KIDS COUNT Data Center
(datacenter.aecf.org).

Schema (discovered via Playwright reconnaissance):
  /api/indicator-selection?abbreviation=<state>&loct=2
     → JSON tree of categories → subcategories → indicators (id + name)
  /api/customreportlocation?id=<state_id>
     → JSON tree of available loct levels (county, school district, etc.)
  /data/tables/<ind>-<slug>?loc=<state_id>&loct=<loct_id>
     → HTML page; parse <input id="loct-N" data-years="...">/etc to discover
       valid tf values per loct level
  /api/reports/detailedtable?ind=<id>&loc=<loc_id>&loct=<loct_id>&
        tf=<years>&fmt=<formats>&ch=<categories>&cmploc=1&inccmploc=false
     → {html: "<table>...</table>"} — multi-dim cross-tab

Output:
  raw/aecf/<STATE>/indicator_<id>.parquet  (long-form: location, year, dim, value)
  processed/canonical/aecf_<STATE>_indicator_index.parquet  (id → name + counts)
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "src"))

from utils.config_loader import load_dotenv  # noqa: E402
load_dotenv()

import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup

import config.r2_client as r2  # noqa: E402

logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s %(levelname)s :: %(message)s",
                       datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# loct values to pull. loct=2 (state), 5 (county), 23 (county school dist),
# 24 (city school dist) — skip congressional districts for now.
LOCT_TARGETS = [
    (2, "state"),
    (5, "county"),
    (23, "county_school_district"),
    (24, "city_school_district"),
    (21, "ward"),  # DC (and similar single-jurisdiction territories)
]
HDR_BASE = {"Accept": "application/json",
              "Referer": "https://datacenter.aecf.org/"}


# --------------------------------------------------------------------------- #
# Discovery
# --------------------------------------------------------------------------- #

def harvest_indicators(scraper, state_abbr: str) -> list[dict]:
    """Walk the category tree and return [{id, name, category, subcategory}, ...].

    DC and some territories return empty under loct=2 (state); fall back to
    other loct values (21=ward, 113/6/120=delegate district) until one yields
    indicators.
    """
    loct_attempts = [2, 21, 23, 24, 6, 113, 120]

    def _fetch(loct_id: int) -> list[dict]:
        r = scraper.get(
            f"https://datacenter.aecf.org/api/indicator-selection"
            f"?abbreviation={state_abbr}&loct={loct_id}",
            headers=HDR_BASE, timeout=120)
        r.raise_for_status()
        data = r.json()
        out: list[dict] = []

        def walk(cat_path: list[str], node: dict):
            for ind in node.get("indicators", []):
                out.append({
                    "id": ind.get("id"),
                    "name": ind.get("name"),
                    "category": cat_path[0] if cat_path else None,
                    "subcategory": " / ".join(cat_path[1:]) if len(cat_path) > 1 else None,
                    "slug": ind.get("slug") or "",
                })
            for sc in node.get("subCategories", node.get("subcategories", [])):
                walk(cat_path + [sc.get("name", "")], sc)

        for cat in data.get("categories", []):
            walk([cat.get("name", "")], cat)
        walk(["Other"], data.get("otherCategory", {}))
        return out

    for loct_id in loct_attempts:
        out = _fetch(loct_id)
        if out:
            if loct_id != 2:
                log.info("  [%s] indicator discovery via loct=%d (loct=2 empty)",
                          state_abbr, loct_id)
            return out
    return []


def harvest_state_locations(scraper, state_id: int) -> dict:
    """Returns {loct_id: [{id, name}, ...]} for the state's child locations."""
    r = scraper.get(
        f"https://datacenter.aecf.org/api/customreportlocation?id={state_id}",
        headers=HDR_BASE, timeout=60)
    r.raise_for_status()
    d = r.json()
    out: dict[int, list[dict]] = {}
    for ct in d.get("childLocationTypes", []):
        out[ct.get("id")] = [{"id": lc.get("id"), "name": lc.get("name")}
                                for lc in ct.get("locations", [])]
    return out


# --------------------------------------------------------------------------- #
# Per-indicator detail-page recon
# --------------------------------------------------------------------------- #

def discover_indicator_params(scraper, ind_id: int, slug: str,
                                  state_id: int) -> dict:
    """Visit the indicator's table page; parse all <input id='loct-N'
    data-years='...'> + the default ch and fmt values."""
    url = (f"https://datacenter.aecf.org/data/tables/{ind_id}-{slug}"
             f"?loc={state_id}&loct=2")
    r = scraper.get(url, timeout=60)
    if r.status_code != 200:
        return {"locts": {}, "fmts": [], "chs": []}
    text = r.text
    # Per-loct year list
    locts: dict[int, list[int]] = {}
    for m in re.finditer(r'id="loct-(\d+)"[^>]*data-years="([^"]*)"', text):
        loct_id = int(m.group(1))
        yrs = [int(y) for y in m.group(2).split(",") if y.strip().isdigit()]
        if yrs:
            locts[loct_id] = yrs
    # Format IDs — `<input ... name="fmt" value="N">` for each available format
    fmts = sorted(set(int(m.group(1)) for m in
                          re.finditer(r'name="fmt"[^>]*value="(\d+)"', text)))
    if not fmts:
        # Some pages have value first then name
        fmts = sorted(set(int(m.group(1)) for m in
                              re.finditer(r'value="(\d+)"[^>]*name="fmt"', text)))
    # Find all checkbox values that look like categories (data-name + value)
    chs = sorted(set(int(m.group(2)) for m in
                         re.finditer(r'<input[^>]*type="checkbox"[^>]*value="(\d+)"[^>]*'
                                          r'data-name="([^"]+)"', text)
                         if m.group(2).isdigit()))
    return {"locts": locts, "fmts": fmts, "chs": chs}


# --------------------------------------------------------------------------- #
# Data pull + parse
# --------------------------------------------------------------------------- #

def fetch_indicator_table(scraper, ind_id: int, loc_csv: str, loct_id: int,
                             tf: list[int], fmt: list[int], ch: list[int]) -> str | None:
    """Returns the inner HTML table for one (indicator, loct) view.

    loc_csv is a comma-separated list of location IDs at the requested loct
    level (e.g., for state-level pass '2'; for AL counties pass all 67 IDs)."""
    params = {
        "ind": ind_id, "loc": loc_csv, "loct": loct_id,
        "tf": ",".join(str(t) for t in tf),
        "fmt": ",".join(str(f) for f in fmt) if fmt else "421",
        "ch": ",".join(str(c) for c in ch) if ch else "",
        "cmploc": 1, "inccmploc": "false",
    }
    r = scraper.get("https://datacenter.aecf.org/api/reports/detailedtable",
                       params=params, headers=HDR_BASE, timeout=120)
    if r.status_code != 200:
        return None
    try:
        return r.json().get("html")
    except Exception:  # noqa: BLE001
        return None


def parse_table(html: str) -> pd.DataFrame:
    """Parse the AECF table HTML into long-form rows.

    Schema:
      <thead><tr> Location | <dim cols...> | Data Type | <year cols...> </tr></thead>
      <tbody><tr> <td>Loc name</td> ... <td>Type</td> <td>val</td>... </tr></tbody>
    """
    soup = BeautifulSoup(html, "html.parser")
    thead = soup.find("thead")
    if not thead: return pd.DataFrame()
    ths = thead.find_all("th")
    headers = [th.get_text(strip=True) for th in ths]
    # Year columns identified by 'data-tf-class="tf-<id>"' on the TH
    year_idx: dict[int, int] = {}
    for i, th in enumerate(ths):
        cls = th.get("data-tf-class") or ""
        m = re.match(r"tf-(\d+)", cls)
        if m:
            year_idx[i] = int(m.group(1))
    rows = []
    for tr in soup.find("tbody").find_all("tr") if soup.find("tbody") else []:
        tds = tr.find_all(["td", "th"])
        if len(tds) < 2: continue
        loc_name = tds[0].get_text(strip=True)
        # Dim columns are everything between location and the first year col
        first_year_col = min(year_idx.keys()) if year_idx else len(tds)
        dim_vals = [tds[i].get_text(strip=True) for i in range(1, first_year_col)]
        dim_labels = [headers[i] for i in range(1, first_year_col)]
        for col_idx, tf_id in year_idx.items():
            if col_idx >= len(tds): continue
            txt = tds[col_idx].get_text(strip=True).replace(",", "").replace("%", "")
            if txt in ("", "N.A.", "S", "—", "-"): continue
            try: val = float(txt)
            except ValueError: continue
            row = {"location_name": loc_name, "tf_id": tf_id,
                      "year_label": headers[col_idx], "value": val}
            for lbl, v in zip(dim_labels, dim_vals):
                row[f"dim_{lbl}"] = v
            rows.append(row)
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Orchestrator
# --------------------------------------------------------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-abbr", required=True, help="e.g. AL")
    ap.add_argument("--state-id", required=True, type=int, help="AECF location id; AL=2")
    ap.add_argument("--limit", type=int, default=None,
                       help="cap on indicators (smoke testing)")
    ap.add_argument("--rate-limit-s", type=float, default=0.3)
    args = ap.parse_args()

    s = cloudscraper.create_scraper(browser={"browser": "chrome",
                                                    "platform": "windows"})
    # Warm session
    s.get(f"https://datacenter.aecf.org/data?location={args.state_abbr}",
            timeout=120)

    # 1. Discover indicators + locations
    inds = harvest_indicators(s, args.state_abbr)
    log.info("[%s] harvested %d indicators", args.state_abbr, len(inds))
    locs = harvest_state_locations(s, args.state_id)
    for loct_id, lst in locs.items():
        log.info("  loct=%d  n=%d  e.g. %s", loct_id, len(lst),
                  ", ".join(l["name"] for l in lst[:2]))

    # Save index
    idx_local = (REPO / "data_cache" / "raw" / "aecf"
                    / args.state_abbr / "indicator_index.parquet")
    idx_local.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(inds).to_parquet(idx_local, index=False)
    r2.upload(idx_local, f"raw/aecf/{args.state_abbr}/indicator_index.parquet")

    # 2. For each indicator, recon + pull
    all_rows = []
    skipped = 0
    t0 = time.time()
    for ti, ind in enumerate(inds[: args.limit] if args.limit else inds):
        ind_id = ind["id"]; slug = ind.get("slug") or str(ind_id)
        log.info("[%d/%d] ind=%d %s", ti+1, len(inds), ind_id,
                  ind["name"][:60])
        params = discover_indicator_params(s, ind_id, slug, args.state_id)
        if not params["locts"]:
            log.warning("  no locts discovered; skip"); skipped += 1; continue
        # Default fmt/ch: if empty, use the captured-default for AL gender 102:
        # fmt=421,422 and ch=14,15,65. But these are indicator-specific.
        # Conservative: if ch empty, pass empty; if fmt empty, default to 421.
        fmt = params["fmts"] or [421, 422]
        ch = params["chs"] or []
        # Pull at each available loct level (intersect with targets)
        for loct_id, _label in LOCT_TARGETS:
            if loct_id not in params["locts"]: continue
            yrs = params["locts"][loct_id]
            # loc CSV depends on level:
            #   state (loct=2): just the state ID
            #   sub-state (county / school district): comma-list of child IDs
            if loct_id == 2:
                loc_csv = str(args.state_id)
            else:
                child_locs = locs.get(loct_id, [])
                if not child_locs: continue
                loc_csv = ",".join(str(l["id"]) for l in child_locs)
            try:
                html = fetch_indicator_table(s, ind_id, loc_csv, loct_id,
                                                  yrs, fmt, ch)
            except Exception as e:  # noqa: BLE001
                log.warning("  loct=%d FAILED: %s", loct_id, e); continue
            if not html: continue
            df = parse_table(html)
            if df.empty: continue
            df["indicator_id"] = ind_id
            df["indicator_name"] = ind["name"]
            df["category"] = ind["category"]
            df["subcategory"] = ind["subcategory"]
            df["loct_id"] = loct_id
            all_rows.append(df)
            time.sleep(args.rate_limit_s)

    log.info("[%s] pulled %d data rows from %d indicators (skipped=%d) in %.0fs",
              args.state_abbr,
              sum(len(d) for d in all_rows), len(all_rows), skipped,
              time.time() - t0)

    if not all_rows:
        log.error("no data captured"); return 1
    panel = pd.concat(all_rows, ignore_index=True)
    out = (REPO / "data_cache" / "raw" / "aecf"
              / args.state_abbr / "all_indicators.parquet")
    out.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(out, index=False)
    r2.upload(out, f"raw/aecf/{args.state_abbr}/all_indicators.parquet")
    log.info("[%s] uploaded: %d rows × %d cols", args.state_abbr,
              len(panel), panel.shape[1])
    log.info("  unique indicators: %d", panel["indicator_id"].nunique())
    log.info("  loct breakdown: %s",
              dict(panel.groupby("loct_id").size()))
    log.info("  unique locations: %d", panel["location_name"].nunique())
    return 0


if __name__ == "__main__":
    sys.exit(main())
