"""Phase 17 — kidsdata.org bullying & harassment indicators (all 48, multi-year).

Source: 48 indicators under /topic/71/.../summary, organized as:
  8 bullying types × 6 breakdown views = 48 indicators
  (overall, bias, disability, gender, race, religion, sexual-orient, cyber)
  × (gender×grade, connectedness, parent-ed, race, sexual-orient, grade)

API: /api/table/table with the full param schema (discovered via Playwright):
  ind=<topic_id>, loc=<58_county_ids>, tf=<year-range_id>, fmt=<number-fmt_id>,
  ch=<all_dim_category_ids>, pdist=33

Pipeline:
  Stage A — discovery: visit each indicator detail page, parse its <select>
    elements to learn the valid tf values (year ranges) + ch values (cat IDs)
  Stage B — pull: for each (indicator, tf), call the API + save raw payload
  Stage C — parse: each county's HTML table → long-form rows
  Stage D — write canonical:
    processed/canonical/kidsdata_bullying_breakdowns.parquet (long-form)
    processed/canonical/kidsdata_bullying_overall.parquet
      (county-year × 8 bullying types, scalar % from pooled-Some column)
"""
from __future__ import annotations

import json
import logging
import re
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "src"))

import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup

import config.r2_client as r2  # noqa: E402

logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s %(levelname)s :: %(message)s",
                       datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

ALL_CA_COUNTY_LOC_IDS = ("2,127,347,1763,331,348,336,171,321,345,357,332,324,"
                              "369,358,362,360,337,327,364,356,217,353,328,354,"
                              "323,352,320,339,334,365,343,330,367,344,355,366,"
                              "368,265,349,361,4,273,59,370,326,333,322,341,338,"
                              "350,342,329,325,359,351,363,340,335")

# 48 indicators harvested from /topic/71/.../summary, plus the 8 "by Grade
# Level" overall views. (id, slug) pairs.
INDICATORS = [
    # (topic_id, type, breakdown)
    (620, "bullying", "gender_grade"),
    (623, "bullying", "connectedness"),
    (2024, "bullying", "parent_education"),
    (622, "bullying", "race"),
    (2025, "bullying", "sexual_orientation"),
    (621, "bullying", "grade"),
    (625, "bias", "gender_grade"),
    (624, "bias", "connectedness"),
    (2026, "bias", "parent_education"),
    (627, "bias", "race"),
    (2027, "bias", "sexual_orientation"),
    (626, "bias", "grade"),
    (424, "disability", "gender_grade"),
    (426, "disability", "connectedness"),
    (2028, "disability", "parent_education"),
    (425, "disability", "race"),
    (2029, "disability", "sexual_orientation"),
    (628, "disability", "grade"),
    (430, "gender", "gender_grade"),
    (432, "gender", "connectedness"),
    (2030, "gender", "parent_education"),
    (431, "gender", "race"),
    (2031, "gender", "sexual_orientation"),
    (629, "gender", "grade"),
    (418, "race", "gender_grade"),
    (420, "race", "connectedness"),
    (2032, "race", "parent_education"),
    (419, "race", "race"),
    (2033, "race", "sexual_orientation"),
    (630, "race", "grade"),
    (421, "religion", "gender_grade"),
    (423, "religion", "connectedness"),
    (2034, "religion", "parent_education"),
    (422, "religion", "race"),
    (2035, "religion", "sexual_orientation"),
    (632, "religion", "grade"),
    (427, "sexual_orientation", "gender_grade"),
    (429, "sexual_orientation", "connectedness"),
    (2036, "sexual_orientation", "parent_education"),
    (428, "sexual_orientation", "race"),
    (2037, "sexual_orientation", "sexual_orientation"),
    (633, "sexual_orientation", "grade"),
    (1813, "cyberbullying", "gender_grade"),
    (1814, "cyberbullying", "connectedness"),
    (2038, "cyberbullying", "parent_education"),
    (1815, "cyberbullying", "race"),
    (2039, "cyberbullying", "sexual_orientation"),
    (1812, "cyberbullying", "grade"),
]

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


def build_slug_map(scraper) -> dict:
    """Scrape the summary page once to map topic_id → kidsdata's real slug."""
    r = scraper.get(
        "https://www.kidsdata.org/topic/71/bullying-and-harassment-at-school/summary",
        timeout=120)
    mp = {}
    for m in re.findall(r'href="/topic/(\d+)/([a-z0-9-]+)', r.text):
        mp[int(m[0])] = m[1]
    return mp


def discover_dims(scraper, topic_id: int, slug: str) -> dict:
    """Visit the indicator detail page and parse its dropdowns."""
    url = f"https://www.kidsdata.org/topic/{topic_id}/{slug}/table"
    r = scraper.get(url, timeout=60, allow_redirects=True)
    if r.status_code != 200:
        log.warning("[discover %d] status=%d", topic_id, r.status_code)
        return {"tfs": [], "chs": []}
    soup = BeautifulSoup(r.text, "html.parser")
    selects = soup.find_all("select", class_="medium")
    tfs = []         # time-frame IDs (year ranges OR single years)
    chs_per_dim = [] # category IDs per breakdown dimension
    for sel in selects:
        opts = sel.find_all("option")
        labels = [o.get_text(strip=True) for o in opts]
        vals = [o.get("value", "") for o in opts]
        # Time-frame selector: options like '2017-2019' OR '2020' (single year)
        is_year_select = all(
            re.match(r"^\d{4}(-\d{4})?$", l) for l in labels if l
        )
        if is_year_select and labels:
            tfs = [int(v) for v in vals if v.isdigit()]
        else:
            dim_ids = [int(v) for v in vals if v.isdigit()]
            if dim_ids:
                chs_per_dim.append(dim_ids)
    # Flatten chs into one comma-string
    all_ch_ids = []
    for d in chs_per_dim:
        all_ch_ids.extend(d)
    return {"tfs": tfs, "chs": all_ch_ids}


def pull_one(scraper, topic_id: int, tf: int, ch: str) -> list:
    """One API call. Returns list of HTML strings (one per county)."""
    url = "https://www.kidsdata.org/api/table/table"
    params = {"ind": topic_id, "loc": ALL_CA_COUNTY_LOC_IDS, "tf": tf,
                "fmt": 873, "ch": ch, "pdist": 33}
    r = scraper.get(url, params=params,
                       headers={"Accept": "application/json",
                                  "Referer": "https://www.kidsdata.org/"},
                       timeout=120)
    r.raise_for_status()
    return r.json()


def _build_leaf_column_labels(thead) -> list[str]:
    """Walk the multi-level thead row-by-row, expanding each TH by colspan,
    to compose leaf column names. Returns one label per leaf cell, in body-column
    order, formatted as 'Parent | Leaf' when multi-level."""
    rows = thead.find_all("tr") if thead else []
    if not rows:
        return []
    # Expand each row into a flat list of (label, source_row_index) per leaf col
    # accounting for colspan + rowspan. We track which thead row each TH lives
    # in to compose hierarchical labels.
    n_rows = len(rows)
    # Build a 2-D grid: grid[row][col] = label (or None if continuation)
    # First find total number of leaf columns from any row
    max_cols = 0
    for tr in rows:
        cspan = sum(int(th.get("colspan", 1)) for th in tr.find_all("th"))
        max_cols = max(max_cols, cspan)
    grid: list[list[str | None]] = [[None] * max_cols for _ in range(n_rows)]
    for ri, tr in enumerate(rows):
        ci = 0
        for th in tr.find_all("th"):
            # Find next free column in this row
            while ci < max_cols and grid[ri][ci] is not None:
                ci += 1
            if ci >= max_cols:
                break
            label = th.get_text(strip=True)
            # Skip the table-title (county name) and fmt (Percent) cells —
            # we only care about cat ('ch') labels for column composition
            is_ch = th.get("data-entity") == "ch"
            cs = int(th.get("colspan", 1))
            rs = int(th.get("rowspan", 1))
            for dr in range(rs):
                for dc in range(cs):
                    if ri + dr < n_rows and ci + dc < max_cols:
                        # Only record labels for 'ch' entities
                        grid[ri + dr][ci + dc] = label if is_ch else (
                            grid[ri + dr][ci + dc] or "")
            ci += cs
    # Now collapse each column down to its hierarchical label
    out = []
    for c in range(max_cols):
        parts = [grid[r][c] for r in range(n_rows)
                    if grid[r][c] and grid[r][c] != ""]
        out.append(" | ".join(parts) if parts else f"col_{c}")
    return out


def parse_county_table(html: str) -> tuple[str, pd.DataFrame]:
    """Parse one county's HTML table → (county_name, long-form rows).

    Multi-level header: data-entity='ch' THs are category labels. Compose
    'Parent | Leaf' column labels via colspan grid expansion.
    """
    soup = BeautifulSoup(html, "html.parser")
    first_th = soup.select_one("thead th")
    if not first_th:
        return ("?", pd.DataFrame())
    county = first_th.get_text(strip=True)

    thead = soup.find("thead")
    column_labels = _build_leaf_column_labels(thead)
    # The "Percent" / fmt column spans across all leaves — strip it from labels
    # by removing any "Percent" or "Rate" parent token, since it carries no
    # category info
    column_labels = [c.replace("Percent | ", "").replace("Rate | ", "")
                       for c in column_labels]
    # The county column is itself one of the leaves (column 0 in this layout
    # is the row label, not a data column). Drop the leftmost label since
    # body row 0 cell is a TH (rowlabel), not a TD.

    rows_out = []
    for tr in soup.select("tbody tr"):
        tds = tr.find_all("td")
        rowlabel_th = tr.find("th")
        rowlabel = rowlabel_th.get_text(strip=True) if rowlabel_th else None
        # Body data cells correspond to leaf columns after the row-label slot.
        # Skip the leftmost leaf label (corresponds to the rowlabel TH).
        data_labels = column_labels[1:] if len(column_labels) > len(tds) else column_labels
        for i, td in enumerate(tds):
            txt = td.get_text(strip=True).replace("%", "").replace(",", "")
            try: val = float(txt)
            except ValueError: continue
            col_label = data_labels[i] if i < len(data_labels) else f"col_{i}"
            rows_out.append({"county_name": county, "row_label": rowlabel,
                                "col_label": col_label, "value": val})
    return (county, pd.DataFrame(rows_out))


def main() -> int:
    s = cloudscraper.create_scraper(browser={"browser": "chrome",
                                                    "platform": "windows"})
    s.get("https://www.kidsdata.org/topic/71/bullying-and-harassment-at-school/summary",
            timeout=120)

    slug_map = build_slug_map(s)
    log.info("[bully] built slug_map: %d entries", len(slug_map))

    all_rows = []
    t0 = time.time()
    for ti, (topic_id, kind, breakdown) in enumerate(INDICATORS):
        slug = slug_map.get(topic_id)
        if not slug:
            log.warning("[%d/%d] ind=%d — no slug in summary map; skip",
                          ti+1, len(INDICATORS), topic_id); continue
        log.info("[%d/%d] %s/%s (ind=%d, slug=%s)", ti+1, len(INDICATORS),
                  kind, breakdown, topic_id, slug)
        try:
            dims = discover_dims(s, topic_id, slug)
        except Exception as e:  # noqa: BLE001
            log.warning("  discovery failed: %s", e); continue
        if not dims["tfs"] or not dims["chs"]:
            log.warning("  no dims discovered tfs=%s chs=%s", dims["tfs"], dims["chs"]); continue
        ch_str = ",".join(str(c) for c in dims["chs"])
        for tf in dims["tfs"]:
            try:
                payload = pull_one(s, topic_id, tf, ch_str)
            except Exception as e:  # noqa: BLE001
                log.warning("  tf=%d FAILED: %s", tf, e); continue
            if not payload:
                continue
            for html in payload:
                if not isinstance(html, str): continue
                county, df = parse_county_table(html)
                if df.empty: continue
                df["indicator_topic_id"] = topic_id
                df["indicator_kind"] = kind
                df["indicator_breakdown"] = breakdown
                df["tf"] = tf
                all_rows.append(df)
            # tiny delay to be polite
            time.sleep(0.1)

    if not all_rows:
        log.error("no data captured"); return 1

    panel = pd.concat(all_rows, ignore_index=True)
    log.info("[bully] total parsed rows: %d (%.1fs)",
              len(panel), time.time()-t0)

    # Resolve county FIPS
    panel["county_code"] = panel["county_name"].map(CA_COUNTY_NAME_TO_FIPS)
    state_rows = panel["county_code"].isna().sum()
    log.info("[bully] state-level rows (no FIPS): %d", state_rows)
    # Keep both — state-level rows have county_name='California'

    # Year-range → year_num: tf 134 = '2017-2019' → use last year (2019)
    # Use a lookup learned from the discovery; for now derive from tf via a quick
    # scrape:
    yr_lookup_url = "https://www.kidsdata.org/topic/620/bullying-gender/table"
    rr = s.get(yr_lookup_url, timeout=60)
    soup = BeautifulSoup(rr.text, "html.parser")
    tf_to_year = {}
    for sel in soup.find_all("select", class_="medium"):
        for o in sel.find_all("option"):
            txt = o.get_text(strip=True)
            m = re.match(r"^\d{4}-(\d{4})$", txt)
            if m and o.get("value", "").isdigit():
                tf_to_year[int(o["value"])] = int(m.group(1))
    panel["year_num"] = panel["tf"].map(tf_to_year)
    # Fallback: ratio-derive (tf 134 ≈ 2019; tf 122 ≈ 2017; tf 93 ≈ 2015; tf 81 ≈ 2013)
    fallback = {134:2019, 122:2017, 93:2015, 81:2013}
    panel["year_num"] = panel["year_num"].fillna(panel["tf"].map(fallback))
    log.info("[bully] tf→year_num lookup: %s", tf_to_year or fallback)

    # ---- Long-form breakdowns parquet ---- #
    breakdowns_local = (REPO / "data_cache" / "processed" / "canonical"
                            / "kidsdata_bullying_breakdowns.parquet")
    breakdowns_local.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(breakdowns_local, index=False)
    r2.upload(breakdowns_local,
                 "processed/canonical/kidsdata_bullying_breakdowns.parquet")
    log.info("[bully] breakdowns canonical uploaded: %d rows (%d indicators × ~%d counties × %d years)",
              len(panel), panel["indicator_topic_id"].nunique(),
              panel["county_name"].nunique(), panel["year_num"].nunique())

    # ---- Overall rates per bullying type per (county, year) ---- #
    # For each kind, take the mean of all values per (county, year). Each
    # indicator's table reports % of respondents in each demographic bucket
    # who experienced bullying ('Some'); averaging across breakdown categories
    # gives a coarse but interpretable overall rate.
    # The "experienced bullying" column ends with "Some" (vs "None"). For
    # indicators with binary Yes/No columns, the leaf label is "Yes".
    is_affirmative = (panel["col_label"].str.endswith("| Some")
                          | panel["col_label"].str.endswith("| Yes")
                          | (panel["col_label"] == "Some")
                          | (panel["col_label"] == "Yes"))
    overall = (panel[is_affirmative]
                .groupby(["county_code", "county_name", "year_num", "indicator_kind"])
                  ["value"].mean().reset_index())
    overall = overall.pivot_table(
        index=["county_code", "county_name", "year_num"],
        columns="indicator_kind", values="value", aggfunc="mean"
    ).reset_index()
    overall.columns = [f"kidsdata_{c}_pct_some" if c in
                          ("bullying","bias","disability","gender","race","religion",
                            "sexual_orientation","cyberbullying") else c
                          for c in overall.columns]
    overall_local = (REPO / "data_cache" / "processed" / "canonical"
                          / "kidsdata_bullying_overall.parquet")
    overall.to_parquet(overall_local, index=False)
    r2.upload(overall_local,
                 "processed/canonical/kidsdata_bullying_overall.parquet")
    log.info("[bully] overall canonical uploaded: %d rows × %d cols",
              len(overall), overall.shape[1])
    return 0


if __name__ == "__main__":
    sys.exit(main())
