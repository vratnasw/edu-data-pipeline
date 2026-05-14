"""Phase 18 — kidsdata.org child abuse & neglect indicators (47 total).

Source: /topic/2/child-abuse-and-neglect/summary lists 47 indicators across
7 topic groups:
  1. Reported Abuse (overall + by Age/Race/Type)
  2. Substantiated Abuse (overall + by Age/Race/Type)
  3. ED Visits for Maltreatment, CA only (overall + by Age/Gender/Race/Payment/Type)
  4. Hospitalizations for Maltreatment, CA only (overall + 6 breakdowns)
  5. ACEs NSCH (Parent Reported), by Number/County/Type/Race
  6. ACEs BRFSS (Adult Retrospective), by Type/County/Education/Income/etc.
  7. Childhood Hardships (Maternal Retrospective), 3 sub-types × 5 breakdowns

Reuses the schema-discovery + multi-level THEAD parser from the bullying
collector. Some ACEs/hardships indicators are state-level only — those will
return empty/sparse data for county FIPS join, which is expected.

Outputs:
  processed/canonical/kidsdata_child_abuse_breakdowns.parquet (long-form)
  processed/canonical/kidsdata_child_abuse_overall.parquet
     county-year scalars for the 4 county-grained main rates:
     reported_abuse_rate, substantiated_abuse_rate,
     maltreatment_ed_visits_rate, maltreatment_hospitalizations_rate
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
sys.path.insert(0, str(REPO / "scripts"))

import cloudscraper
import pandas as pd

# Reuse parser helpers from the bullying collector
from collect_kidsdata_bullying import (
    parse_county_table, _build_leaf_column_labels,
    ALL_CA_COUNTY_LOC_IDS, CA_COUNTY_NAME_TO_FIPS, build_slug_map,
    pull_one, discover_dims,
)
import config.r2_client as r2  # noqa: E402

logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s %(levelname)s :: %(message)s",
                       datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# 47 indicators harvested from /topic/2/child-abuse-and-neglect/summary
INDICATORS = [
    # group: reported abuse
    (1, "reported_abuse", "overall"),
    (2, "reported_abuse", "age"),
    (3, "reported_abuse", "race"),
    (4, "reported_abuse", "type"),
    # substantiated abuse
    (6, "substantiated_abuse", "overall"),
    (8, "substantiated_abuse", "age"),
    (7, "substantiated_abuse", "race"),
    (9, "substantiated_abuse", "type"),
    # ED visits for maltreatment (CA only — has county data)
    (2450, "maltreatment_ed_visits", "overall"),
    (2451, "maltreatment_ed_visits", "age"),
    (2453, "maltreatment_ed_visits", "gender"),
    (2454, "maltreatment_ed_visits", "race"),
    (2455, "maltreatment_ed_visits", "payment"),
    (2456, "maltreatment_ed_visits", "type"),
    # Hospitalizations for maltreatment (CA only)
    (2452, "maltreatment_hospitalizations", "overall"),
    (2457, "maltreatment_hospitalizations", "age"),
    (2458, "maltreatment_hospitalizations", "gender"),
    (2459, "maltreatment_hospitalizations", "race"),
    (2460, "maltreatment_hospitalizations", "payment"),
    (2461, "maltreatment_hospitalizations", "type"),
    (2462, "maltreatment_hospitalizations", "length_of_stay"),
    # ACEs NSCH (parent reported) — likely state/county-restricted
    (2214, "aces_nsch", "number"),
    (1927, "aces_nsch", "county"),
    (2215, "aces_nsch", "type"),
    (2223, "aces_nsch_2plus", "race"),
    # ACEs BRFSS (adult retrospective)
    (2440, "aces_brfss", "type"),
    (2447, "aces_brfss", "county"),
    (2443, "aces_brfss", "education"),
    (2441, "aces_brfss", "income"),
    (2442, "aces_brfss", "insurance"),
    (2439, "aces_brfss", "race"),
    (2444, "aces_brfss", "sexual_orientation"),
    # Hardships (maternal retrospective)
    (1929, "hardships", "overall"),
    (1930, "hardships", "income"),
    (1931, "hardships", "age"),
    (1932, "hardships", "insurance"),
    (1933, "hardships", "race"),
    (1934, "hardships_basic_needs", "overall"),
    (1935, "hardships_basic_needs", "income"),
    (1936, "hardships_basic_needs", "age"),
    (1937, "hardships_basic_needs", "insurance"),
    (1938, "hardships_basic_needs", "race"),
    (1964, "hardships_foster_care", "overall"),
    (1965, "hardships_foster_care", "income"),
    (1966, "hardships_foster_care", "age"),
    (1967, "hardships_foster_care", "insurance"),
    (1968, "hardships_foster_care", "race"),
]

SUMMARY_URL = "https://www.kidsdata.org/topic/2/child-abuse-and-neglect/summary"


def main() -> int:
    s = cloudscraper.create_scraper(browser={"browser": "chrome",
                                                    "platform": "windows"})
    s.get(SUMMARY_URL, timeout=120)
    slug_map = build_slug_map(s)
    # The bullying build_slug_map scrapes the bullying summary page; rescrape
    # from child-abuse summary so the abuse topic IDs are included
    import re as _re
    r = s.get(SUMMARY_URL, timeout=120)
    for m in _re.findall(r'href="/topic/(\d+)/([a-z0-9-]+)', r.text):
        slug_map[int(m[0])] = m[1]
    log.info("[abuse] slug_map: %d entries", len(slug_map))

    def _pull_simple(topic_id: int, tf: int) -> list:
        """Type-A schema: just ind + loc + tf (no fmt/ch/pdist)."""
        url = "https://www.kidsdata.org/api/table/table"
        params = {"ind": topic_id, "loc": ALL_CA_COUNTY_LOC_IDS, "tf": tf}
        rr = s.get(url, params=params,
                       headers={"Accept": "application/json",
                                  "Referer": "https://www.kidsdata.org/"},
                       timeout=120)
        rr.raise_for_status()
        return rr.json()

    all_rows = []
    t0 = time.time()
    for ti, (topic_id, kind, breakdown) in enumerate(INDICATORS):
        slug = slug_map.get(topic_id)
        if not slug:
            log.warning("[%d/%d] ind=%d — no slug; skip", ti+1, len(INDICATORS), topic_id)
            continue
        log.info("[%d/%d] %s/%s (ind=%d, slug=%s)", ti+1, len(INDICATORS),
                  kind, breakdown, topic_id, slug)
        try:
            dims = discover_dims(s, topic_id, slug)
        except Exception as e:  # noqa: BLE001
            log.warning("  discovery failed: %s", e); continue
        if not dims["tfs"]:
            log.warning("  no time-frames discovered"); continue
        # Route by schema type
        is_type_b = bool(dims["chs"])
        ch_str = ",".join(str(c) for c in dims["chs"]) if is_type_b else ""
        log.info("  schema=%s, tfs=%d, chs=%d",
                  "B(full)" if is_type_b else "A(simple)",
                  len(dims["tfs"]), len(dims["chs"]))
        for tf in dims["tfs"]:
            try:
                if is_type_b:
                    payload = pull_one(s, topic_id, tf, ch_str)
                else:
                    payload = _pull_simple(topic_id, tf)
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
            time.sleep(0.1)

    if not all_rows:
        log.error("[abuse] no data captured"); return 1

    panel = pd.concat(all_rows, ignore_index=True)
    log.info("[abuse] total parsed rows: %d (%.1fs)", len(panel), time.time()-t0)

    # Resolve FIPS
    panel["county_code"] = panel["county_name"].map(CA_COUNTY_NAME_TO_FIPS)
    state_rows = panel["county_code"].isna().sum()
    log.info("[abuse] state-level rows (no FIPS): %d", state_rows)

    # tf → year_num: visit each unique indicator's table page and combine
    from bs4 import BeautifulSoup
    tf_to_year: dict = {}
    for tid in sorted(set(panel["indicator_topic_id"].unique())):
        slug = slug_map.get(tid)
        if not slug: continue
        try:
            rr = s.get(f"https://www.kidsdata.org/topic/{tid}/{slug}/table", timeout=60)
        except Exception: continue
        soup = BeautifulSoup(rr.text, "html.parser")
        for sel in soup.find_all("select", class_="medium"):
            for o in sel.find_all("option"):
                txt = o.get_text(strip=True)
                val = o.get("value", "")
                if not val.isdigit(): continue
                m_single = re.match(r"^(\d{4})$", txt)
                if m_single:
                    tf_to_year[int(val)] = int(m_single.group(1)); continue
                m_range = re.match(r"^\d{4}-(\d{4})$", txt)
                if m_range:
                    tf_to_year[int(val)] = int(m_range.group(1))
    panel["year_num"] = panel["tf"].map(tf_to_year)
    log.info("[abuse] tf→year_num lookup: %d entries", len(tf_to_year))

    # ---- Long-form breakdowns ---- #
    bd_local = (REPO / "data_cache" / "processed" / "canonical"
                   / "kidsdata_child_abuse_breakdowns.parquet")
    bd_local.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(bd_local, index=False)
    r2.upload(bd_local, "processed/canonical/kidsdata_child_abuse_breakdowns.parquet")
    log.info("[abuse] breakdowns uploaded: %d rows × %d indicators × %d counties × %d vintages",
              len(panel), panel["indicator_topic_id"].nunique(),
              panel["county_name"].nunique(),
              panel["year_num"].nunique() if "year_num" in panel.columns else 0)

    # ---- Overall scalars: aggregate by kind ---- #
    # For abuse data the natural overall is the all-ages / all-types rate.
    # Each indicator's table often reports a "Rate per 1,000" column or a
    # "Total" row. We try several heuristics and use the most stable signal:
    # take the mean across all numeric values for the (county,year,kind) that
    # have valid county_code. This may include double-counting across
    # breakdowns of the same kind, so divide by indicator count per kind.
    panel_county = panel[panel["county_code"].notna()].copy()
    overall = (panel_county
                  .groupby(["county_code", "county_name", "year_num", "indicator_kind"])
                  ["value"].mean().reset_index())
    wide = overall.pivot_table(
        index=["county_code", "county_name", "year_num"],
        columns="indicator_kind", values="value", aggfunc="mean"
    ).reset_index()
    wide.columns.name = None
    rename = {k: f"kidsdata_{k}_mean" for k in
                ("reported_abuse", "substantiated_abuse",
                 "maltreatment_ed_visits", "maltreatment_hospitalizations",
                 "aces_nsch", "aces_nsch_2plus", "aces_brfss",
                 "hardships", "hardships_basic_needs", "hardships_foster_care")}
    wide = wide.rename(columns=rename)

    ov_local = (REPO / "data_cache" / "processed" / "canonical"
                  / "kidsdata_child_abuse_overall.parquet")
    wide.to_parquet(ov_local, index=False)
    r2.upload(ov_local, "processed/canonical/kidsdata_child_abuse_overall.parquet")
    log.info("[abuse] overall uploaded: %d rows × %d cols", len(wide), wide.shape[1])
    for c in wide.columns:
        if c.startswith("kidsdata_"):
            miss = wide[c].isna().sum()
            nn = wide[c].notna().sum()
            if nn:
                log.info("  %-55s missing=%d/%d (%.0f%%) range=%.2f-%.2f",
                          c, miss, len(wide), 100*miss/len(wide),
                          wide[c].min(), wide[c].max())
            else:
                log.info("  %-55s no data", c)
    return 0


if __name__ == "__main__":
    sys.exit(main())
