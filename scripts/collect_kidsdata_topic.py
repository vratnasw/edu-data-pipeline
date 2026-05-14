"""Generic kidsdata.org topic collector.

Usage:
  python scripts/collect_kidsdata_topic.py \\
    --summary-url https://www.kidsdata.org/topic/95/.../summary \\
    --output-prefix kidsdata_resilience

For any kidsdata summary page:
  1. Scrape /topic/N/<slug>/summary to harvest left-side indicator links
  2. For each indicator, discover its tf + ch dim IDs from its /table page
  3. Route to Type A (ind+loc+tf) or Type B (ind+loc+tf+fmt=873+ch+pdist=33)
  4. Parse multi-level THEAD via colspan grid → long-form (county, year, ...)
  5. Save:
       processed/canonical/<prefix>_breakdowns.parquet (long-form)
       processed/canonical/<prefix>_overall.parquet (pivoted, scalar per kind)
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
sys.path.insert(0, str(REPO / "scripts"))

import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup

from collect_kidsdata_bullying import (
    parse_county_table, ALL_CA_COUNTY_LOC_IDS, CA_COUNTY_NAME_TO_FIPS,
    discover_dims, pull_one,
)
import config.r2_client as r2  # noqa: E402

logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s %(levelname)s :: %(message)s",
                       datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


def harvest_indicators(scraper, summary_url: str) -> list[tuple[int, str, str]]:
    """Returns list of (topic_id, slug, label) for every left-side indicator."""
    r = scraper.get(summary_url, timeout=120)
    inds = re.findall(r'href="(/topic/(\d+)/([a-z0-9-]+))/(?:summary|table|trend)?"[^>]*>([^<]+)<',
                          r.text)
    seen = set(); out = []
    for href, tid, slug, label in inds:
        if href in seen: continue
        seen.add(href)
        out.append((int(tid), slug, label.strip()))
    return out


def kind_breakdown_from_slug(slug: str) -> tuple[str, str]:
    """Derive (kind, breakdown) from a kidsdata slug like 'school-safety-gender'.
    Returns (kind='school_safety', breakdown='gender'). For slugs without a
    breakdown suffix, breakdown='overall'."""
    BREAKDOWN_SUFFIXES = (
        "-gender", "-connectedness", "-parent-education", "-race",
        "-sexual-orientation", "-grade", "-age", "-county", "-type",
        "-income", "-insurance", "-education", "-payment", "-length",
        "-number", "-10k", "-legis", "-adult", "-pces",
        "-anxiety", "-depression", "-suicide-feelings", "-suicide-attempt",
    )
    s = slug
    for suf in sorted(BREAKDOWN_SUFFIXES, key=len, reverse=True):
        if s.endswith(suf):
            return (s[:-len(suf)].replace("-", "_"), suf.lstrip("-").replace("-", "_"))
    return (slug.replace("-", "_"), "overall")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary-url", required=True)
    ap.add_argument("--output-prefix", required=True,
                       help="e.g. kidsdata_school_safety")
    ap.add_argument("--rate-limit-s", type=float, default=0.1)
    args = ap.parse_args()

    s = cloudscraper.create_scraper(browser={"browser": "chrome",
                                                    "platform": "windows"})
    s.get(args.summary_url, timeout=120)
    inds = harvest_indicators(s, args.summary_url)
    log.info("[%s] harvested %d indicators from %s",
              args.output_prefix, len(inds), args.summary_url)

    def _pull_simple(topic_id: int, tf: int) -> list:
        url = "https://www.kidsdata.org/api/table/table"
        rr = s.get(url, params={"ind": topic_id, "loc": ALL_CA_COUNTY_LOC_IDS,
                                       "tf": tf},
                       headers={"Accept": "application/json",
                                  "Referer": "https://www.kidsdata.org/"},
                       timeout=120)
        rr.raise_for_status()
        return rr.json()

    all_rows = []
    t0 = time.time()
    n_skipped = 0
    for ti, (topic_id, slug, label) in enumerate(inds):
        kind, breakdown = kind_breakdown_from_slug(slug)
        log.info("[%d/%d] %s/%s (ind=%d, slug=%s)",
                  ti+1, len(inds), kind, breakdown, topic_id, slug)
        try:
            dims = discover_dims(s, topic_id, slug)
        except Exception as e:  # noqa: BLE001
            log.warning("  discovery failed: %s", e); n_skipped += 1; continue
        if not dims["tfs"]:
            log.warning("  no time-frames discovered"); n_skipped += 1; continue
        is_type_b = bool(dims["chs"])
        ch_str = ",".join(str(c) for c in dims["chs"]) if is_type_b else ""
        for tf in dims["tfs"]:
            try:
                payload = pull_one(s, topic_id, tf, ch_str) if is_type_b \
                            else _pull_simple(topic_id, tf)
            except Exception as e:  # noqa: BLE001
                log.warning("  tf=%d FAILED: %s", tf, e); continue
            if not payload: continue
            for html in payload:
                if not isinstance(html, str): continue
                county, df = parse_county_table(html)
                if df.empty: continue
                df["indicator_topic_id"] = topic_id
                df["indicator_kind"] = kind
                df["indicator_breakdown"] = breakdown
                df["tf"] = tf
                all_rows.append(df)
            time.sleep(args.rate_limit_s)

    if not all_rows:
        log.error("[%s] no data captured", args.output_prefix); return 1

    panel = pd.concat(all_rows, ignore_index=True)
    panel["county_code"] = panel["county_name"].map(CA_COUNTY_NAME_TO_FIPS)
    n_state = panel["county_code"].isna().sum()
    log.info("[%s] total parsed rows: %d (%.0fs)  state-rows: %d  skipped indicators: %d",
              args.output_prefix, len(panel), time.time()-t0, n_state, n_skipped)

    # tf → year_num lookup across all indicators we did pull
    tf_to_year: dict = {}
    seen_tids = sorted(set(panel["indicator_topic_id"].unique()))
    for tid in seen_tids[:30]:
        slug = next((s for (t, s, _) in inds if t == tid), None)
        if not slug: continue
        try:
            rr = s.get(f"https://www.kidsdata.org/topic/{tid}/{slug}/table", timeout=60)
        except Exception: continue
        soup = BeautifulSoup(rr.text, "html.parser")
        for sel in soup.find_all("select", class_="medium"):
            for o in sel.find_all("option"):
                txt = o.get_text(strip=True); val = o.get("value", "")
                if not val.isdigit(): continue
                m1 = re.match(r"^(\d{4})$", txt)
                if m1: tf_to_year[int(val)] = int(m1.group(1)); continue
                m2 = re.match(r"^\d{4}-(\d{4})$", txt)
                if m2: tf_to_year[int(val)] = int(m2.group(1))
    panel["year_num"] = panel["tf"].map(tf_to_year)

    # ---- Breakdowns (long-form) ---- #
    bd_local = (REPO / "data_cache" / "processed" / "canonical"
                   / f"{args.output_prefix}_breakdowns.parquet")
    bd_local.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(bd_local, index=False)
    r2.upload(bd_local, f"processed/canonical/{args.output_prefix}_breakdowns.parquet")
    log.info("[%s] breakdowns uploaded: %d rows × %d indicators × %d counties × %d years",
              args.output_prefix, len(panel),
              panel["indicator_topic_id"].nunique(),
              panel["county_name"].nunique(),
              panel["year_num"].nunique() if panel["year_num"].notna().any() else 0)

    # ---- Overall (per-kind county-year scalar; mean of values, county-only) ---- #
    p_county = panel[panel["county_code"].notna()].copy()
    overall = (p_county
                  .groupby(["county_code", "county_name", "year_num", "indicator_kind"])
                  ["value"].mean().reset_index())
    wide = overall.pivot_table(
        index=["county_code", "county_name", "year_num"],
        columns="indicator_kind", values="value", aggfunc="mean"
    ).reset_index()
    wide.columns.name = None
    rename = {k: f"{args.output_prefix}_{k}_mean" for k in wide.columns
                if k not in ("county_code", "county_name", "year_num")}
    wide = wide.rename(columns=rename)

    ov_local = (REPO / "data_cache" / "processed" / "canonical"
                  / f"{args.output_prefix}_overall.parquet")
    wide.to_parquet(ov_local, index=False)
    r2.upload(ov_local, f"processed/canonical/{args.output_prefix}_overall.parquet")
    log.info("[%s] overall uploaded: %d rows × %d cols", args.output_prefix,
              len(wide), wide.shape[1])

    # Coverage report
    for c in wide.columns:
        if not c.startswith(args.output_prefix): continue
        nn = wide[c].notna().sum()
        if nn:
            log.info("  %-65s n=%d/%d range=%.2f-%.2f",
                      c, nn, len(wide), wide[c].min(), wide[c].max())
    return 0


if __name__ == "__main__":
    sys.exit(main())
