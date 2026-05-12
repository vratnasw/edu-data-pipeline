"""Phase 9 — Opportunity Insights tract-level neighborhood mobility.

Source: OI's 2024 vintage tract_outcomes_late_simple.csv (≈41 MB) at
  https://opportunityinsights.org/wp-content/uploads/2024/08/tract_outcomes_late_simple.csv

Aggregates to county-level for join to the master panel (district-level
aggregation deferred — needs TIGER SD polygon spatial join).

Output columns (canonical, county-level):
  oi_kfr_pooled_p25    : mean rank income at age 35 for kids born to p25 parents
  oi_kfr_pooled_p75    : same for p75 parents
  oi_top20_pooled_p25  : fraction reaching top quintile, p25 parents
  oi_jail_pooled_p25   : incarceration rate, p25 parents (proxy)
"""
from __future__ import annotations

import io
import logging
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "src"))

import pandas as pd
import requests

import config.r2_client as r2  # noqa: E402
from utils.tract_aggregation import aggregate_to_county  # noqa: E402

logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s %(levelname)s :: %(message)s",
                       datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

URL = "https://opportunityinsights.org/wp-content/uploads/2024/08/tract_outcomes_late_simple.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 edu-data-pipeline"}

# Column name candidates (OI files use lowercase with underscores)
COL_CANDIDATES = {
    "oi_kfr_pooled_p25":   ["kfr_pooled_pooled_p25", "kfr_pooled_p25", "kfr_p_p25"],
    "oi_kfr_pooled_p75":   ["kfr_pooled_pooled_p75", "kfr_pooled_p75", "kfr_p_p75"],
    "oi_top20_pooled_p25": ["kfr_top20_pooled_pooled_p25", "top20_pooled_p25"],
    "oi_jail_pooled_p25":  ["jail_pooled_pooled_p25", "jail_pooled_p25"],
}
TRACT_COL_CANDIDATES = ["tract", "geoid", "geoid10", "tract_geoid", "tractfips"]
POP_COL_CANDIDATES = ["count_pooled_pooled", "count_pooled", "n_pooled", "popincens"]


def _find_col(cols: list[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in cols:
            return c
    return None


def main() -> int:
    log.info("[oi] downloading %s (~41 MB)", URL)
    r = requests.get(URL, headers=HEADERS, timeout=600)
    r.raise_for_status()
    log.info("[oi] %.1f MB received", len(r.content) / 1e6)

    # CSV is large but tractable. Stream-read just the columns we need by
    # first inspecting the header.
    head_bytes = r.content[:8192]
    header_line = head_bytes.decode("utf-8", errors="ignore").split("\n", 1)[0]
    cols_avail = [c.strip() for c in header_line.split(",")]
    log.info("[oi] %d columns in source", len(cols_avail))

    # OI uses 3 separate columns: state, county, tract — compose GEOID downstream
    if not all(c in cols_avail for c in ("state", "county", "tract")):
        log.error("[oi] expected state/county/tract triple; got %s", cols_avail[:10])
        return 1

    name_to_src = {
        "oi_kfr_pooled_p25": "kfr_pooled_pooled_p25",
        "oi_jail_pooled_p25": "jail_pooled_pooled_p25",
    }
    pop_col = "pooled_pooled_count"  # OI's denominator for the pooled outcome
    use_cols = ["state", "county", "tract", pop_col, *name_to_src.values()]
    log.info("[oi] reading columns: %s", use_cols)

    df = pd.read_csv(io.BytesIO(r.content), usecols=use_cols, low_memory=False)
    log.info("[oi] loaded %d tract rows", len(df))

    # Compose 11-digit GEOID
    df["tract_geoid"] = (
        df["state"].astype("Int64").astype(str).str.zfill(2)
        + df["county"].astype("Int64").astype(str).str.zfill(3)
        + df["tract"].astype("Int64").astype(str).str.zfill(6)
    )
    df = df[df["state"] == 6].copy()
    log.info("[oi] CA tract rows: %d", len(df))

    # Rename to canonical
    df = df.rename(columns={v: k for k, v in name_to_src.items()})
    tract_col = "tract_geoid"

    # Raw upload (CA only)
    raw_local = REPO / "data_cache" / "raw" / "opportunity_insights" / "tract_outcomes_late_2024.parquet"
    raw_local.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(raw_local, index=False)
    r2.upload(raw_local, "raw/opportunity_insights/tract_outcomes_late_2024_ca.parquet")
    log.info("[oi] raw uploaded")

    # Aggregate to county
    out = aggregate_to_county(df, tract_col=tract_col,
                                  value_cols=list(name_to_src.keys()),
                                  pop_col=pop_col, state_filter="06")

    local = REPO / "data_cache" / "processed" / "canonical" / "opportunity_insights_mobility.parquet"
    local.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(local, index=False)
    r2.upload(local, "processed/canonical/opportunity_insights_mobility.parquet")
    log.info("[oi] canonical uploaded (%d counties, %d cols)", len(out), out.shape[1])

    for c in name_to_src.keys():
        miss = out[c].isna().sum()
        log.info("  %-30s missingness: %d/%d (%.1f%%)  range: %.3f-%.3f",
                  c, miss, len(out), 100 * miss / len(out),
                  out[c].min(), out[c].max())
    return 0


if __name__ == "__main__":
    sys.exit(main())
