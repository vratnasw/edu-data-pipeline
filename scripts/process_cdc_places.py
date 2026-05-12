"""Phase 3 — CDC PLACES tract → county aggregation.

The raw CDC PLACES data at raw/cdc_places/latest.parquet is tract-level for
CA, vintage 2023, with 4 health measures:
  DEPRESSION, CASTHMA (asthma), LPA (no leisure-time physical activity), OBESITY

Output: processed/canonical/cdc_places.parquet keyed on county_code (5-digit
FIPS), one row per county, with population-weighted means of each prevalence
measure.

Master-panel joining: the panel has (cds, year_num). CDC PLACES is a single
vintage (2023). The processor broadcasts these to all panel years on join
since health prevalence is reasonably stable over the panel horizon and only
2023 is available from CDC.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "src"))

import pandas as pd

import config.r2_client as r2  # noqa: E402
from utils.tract_aggregation import aggregate_to_county  # noqa: E402

logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s %(levelname)s :: %(message)s",
                       datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

MEASUREID_TO_COL = {
    "DEPRESSION": "cdc_places_depression_prevalence",
    "CASTHMA": "cdc_places_asthma_prevalence",
    "LPA": "cdc_places_phys_inactivity_prevalence",
    "OBESITY": "cdc_places_obesity_prevalence",
}


def main() -> int:
    raw = r2.download("raw/cdc_places/latest.parquet")
    log.info("loaded raw: %d rows, measures=%s", len(raw),
              sorted(raw["measureid"].unique()))

    # Pivot: one row per (locationname=tract, measureid) → one row per tract
    p = raw.pivot_table(index=["locationname", "totalpopulation"],
                            columns="measureid", values="data_value",
                            aggfunc="first").reset_index()
    p = p.rename(columns={"locationname": "tract_fips",
                              "totalpopulation": "pop"})
    keep_cols = [c for c in MEASUREID_TO_COL.keys() if c in p.columns]
    p = p.rename(columns={k: MEASUREID_TO_COL[k] for k in keep_cols})
    log.info("pivoted: %d tracts, value_cols=%s", len(p),
              [MEASUREID_TO_COL[k] for k in keep_cols])

    out = aggregate_to_county(
        p, tract_col="tract_fips",
        value_cols=[MEASUREID_TO_COL[k] for k in keep_cols],
        pop_col="pop", state_filter="06")

    # Add vintage year metadata (single year)
    out["cdc_places_vintage"] = 2023

    # Local + R2
    local = REPO / "data_cache" / "processed" / "canonical" / "cdc_places.parquet"
    local.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(local, index=False)
    rep = r2.upload(local, "processed/canonical/cdc_places.parquet")
    log.info("[cdc_places] uploaded %s (%d counties, %d cols)",
              rep["key"], len(out), out.shape[1])

    # Coverage: CA has 58 counties
    log.info("[cdc_places] county coverage: %d/58 = %.0f%%",
              len(out), 100 * len(out) / 58)
    for c in keep_cols:
        col = MEASUREID_TO_COL[c]
        miss = out[col].isna().sum()
        log.info("  %-50s missingness: %d/%d (%.1f%%)  range: %.1f-%.1f",
                  col, miss, len(out), 100 * miss / len(out),
                  out[col].min(), out[col].max())
    return 0


if __name__ == "__main__":
    sys.exit(main())
