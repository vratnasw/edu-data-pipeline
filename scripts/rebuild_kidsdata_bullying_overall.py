"""Rebuild kidsdata_bullying_overall.parquet from the existing breakdowns
parquet on R2, handling both schema types:

  * binary indicators (bullying, bias): col_label = 'Some'/'None' or
    '<Parent> | Some'/'<Parent> | None'. Affirmative rate = mean of 'Some'.
  * frequency indicators (disability, race, gender, religion,
    sexual_orientation, cyberbullying): col_label = '0 Times' / '1 Time' /
    '2 to 3 Times' / '4 or More Times' (with optional parent). Affirmative
    rate = 100 - mean of '0 Times'.

Output: county-year × 8 bullying types, with one column per kind:
  kidsdata_<kind>_pct_experienced
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import pandas as pd

import config.r2_client as r2  # noqa: E402

logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s %(levelname)s :: %(message)s",
                       datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


def main() -> int:
    b = r2.download("processed/canonical/kidsdata_bullying_breakdowns.parquet")
    log.info("loaded breakdowns: %s", b.shape)

    # Drop state-level rows for the panel (no FIPS)
    b = b[b["county_code"].notna()].copy()

    # Build masks
    binary_aff = (b["col_label"].isin(["Some", "Yes"])
                     | b["col_label"].str.endswith("| Some")
                     | b["col_label"].str.endswith("| Yes"))
    zero_times = (b["col_label"].isin(["0 Times"])
                     | b["col_label"].str.endswith("| 0 Times"))

    # Per (county, year, kind), pick which schema applies
    rows = []
    for keys, g in b.groupby(["county_code", "county_name", "year_num",
                                   "indicator_kind"]):
        bin_g = g[g.index.isin(b[binary_aff].index)]
        zt_g = g[g.index.isin(b[zero_times].index)]
        if len(bin_g) > 0:
            rate = bin_g["value"].mean()
        elif len(zt_g) > 0:
            rate = 100.0 - zt_g["value"].mean()
        else:
            continue
        rows.append({"county_code": keys[0], "county_name": keys[1],
                        "year_num": keys[2], "indicator_kind": keys[3],
                        "rate": rate})
    long = pd.DataFrame(rows)
    log.info("long: %s; kinds covered: %s", long.shape,
              sorted(long["indicator_kind"].unique()))

    wide = long.pivot_table(index=["county_code", "county_name", "year_num"],
                                 columns="indicator_kind", values="rate",
                                 aggfunc="mean").reset_index()
    wide.columns.name = None
    wide = wide.rename(columns={k: f"kidsdata_{k}_pct_experienced"
                                       for k in ("bullying", "bias", "disability",
                                                  "gender", "race", "religion",
                                                  "sexual_orientation", "cyberbullying")})
    log.info("wide: %s cols=%s", wide.shape, list(wide.columns))

    out = REPO / "data_cache" / "processed" / "canonical" / "kidsdata_bullying_overall.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    wide.to_parquet(out, index=False)
    r2.upload(out, "processed/canonical/kidsdata_bullying_overall.parquet")
    log.info("overall uploaded: %d rows × %d cols", len(wide), wide.shape[1])
    # Per-kind missingness
    for c in wide.columns:
        if c.startswith("kidsdata_"):
            miss = wide[c].isna().sum()
            log.info("  %-50s missing=%d/%d (%.1f%%)  range=%.1f-%.1f",
                      c, miss, len(wide), 100*miss/len(wide),
                      wide[c].min(), wide[c].max())
    return 0


if __name__ == "__main__":
    sys.exit(main())
