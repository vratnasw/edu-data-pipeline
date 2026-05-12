"""Phase 2 — CalEnviroScreen 4.0 tract-level environmental burden.

Source: OEHHA Cal EnviroScreen 4.0 results zip at
  https://oehha.ca.gov/media/downloads/calenviroscreen/document/calenviroscreen40resultsdatadictionaryf2021.zip

Extracts the Excel results sheet, normalizes tract FIPS, aggregates to county
level (population-weighted) for join to the master panel.

Output columns (county-level):
  calenviroscreen_score
  calenviroscreen_pollution_burden_score
  calenviroscreen_pop_characteristics_score
  + component scores (PM2.5, ozone, diesel PM, drinking water, lead,
    pesticides, toxic releases, traffic, asthma, cardiovascular,
    educational attainment, housing burden, linguistic isolation,
    poverty, unemployment)
"""
from __future__ import annotations

import io
import logging
import sys
import zipfile
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

URL = ("https://oehha.ca.gov/media/downloads/calenviroscreen/document/"
         "calenviroscreen40resultsdatadictionaryf2021.zip")
HEADERS = {"User-Agent": "Mozilla/5.0 edu-data-pipeline"}

# Component score names → canonical column names (CES4 column names vary;
# we'll match case-insensitive substrings)
COMPONENT_MATCH = {
    "calenviroscreen_score":                    "ces 4.0 score",
    "calenviroscreen_pollution_burden_score":   "pollution burden score",
    "calenviroscreen_pop_characteristics_score": "pop. char. score",
    "calenviroscreen_pm25":                     "pm2.5",
    "calenviroscreen_ozone":                    "ozone",
    "calenviroscreen_diesel_pm":                "diesel pm",
    "calenviroscreen_drinking_water":           "drinking water",
    "calenviroscreen_lead":                     "lead",
    "calenviroscreen_pesticides":               "pesticides",
    "calenviroscreen_tox_releases":             "tox. release",
    "calenviroscreen_traffic":                  "traffic",
    "calenviroscreen_asthma":                   "asthma",
    "calenviroscreen_cardio":                   "cardiovascular",
    "calenviroscreen_education":                "education",
    "calenviroscreen_housing_burden":           "housing burden",
    "calenviroscreen_linguistic_isolation":     "linguistic",
    "calenviroscreen_poverty":                  "poverty",
    "calenviroscreen_unemployment":             "unemployment",
}


def _resolve_columns(cols: list[str]) -> dict:
    """Return {canonical: source_col} based on substring match (case-insens)."""
    out = {}
    lower = {c: str(c).lower() for c in cols}
    for canon, needle in COMPONENT_MATCH.items():
        # Match the score column for each indicator (not _Pctl, _Score variants)
        # Prefer columns ending in just the metric name (not pctl)
        candidates = [c for c, cl in lower.items()
                          if needle in cl and "pctl" not in cl]
        if not candidates:
            continue
        # Prefer shortest matching column
        out[canon] = sorted(candidates, key=len)[0]
    return out


def main() -> int:
    log.info("[ces] downloading %s", URL)
    r = requests.get(URL, headers=HEADERS, timeout=300)
    r.raise_for_status()
    log.info("[ces] %.1f MB received", len(r.content) / 1e6)

    # Open zip, find the Excel file inside
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    xlsx_names = [n for n in zf.namelist() if n.lower().endswith((".xlsx", ".xls"))]
    log.info("[ces] excel files in zip: %s", xlsx_names)
    if not xlsx_names:
        log.error("[ces] no Excel in zip; contents: %s", zf.namelist()[:10])
        return 1
    # Prefer "results" file
    fname = next((n for n in xlsx_names if "results" in n.lower()), xlsx_names[0])
    log.info("[ces] reading %s", fname)
    raw_xlsx = zf.read(fname)

    df = pd.read_excel(io.BytesIO(raw_xlsx), sheet_name=0)
    log.info("[ces] %d rows, %d cols", len(df), df.shape[1])

    # Find tract column (census tract)
    tract_col = None
    for c in df.columns:
        cl = str(c).lower()
        if "tract" in cl or "fips" in cl or "geoid" in cl:
            tract_col = c
            break
    if tract_col is None:
        log.error("[ces] no tract column; cols=%s", list(df.columns)[:20])
        return 1
    log.info("[ces] tract column: %s", tract_col)

    # Find population column
    pop_col = None
    for c in df.columns:
        cl = str(c).lower()
        if "population" in cl or "total pop" in cl:
            pop_col = c
            break
    log.info("[ces] population column: %s", pop_col)

    # Map indicator columns
    col_map = _resolve_columns(list(df.columns))
    log.info("[ces] resolved %d/%d indicators: %s",
              len(col_map), len(COMPONENT_MATCH), col_map)

    # Rename to canonical, normalize tract FIPS to 11-digit
    df_proc = df.rename(columns={v: k for k, v in col_map.items()}).copy()
    df_proc["tract_fips_11"] = (
        df_proc[tract_col].astype(str).str.replace(r"\D", "", regex=True).str.zfill(11)
    )

    # Raw upload (CA tract level)
    raw_local = REPO / "data_cache" / "raw" / "calenviroscreen" / "ces40_2021.parquet"
    raw_local.parent.mkdir(parents=True, exist_ok=True)
    df_proc.to_parquet(raw_local, index=False)
    r2.upload(raw_local, "raw/calenviroscreen/ces40_2021.parquet")
    log.info("[ces] raw uploaded")

    # County aggregation
    value_cols = list(col_map.keys())
    out = aggregate_to_county(df_proc, tract_col="tract_fips_11",
                                  value_cols=value_cols,
                                  pop_col=pop_col, state_filter="06")
    out["calenviroscreen_vintage"] = 2021

    local = REPO / "data_cache" / "processed" / "canonical" / "calenviroscreen.parquet"
    local.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(local, index=False)
    r2.upload(local, "processed/canonical/calenviroscreen.parquet")
    log.info("[ces] canonical uploaded (%d counties, %d cols)", len(out), out.shape[1])

    # Coverage report
    log.info("[ces] county coverage: %d/58 = %.0f%%", len(out), 100 * len(out) / 58)
    for c in value_cols:
        miss = out[c].isna().sum()
        log.info("  %-50s missingness: %d/%d (%.1f%%)",
                  c, miss, len(out), 100 * miss / len(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
