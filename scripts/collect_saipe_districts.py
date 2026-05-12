"""Phase 1 — Census SAIPE school-district poverty.

Source: Census Small Area Income & Poverty Estimates (SAIPE) annual school
district file at:
  https://www2.census.gov/programs-surveys/saipe/datasets/<YEAR>/<YEAR>-school-districts/ussd<YY>.xls

Columns shipped: State Postal, State FIPS, District ID, Name, Estimated Total
Population, Estimated Population 5-17, Children-in-poverty (5-17).

Computed:
  census_saipe_child_poverty_rate = 100 * n_children_in_poverty / pop_5_17
  census_saipe_n_poverty            = n_children_in_poverty
  census_saipe_pop_5_17             = pop_5_17

NOTE: median household income is NOT in this file — it's in the SAIPE
state/county file, which we already cover via Census ACS5 (Track 1.5).

District-to-CDS join: the SAIPE District ID is a 5-digit Census-issued LEAID,
NOT a CDS code. We name-match SAIPE district names to CA district names from
districts.geojson. Unmatched rows are saved with cds=NaN.
"""
from __future__ import annotations

import argparse
import io
import logging
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "src"))

import pandas as pd
import requests

import config.r2_client as r2  # noqa: E402

logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s %(levelname)s :: %(message)s",
                       datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

CA_STATE_FIPS = 6
YEARS = list(range(2010, 2024))  # 2010 .. 2023 inclusive

HEADERS = {"User-Agent": "Mozilla/5.0 edu-data-pipeline"}


def _file_url(year: int) -> str:
    yy = str(year)[-2:]
    return (f"https://www2.census.gov/programs-surveys/saipe/datasets/{year}/"
              f"{year}-school-districts/ussd{yy}.xls")


def fetch_year(year: int) -> pd.DataFrame:
    url = _file_url(year)
    log.info("[saipe %d] GET %s", year, url)
    r = requests.get(url, headers=HEADERS, timeout=180)
    r.raise_for_status()
    df = pd.read_excel(io.BytesIO(r.content), header=2)
    # normalize columns
    df.columns = [str(c).strip() for c in df.columns]
    rename = {}
    for c in df.columns:
        cl = c.lower()
        if "state postal" in cl:           rename[c] = "state_postal"
        elif "state fips" in cl:           rename[c] = "state_fips"
        elif "district id" in cl:          rename[c] = "district_id"
        elif cl == "name":                 rename[c] = "district_name"
        elif "estimated total population" in cl: rename[c] = "pop_total"
        elif "estimated population 5-17" in cl or "estimated population 5 to 17" in cl:
            rename[c] = "pop_5_17"
        elif "in poverty" in cl:           rename[c] = "n_poverty_5_17"
    df = df.rename(columns=rename)
    df = df[df["state_fips"].astype("Int64") == CA_STATE_FIPS].copy()
    df["year_num"] = int(year)
    df["district_id"] = df["district_id"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(5)
    for c in ("pop_total", "pop_5_17", "n_poverty_5_17"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["census_saipe_child_poverty_rate"] = (
        100.0 * df["n_poverty_5_17"] / df["pop_5_17"].replace(0, pd.NA)
    )
    df = df.rename(columns={"n_poverty_5_17": "census_saipe_n_poverty",
                              "pop_5_17": "census_saipe_pop_5_17"})
    cols = ["year_num", "state_fips", "district_id", "district_name",
              "pop_total", "census_saipe_pop_5_17",
              "census_saipe_n_poverty", "census_saipe_child_poverty_rate"]
    return df[cols]


_STRIP_WORDS = (
    "school district", "office of education",
    " unified", " elementary", " high", " joint", " union", " county",
    " city", " school", " district", " public",
)


def normalize_name(s: str) -> str:
    s = str(s).lower().strip()
    # Strip parenthetical annotations like "(State Special Schl)"
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    for w in _STRIP_WORDS:
        s = s.replace(w, " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def build_name_to_cds_map() -> dict:
    """Build a normalized-name → CDS map from districts.geojson.

    Districts geojson has unique CDS codes; we collapse to district-only CDS
    (first 7 digits + zero-pad)."""
    import geopandas as gpd
    geo = REPO.parent / "SFUSD_DATA_ANALYSIS" / "dashboard" / "public" / "data" / "ca" / "geo" / "districts.geojson"
    g = gpd.read_file(geo)
    # district CDS = first 7 chars of full CDS (drop dashes first)
    g["cds_district"] = g["cds"].astype(str).str.replace("-", "").str[:7]
    name_col = "district_name" if "district_name" in g.columns else "name"
    g["_norm"] = g[name_col].fillna("").map(normalize_name)
    # Deduplicate: one CDS per normalized name (keep first)
    return dict(g.dropna(subset=["cds_district"]).groupby("_norm")["cds_district"].first().items())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, default=YEARS[0])
    ap.add_argument("--end", type=int, default=YEARS[-1])
    ap.add_argument("--skip-upload", action="store_true",
                       help="local cache only (no R2)")
    args = ap.parse_args()

    all_rows = []
    for y in range(args.start, args.end + 1):
        try:
            df = fetch_year(y)
        except Exception as e:  # noqa: BLE001
            log.warning("[saipe %d] FAILED: %s", y, e)
            continue
        log.info("[saipe %d] %d CA districts", y, len(df))
        # Upload raw per-year
        if not args.skip_upload:
            from collectors._base import upload_dataframe, make_raw_key
            rep = upload_dataframe(df, make_raw_key("census_saipe_districts", y))
            log.info("[saipe %d] uploaded raw: %s", y, rep.get("key"))
        all_rows.append(df)

    if not all_rows:
        log.error("no years collected — aborting")
        return 1

    panel = pd.concat(all_rows, ignore_index=True)
    log.info("[saipe] total rows: %d  range: %d–%d",
              len(panel), int(panel.year_num.min()), int(panel.year_num.max()))

    # Name-match to CDS
    name_map = build_name_to_cds_map()
    panel["_norm"] = panel["district_name"].fillna("").map(normalize_name)
    panel["cds_district"] = panel["_norm"].map(name_map)
    n_match = int(panel["cds_district"].notna().sum())
    pct_match = 100 * n_match / len(panel)
    log.info("[saipe] name-match: %d/%d rows joined to CDS (%.1f%%)",
              n_match, len(panel), pct_match)

    canon = panel[["year_num", "cds_district", "district_id", "district_name",
                      "census_saipe_pop_5_17", "census_saipe_n_poverty",
                      "census_saipe_child_poverty_rate"]].copy()
    canon = canon.rename(columns={"cds_district": "cds"})

    # Local cache
    out_local = REPO / "data_cache" / "processed" / "canonical" / "census_saipe.parquet"
    out_local.parent.mkdir(parents=True, exist_ok=True)
    canon.to_parquet(out_local, index=False)
    log.info("[saipe] local cache: %s", out_local)

    if not args.skip_upload:
        rep = r2.upload(out_local, "processed/canonical/census_saipe.parquet")
        log.info("[saipe] uploaded canonical: %s (%d rows, %d cols)",
                  rep["key"], len(canon), canon.shape[1])

    # Missingness report
    missing = canon["census_saipe_child_poverty_rate"].isna().sum()
    log.info("[saipe] canonical missingness: child_poverty_rate %d/%d (%.1f%%)",
              missing, len(canon), 100 * missing / len(canon))
    return 0


if __name__ == "__main__":
    sys.exit(main())
