"""Build the CA wide-feature spine from the Layer 1 SFUSD parquet
and upload to R2 at processed/education/ca_wide_features.parquet.

Uses the same long → wide pivot logic the downstream layers (4-7) consume,
keyed on (cds, year_num) with: caaspp_math/ela_met_pct, suspension_rate_pct,
chronic_absenteeism_rate, graduation_rate_pct + demographics + enrollment.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO)); sys.path.insert(0, str(REPO / "src"))

from utils.config_loader import load_dotenv  # noqa: E402
import config.r2_client as r2  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

SFUSD_PARQUET = (REPO.parent / "SFUSD_DATA_ANALYSIS"
                       / "dashboard" / "public" / "data" / "ca" / "data.parquet")

# Chart-type → outcome mapping (canonical, see ca-ed-data-chart-types memory)
_CT_ELA, _CT_MATH = 46, 57
_CT_SUSP = 210
_CT_ABSENT, _CT_ABSENT_DENOM = 248, 258
_CT_GRAD, _CT_ENROLL = 89, 12
_CT_ELL, _CT_FRPM, _CT_ETHNIC = 26, 25, 33
_CT_FOSTER, _CT_TEACHER = 32, 194


def _classify_district(cds: pd.Series) -> pd.Series:
    cds = cds.astype(str)
    return cds.str.endswith("-0000000") & ~cds.str.slice(3, 8).eq("00000")


def _parse_year(s: pd.Series) -> pd.Series:
    return s.str.split("-", n=1).str[0].astype(int)


def _simple(df: pd.DataFrame, ct: int, name: str,
              labels: list[str] | None = None) -> pd.DataFrame:
    labels = labels or ["Total", "All Students"]
    return (df[(df["chart_type"] == ct) & df["row_label"].isin(labels)]
                [["cds", "year_num", "value"]].rename(columns={"value": name})
                .groupby(["cds", "year_num"], as_index=False, observed=True)[name].mean())


def _ml(df: pd.DataFrame, ct: int, name: str) -> pd.DataFrame:
    m = df["row_label"].isin(["Std Met Level 3", "Std Exceeded Level 4"])
    return (df[(df["chart_type"] == ct) & m]
                .groupby(["cds", "year_num"], observed=True)["value"]
                .sum(min_count=1).reset_index(name=name))


def build_spine(parquet_path: Path) -> pd.DataFrame:
    log.info("loading %s", parquet_path)
    df = pd.read_parquet(parquet_path)
    log.info("raw: %d rows", len(df))
    df = df[_classify_district(df["cds"])].copy()
    df["year_num"] = _parse_year(df["year"])
    log.info("district-only: %d rows", len(df))

    parts = []
    parts.append(_ml(df, _CT_ELA, "caaspp_ela_met_pct"))
    parts.append(_ml(df, _CT_MATH, "caaspp_math_met_pct"))
    parts.append(_simple(df, _CT_SUSP, "suspension_rate_pct", ["Total"]))
    abs_n = (df[(df["chart_type"] == _CT_ABSENT) & (df["row_label"] == "All Students")]
                [["cds", "year_num", "value"]].rename(columns={"value": "_n"}))
    abs_d = (df[(df["chart_type"] == _CT_ABSENT_DENOM) & (df["row_label"] == "All Students")]
                [["cds", "year_num", "value"]].rename(columns={"value": "_d"}))
    abs_df = abs_n.merge(abs_d, on=["cds", "year_num"], how="outer")
    abs_df["chronic_absenteeism_rate"] = np.where(
        abs_df["_d"] > 0, 100.0 * abs_df["_n"] / abs_df["_d"], np.nan)
    parts.append(abs_df[["cds", "year_num", "chronic_absenteeism_rate"]])

    grad_n = (df[(df["chart_type"] == _CT_GRAD) & (df["row_label"] == "All Students")]
                  [["cds", "year_num", "value"]].rename(columns={"value": "_g"}))
    enroll_d = (df[(df["chart_type"] == _CT_ENROLL) & (df["row_label"] == "All Students")]
                    [["cds", "year_num", "value"]].rename(columns={"value": "_e"}))
    grad = grad_n.merge(enroll_d, on=["cds", "year_num"], how="outer")
    grad["graduation_rate_pct"] = np.where(grad["_e"] > 0,
                                                  100.0 * grad["_g"] / grad["_e"], np.nan)
    parts.append(grad[["cds", "year_num", "graduation_rate_pct", "_e"]]
                  .rename(columns={"_e": "enrollment"}))

    parts.append(_simple(df, _CT_FRPM, "pct_frl"))
    parts.append(_simple(df, _CT_ELL, "pct_ell"))
    parts.append(_simple(df, _CT_TEACHER, "pct_experienced_teachers", ["Experienced"]))
    parts.append(_simple(df, _CT_FOSTER, "foster_count"))

    eth_map = {"African American": "pct_black", "Black or African American": "pct_black",
                "Hispanic or Latino": "pct_hispanic", "White": "pct_white",
                "Asian": "pct_asian"}
    for label, col in eth_map.items():
        e = (df[(df["chart_type"] == _CT_ETHNIC) & (df["row_label"].str.strip() == label)]
                  [["cds", "year_num", "value"]].rename(columns={"value": col}))
        if not e.empty: parts.append(e)

    wide = parts[0]
    for p in parts[1:]:
        wide = wide.merge(p, on=["cds", "year_num"], how="outer")
    wide = wide.sort_values(["cds", "year_num"]).reset_index(drop=True)
    wide["county_code"] = wide["cds"].astype(str).str.slice(0, 2).str.zfill(2)
    log.info("spine built: %d rows × %d cols, %d districts, years %d..%d",
              len(wide), wide.shape[1], wide["cds"].nunique(),
              wide["year_num"].min(), wide["year_num"].max())
    return wide


def main() -> int:
    load_dotenv()
    if not SFUSD_PARQUET.exists():
        log.error("SFUSD parquet not found at %s", SFUSD_PARQUET); return 1
    smoke = r2.smoke_check()
    if not smoke["ok"]:
        log.error("R2 not configured: %s", smoke); return 1

    spine = build_spine(SFUSD_PARQUET)
    out = REPO / "data_cache" / "ca_wide_features.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    spine.to_parquet(out, index=False)
    log.info("local: %s (%.1f MB)", out, out.stat().st_size / 1024 / 1024)

    rep = r2.upload(out, "processed/education/ca_wide_features.parquet")
    log.info("uploaded: %s", rep)
    return 0


if __name__ == "__main__":
    sys.exit(main())
