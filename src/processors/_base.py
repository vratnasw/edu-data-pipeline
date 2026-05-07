"""Shared processor utilities: column standardization + numeric range checks."""
from __future__ import annotations

import logging
import re
from pathlib import Path
import sys

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
import config.r2_client as r2  # noqa: E402

log = logging.getLogger(__name__)


def snake_case(s: str) -> str:
    """ColumnName ABC.foo → column_name_abc_foo"""
    s = re.sub(r"[^A-Za-z0-9]+", "_", str(s)).strip("_")
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [snake_case(c) for c in df.columns]
    return df


def fips_pad(s: pd.Series, width: int) -> pd.Series:
    return s.astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(width)


def fiscal_to_yyyy_yy(year_value) -> str:
    """Convert various year formats to 'YYYY-YY' matching the CDE convention."""
    s = str(year_value)
    if "-" in s and len(s.split("-", 1)[0]) == 4:
        return s
    if s.isdigit() and len(s) == 4:
        y = int(s); return f"{y}-{str(y + 1)[-2:]}"
    return s


def validate_numeric(df: pd.DataFrame, col: str,
                       lo: float, hi: float) -> dict:
    """Log a warning + return stats if any value falls outside [lo, hi]."""
    if col not in df.columns: return {"checked": False, "reason": "no col"}
    s = pd.to_numeric(df[col], errors="coerce")
    n_oob = int(((s < lo) | (s > hi)).sum())
    if n_oob > 0:
        log.warning("processor: %s out-of-range %s (lo=%s, hi=%s) n=%d",
                      col, n_oob, lo, hi, n_oob)
    return {"checked": True, "n_oob": n_oob, "n": int(s.notna().sum()),
              "min": float(s.min()) if s.notna().any() else None,
              "max": float(s.max()) if s.notna().any() else None}


def filter_california(df: pd.DataFrame) -> pd.DataFrame:
    """Best-effort CA filter using common state-id columns."""
    cands = ["state", "state_abbr", "stateabbr", "stusab",
              "state_fips", "statefips", "statefp"]
    for c in cands:
        if c in df.columns:
            v = df[c].astype(str).str.upper()
            if v.str.match(r"^06").any():
                return df[v.str.match(r"^06|^CA$|^CALIFORNIA$")].copy()
            if v.str.match(r"^CA$|^CALIFORNIA$").any():
                return df[v.str.match(r"^CA$|^CALIFORNIA$")].copy()
    return df


def save_processed(df: pd.DataFrame, source_name: str, year=None) -> str:
    """Save processed parquet to R2 under processed/<source>/<year>.parquet."""
    import tempfile
    from pathlib import Path
    DATA_CACHE = _REPO / "data_cache"
    DATA_CACHE.mkdir(parents=True, exist_ok=True)
    f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False,
                                          dir=str(DATA_CACHE))
    f.close(); p = Path(f.name)
    df.to_parquet(p, index=False)
    key = f"processed/{source_name}/{year or 'latest'}.parquet"
    r2.upload(p, key)
    return key
