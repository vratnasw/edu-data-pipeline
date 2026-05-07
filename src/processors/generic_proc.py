"""Generic processor for source groups whose cleaning is straightforward
(filter to CA, standardize columns). The economic processor has its own
file because the variable-derivation logic is more involved.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
import config.r2_client as r2  # noqa: E402

from processors._base import (  # noqa: E402
    save_processed, standardize_columns, filter_california,
)

log = logging.getLogger(__name__)


def process(source: str, year=None) -> dict:
    raw_key = f"raw/{source}/{year or 'latest'}.parquet"
    info = r2.exists(raw_key)
    if info is None:
        return {"ok": False, "skipped": True, "reason": "raw missing", "key": raw_key}
    df = r2.download(raw_key)
    df = standardize_columns(df)
    df = filter_california(df)
    out_key = save_processed(df, source, year)
    return {"ok": True, "key": out_key, "rows": int(len(df)),
              "cols": int(df.shape[1])}
