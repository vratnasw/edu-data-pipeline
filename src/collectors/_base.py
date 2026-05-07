"""Base contract for collectors.

Every concrete collector MUST expose two functions registered via
@register_collector:
  - collect(year=None, force=False) -> dict
  - check_update(year=None) -> dict      # returns {needs_update: bool, ...}

The dispatcher in scripts/run_pipeline.py walks the registry.
"""
from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pandas as pd

# Make the repo root + config dir importable (so we can pull r2_client)
_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
import config.r2_client as r2  # noqa: E402

log = logging.getLogger(__name__)
DATA_CACHE = _REPO / "data_cache"


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #

@dataclass
class CollectorSpec:
    name: str
    group: str
    collect: Callable
    check_update: Callable
    api_key_env: str | None = None
    extra: dict = field(default_factory=dict)


_REGISTRY: dict[str, CollectorSpec] = {}


def register_collector(name: str, group: str, api_key_env: str | None = None,
                          **extra):
    def deco(fn_collect):
        # Pair with `<name>_check_update` defined in same module if present
        mod = sys.modules[fn_collect.__module__]
        check = getattr(mod, f"{name}_check_update",
                          getattr(mod, "_default_check_update"))
        _REGISTRY[name] = CollectorSpec(name=name, group=group,
                                              collect=fn_collect,
                                              check_update=check,
                                              api_key_env=api_key_env,
                                              extra=extra)
        return fn_collect
    return deco


def list_collectors(group: str | None = None) -> list[CollectorSpec]:
    if group is None:
        return list(_REGISTRY.values())
    return [c for c in _REGISTRY.values() if c.group == group]


def get_collector(name: str) -> CollectorSpec:
    return _REGISTRY[name]


# --------------------------------------------------------------------------- #
# Helpers shared by collectors
# --------------------------------------------------------------------------- #

def has_api_key(env_var: str | None) -> bool:
    return bool(env_var) and bool(os.environ.get(env_var))


def make_raw_key(source_name: str, year: int | str = "latest",
                   suffix: str = "parquet") -> str:
    return f"raw/{source_name}/{year}.{suffix}"


def upload_dataframe(df: pd.DataFrame, key: str) -> dict:
    """Save df as parquet to a temp file, upload to R2, return upload report."""
    DATA_CACHE.mkdir(parents=True, exist_ok=True)
    f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False,
                                          dir=str(DATA_CACHE))
    f.close(); p = Path(f.name)
    df.to_parquet(p, index=False)
    rep = r2.upload(p, key)
    rep.update({"rows": int(len(df)), "cols": int(df.shape[1])})
    return rep


def cache_path(source: str, year: int | str = "latest", suffix: str = "parquet") -> Path:
    """Local cache path; useful for offline development."""
    p = DATA_CACHE / source / f"{year}.{suffix}"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _default_check_update(year: int | str | None = None,
                              source_name: str | None = None) -> dict:
    """Conservative default — always returns needs_update=True if R2 has no
    object yet, False if the object exists and is <30 days old."""
    if source_name is None:
        return {"needs_update": True, "reason": "no source_name passed"}
    key = make_raw_key(source_name, year if year is not None else "latest")
    info = r2.exists(key)
    if info is None:
        return {"needs_update": True, "reason": "not in R2", "key": key}
    # crude staleness gate: 30d
    from datetime import datetime, timezone
    try:
        lm = info["last_modified"]
        age_days = (datetime.now(timezone.utc) - datetime.fromisoformat(
            lm.replace("Z", "+00:00"))).days
    except Exception:
        age_days = 999
    return {"needs_update": age_days > 30, "reason": "age", "age_days": age_days,
              "key": key}
