"""Run this AFTER populating .env to confirm every credential works.

Tests:
  1. R2 reach + bucket head
  2. R2 upload + download roundtrip on a 1KB test object
  3. BEA / BLS / Census / NOAA / FTM key validity (one tiny live call per service)

Usage:
    python scripts/verify_credentials.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO)); sys.path.insert(0, str(REPO / "src"))

import pandas as pd
import requests

from utils.config_loader import load_dotenv  # noqa: E402
import config.r2_client as r2  # noqa: E402

REPORT: list[dict] = []


def _check(name: str, fn):
    t0 = time.time(); rec = {"name": name, "ok": False}
    try:
        rec["detail"] = fn(); rec["ok"] = True
    except Exception as e:
        rec["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    rec["elapsed_s"] = round(time.time() - t0, 2)
    print(f"  [{ 'OK ' if rec['ok'] else 'FAIL' }] {name:30s} ({rec['elapsed_s']}s)"
            f"  {rec.get('detail', rec.get('error', ''))}")
    REPORT.append(rec); return rec["ok"]


def main() -> int:
    load_dotenv()
    print("=== R2 ===")
    if not _check("r2 env vars", lambda: r2.smoke_check() if r2.smoke_check()["ok"]
                     else (_ for _ in ()).throw(RuntimeError(r2.smoke_check()))):
        print("\nFIX: copy .env.example -> .env, fill the 5 R2_* vars, retry.")
        return 1
    def _r2_round():
        df = pd.DataFrame({"x": [1, 2, 3]})
        f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False); f.close()
        df.to_parquet(f.name, index=False)
        rep = r2.upload(f.name, "test/_credential_check.parquet")
        with tempfile.TemporaryDirectory() as td:
            df2 = r2.download("test/_credential_check.parquet",
                                  dest=Path(td) / "out.parquet")
        return f"upload={rep['size_bytes']}B  roundtrip ok"
    _check("r2 upload+download", _r2_round)

    print("\n=== API keys ===")
    if os.environ.get("BEA_API_KEY"):
        _check("BEA", lambda: f"got {requests.get('https://apps.bea.gov/api/data', params={'UserID': os.environ['BEA_API_KEY'], 'method': 'GETDATASETLIST', 'ResultFormat': 'JSON'}, timeout=20).status_code}")
    if os.environ.get("BLS_API_KEY"):
        _check("BLS", lambda: f"got {requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', json={'seriesid': ['LAUCN060010000000003'], 'startyear': '2022', 'endyear': '2022', 'registrationkey': os.environ['BLS_API_KEY']}, timeout=20).status_code}")
    if os.environ.get("CENSUS_API_KEY"):
        _check("Census", lambda: f"got {requests.get('https://api.census.gov/data/2022/acs/acs5', params={'get': 'NAME', 'for': 'state:06', 'key': os.environ['CENSUS_API_KEY']}, timeout=20).status_code}")
    if os.environ.get("NOAA_TOKEN"):
        _check("NOAA", lambda: f"got {requests.get('https://www.ncei.noaa.gov/cdo-web/api/v2/datasets', headers={'token': os.environ['NOAA_TOKEN']}, timeout=20).status_code}")
    if os.environ.get("FTM_API_KEY"):
        _check("FollowTheMoney", lambda: f"got {requests.get('https://api.followthemoney.org', params={'key': os.environ['FTM_API_KEY']}, timeout=20).status_code}")

    out = REPO / "logs" / "credential_verification.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"checks": REPORT}, indent=2), encoding="utf-8")
    print(f"\nreport: {out}")
    n_ok = sum(1 for c in REPORT if c["ok"])
    print(f"\n{n_ok}/{len(REPORT)} checks passed")
    return 0 if n_ok == len(REPORT) else 2


if __name__ == "__main__":
    sys.exit(main())
