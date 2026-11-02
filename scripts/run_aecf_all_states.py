"""Serial driver: for a given ordered list of state abbreviations, invoke
collect_aecf_state.py one at a time, track results in aecf_pull_progress.json,
and skip states already complete (via R2 check + progress JSON).

Usage:
    python scripts/run_aecf_all_states.py --order "VT,WY,AK,ND,SD,RI,MT"
    python scripts/run_aecf_all_states.py --batch smoke
    python scripts/run_aecf_all_states.py --batch major
    python scripts/run_aecf_all_states.py --batch rest

The 60-min per-state cap is enforced via subprocess timeout.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

# Windows console defaults to cp1252; subprocess output contains unicode
# (em-dashes, U+FFFD replacement chars, etc.) that crashes sys.stdout.write.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "src"))

from utils.config_loader import load_dotenv  # noqa: E402
load_dotenv()

import config.r2_client as r2  # noqa: E402

CACHE = REPO / "data_cache"
STATE_IDS_JSON = CACHE / "aecf_state_ids.json"
PROGRESS_JSON = CACHE / "aecf_pull_progress.json"

SMOKE = ["VT", "WY", "AK", "ND", "SD", "RI", "MT"]
MAJOR = ["CA", "TX", "NY", "FL", "PA", "IL"]
# alphabetical for everything else (excluding AL which is already done + smoke + major)
ALL = sorted([
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA",
    "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX",
    "UT", "VT", "VA", "WA", "WV", "WI", "WY",
])
REST = [s for s in ALL if s not in SMOKE and s not in MAJOR and s != "AL"]

PER_STATE_TIMEOUT_S = 60 * 60  # 60 min hard cap


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def load_progress() -> dict:
    if PROGRESS_JSON.exists():
        return json.loads(PROGRESS_JSON.read_text(encoding="utf-8"))
    return {"started_at": now_iso(), "states": {}}


def save_progress(p: dict):
    PROGRESS_JSON.write_text(json.dumps(p, indent=2), encoding="utf-8")


def already_done(abbr: str, progress: dict) -> tuple[bool, str]:
    """Returns (is_done, reason)."""
    rec = progress["states"].get(abbr)
    if rec and rec.get("status") == "PASS":
        return True, "progress_json_PASS"
    # Check R2
    key = f"raw/aecf/{abbr}/all_indicators.parquet"
    info = r2.exists(key)
    if info and info.get("size", 0) > 10_000:
        return True, f"r2_exists_{info['size']}_bytes"
    return False, ""


def parse_collector_stdout(text: str) -> dict:
    """Extract counts from collector output."""
    out = {"n_indicators": None, "n_rows": None, "n_locations": None,
           "loct_breakdown": None}
    # "pulled X data rows from Y indicators (skipped=Z) in N s"
    m = re.search(r"pulled\s+(\d+)\s+data rows from\s+(\d+)\s+indicators", text)
    if m:
        out["n_rows"] = int(m.group(1))
        out["n_indicators"] = int(m.group(2))
    m = re.search(r"unique locations:\s+(\d+)", text)
    if m:
        out["n_locations"] = int(m.group(1))
    m = re.search(r"loct breakdown:\s+(\{[^}]+\})", text)
    if m:
        out["loct_breakdown"] = m.group(1)
    # Also harvest the "uploaded: X rows × Y cols" line
    m = re.search(r"uploaded:\s+(\d+)\s+rows\s+.\s+(\d+)\s+cols", text)
    if m and out["n_rows"] is None:
        out["n_rows"] = int(m.group(1))
    return out


def run_one_state(abbr: str, state_id: int, progress: dict) -> dict:
    """Invoke collect_aecf_state.py for one state. Returns the result dict."""
    rec = {
        "state_id": state_id,
        "started_at": now_iso(),
        "status": "RUNNING",
    }
    progress["states"][abbr] = rec
    save_progress(progress)

    print(f"\n{'='*70}\nSTART {abbr} (id={state_id}) at {rec['started_at']}\n{'='*70}",
          flush=True)
    t0 = time.time()
    log_tail = ""
    try:
        # Stream stdout so we can see progress in real time
        child_env = {**os.environ, "PYTHONIOENCODING": "utf-8",
                     "PYTHONUTF8": "1"}
        proc = subprocess.Popen(
            [sys.executable, str(REPO / "scripts" / "collect_aecf_state.py"),
             "--state-abbr", abbr, "--state-id", str(state_id)],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace",
            cwd=str(REPO), env=child_env,
        )
        captured = []
        try:
            for line in proc.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                captured.append(line)
                if time.time() - t0 > PER_STATE_TIMEOUT_S:
                    proc.kill()
                    rec["status"] = "FAIL"
                    rec["error"] = "exceeded_60min_timeout"
                    rec["duration_s"] = round(time.time() - t0, 1)
                    rec["ended_at"] = now_iso()
                    save_progress(progress)
                    return rec
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            rec["status"] = "FAIL"
            rec["error"] = "subprocess_wait_timeout"
        rc = proc.returncode
        full_log = "".join(captured)
        log_tail = "\n".join(full_log.splitlines()[-30:])
        rec["duration_s"] = round(time.time() - t0, 1)
        rec["ended_at"] = now_iso()
        rec["log_excerpt"] = log_tail
        parsed = parse_collector_stdout(full_log)
        rec.update(parsed)
        if rc == 0:
            # Verify R2 upload completed
            r2_info = r2.exists(f"raw/aecf/{abbr}/all_indicators.parquet")
            if r2_info and r2_info.get("size", 0) > 10_000:
                rec["status"] = "PASS"
                rec["r2_size_bytes"] = r2_info["size"]
            else:
                rec["status"] = "FAIL"
                rec["error"] = (rec.get("error") or
                                f"r2_missing_or_too_small: {r2_info}")
        else:
            rec["status"] = "FAIL"
            rec["error"] = rec.get("error") or f"collector_exit_code_{rc}"
    except Exception as e:
        rec["status"] = "FAIL"
        rec["error"] = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        rec["duration_s"] = round(time.time() - t0, 1)
        rec["ended_at"] = now_iso()
        rec["log_excerpt"] = log_tail

    progress["states"][abbr] = rec
    save_progress(progress)
    print(f"\n{'='*70}\nEND {abbr}: {rec['status']} in {rec.get('duration_s', '?')}s",
          flush=True)
    if rec.get("error"):
        print(f"  error: {rec['error']}", flush=True)
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--order", help="comma-separated state abbrs to process")
    ap.add_argument("--batch", choices=("smoke", "major", "rest", "all"))
    ap.add_argument("--force", action="store_true",
                    help="re-run states already marked PASS / present in R2")
    args = ap.parse_args()

    if args.order:
        order = [s.strip().upper() for s in args.order.split(",") if s.strip()]
    elif args.batch == "smoke":
        order = SMOKE
    elif args.batch == "major":
        order = MAJOR
    elif args.batch == "rest":
        order = REST
    elif args.batch == "all":
        order = SMOKE + MAJOR + REST
    else:
        ap.error("provide --order or --batch")

    state_ids = json.loads(STATE_IDS_JSON.read_text(encoding="utf-8"))
    progress = load_progress()
    if "started_at" not in progress:
        progress["started_at"] = now_iso()

    consecutive_fails = 0
    for abbr in order:
        if abbr not in state_ids:
            print(f"SKIP {abbr}: no state_id in {STATE_IDS_JSON}", flush=True)
            continue
        sid = state_ids[abbr]["state_id"]
        if not args.force:
            done, reason = already_done(abbr, progress)
            if done:
                print(f"SKIP {abbr}: already done ({reason})", flush=True)
                # Make sure progress reflects skip status if not yet recorded
                if abbr not in progress["states"]:
                    progress["states"][abbr] = {
                        "state_id": sid, "status": "PASS",
                        "skipped_reason": reason,
                        "ended_at": now_iso(),
                    }
                    save_progress(progress)
                continue
        rec = run_one_state(abbr, sid, progress)
        if rec["status"] == "FAIL":
            consecutive_fails += 1
            if consecutive_fails >= 5:
                progress["chain_aborted_at"] = now_iso()
                progress["chain_aborted_reason"] = (
                    f"5 consecutive failures ending with {abbr}")
                save_progress(progress)
                print("\nCHAIN ABORTED: 5 consecutive failures", flush=True)
                return 2
        else:
            consecutive_fails = 0

    print("\nALL STATES IN ORDER PROCESSED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
