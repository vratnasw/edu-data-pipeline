"""Master orchestrator for collectors → processors → joiners → master panel."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

# Trigger collector registration by importing all 9 collector modules
from collectors import (  # noqa: F401 E402
    economic, health, environment, housing, safety, social, political,
    infrastructure, education,
)
from collectors._base import list_collectors  # noqa: E402
from utils.config_loader import load_dotenv  # noqa: E402
from joblib import Parallel, delayed  # noqa: E402

import config.r2_client as r2  # noqa: E402

GROUPS = ["economic", "health", "environment", "housing", "safety",
            "social", "political", "infrastructure", "education"]

logging.basicConfig(level=logging.INFO,
                      format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
                      datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


def _do_one(spec, force: bool, dry_run: bool) -> dict:
    name = spec.name; group = spec.group
    t0 = time.time()
    rec: dict = {"source": name, "group": group, "started_at":
                  dt.datetime.now(dt.timezone.utc).isoformat()}
    if dry_run:
        check = {}
        try: check = spec.check_update()
        except Exception as e:  # noqa: BLE001
            check = {"error": f"{type(e).__name__}: {e}"}
        rec.update({"action": "dry_run", "check": check, "ok": True})
        return rec
    try:
        out = spec.collect(force=force)
        rec.update({"action": "collect", **out})
    except Exception as e:  # noqa: BLE001
        rec.update({"action": "collect", "ok": False, "error":
                      f"{type(e).__name__}: {e}"})
    rec["duration_s"] = round(time.time() - t0, 2)
    return rec


def main() -> int:
    ap = argparse.ArgumentParser(description="Education data pipeline")
    for g in GROUPS:
        ap.add_argument(f"--{g}", action="store_true")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--force", action="store_true",
                       help="ignore R2 freshness, re-download all")
    ap.add_argument("--dry-run", action="store_true",
                       help="print what would run; touch nothing")
    ap.add_argument("--n-jobs", type=int, default=4)
    ap.add_argument("--skip-joiners", action="store_true")
    args = ap.parse_args()

    load_dotenv()

    selected_groups = ([g for g in GROUPS if getattr(args, g)]
                          if not args.all else GROUPS)
    if not selected_groups:
        log.error("no group selected. pass --all or one of: %s", GROUPS)
        return 1
    log.info("selected groups: %s", selected_groups)

    # ---- Pre-flight: R2 reachability + missing API keys ---- #
    r2_status = r2.smoke_check()
    log.info("R2 smoke: %s", r2_status)

    pipeline_t0 = time.time()
    all_specs = [s for s in list_collectors() if s.group in selected_groups]
    log.info("dispatching %d collectors across %d groups", len(all_specs),
              len(selected_groups))

    # ---- Collectors ---- #
    collect_records = Parallel(n_jobs=args.n_jobs, backend="threading")(
        delayed(_do_one)(s, force=args.force, dry_run=args.dry_run)
        for s in all_specs
    )
    log.info("collectors complete: %d ok / %d total",
              sum(1 for r in collect_records if r.get("ok")),
              len(collect_records))

    process_records: list[dict] = []
    join_records: list[dict] = []

    if not args.dry_run and not args.skip_joiners:
        # ---- Processors ---- #
        from processors.__main__processors import dispatch as _dispatch
        process_records = Parallel(n_jobs=args.n_jobs, backend="threading")(
            delayed(_run_safe)(_dispatch, s.name, None) for s in all_specs
        )

        # ---- Joiners (sequential dependency order) ---- #
        if r2_status.get("ok"):
            from joiners import county_join, tract_spatial_join, proximity_join, master_panel
            for label, fn in [
                ("county_join", county_join.run_county_join),
                ("tract_spatial_join", tract_spatial_join.run_tract_spatial_join),
                ("proximity_join", proximity_join.run_proximity_join),
                ("master_panel", master_panel.run_master_panel),
            ]:
                t0 = time.time()
                try:
                    out = fn()
                    join_records.append({"step": label, "ok": True, "result": out,
                                            "duration_s": round(time.time() - t0, 2)})
                except Exception as e:  # noqa: BLE001
                    join_records.append({"step": label, "ok": False,
                                            "error": f"{type(e).__name__}: {e}",
                                            "duration_s": round(time.time() - t0, 2)})

    pipeline_elapsed = round(time.time() - pipeline_t0, 1)
    report = {
        "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "elapsed_s": pipeline_elapsed,
        "selected_groups": selected_groups,
        "force": args.force, "dry_run": args.dry_run,
        "r2_smoke": r2_status,
        "collectors": collect_records,
        "processors": process_records,
        "joiners": join_records,
        "n_collectors_ok": sum(1 for r in collect_records if r.get("ok")),
        "n_collectors_total": len(collect_records),
    }
    ts = dt.datetime.now().strftime("%Y%m%dT%H%M%S")
    out = REPO_ROOT / "logs" / f"pipeline_run_{ts}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    log.info("pipeline complete in %.1fs — log: %s", pipeline_elapsed, out)
    return 0


def _run_safe(fn, *args, **kwargs) -> dict:
    try:
        return fn(*args, **kwargs)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


if __name__ == "__main__":
    sys.exit(main())
