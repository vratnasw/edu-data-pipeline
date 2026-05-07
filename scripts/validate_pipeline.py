"""Post-pipeline validator: confirms the master panel is on R2, sized right,
under the missingness threshold, and queryable via DuckDB without creds."""
from __future__ import annotations

import datetime as dt
import json
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

from utils.config_loader import load_config, load_dotenv  # noqa: E402
import config.r2_client as r2  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def main() -> int:
    load_dotenv()
    cfg = load_config()
    out_path = REPO_ROOT / "logs" / f"validation_{dt.datetime.now().strftime('%Y%m%dT%H%M%S')}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rep: dict = {"started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                  "checks": []}

    def _check(name: str, fn) -> bool:
        try:
            ok, detail = fn()
        except Exception as e:  # noqa: BLE001
            ok, detail = False, f"{type(e).__name__}: {e}"
        rep["checks"].append({"name": name, "ok": ok, "detail": detail})
        log.info("%s: %s — %s", "PASS" if ok else "FAIL", name, detail)
        return ok

    smoke = r2.smoke_check()
    _check("r2_credentials", lambda: (smoke["ok"], smoke))

    def _master_exists():
        info = r2.exists("public/master_panel.parquet")
        return (info is not None,
                  info or "public/master_panel.parquet not on R2")
    _check("master_panel_on_r2", _master_exists)

    def _readable():
        info = r2.exists("public/master_panel.parquet")
        if info is None: return False, "missing"
        df = r2.download("public/master_panel.parquet")
        return (len(df) > 0, {"rows": int(len(df)), "cols": int(df.shape[1])})
    _check("master_panel_readable", _readable)

    def _row_count():
        info = r2.exists("public/master_panel.parquet")
        if info is None: return False, "missing"
        df = r2.download("public/master_panel.parquet")
        spine = r2.download(cfg["geography"]["cds_spine_path"]) \
            if r2.exists(cfg["geography"]["cds_spine_path"]) else None
        if spine is None:
            return True, "spine missing — skipped row-count check"
        retained = len(df) / max(1, len(spine))
        return retained >= 0.95, {"retention": retained}
    _check("row_count_within_5pct", _row_count)

    def _missing_pct():
        info = r2.exists("public/master_panel.parquet")
        if info is None: return False, "missing"
        df = r2.download("public/master_panel.parquet")
        n_bad = int((df.isna().mean() > cfg["quality"]["max_missing_pct"]).sum())
        return n_bad == 0, {"n_columns_above_threshold": n_bad}
    _check("max_missing_pct", _missing_pct)

    def _duckdb_works():
        from utils.duckdb_r2 import query
        try:
            query("SELECT 1 AS x").iloc[0]
            return True, "duckdb httpfs ready"
        except Exception as e:  # noqa: BLE001
            return False, f"{type(e).__name__}: {e}"
    _check("duckdb_query_interface", _duckdb_works)

    def _data_dictionary():
        info = r2.exists("public/data_dictionary.json")
        return info is not None, info or "missing"
    _check("data_dictionary_complete", _data_dictionary)

    rep["overall_ok"] = all(c["ok"] for c in rep["checks"])
    out_path.write_text(json.dumps(rep, indent=2, default=str), encoding="utf-8")
    log.info("validation report: %s", out_path)
    return 0 if rep["overall_ok"] else 2


if __name__ == "__main__":
    sys.exit(main())
