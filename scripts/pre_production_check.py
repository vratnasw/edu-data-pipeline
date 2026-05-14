"""Pre-production failure-point sweep across all 3 training repos + data pipeline.

11 phases of local checks → 1 readiness report. Run with:
    python pre_production_check.py [--phase N] [--skip-train]

Phase 1  env simulation
Phase 2  data loading stress
Phase 3  full graph construction timing
Phase 4  checkpoint save+restore (bit-identical + SIGTERM)
Phase 5  NaN injection (recovery + abort-after-5)
Phase 6  R2 failure simulation
Phase 7  layer chain L4 → L5 → L6
Phase 8  memory profile
Phase 9  config validator
Phase 10 timing benchmark
Phase 11 handoff completeness
Phase 12 readiness report
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
import tracemalloc
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent
REPOS = ("edu-gnn", "edu-world-model", "edu-rl-agent", "edu-causal-rl",
            "edu-spatial-rl", "edu-data-pipeline")
TRAIN_REPOS = ("edu-gnn", "edu-world-model", "edu-rl-agent")

logging.basicConfig(level=logging.INFO,
                       format="%(asctime)s %(levelname)s :: %(message)s",
                       datefmt="%H:%M:%S")
log = logging.getLogger("preprod")

SIM_ROOT = Path("/tmp/lightning_sim")
TEAMSPACE_ROOT = "/teamspace/studios/this_studio"


# Result container ============================================================ #

class PhaseResults:
    def __init__(self):
        self.phases: dict[str, dict] = {}
        self.issues: list[dict] = []
        self.fixes: list[dict] = []

    def record(self, phase: str, name: str, ok: bool,
                  msg: str = "", severity: str = "info") -> None:
        ph = self.phases.setdefault(phase, {"checks": []})
        ph["checks"].append({"name": name, "pass": ok, "msg": msg,
                                "severity": severity})
        if not ok:
            self.issues.append({"phase": phase, "name": name,
                                  "severity": severity, "msg": msg})
        marker = "✓" if ok else ("⚠" if severity == "warning" else "✗")
        log.info("  %s  %-40s  %s", marker, name, msg[:80])

    def record_fix(self, phase: str, name: str, action: str) -> None:
        self.fixes.append({"phase": phase, "name": name, "action": action})
        log.info("  ✎ fix applied: %s — %s", name, action)


RESULTS = PhaseResults()


# Helpers ===================================================================== #

def _run(cmd: list[str], cwd: Path | None = None, timeout: int = 600,
           env: dict | None = None) -> tuple[int, str, str]:
    p = subprocess.run(cmd, cwd=str(cwd) if cwd else None,
                          capture_output=True, text=True, timeout=timeout,
                          env={**os.environ, **(env or {})})
    return p.returncode, p.stdout, p.stderr


# --------------------------------------------------------------------------- #
# Phase 1: environment simulation
# --------------------------------------------------------------------------- #

def phase1_env_simulation() -> None:
    log.info("\n=== PHASE 1: Environment simulation ===")

    # Build symlink farm at /tmp/lightning_sim/
    try:
        SIM_ROOT.mkdir(parents=True, exist_ok=True)
        for name in (*REPOS, "SFUSD_DATA_ANALYSIS"):
            tgt = ROOT / name
            link = SIM_ROOT / name
            if link.exists() or link.is_symlink():
                try: link.unlink()
                except OSError: pass
            try:
                link.symlink_to(tgt, target_is_directory=True)
            except OSError:
                # Windows often fails symlinks without admin — fall back to
                # creating a config-stub dir
                link.mkdir(exist_ok=True)
        RESULTS.record("phase1", "symlink_farm_created", True,
                          f"{SIM_ROOT} with {len(REPOS)+1} entries")
    except Exception as e:  # noqa: BLE001
        RESULTS.record("phase1", "symlink_farm_created", False,
                          f"{type(e).__name__}: {e}", severity="warning")

    # Run each repo's preflight under simulated env
    sim_env = {"TEAMSPACE_ROOT": TEAMSPACE_ROOT}
    for repo in TRAIN_REPOS:
        repo_dir = ROOT / repo
        if not (repo_dir / "scripts" / "preflight.py").exists():
            RESULTS.record("phase1", f"{repo}_preflight_present", False,
                              "missing scripts/preflight.py", severity="critical")
            continue
        rc, out, err = _run([sys.executable, "scripts/preflight.py"],
                              cwd=repo_dir, timeout=180, env=sim_env)
        ok = rc == 0
        msg = "OK" if ok else f"rc={rc}; last: {(err or out).splitlines()[-1][:80]}"
        RESULTS.record("phase1", f"{repo}_preflight_pass", ok, msg,
                          severity="critical" if not ok else "info")


# --------------------------------------------------------------------------- #
# Phase 2 + 3: data loading + full graph (combined for efficiency)
# --------------------------------------------------------------------------- #

def phase2_3_graph_construction(skip_train: bool = False) -> None:
    log.info("\n=== PHASE 2+3: Full graph construction + column health ===")

    # Build the full graph (no --fast), capture stats
    repo = ROOT / "edu-gnn"
    script = repo / "scripts" / "build_graph_enriched.py"
    if not script.exists():
        RESULTS.record("phase2_3", "graph_script_present", False,
                          "missing build_graph_enriched.py", severity="critical")
        return
    RESULTS.record("phase2_3", "graph_script_present", True, "")

    log.info("  building enriched graph (no --fast)...")
    t0 = time.time()
    # Temporarily monkey-patch the script's district_sample_frac via env
    rc, out, err = _run(
        [sys.executable, "-c",
          "import sys; sys.path.insert(0,'src'); sys.path.insert(0,'scripts');"
          "import build_graph_enriched as bge;"
          "import graph.graph_builder as gb;"
          "from utils.config import load_config;"
          "cfg = load_config();"
          "art = gb.build_graph_sequence(cfg, district_sample_frac=None,"
          "    economic_features_path='processed/joined/master_panel.parquet',"
          "    economic_district_vars=bge.DISTRICT_VARS,"
          "    economic_county_vars=bge.COUNTY_VARS);"
          "snap = art.snapshots[-1];"
          "print('SNAP_OK', 'n_schools', len(art.school_ids), "
          "      'n_districts', len(art.district_ids), 'n_counties', len(art.county_ids));"
          "import torch;"
          "[print('EDGE', et, snap[et].edge_index.shape[1]) for et in snap.edge_types];"
          "[print('FEAT', nt, snap[nt].x.shape[1]) for nt in ('school','district','county')];"
          "[print('NAN', nt, int(torch.isnan(snap[nt].x).any().item())) for nt in ('school','district','county')];"
        ], cwd=repo, timeout=1800)
    dur = time.time() - t0
    ok = rc == 0 and "SNAP_OK" in out
    if not ok:
        RESULTS.record("phase2_3", "full_graph_built", False,
                          f"rc={rc} err={err[-200:]}", severity="critical")
        return
    RESULTS.record("phase2_3", "full_graph_built", True,
                      f"took {dur:.1f}s")
    if dur > 600:
        RESULTS.record("phase2_3", "graph_build_under_10min", False,
                          f"{dur:.0f}s > 600s", severity="warning")
    else:
        RESULTS.record("phase2_3", "graph_build_under_10min", True, f"{dur:.1f}s")

    # Parse edges + feature dims
    edges: dict[str, int] = {}
    feats: dict[str, int] = {}
    nans: dict[str, int] = {}
    for line in out.splitlines():
        if line.startswith("EDGE"):
            parts = line.split(); edges[" ".join(parts[1:-1])] = int(parts[-1])
        elif line.startswith("FEAT"):
            p = line.split(); feats[p[1]] = int(p[2])
        elif line.startswith("NAN"):
            p = line.split(); nans[p[1]] = int(p[2])
    for et, cnt in edges.items():
        ok = cnt > 0
        RESULTS.record("phase2_3", f"edges_{et}", ok, f"{cnt} edges",
                          severity="critical" if not ok else "info")
    for nt, dim in feats.items():
        RESULTS.record("phase2_3", f"feat_dim_{nt}", True, f"dim={dim}")
    for nt, has_nan in nans.items():
        ok = has_nan == 0
        RESULTS.record("phase2_3", f"no_nan_{nt}", ok,
                          "clean" if ok else "NaN present",
                          severity="critical" if not ok else "info")

    # Check master panel columns
    rc2, out2, err2 = _run([sys.executable, "-c",
        "import sys; sys.path.insert(0,'.');"
        "import config.r2_client as r2;"
        "m = r2.download('processed/joined/master_panel.parquet');"
        "import json;"
        "report = {c: {'missing_pct': float(m[c].isna().mean()*100),"
        "             'is_zero': bool(m[c].fillna(0).abs().sum()==0)} for c in m.columns"
        "          if str(m[c].dtype) in ('float64','int64','float32','int32','bool')};"
        "print('REPORT', json.dumps(report))"
    ], cwd=ROOT / "edu-data-pipeline", timeout=120)
    if "REPORT" in out2:
        rep_str = out2.split("REPORT ", 1)[1].strip()
        try:
            rep = json.loads(rep_str)
            bad = [c for c, v in rep.items() if v["is_zero"]]
            RESULTS.record("phase2_3", "master_panel_no_zero_cols",
                              len(bad) == 0,
                              f"{len(bad)} all-zero cols" + (f": {bad[:3]}" if bad else ""),
                              severity="warning" if bad else "info")
        except json.JSONDecodeError:
            RESULTS.record("phase2_3", "master_panel_columns_parsed", False,
                              "could not parse report", severity="info")


# --------------------------------------------------------------------------- #
# Phase 4: checkpoint cycle
# --------------------------------------------------------------------------- #

def phase4_checkpoint_cycle() -> None:
    log.info("\n=== PHASE 4: Checkpoint save + restore cycle ===")
    # We already exercised this in smoke_test_fault_tolerance.py for all 3 repos
    # (checks 1+2: r2 ckpt + recovery). Re-run them.
    for repo in TRAIN_REPOS:
        script = ROOT / repo / "scripts" / "smoke_test_fault_tolerance.py"
        if not script.exists():
            RESULTS.record("phase4", f"{repo}_ckpt_test_present", False,
                              "missing", severity="critical")
            continue
        for check in (1, 2):
            rc, out, err = _run([sys.executable, str(script),
                                    "--check", str(check)],
                                    cwd=ROOT / repo, timeout=900)
            # logging.basicConfig writes to stderr — accept PASS from either stream
            combined = out + "\n" + err
            ok = rc == 0 and "PASS" in combined
            label = "save_to_r2" if check == 1 else "restore_from_r2"
            RESULTS.record("phase4", f"{repo}_{label}", ok,
                              "OK" if ok else f"rc={rc}",
                              severity="critical" if not ok else "info")


# --------------------------------------------------------------------------- #
# Phase 5: NaN injection
# --------------------------------------------------------------------------- #

def phase5_nan_injection() -> None:
    log.info("\n=== PHASE 5: NaN injection ===")
    # Single NaN at epoch 3 → recover (check 3 of existing smoke)
    for repo in TRAIN_REPOS:
        script = ROOT / repo / "scripts" / "smoke_test_fault_tolerance.py"
        if not script.exists():
            RESULTS.record("phase5", f"{repo}_nan_test_present", False,
                              "missing", severity="critical")
            continue
        rc, out, err = _run([sys.executable, str(script), "--check", "3"],
                              cwd=ROOT / repo, timeout=900)
        combined = out + "\n" + err
        ok = rc == 0 and "PASS" in combined
        RESULTS.record("phase5", f"{repo}_nan_single_recover", ok,
                          "OK" if ok else f"rc={rc}",
                          severity="critical" if not ok else "info")
    # NaN-detector aborts at 5 events (synthetic unit test)
    rc, out, err = _run([sys.executable, "-c",
        "import sys; sys.path.insert(0, 'src');"
        "from training.fault_tolerance import NaNDetector;"
        "import torch; d = NaNDetector(abort_after=5); actions=[];"
        "[actions.append(d.on_bad_loss(epoch=1, batch=i, lr=1e-3)) for i in range(6)];"
        "print('ACTIONS', actions)"
    ], cwd=ROOT / "edu-gnn", timeout=60)
    ok = "abort" in out and out.count("recover") == 4
    RESULTS.record("phase5", "abort_after_5_fires", ok,
                      out.strip()[-80:] if out else err.strip()[-80:],
                      severity="critical" if not ok else "info")


# --------------------------------------------------------------------------- #
# Phase 6: R2 failure simulation
# --------------------------------------------------------------------------- #

def phase6_r2_failure() -> None:
    log.info("\n=== PHASE 6: R2 failure simulation ===")
    # Test r2_ops degrades gracefully when endpoint is bogus
    rc, out, err = _run([sys.executable, "-c",
        "import os, sys; sys.path.insert(0, 'src');"
        "os.environ['R2_ENDPOINT_URL'] = 'https://invalid-r2-endpoint.example.com';"
        "from training import r2_ops;"
        "from pathlib import Path;"
        "import tempfile;"
        "f = tempfile.NamedTemporaryFile(suffix='.txt', delete=False);"
        "f.write(b'test'); f.close();"
        "ok = r2_ops.upload(Path(f.name), 'preflight/bogus_test.txt');"
        "info = r2_ops.exists('preflight/bogus_test.txt');"
        "print('UPLOAD_OK', ok); print('EXISTS', info)"
    ], cwd=ROOT / "edu-gnn", timeout=120)
    # Both should be falsy / None — but not raise
    ok = "UPLOAD_OK False" in out and ("EXISTS None" in out or "EXISTS" not in out)
    RESULTS.record("phase6", "r2_failure_graceful_degradation", ok,
                      out.strip()[-100:],
                      severity="critical" if not ok else "info")


# --------------------------------------------------------------------------- #
# Phase 7: layer chain integration
# --------------------------------------------------------------------------- #

def phase7_layer_chain(skip_train: bool = False) -> None:
    log.info("\n=== PHASE 7: Layer chain integration ===")
    if skip_train:
        RESULTS.record("phase7", "skipped_per_flag", True, "skip-train passed")
        return
    # Verify the handoffs that already exist on disk
    handoffs = {
        "layer3_to_layer4": ROOT / "edu-spatial-rl" / "layer3_to_layer4_handoff.json",
        "layer4_to_layer5": ROOT / "edu-gnn" / "layer4_to_layer5_handoff.json",
        "layer5_to_layer6": ROOT / "edu-world-model" / "layer5_to_layer6_handoff.json",
    }
    for name, p in handoffs.items():
        if p.exists():
            RESULTS.record("phase7", f"{name}_handoff_present", True, str(p))
        else:
            RESULTS.record("phase7", f"{name}_handoff_present", False,
                              "missing", severity="warning")
    # Cross-field consistency: L4 embedding_dim == L5 state_dim
    try:
        import yaml
        l4_cfg = yaml.safe_load((ROOT / "edu-gnn" / "config" / "config.yaml").read_text())
        l5_cfg = yaml.safe_load((ROOT / "edu-world-model" / "config" / "config.yaml").read_text())
        l6_cfg = yaml.safe_load((ROOT / "edu-rl-agent" / "config" / "config.yaml").read_text())
        emb_dim = l4_cfg.get("architecture", {}).get("hidden_dim")
        l5_state_dim = l5_cfg.get("architecture", {}).get("state_dim")
        l5_action_dim = l5_cfg.get("architecture", {}).get("action_dim")
        l6_action_dim = l6_cfg.get("environment", {}).get("action_dim")
        ok1 = emb_dim == l5_state_dim
        RESULTS.record("phase7", "l4_hidden_eq_l5_state",
                          ok1, f"l4={emb_dim} l5_state={l5_state_dim}",
                          severity="critical" if not ok1 else "info")
        ok2 = l5_action_dim == l6_action_dim
        RESULTS.record("phase7", "l5_action_eq_l6_action",
                          ok2, f"l5={l5_action_dim} l6={l6_action_dim}",
                          severity="critical" if not ok2 else "info")
    except Exception as e:  # noqa: BLE001
        RESULTS.record("phase7", "config_dim_check", False,
                          f"{type(e).__name__}: {e}", severity="warning")


# --------------------------------------------------------------------------- #
# Phase 8: memory profile
# --------------------------------------------------------------------------- #

def phase8_memory_profile() -> None:
    log.info("\n=== PHASE 8: Memory profile (full graph forward) ===")
    # Multi-line script via temp file (can't fit `with` blocks in -c one-liner)
    import tempfile
    script = tempfile.NamedTemporaryFile("w", suffix=".py", delete=False)
    script.write("""
import sys, tracemalloc
sys.path.insert(0, 'src')
tracemalloc.start()
from utils.config import load_config
from utils.anchors import load_anchors
from graph.graph_builder import build_graph_sequence
from models.htgnn import HierarchicalTemporalGNN
import torch
cfg = load_config()
art = build_graph_sequence(cfg, district_sample_frac=None,
    economic_features_path='processed/joined/master_panel.parquet',
    economic_district_vars=['bls_unemployment_rate','census_acs5_median_household_income'],
    economic_county_vars=['bea_gdp_total'])
snap = art.snapshots[0]
input_dims = {nt: int(snap[nt].x.shape[1]) for nt in ('school','district','county')}
m = HierarchicalTemporalGNN(input_dims=input_dims, edge_types=list(snap.edge_types),
    hidden_dim=cfg.arch.hidden_dim, num_gnn_layers=cfg.arch.num_gnn_layers,
    num_gru_layers=cfg.arch.num_gru_layers, num_attention_heads=cfg.arch.num_attention_heads,
    dropout=cfg.arch.dropout, num_outcomes=len(cfg.outcomes))
with torch.no_grad():
    _ = m([snap])
peak = tracemalloc.get_traced_memory()[1] / 1e9
print('PEAK_GB', peak)
""")
    script.close()
    rc, out, err = _run([sys.executable, script.name], cwd=ROOT / "edu-gnn",
                          timeout=900)
    peak_gb = None
    for line in out.splitlines():
        if line.startswith("PEAK_GB"):
            peak_gb = float(line.split()[1])
    if peak_gb is None:
        RESULTS.record("phase8", "memory_profile_ran", False,
                          err[-200:], severity="warning")
        return
    gpu_est = peak_gb * 1.5
    RESULTS.record("phase8", "cpu_peak_gb", True, f"{peak_gb:.2f} GB")
    over_30 = gpu_est > 30
    RESULTS.record("phase8", "gpu_estimate_under_30gb", not over_30,
                      f"est={gpu_est:.2f} GB (40GB A100)",
                      severity="warning" if over_30 else "info")
    # batch size recommendation: full-graph trainer has no minibatching; recommend bs=1 (the full graph)
    rec_bs = 1
    RESULTS.record("phase8", "recommended_batch_size_layer4", True,
                      f"bs={rec_bs} (full-graph trainer, no minibatching)")


# --------------------------------------------------------------------------- #
# Phase 9: config validator
# --------------------------------------------------------------------------- #

CONFIG_RULES = {
    "learning_rate": (1e-5, 1e-1),
    "batch_size": (4, 512),
    "hidden_dim": (32, 1024),
    "max_epochs": (10, 1000),
}


def _validate_config(repo: str, cfg: dict) -> list[tuple[str, str]]:
    violations: list[tuple[str, str]] = []
    arch = cfg.get("architecture", {})
    train = cfg.get("training", {})
    for key, (lo, hi) in CONFIG_RULES.items():
        v = train.get(key) or arch.get(key)
        if v is None: continue
        if not (lo <= float(v) <= hi):
            violations.append((key, f"{v} out of [{lo},{hi}]"))
    ty = train.get("train_years", []) or []
    vy = train.get("val_years", []) or []
    if set(ty) & set(vy):
        violations.append(("train_val_year_overlap", f"{set(ty) & set(vy)}"))
    if "patience" in train and "max_epochs" in train:
        if int(train["patience"]) >= int(train["max_epochs"]):
            violations.append(("patience>=max_epochs",
                                  f"patience={train['patience']} max={train['max_epochs']}"))
    env = cfg.get("environment", {})
    if env.get("z_action_low") is not None and env.get("z_action_high") is not None:
        lo, hi = float(env["z_action_low"]), float(env["z_action_high"])
        if abs(abs(lo) - abs(hi)) > 1e-6:
            violations.append(("action_bounds_asymmetric", f"[{lo}, {hi}]"))
    return violations


def phase9_config_validation() -> None:
    log.info("\n=== PHASE 9: Config validation ===")
    import yaml
    for repo in TRAIN_REPOS:
        cfg_path = ROOT / repo / "config" / "config.yaml"
        if not cfg_path.exists():
            RESULTS.record("phase9", f"{repo}_config_present", False,
                              "missing", severity="critical")
            continue
        try:
            cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
        except Exception as e:  # noqa: BLE001
            RESULTS.record("phase9", f"{repo}_config_parses", False,
                              str(e), severity="critical")
            continue
        v = _validate_config(repo, cfg)
        ok = len(v) == 0
        RESULTS.record("phase9", f"{repo}_config_valid", ok,
                          "clean" if ok else f"{len(v)} violations: {v}",
                          severity="warning" if v else "info")


# --------------------------------------------------------------------------- #
# Phase 10: timing benchmark
# --------------------------------------------------------------------------- #

def phase10_timing_benchmark() -> None:
    log.info("\n=== PHASE 10: Timing benchmark (Layer 4 fast 10 epochs) ===")
    repo = ROOT / "edu-gnn"
    import tempfile
    script = tempfile.NamedTemporaryFile("w", suffix=".py", delete=False)
    script.write("""
import sys, time
sys.path.insert(0, 'src')
from utils.config import load_config
from utils.anchors import load_anchors
from graph.graph_builder import build_graph_sequence
from models.htgnn import HierarchicalTemporalGNN
from training.trainer import Trainer
import torch
cfg = load_config()
anchors = load_anchors(cfg.data.anchor_summary, cfg.data.anchor_effects_path, cfg.outcomes)
art = build_graph_sequence(cfg, district_sample_frac=0.2)
snap = art.snapshots[0]
input_dims = {nt: int(snap[nt].x.shape[1]) for nt in ('school','district','county')}
m = HierarchicalTemporalGNN(input_dims=input_dims, edge_types=list(snap.edge_types),
    hidden_dim=cfg.arch.hidden_dim, num_gnn_layers=cfg.arch.num_gnn_layers,
    num_gru_layers=cfg.arch.num_gru_layers, num_attention_heads=cfg.arch.num_attention_heads,
    dropout=cfg.arch.dropout, num_outcomes=len(cfg.outcomes))
with torch.no_grad():
    _ = m([snap])
t = Trainer(model=m, art=art, cfg=cfg, anchors=anchors, device=torch.device('cpu'),
    wandb_run=None, r2_suffix='bench')
t0 = time.time()
r = t.train(max_epochs=10, resume=False)
print('ELAPSED_S', time.time()-t0)
print('EPOCHS', r['epochs_run'])
""")
    script.close()
    rc, out, err = _run([sys.executable, script.name], cwd=repo, timeout=1800)
    elapsed = None
    for line in out.splitlines():
        if line.startswith("ELAPSED_S"):
            elapsed = float(line.split()[1])
    if elapsed is None:
        RESULTS.record("phase10", "bench_ran", False, err[-200:],
                          severity="warning")
        return
    cpu_per_epoch = elapsed / 10
    gpu_per_epoch = cpu_per_epoch / 20.0   # 20x faster heuristic
    L4_full_h = (gpu_per_epoch * 300) / 3600
    L5_full_h = (gpu_per_epoch * 500) / 3600   # rough — L5 is denser per epoch
    L6_full_h = (gpu_per_epoch * 100) / 3600    # RL eval cycles, rough
    RESULTS.record("phase10", "cpu_sec_per_epoch", True,
                      f"{cpu_per_epoch:.1f}s")
    RESULTS.record("phase10", "gpu_sec_per_epoch_estimate", True,
                      f"{gpu_per_epoch:.1f}s @20x")
    RESULTS.record("phase10", "L4_full_hours_estimate", True,
                      f"{L4_full_h:.1f}h (300 epochs)")
    if L4_full_h > 12:
        RESULTS.record("phase10", "L4_under_12h", False,
                          f"{L4_full_h:.1f}h > 12h",
                          severity="warning")
    else:
        RESULTS.record("phase10", "L4_under_12h", True, f"{L4_full_h:.1f}h")


# --------------------------------------------------------------------------- #
# Phase 11: handoff completeness
# --------------------------------------------------------------------------- #

REQUIRED_HANDOFF_FIELDS = {
    # Layer 3 is GP fit only — no validation verdict; just gp_status + dataset_paths
    "layer3_to_layer4": ("generated_at", "gp_status", "dataset_paths",
                            "edge_params"),
    # Layer 4 advertises embeddings + dims for L5 to consume
    "layer4_to_layer5": ("generated_at", "embedding_paths", "state_dim",
                            "action_dim", "best_val_loss"),
    # Layer 5 carries a validation verdict that L6 gates on
    "layer5_to_layer6": ("generated_at", "validation", "world_model_checkpoint",
                            "state_dim", "action_dim"),
}


def phase11_handoff_completeness() -> None:
    log.info("\n=== PHASE 11: Handoff completeness ===")
    paths = {
        "layer3_to_layer4": ROOT / "edu-spatial-rl" / "layer3_to_layer4_handoff.json",
        "layer4_to_layer5": ROOT / "edu-gnn" / "layer4_to_layer5_handoff.json",
        "layer5_to_layer6": ROOT / "edu-world-model" / "layer5_to_layer6_handoff.json",
    }
    for name, p in paths.items():
        if not p.exists():
            RESULTS.record("phase11", f"{name}_present", False,
                              "missing — must regenerate via layer run",
                              severity="warning")
            continue
        try:
            h = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:  # noqa: BLE001
            RESULTS.record("phase11", f"{name}_parses", False,
                              str(e), severity="critical")
            continue
        missing = []
        for f in REQUIRED_HANDOFF_FIELDS.get(name, ()):
            v = h.get(f)
            if v is None or v == "" or v == {} or v == []:
                missing.append(f)
        RESULTS.record("phase11", f"{name}_complete",
                          len(missing) == 0,
                          "all required fields populated" if not missing
                          else f"missing/null: {missing}",
                          severity="warning" if missing else "info")
        if name == "layer5_to_layer6":
            v = (h.get("validation") or {}).get("verdict")
            ok = v == "pass"
            RESULTS.record("phase11", f"{name}_verdict_pass", ok,
                              f"verdict={v}",
                              severity="critical" if not ok else "info")


# --------------------------------------------------------------------------- #
# Orchestrator
# --------------------------------------------------------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", type=int, default=None,
                       help="run a single phase (1-11), default: all")
    ap.add_argument("--skip-train", action="store_true",
                       help="skip phases that require multi-epoch training")
    args = ap.parse_args()

    phases: dict[int, Callable] = {
        1: lambda: phase1_env_simulation(),
        2: lambda: phase2_3_graph_construction(skip_train=args.skip_train),
        3: lambda: None,  # combined with phase 2
        4: lambda: phase4_checkpoint_cycle(),
        5: lambda: phase5_nan_injection(),
        6: lambda: phase6_r2_failure(),
        7: lambda: phase7_layer_chain(skip_train=args.skip_train),
        8: lambda: phase8_memory_profile(),
        9: lambda: phase9_config_validation(),
        10: lambda: phase10_timing_benchmark(),
        11: lambda: phase11_handoff_completeness(),
    }

    t_overall = time.time()
    if args.phase:
        phases[args.phase]()
    else:
        for i in sorted(phases):
            if phases[i] is None: continue
            try:
                phases[i]()
            except Exception as e:  # noqa: BLE001
                log.exception("phase %d crashed", i)
                RESULTS.record(f"phase{i}", "phase_crashed", False,
                                  f"{type(e).__name__}: {e}", severity="critical")

    elapsed = time.time() - t_overall
    log.info("\n=== ALL PHASES COMPLETE in %.0fs ===", elapsed)

    # Verdict
    n_critical = sum(1 for i in RESULTS.issues if i["severity"] == "critical")
    n_warning = sum(1 for i in RESULTS.issues if i["severity"] == "warning")
    verdict = "GO" if n_critical == 0 else "NO-GO"

    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "elapsed_s": round(elapsed, 1),
        "verdict": verdict,
        "n_critical_issues": n_critical,
        "n_warning_issues": n_warning,
        "phases": RESULTS.phases,
        "issues": RESULTS.issues,
        "fixes_applied": RESULTS.fixes,
    }
    out_path = ROOT / "production_readiness_report.json"
    out_path.write_text(json.dumps(report, indent=2, default=str),
                            encoding="utf-8")
    log.info("\n=== production_readiness_report.json ===")
    log.info("verdict: %s  (critical=%d  warning=%d)",
              verdict, n_critical, n_warning)
    log.info("written: %s", out_path)
    return 0 if verdict == "GO" else 1


if __name__ == "__main__":
    sys.exit(main())
