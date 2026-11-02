"""Build aecf_national_summary.md from aecf_pull_progress.json and upload
both to R2 (processed/aecf/)."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import config.r2_client as r2  # noqa: E402

CACHE = REPO / "data_cache"
PROGRESS = CACHE / "aecf_pull_progress.json"
STATE_IDS = CACHE / "aecf_state_ids.json"
SUMMARY = CACHE / "aecf_national_summary.md"


def main():
    if not PROGRESS.exists():
        print(f"No progress file at {PROGRESS}")
        return 1
    p = json.loads(PROGRESS.read_text(encoding="utf-8"))
    state_ids = json.loads(STATE_IDS.read_text(encoding="utf-8"))
    states = p.get("states", {})

    passes = {a: r for a, r in states.items() if r.get("status") == "PASS"}
    fails = {a: r for a, r in states.items() if r.get("status") == "FAIL"}
    other = {a: r for a, r in states.items()
             if r.get("status") not in ("PASS", "FAIL")}

    total_rows = sum(r.get("n_rows") or 0 for r in passes.values())
    total_inds = sum(r.get("n_indicators") or 0 for r in passes.values())

    lines = []
    lines.append("# AECF KIDS COUNT National Pull Summary")
    lines.append("")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Started: {p.get('started_at', 'unknown')}")
    if p.get("chain_aborted_at"):
        lines.append(f"**CHAIN ABORTED**: {p.get('chain_aborted_at')} — "
                     f"{p.get('chain_aborted_reason')}")
    lines.append("")
    lines.append(f"- States processed: {len(states)}/51")
    lines.append(f"- PASS: {len(passes)}")
    lines.append(f"- FAIL: {len(fails)}")
    if other:
        lines.append(f"- OTHER (in-flight/unknown): {len(other)}")
    lines.append(f"- Total rows (passes): {total_rows:,}")
    lines.append(f"- Total indicator-pulls (passes): {total_inds:,}")
    lines.append("")
    lines.append("## Per-state breakdown (PASS)")
    lines.append("")
    lines.append("| State | ID | Indicators | Rows | Locations | Duration (s) |")
    lines.append("|---|---|---|---|---|---|")
    for abbr in sorted(passes.keys()):
        r = passes[abbr]
        sid = r.get("state_id", state_ids.get(abbr, {}).get("state_id", "?"))
        ninds = r.get("n_indicators")
        nrows = r.get("n_rows")
        nlocs = r.get("n_locations")
        dur = r.get("duration_s")
        lines.append(f"| {abbr} | {sid} | "
                     f"{ninds if ninds is not None else '-'} | "
                     f"{nrows if nrows is not None else '-':,} | "
                     f"{nlocs if nlocs is not None else '-'} | "
                     f"{dur if dur is not None else '-'} |"
                     if isinstance(nrows, int)
                     else f"| {abbr} | {sid} | {ninds} | {nrows} | {nlocs} | {dur} |")

    lines.append("")
    lines.append("## Failures")
    lines.append("")
    if not fails:
        lines.append("_None._")
    else:
        for abbr in sorted(fails.keys()):
            r = fails[abbr]
            sid = r.get("state_id", "?")
            lines.append(f"### {abbr} (id={sid})")
            lines.append(f"- duration: {r.get('duration_s', '?')}s")
            err = r.get("error", "")
            lines.append("- error:")
            lines.append("```")
            lines.append(str(err)[:2000])
            lines.append("```")
            tail = r.get("log_excerpt", "")
            if tail:
                lines.append("- log tail:")
                lines.append("```")
                lines.append(tail[:2000])
                lines.append("```")
            lines.append("")
    if other:
        lines.append("## In-flight / unknown")
        lines.append("")
        for abbr in sorted(other.keys()):
            r = other[abbr]
            lines.append(f"- {abbr} (id={r.get('state_id', '?')}): "
                         f"status={r.get('status')}")
    lines.append("")
    lines.append("## Jurisdictions still missing")
    lines.append("")
    missing = [a for a in state_ids if a not in states]
    if missing:
        lines.append(", ".join(sorted(missing)))
    else:
        lines.append("_None — all 51 jurisdictions accounted for._")
    lines.append("")

    SUMMARY.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {SUMMARY}")

    # Upload to R2
    try:
        r2.upload(SUMMARY, "processed/aecf/national_summary.md")
        r2.upload(PROGRESS, "processed/aecf/pull_progress.json")
        print("Uploaded summary + progress JSON to R2")
    except Exception as e:
        print(f"R2 upload failed: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
