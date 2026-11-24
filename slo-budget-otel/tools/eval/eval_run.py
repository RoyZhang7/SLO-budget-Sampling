#!/usr/bin/env python3
"""Stage 3: one cluster run directory -> summary.json (the simulator's metrics against the shadow).

    ~/slo-budget-sim/.venv/bin/python3 tools/eval/eval_run.py /storage/royno7/stage3/runs/<run_id> \
        [--sim ~/slo-budget-sim] [--callgraph deploy/callgraph.json] [--recon ~/bin/recon | --no-recon] [--out FILE]

Reads meta.json, shadow.jsonl.zst (or .gz / plain), export.json, gate.jsonl (or the
`capgate period` lines of central.log), central-status.json, telemetry/*.prom and the
collectors' logs; runs tools/recon on the export unless a recon-report.json exists.
See tools/eval/README.md for every metric.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from sloeval.evaluate import evaluate  # noqa: E402


def _clean(o):
    """NaN / inf -> None so the summary is strict JSON."""
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    if isinstance(o, dict):
        return {k: _clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_clean(v) for v in o]
    if hasattr(o, "item"):            # numpy scalars
        return _clean(o.item())
    return o


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run_dir")
    ap.add_argument("--sim", default=os.path.expanduser("~/slo-budget-sim"), help="checkout holding the slo_budget_sim package")
    ap.add_argument("--callgraph", default=os.path.join(HERE, "..", "..", "deploy", "callgraph.json"),
                    help="the truth's call graph, the same for every run whatever its central had (the common truth, *_ws, has the staged one)")
    ap.add_argument("--recon", default=os.path.expanduser("~/bin/recon"), help="tools/recon binary (run when no recon-report.json exists)")
    ap.add_argument("--no-recon", action="store_true")
    ap.add_argument("--out", help="default: <run_dir>/summary.json")
    ap.add_argument("--print", action="store_true", help="also print the summary")
    a = ap.parse_args()
    summary = _clean(evaluate(a.run_dir, a.sim, os.path.abspath(a.callgraph), a.recon, do_recon=not a.no_recon))
    out = a.out or os.path.join(a.run_dir, "summary.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=1, sort_keys=True)
    if a.print:
        print(json.dumps(summary, indent=1, sort_keys=True))
    else:
        keys = ("run_id", "vol_ratio", "outlier_share", "outlier_precision", "evidence_s", "recall_culprit", "identified",
                "attrib_explained", "attrib_loss", "p95_err_svc_mean", "p95_err_svc_max", "agent_cpu_us_per_span", "timing", "peak_rss_mb")
        print(json.dumps({k: summary.get(k) for k in keys}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
