#!/usr/bin/env python3
"""Stage 3 root-cause ranking tables from the runs' rca.json (tools/eval/rca_run.py).

    python3 tools/eval/rca_tables.py /storage/royno7/stage3/runs [--volume 0.12] [--md out.md]

Per scenario and volume: first the ceiling (the 100 % shadow, every run of the scenario pooled),
then per arm and variant (recon, true, true_ref) Acc@1, Acc@3 and MRR of TraceRCA and MicroRank
over the runs (n; failures = runs where the method raised), and the spans per trace of the arm's
normal set (recon and true share the kept spans; true_ref's normal set is the shadow's).
noisy_neighbour is scored against the node-level truth, the other scenarios against the culprit.
Each method twice: RCAEval's mapping of an operation to a service (the span's own service) and
"client -> callee" (a client operation counts for the service it calls; see sloeval/rca.py).
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os

SCENARIOS = ("saturated", "cascade", "cold_start", "noisy_neighbour")
ARMS = ("chosen", "head", "tail", "reservoir", "canopy", "keep-parent", "threshold")
ARM_VARIANTS = ("recon", "true", "true_ref")
METHODS = ("tracerca", "microrank")


def load(root: str) -> list[dict]:
    out = []
    for p in sorted(glob.glob(os.path.join(root, "*", "rca.json"))):
        if os.path.basename(os.path.dirname(p)).startswith(("2d-", "invalid-", "smoke-")):
            continue
        with open(p, encoding="utf-8") as f:
            out.append(json.load(f))
    return out


def acc(results: list[dict], sfx: str = "") -> dict:
    rs = [r for r in results if r]
    n = len(rs)
    if not n:
        return {"n": 0}
    return {"n": n, "acc1": sum(bool(r.get("hit1" + sfx)) for r in rs) / n, "acc3": sum(bool(r.get("hit3" + sfx)) for r in rs) / n,
            "mrr": sum(float(r.get("rr" + sfx) or 0.0) for r in rs) / n, "failed": sum(1 for r in rs if r.get("error"))}


def cells(vs: list[dict]) -> str:
    return " | ".join(fmt(acc([v.get(m) for v in vs], sfx)) for m in METHODS for sfx in ("", "_callee"))


def fmt(a: dict) -> str:
    if not a.get("n"):
        return "-"
    s = f"{a['acc1']:.2f} / {a['acc3']:.2f} / {a['mrr']:.2f}"
    return s + (f" ({a['failed']} failed)" if a.get("failed") else "")


def _mean(xs) -> float:
    xs = [x for x in xs if isinstance(x, (int, float)) and math.isfinite(x)]
    return sum(xs) / len(xs) if xs else math.nan


def tables(runs: list[dict], volume: float | None) -> str:
    lines = []
    vols = sorted({float(r["volume_frac"]) for r in runs}) if volume is None else [volume]
    for vol in vols:
        for sc in SCENARIOS:
            rs = [r for r in runs if r.get("scenario") == sc and abs(float(r["volume_frac"]) - vol) < 1e-6]
            if not rs:
                continue
            truth = "node-level truth (a service only on the culprit's node)" if sc == "noisy_neighbour" else "single culprit"
            lines += [f"### {sc.replace('_', '-')}, V* = {vol:.0%}, {truth}", "",
                      "Acc@1 / Acc@3 / MRR over runs.", "",
                      "| arm | variant | runs | TraceRCA | TraceRCA, client -> callee | MicroRank | MicroRank, client -> callee "
                      "| normal set spans/trace | fault set spans/trace |",
                      "|---|---|---|---|---|---|---|---|---|"]
            sh = [r["variants"].get("shadow") for r in rs if r["variants"].get("shadow")]
            lines.append(f"| (all: 100 % shadow) | ceiling | {len(sh)} | {cells(sh)} | "
                         f"{_mean([v['normal_set']['spans_per_trace'] for v in sh]):.1f} | "
                         f"{_mean([v['fault_set']['spans_per_trace'] for v in sh]):.1f} |")
            for arm in ARMS:
                ar = [r for r in rs if r.get("arm") == arm]
                if not ar:
                    continue
                own = [r["variants"].get("shadow") for r in ar if r["variants"].get("shadow")]
                lines.append(f"| {arm} | its runs' shadow | {len(own)} | {cells(own)} | | |")
                for var in ARM_VARIANTS:
                    vs = [r["variants"].get(var) for r in ar if r["variants"].get(var) and not r["variants"][var].get("error")]
                    if not vs:
                        continue
                    lines.append(f"| {arm} | {var} | {len(vs)} | {cells(vs)} | "
                                 f"{_mean([v['normal_set']['spans_per_trace'] for v in vs]):.2f} | "
                                 f"{_mean([v['fault_set']['spans_per_trace'] for v in vs]):.2f} |")
            lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs_root")
    ap.add_argument("--volume", type=float)
    ap.add_argument("--md")
    a = ap.parse_args()
    text = tables(load(a.runs_root), a.volume)
    if a.md:
        with open(a.md, "w", encoding="utf-8") as f:
            f.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
