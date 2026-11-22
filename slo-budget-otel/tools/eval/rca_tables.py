#!/usr/bin/env python3
"""Stage 3 root-cause ranking tables from the runs' rca.json (tools/eval/rca_run.py).

    python3 tools/eval/rca_tables.py /storage/royno7/stage3/runs [--volume 0.12] [--md out.md]

Per scenario and volume: first the full data (the 100 % shadow, every run of the scenario pooled),
then per arm and variant (recon, true, true_ref) Acc@1, Acc@3 and MRR of TraceRCA and MicroRank,
and the spans per trace of the arm's normal set (recon and true share the kept spans; true_ref's
normal set is the shadow's).  Headline: RCAEval's own scoring (an operation counts for the
service that emitted the span); for saturated, cascade and cold_start, next to it the version
where a client operation counts for the service it calls (noisy_neighbour: RCAEval's scoring
only).  In each cell the headline counts a run where the method crashed (an operation
with no pre-fault span: KeyError on the normal statistics; no ranking) as a miss; when a run
crashed, the ranked-only numbers follow in parentheses, and the last table counts the crashes per
arm and volume.  noisy_neighbour is scored against the node-level truth, the other scenarios
against the culprit.
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
NAMES = {"tracerca": "TraceRCA", "microrank": "MicroRank"}


def load(root: str) -> list[dict]:
    out = []
    for p in sorted(glob.glob(os.path.join(root, "*", "rca.json"))):
        if os.path.basename(os.path.dirname(p)).startswith(("2d-", "invalid-", "smoke-")):
            continue
        with open(p, encoding="utf-8") as f:
            out.append(json.load(f))
    return out


def crashed(r: dict | None) -> str | None:
    """'no-prefault' (KeyError on the normal statistics), 'other', or None (the method ranked)."""
    if not r or not r.get("error"):
        return None
    return "no-prefault" if str(r["error"]).startswith("KeyError") else "other"


def _scores(pool: list[dict], sfx: str) -> dict | None:
    n = len(pool)
    if not n:
        return None
    return {"acc1": sum(bool(r.get("hit1" + sfx)) for r in pool) / n, "acc3": sum(bool(r.get("hit3" + sfx)) for r in pool) / n,
            "mrr": sum(float(r.get("rr" + sfx) or 0.0) for r in pool) / n}


def acc(results: list[dict], sfx: str = "") -> dict:
    """`all`: every scored run, a crash (no ranking) counting as a miss (the headline);
    `ranked`: only the runs the method ranked."""
    rs = [r for r in results if r]
    ranked = [r for r in rs if crashed(r) is None]
    return {"runs": len(rs), "crashed": len(rs) - len(ranked), "all": _scores(rs, sfx), "ranked": _scores(ranked, sfx)}


def _triple(x: dict) -> str:
    return f"{x['acc1']:.2f} / {x['acc3']:.2f} / {x['mrr']:.2f}"


def fmt(a: dict) -> str:
    if not a.get("runs"):
        return "-"
    s = _triple(a["all"])
    if a["crashed"]:
        s += f" (ranked only: {_triple(a['ranked']) if a['ranked'] else '-'}; {a['crashed']} of {a['runs']} crashed)"
    return s


def cells(vs: list[dict], sfxs: tuple[str, ...]) -> str:
    return " | ".join(fmt(acc([v.get(m) for v in vs], sfx)) for m in METHODS for sfx in sfxs)


def _mean(xs) -> float:
    xs = [x for x in xs if isinstance(x, (int, float)) and math.isfinite(x)]
    return sum(xs) / len(xs) if xs else math.nan


def _vol(r: dict) -> float:
    return float(r["volume_frac"])


def tables(runs: list[dict], volume: float | None) -> str:
    lines = []
    vols = sorted({_vol(r) for r in runs}) if volume is None else [volume]
    for vol in vols:
        for sc in SCENARIOS:
            rs = [r for r in runs if r.get("scenario") == sc and abs(_vol(r) - vol) < 1e-6]
            if not rs:
                continue
            sfxs = ("",) if sc == "noisy_neighbour" else ("", "_callee")
            header = ("| arm | variant | runs | " + " | ".join(f"{NAMES[m]}{', client counts for callee' if sfx else ' (RCAEval scoring)'}"
                                                           for m in METHODS for sfx in sfxs)
                      + " | normal set spans/trace | fault set spans/trace |")
            truth = "node-level truth (a service only on the culprit's node)" if sc == "noisy_neighbour" else "single culprit"
            lines += [f"### {sc.replace('_', '-')}, V* = {vol:.0%}, {truth}", "",
                      "Acc@1 / Acc@3 / MRR over every scored run, a crash counting as a miss; when runs crashed, the "
                      "ranked-only numbers follow in parentheses.", "", header, "|---" * (5 + 2 * len(sfxs)) + "|"]
            sh = [r["variants"].get("shadow") for r in rs if r["variants"].get("shadow")]
            lines.append(f"| (every run) | full data (100 % shadow) | {len(sh)} | {cells(sh, sfxs)} | "
                         f"{_mean([v['normal_set']['spans_per_trace'] for v in sh]):.1f} | "
                         f"{_mean([v['fault_set']['spans_per_trace'] for v in sh]):.1f} |")
            for arm in ARMS:
                ar = [r for r in rs if r.get("arm") == arm]
                if not ar:
                    continue
                own = [r["variants"].get("shadow") for r in ar if r["variants"].get("shadow")]
                lines.append(f"| {arm} | full data of its runs | {len(own)} | {cells(own, sfxs)} | | |")
                for var in ARM_VARIANTS:
                    vs = [r["variants"].get(var) for r in ar if r["variants"].get(var) and not r["variants"][var].get("error")]
                    if not vs:
                        continue
                    lines.append(f"| {arm} | {var} | {len(vs)} | {cells(vs, sfxs)} | "
                                 f"{_mean([v['normal_set']['spans_per_trace'] for v in vs]):.2f} | "
                                 f"{_mean([v['fault_set']['spans_per_trace'] for v in vs]):.2f} |")
            lines.append("")
        # crashes per arm at this volume (every scored scenario together)
        rs = [r for r in runs if abs(_vol(r) - vol) < 1e-6]
        cols = [(m, v) for m in METHODS for v in ("shadow",) + ARM_VARIANTS]
        lines += [f"### Crashes (an operation with no pre-fault span), V* = {vol:.0%}", "",
                  "Crashed runs / scored runs, every scored scenario together; `other` counts any other error.", "",
                  "| arm | " + " | ".join(f"{NAMES[m]} {'full data' if v == 'shadow' else v}" for m, v in cols) + " |",
                  "|---" * (len(cols) + 1) + "|"]
        for arm in ARMS:
            ar = [r for r in rs if r.get("arm") == arm]
            if not ar:
                continue
            row = []
            for m, v in cols:
                res = [r["variants"][v].get(m) for r in ar if r["variants"].get(v) and not r["variants"][v].get("error")]
                kinds = [crashed(x) for x in res]
                c, o = kinds.count("no-prefault"), kinds.count("other")
                row.append(f"{c} / {len(res)}" + (f" (+{o} other)" if o else "") if res else "-")
            lines.append(f"| {arm} | " + " | ".join(row) + " |")
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
