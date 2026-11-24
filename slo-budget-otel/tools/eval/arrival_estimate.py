#!/usr/bin/env python3
"""The estimated kernel receive time (the caller's send time plus the edge's healthy median, the common truth of all arms) against
the real eBPF stamp, per edge, for the runs that have it (arrival-vs-estimate.json, written by eval_run.py).

    python3 tools/eval/arrival_estimate.py RUN_DIR [RUN_DIR ...] [--md out.md]

One table per scenario.  Per edge (caller > callee [caller host -> callee host]):
- the calls, and the share of them that has the program's stamp;
- estimate minus stamp in healthy traffic (median): the estimate takes the callee's healthy median wait, arrival to handler start, for
  travel, so it is later than the stamp by about that on every call, in the thresholds too;
- the difference around that median (p5 / p50 / p95), healthy and in the window (the fault, or the burst), what the estimate is wrong by
  from call to call, and the share of the calls where it is over 1 ms;
- the wait by the stamp and by the estimate (which starts from 0 in the median), p50 / p95;
- the spans over their threshold b by both / by the estimate only / by the stamp only (each with its own calibration).
With several runs of a scenario the edges' figures are the means over the runs, the counts the sums.
"""
from __future__ import annotations

import argparse
import json
import os


def mean(xs):
    xs = [x for x in xs if isinstance(x, (int, float)) and x == x]
    return sum(xs) / len(xs) if xs else float("nan")


def f(x, nd=2):
    return "-" if x != x else f"{x:.{nd}f}"


def cell(parts, key, qs):
    return " / ".join(f(mean([p[key].get(q) for p in parts if p[key].get("n")])) for q in qs)


def counts(parts):
    return " / ".join(f"{sum(p[k] for p in parts):,}" for k in ("over_by_both", "over_by_the_estimate_only", "over_by_the_stamp_only"))


def row(name, hs, fs):
    n = lambda ps, key="calls": sum(p[key] for p in ps)
    pct = lambda ps: f(100 * n(ps, "stamped") / n(ps)) if n(ps) else "-"
    return (f"| {name} | {n(hs):,} ({pct(hs)} %) / {n(fs):,} ({pct(fs)} %) | {cell(hs, 'estimate_minus_stamp_ms', ('p50',))} | {cell(hs, 'around_it_ms', ('p5', 'p50', 'p95'))} | "
            f"{cell(fs, 'around_it_ms', ('p5', 'p50', 'p95'))} | {f(100 * mean([p['share_off_by_over_1ms'] for p in hs]))} / {f(100 * mean([p['share_off_by_over_1ms'] for p in fs]))} | "
            f"{cell(hs, 'wait_stamp_ms', ('p50', 'p95'))} | {cell(hs, 'wait_estimate_ms', ('p50', 'p95'))} | {cell(fs, 'wait_stamp_ms', ('p50', 'p95'))} | {cell(fs, 'wait_estimate_ms', ('p50', 'p95'))} | "
            f"{counts(fs)} | {counts(hs)} |")


def table(runs: list[dict]) -> list[str]:
    out = ["| edge | calls (stamped) healthy / window | estimate minus stamp, healthy median, ms | around it, healthy, ms (p5 / p50 / p95) | around it, window | over 1 ms off it, healthy / window, % | "
           "wait by the stamp, healthy, ms (p50 / p95) | by the estimate | wait by the stamp, window (p50 / p95) | by the estimate | over b in the window: both / estimate only / stamp only | "
           "healthy: both / estimate only / stamp only |", "|" + "---|" * 12]
    for e in sorted({e for r in runs for e in r["edges"]}):
        ps = [r["edges"][e] for r in runs if e in r["edges"]]
        out.append(row(e, [p["healthy"] for p in ps], [p["window"] for p in ps]))
    out.append(row("**all**", [r["all"]["healthy"] for r in runs], [r["all"]["window"] for r in runs]))
    return out


def report(runs: list[dict]) -> str:
    out = [f"# The estimated kernel receive time against the eBPF stamp, {len(runs)} run" + ("s" if len(runs) > 1 else ""), "",
           f"Stamped by the program: {100 * mean([r['share_stamped'] for r in runs]):.2f} % of {sum(r['calls'] for r in runs):,} calls.  The estimate takes the callee's healthy median "
           "wait for travel: a constant difference per edge, \"estimate minus stamp\", which its thresholds absorb; \"around it\" is what the estimate is wrong by from call to call."]
    for sc in sorted({r["scenario"] for r in runs}):
        rs = [r for r in runs if r["scenario"] == sc]
        cul = sorted({str(r.get("culprit")) for r in rs if r.get("culprit")})
        win = sorted({str(r.get("window")) for r in rs if r.get("window")})
        out += ["", f"## {sc}" + (f", window: the {', '.join(win)}" if win else "") + (f", culprit {', '.join(cul)}" if cul else "") + ": " + ", ".join(r["run_id"] for r in rs), ""] + table(rs)
    svc = sorted({s for r in runs for s in r["thresholds"]})
    out += ["", "| service | threshold b by the estimate, ms (median over its healthy spans) | by the stamp |", "|---|---|---|"]
    out += [f"| {s} | {f(mean([r['thresholds'][s]['b_estimate_ms'] for r in runs if s in r['thresholds']]))} | {f(mean([r['thresholds'][s]['b_stamp_ms'] for r in runs if s in r['thresholds']]))} |" for s in svc]
    return "\n".join(out) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs", nargs="+")
    ap.add_argument("--md")
    a = ap.parse_args()
    runs = []
    for rd in a.runs:
        p = os.path.join(rd, "arrival-vs-estimate.json")
        if not os.path.exists(p):
            print(f"{rd}: no arrival-vs-estimate.json (no stamp in its 100 % export, or not evaluated yet)")
            continue
        with open(p, encoding="utf-8") as fh:
            runs.append(json.load(fh))
    if not runs:
        return 1
    text = report(runs)
    if a.md:
        with open(a.md, "w", encoding="utf-8") as fh:
            fh.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
