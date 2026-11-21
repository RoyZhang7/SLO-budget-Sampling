"""The `drift` workload variant: the deployment as it ran during a sampled
cluster run, from the spans the SLO-budget agents kept by the plan (an
unbiased sample of every service, p stamped), outside the fault window.

    python tools/drift_workload.py results/remote/run3_kept_spans.json \
        ../slo-budget-otel/results/cluster-20261120-165437/times.json \
        -o slo_budget_sim/workload_dsbsn_drift.json

Per service: the lognormal fit (median, sigma) of the plan-kept durations.
Leaves (no callees in the fitted call tree) have own time = duration, so the
fit is their own-time distribution.  For a service with callees the shed traces
do not give own time (the children are mostly dropped), so its fitted own-time
median is scaled by the ratio of inclusive medians (run / fitted) and its sigma
is kept.  usertimeline additionally gets the held-out choice among the tailfit
families as its `dist`.  The variant names the fitted workload as its
`reference_variant`: the SLO and the oracle T are those of the workload the
deployment drifted away from, as on the cluster.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from slo_budget_sim import tailfit as tf                    # noqa: E402
from slo_budget_sim.synthetic import ROUTE_TREES, WORKLOAD  # noqa: E402


def _leaves(tree, out: set) -> set:
    svc, kids = tree
    if not kids:
        out.add(svc)
    for k in kids:
        _leaves(k, out)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("kept", help="tools/jaeger_kept_spans.py output of the run's export")
    ap.add_argument("times", help="the run's times.json (hog_start / hog_end are excluded)")
    ap.add_argument("-o", "--out", default="slo_budget_sim/workload_dsbsn_drift.json")
    ap.add_argument("--tail-service", default="usertimeline")
    ap.add_argument("--family", default=None, help="force a tailfit candidate for the tail service")
    a = ap.parse_args(argv)
    kept = json.load(open(a.kept, "r", encoding="utf-8"))
    times = json.load(open(a.times, "r", encoding="utf-8"))
    leaves = set()
    for tree in ROUTE_TREES.values():
        _leaves(tree, leaves)
    wl = json.loads(json.dumps(WORKLOAD))
    fitted_dur = {s: v["dur_p50_ms"] for s, v in WORKLOAD["services"].items()}
    report = []
    for svc, rows in sorted(kept["spans"].items()):
        if svc not in wl["services"]:
            continue
        x = np.array([r[0] for r in rows if r[2] == "plan" and not (times["hog_start"] <= r[3] / 1e6 < times["hog_end"])],
                     dtype=float) / 1000.0
        x = x[x > 0]
        if len(x) < 200:
            report.append(f"  {svc:13s} only {len(x)} plan-kept spans: kept as fitted")
            continue
        fit = tf.fit_lognormal(x)
        e = wl["services"][svc]
        dur_med = float(np.median(x))
        ratio = dur_med / fitted_dur[svc] if fitted_dur.get(svc) else 1.0
        if svc in leaves:
            e["median_ms"], e["sigma"] = round(fit["median_ms"], 4), round(fit["sigma"], 3)
            how = "leaf: own = duration"
        else:
            e["median_ms"] = round(e["median_ms"] * ratio, 4)
            how = f"own median x {ratio:.3f} (inclusive median ratio), sigma kept"
        e["drift_n"] = int(len(x))
        e["dur_p50_ms"], e["dur_p95_ms"] = round(dur_med, 4), round(float(np.quantile(x, 0.95)), 4)
        line = f"  {svc:13s} n={len(x):5d} dur p50 {dur_med:7.3f} ms (fitted {fitted_dur[svc]:7.3f}, x{ratio:.2f})  {how}"
        if svc == a.tail_service:
            res = tf.fit_candidates(x)
            chosen = a.family or res["chosen"]
            spec = res["candidates"][chosen]["spec"]
            e["dist"] = {k: (round(v, 6) if isinstance(v, float) else v) for k, v in spec.items()}
            held = {n: round(c["held_out"]["tail_cll"], 4) for n, c in res["candidates"].items()}
            line += f"\n                dist {chosen} {e['dist']}  held-out tail CLL {held}"
        report.append(line)
    wl["variant"] = "drift"
    wl["reference_variant"] = "fitted"
    wl["timings"] = (WORKLOAD.get("timings", "") + f"; drift: every service's level refit from {Path(a.kept).name} "
                     f"(plan-kept server spans outside the fault window of the Stage 2 cluster run), leaves by lognormal fit, "
                     f"callers' own medians scaled by the inclusive-median ratio; {a.tail_service} with the tailfit "
                     f"held-out family; SLO and oracle from the fitted reference")
    Path(a.out).write_bytes((json.dumps(wl, indent=1) + "\n").encode())
    print("\n".join(report))
    print(f"-> {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
