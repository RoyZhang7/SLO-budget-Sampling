#!/usr/bin/env python3
"""Burner calibration (Stage 3): per phase of tools/hog/calibrate.sh, a sample of the phase's
traces from the Jaeger query API (stock collector, 100 % of spans), every service's own time
(duration minus the union of its CLIENT children, as the own-time trigger and truth) against its
own-time share b, the share of requests over the SLO and wrk's non-2xx count.  b comes from the
base phase: the simulator's calibration run once on its server spans, as the evaluation's oracle
on a run's healthy window.  The sizing criterion is the target's own-time P95 / b (Stage 3:
about 2); the other services' own-time P95 against the base phase shows whether the fault stayed
in the target container (placement.txt: the nodes at calibration time).

  ~/slo-budget-sim/.venv/bin/python3 tools/hog/calib_own.py results/calib-<stamp> [--target socialgraph ...]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "eval"))
from sloeval.oracle import build_oracle, import_sim, load_callgraph, lookup_arrays  # noqa: E402
from sloeval.spans import SERVER, own_client, read_export_table  # noqa: E402

SLO_MS = 23.4


def fetch(jaeger: str, t0: float, t1: float, limit: int, path: str):
    if not os.path.exists(path):
        params = {"service": "unknown_service:wrk2api_service_pb_es_proc", "start": int(t0 * 1e6), "end": int(t1 * 1e6), "limit": limit}
        raw = urllib.request.urlopen(f"{jaeger}/api/traces?" + urllib.parse.urlencode(params), timeout=900).read()
        with open(path, "wb") as f:
            f.write(raw)
    return read_export_table(path)


def wrk_counts(path: str) -> tuple[int, int]:
    total = bad = 0
    if os.path.exists(path):
        for line in open(path, errors="replace"):
            if "requests in" in line:
                total = int(line.split()[0])
            if "Non-2xx or 3xx responses:" in line:
                bad = int(line.rsplit(":", 1)[1])
    return total, bad


def placement(run: str) -> dict[str, str]:
    out = {}
    p = os.path.join(run, "placement.txt")
    if os.path.exists(p):
        for line in open(p):
            parts = line.split()
            if len(parts) >= 2 and "-service-" in parts[0]:
                out[parts[0].split("-service-")[0].replace("-", "")] = parts[1]
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run")
    ap.add_argument("--jaeger", default="http://192.168.134.21:16686")
    ap.add_argument("--limit", type=int, default=3000)
    ap.add_argument("--sim", default=os.path.expanduser("~/slo-budget-sim"))
    ap.add_argument("--callgraph", default=os.path.join(HERE, "..", "..", "deploy", "callgraph.json"))
    a = ap.parse_args()
    CallGraph, Calibrator = import_sim(a.sim)
    graph = load_callgraph(os.path.abspath(a.callgraph), CallGraph)
    phases = json.load(open(os.path.join(a.run, "phases.json")))
    place = placement(a.run)
    cal = None
    base: dict[str, float] = {}
    rows = []
    for ph in phases:
        name = ph["phase"]
        tag = name.replace(" ", "_").replace("/", "-")
        t = fetch(a.jaeger, ph["t0"] + 3, ph["t1"], a.limit, os.path.join(a.run, f"traces-{tag}.json"))
        srv = t.kind == SERVER
        if cal is None:          # the first phase is the base: the oracle's b
            cal = build_oracle(t, srv, graph, Calibrator, {"compose_post": SLO_MS / 1000})
        _, B = lookup_arrays(t, cal)
        own = own_client(t) / 1e6
        dur = (t.end - t.start) / 1e6
        roots = srv & t.is_root
        total, bad = wrk_counts(os.path.join(a.run, "wrk-" + name.replace(" ", "_").replace("/", "-") + ".log"))
        row = {"phase": name, "traces": int(t.n_traces), "over_slo": float(np.mean(dur[roots] > SLO_MS)) if roots.any() else float("nan"),
               "root_p95_ms": float(np.percentile(dur[roots], 95)) if roots.any() else float("nan"),
               "requests": total, "non_2xx": bad, "svc": {}}
        for s, svc in enumerate(t.services):
            m = srv & (t.svc == s)
            if m.sum() < 20:
                continue
            b = float(np.nanmedian(B[m])) * 1000
            row["svc"][svc] = {"n": int(m.sum()), "own_p50": float(np.percentile(own[m], 50)), "own_p95": float(np.percentile(own[m], 95)),
                               "dur_p95": float(np.percentile(dur[m], 95)), "b_ms": b,
                               "over_b": float(np.mean(own[m] > B[m] * 1000)), "over_1.1b": float(np.mean(own[m] > 1.1 * B[m] * 1000))}
        if not base:
            base = {svc: v["own_p95"] for svc, v in row["svc"].items()}
        rows.append(row)
    json.dump({"placement": place, "phases": rows}, open(os.path.join(a.run, "calib-own.json"), "w"), indent=1)
    print(f"{'phase':34s} over-SLO  non-2xx   target own p50/p95 ms   b ms  p95/b  own>1.1b   node-mates own p95 x base (max)   others (max)")
    for r in rows:
        parts = r["phase"].split()
        target = parts[1].replace("-", "") if len(parts) > 1 and parts[0] != "base" else None
        line = f"{r['phase']:34s} {100 * r['over_slo']:6.1f} %  {r['non_2xx']:5d}/{r['requests']:<5d}"
        if target and target in r["svc"]:
            v = r["svc"][target]
            line += f"  {target:12s} {v['own_p50']:5.2f}/{v['own_p95']:6.2f}  {v['b_ms']:5.2f}  {v['own_p95'] / v['b_ms']:5.2f}  {v['over_1.1b']:7.3f}"
            node = place.get(target)
            mates = [(s, r["svc"][s]["own_p95"] / base[s]) for s in r["svc"] if s != target and s in base and node and place.get(s) == node]
            others = [(s, r["svc"][s]["own_p95"] / base[s]) for s in r["svc"] if s != target and s in base and (not node or place.get(s) != node)]
            for lab, xs in (("", mates), ("", others)):
                xs.sort(key=lambda kv: -kv[1])
                line += "   " + (" ".join(f"{s}:{x:.2f}" for s, x in xs[:3]) or "-")
        rows_svc = r["svc"]
        if not target:
            line += "  " + " ".join(f"{s}:{v['own_p95']:.2f}/{v['b_ms']:.1f}" for s, v in sorted(rows_svc.items()))
        elif target == "all":     # every service: own-time P95 x base, the four that slowed most
            xs = sorted(((s, v["own_p95"] / base[s]) for s, v in rows_svc.items() if s in base), key=lambda kv: -kv[1])
            line += "  own P95 x base: " + " ".join(f"{s}:{x:.2f}" for s, x in xs[:4])
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
