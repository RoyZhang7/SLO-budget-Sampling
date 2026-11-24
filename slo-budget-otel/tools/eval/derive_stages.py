#!/usr/bin/env python3
"""Derive the sequential stages of every service from complete traces (the runs' 100 % shadows).

    python3 tools/eval/derive_stages.py RUN_DIR [RUN_DIR ...] [--theta 0.99] [--min-spans 1000]
                                        [--merge deploy/callgraph.json --out deploy/callgraph.stages.json] [--json report.json]

A service's calls are the CLIENT spans its server spans opened; the callee is the service of the server span
under the client span.  Over the healthy window of the runs (before the fault or the burst), two callees of a
service are sequential when the first one's client span has ended before the second's starts in at least
--theta of the spans that make both calls, otherwise they overlap.  In the order of their median start a
callee opens a new stage when it is sequential after every callee of the current stage and joins that stage
otherwise; callees within a stage run concurrently, a stage starts when the one before it is done.  The
result is checked: two callees of one stage must not be sequential in --theta of the spans, and callees of
different stages must be.  A service that fails is reported and left out of --out.

The call graph's one `parallel` flag per service (every callee concurrent, or every callee after the other)
cannot say "two groups, one after the other".  --out writes the --merge file with a "stages" key
({route: {service: [[callees], ...]}}), which the stage-aware split (slobudget, slo_budget_sim) uses.
Reads only; needs numpy.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sloeval import runinfo  # noqa: E402
from sloeval.evaluate import healthy_mask  # noqa: E402
from sloeval.spans import SERVER, read_shadow  # noqa: E402

CLIENT = 3


def find(run_dir: str) -> str:
    for nm in ("shadow.jsonl.zst", "shadow.jsonl.gz", "shadow.jsonl"):
        p = os.path.join(run_dir, nm)
        if os.path.exists(p):
            return p
    raise FileNotFoundError(f"no shadow in {run_dir}")


def run_calls(path: str, out: dict) -> None:
    """Add a run's healthy calls to out[(route, service)]: the spans' call intervals, one column per callee."""
    meta = runinfo.load_meta(path)
    t = read_shadow(find(path))
    hm = healthy_mask(meta, t)
    cli = np.flatnonzero((t.kind == CLIENT) & (t.pidx >= 0))
    par = t.pidx[cli]
    ok = (t.kind[par] == SERVER) & hm[par]
    cli, par = cli[ok], par[ok]
    callee = np.full(t.n, -1, dtype=np.int64)  # client span -> service of the server span under it
    srv = np.flatnonzero((t.kind == SERVER) & (t.pidx >= 0))
    srv = srv[t.kind[t.pidx[srv]] == CLIENT]
    callee[t.pidx[srv]] = t.svc[srv]
    caller = t.route[par].astype(np.int64) * len(t.services) + t.svc[par]
    for key in np.unique(caller):
        sel = caller == key
        c, p = cli[sel], par[sel]
        route, name = t.routes[int(key // len(t.services))], t.services[int(key % len(t.services))]
        rec = out.setdefault((route, name), {"spans": 0, "unresolved": 0, "repeats": 0, "parts": []})
        known = callee[c] >= 0
        rec["unresolved"] += int((~known).sum())
        c, p = c[known], p[known]
        if not len(c):
            continue
        parents, row = np.unique(p, return_inverse=True)
        codes, col = np.unique(callee[c], return_inverse=True)
        flat = row * len(codes) + col
        base = t.start[parents]
        size = len(parents) * len(codes)
        first = np.full(size, np.inf)          # a callee called more than once: its first start, its last end
        last = np.full(size, -np.inf)
        np.minimum.at(first, flat, (t.start[c] - base[row]).astype(float))
        np.maximum.at(last, flat, (t.end[c] - base[row]).astype(float))
        seen = np.bincount(flat, minlength=size)
        first[seen == 0] = last[seen == 0] = np.nan
        rec["repeats"] += int((seen > 1).sum())
        rec["spans"] += len(parents)
        shape = (len(parents), len(codes))
        rec["parts"].append(([t.services[k] for k in codes], first.reshape(shape), last.reshape(shape)))


def pool(parts: list) -> tuple[list[str], np.ndarray, np.ndarray]:
    callees = sorted({c for names, _, _ in parts for c in names})
    S, E = [], []
    for names, s, e in parts:
        idx = [names.index(c) if c in names else -1 for c in callees]
        S.append(np.stack([s[:, i] if i >= 0 else np.full(len(s), np.nan) for i in idx], axis=1))
        E.append(np.stack([e[:, i] if i >= 0 else np.full(len(e), np.nan) for i in idx], axis=1))
    return callees, np.concatenate(S), np.concatenate(E)


def derive(parts: list, theta: float) -> dict:
    callees, S, E = pool(parts)
    n = len(callees)
    before = np.full((n, n), np.nan)           # fraction of the spans that make both calls in which i has ended before j starts
    both = np.zeros((n, n), dtype=np.int64)
    for i in range(n):
        for j in range(n):
            if i == j:
                continue
            m = np.isfinite(S[:, i]) & np.isfinite(S[:, j])
            both[i, j] = m.sum()
            if m.any():
                before[i, j] = (E[m, i] <= S[m, j]).mean()
    med = [float(np.nanmedian(S[:, i])) for i in range(n)]
    order = sorted(range(n), key=lambda i: med[i])

    def seq(i, j):
        return both[i, j] > 0 and before[i, j] >= theta

    stages: list[list[int]] = []
    for i in order:
        if stages and all(seq(x, i) for x in stages[-1]):
            stages.append([i])
        elif stages:
            stages[-1].append(i)
        else:
            stages.append([i])
    problems = []
    for a in range(len(stages)):
        for x in range(len(stages[a])):
            for y in stages[a][x + 1:]:
                if seq(stages[a][x], y) or seq(y, stages[a][x]):
                    problems.append(f"{callees[stages[a][x]]} and {callees[y]} are sequential but in one stage")
            for b in range(a + 1, len(stages)):
                for z in stages[b]:
                    if not seq(stages[a][x], z):
                        problems.append(f"{callees[stages[a][x]]} -> {callees[z]}: before in {before[stages[a][x], z]:.4f} of the spans, not a later stage")
    # support: the weakest sequential pair between consecutive stages, the strongest overlap inside a stage
    between, inside = [], []
    for a in range(len(stages) - 1):
        for x in stages[a]:
            for y in stages[a + 1]:
                between.append(before[x, y])
    for st in stages:
        for x in range(len(st)):
            for y in st[x + 1:]:
                inside.append(max(before[st[x], y], before[y, st[x]]))
    gaps = []                                  # ms between the end of one stage and the start of the next
    for a in range(len(stages) - 1):
        both_stages = np.isfinite(E[:, stages[a]]).any(axis=1) & np.isfinite(S[:, stages[a + 1]]).any(axis=1)
        g = (np.nanmin(S[both_stages][:, stages[a + 1]], axis=1) - np.nanmax(E[both_stages][:, stages[a]], axis=1)) / 1e6
        gaps.append([float(np.percentile(g, q)) for q in (1, 50, 99)] if len(g) else [])
    return {"stages": [[callees[i] for i in sorted(st, key=lambda k: callees[k])] for st in stages],
            "median_start_ms": {callees[i]: med[i] / 1e6 for i in range(n)},
            "min_before_between_stages": min(between) if between else None, "max_before_inside_a_stage": max(inside) if inside else None,
            "stage_gap_ms_p1_p50_p99": gaps, "problems": problems, "callees": callees}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs", nargs="+", help="run directories with a 100 %% shadow and meta.json")
    ap.add_argument("--theta", type=float, default=0.99)
    ap.add_argument("--min-spans", type=int, default=1000, help="services with fewer healthy spans are reported but not written")
    ap.add_argument("--merge", help="call graph file the stages are written into")
    ap.add_argument("--out", help="where the merged call graph goes")
    ap.add_argument("--json", help="the full report")
    a = ap.parse_args()
    calls: dict[tuple[str, str], dict] = {}
    for rd in a.runs:
        run_calls(rd, calls)
        print(f"read {os.path.basename(os.path.normpath(rd))}", file=sys.stderr)
    report, stages = {}, {}
    for (route, svc), rec in sorted(calls.items()):
        if not rec["parts"]:
            continue
        d = derive(rec["parts"], a.theta)
        d.update(spans=rec["spans"], unresolved_calls=rec["unresolved"], repeated_calls=rec["repeats"])
        report[f"{route}/{svc}"] = d
        flag = "" if not d["problems"] and rec["spans"] >= a.min_spans else "  <-- not written"
        print(f"{route}/{svc}: {d['stages']}  ({rec['spans']} spans; min before between stages {d['min_before_between_stages']}, "
              f"max before inside a stage {d['max_before_inside_a_stage']}){flag}")
        for p in d["problems"]:
            print("    " + p)
        if not d["problems"] and rec["spans"] >= a.min_spans:
            stages.setdefault(route, {})[svc] = d["stages"]
    if a.json:
        with open(a.json, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=1)
    if a.out:
        with open(a.merge, encoding="utf-8") as f:
            g = json.load(f)
        for route, by_svc in stages.items():
            for svc, st in by_svc.items():
                kids = set((g.get("children") or {}).get(route, {}).get(svc, []))
                named = {c for group in st for c in group}
                if named != kids:
                    print(f"{route}/{svc}: the traces call {sorted(named)}, the file {sorted(kids)}: not written", file=sys.stderr)
                    continue
                g.setdefault("stages", {}).setdefault(route, {})[svc] = st
        with open(a.out, "w", encoding="utf-8") as f:
            json.dump(g, f, indent=1)
            f.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
