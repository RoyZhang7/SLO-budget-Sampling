#!/usr/bin/env python3
"""Oracle T table from a shadow (100 %) export: the simulator's calibration run once on the
complete server spans of a healthy window (q 0.95, square-root split, T_root = SLO), class
agnostic.  Written as route -> service -> seconds, the reservoir arm's ttable_file (the
simulator's reservoir also scores misses against the ground-truth table).

  python3 tools/campaign/oracle_ttable.py <run_dir>/shadow.jsonl[.zst] --out reservoir-ttable.json \
      [--start UNIX] [--end UNIX] [--sim ~/slo-budget-sim] [--callgraph deploy/callgraph.json]
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys

SERVICE_RE = re.compile(r"^(?:unknown_service:)?([A-Za-z0-9_-]+?)(?:_service)?(?:_[a-z]+_es)?(?:_proc)?$")


def norm(name: str) -> str:
    m = SERVICE_RE.match(name)
    return m.group(1).replace("_", "") if m else name


def lines(path: str):
    if path.endswith(".zst"):
        p = subprocess.Popen(["zstd", "-dc", path], stdout=subprocess.PIPE, text=True, bufsize=1 << 20)
        yield from p.stdout
        p.wait()
    else:
        with open(path) as f:
            yield from f


def union(iv):
    if not iv:
        return 0
    iv.sort()
    tot, (cs, ce) = 0, iv[0]
    for s, e in iv[1:]:
        if s > ce:
            tot += ce - cs
            cs, ce = s, e
        elif e > ce:
            ce = e
    return tot + ce - cs


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("shadow")
    ap.add_argument("--out", required=True)
    ap.add_argument("--start", type=float, default=0.0)
    ap.add_argument("--end", type=float, default=1e18)
    ap.add_argument("--sim", default=str(pathlib.Path.home() / "slo-budget-sim"))
    ap.add_argument("--callgraph", default=str(pathlib.Path(__file__).resolve().parents[2] / "deploy" / "callgraph.json"))
    ap.add_argument("--slo-ms", type=float, default=23.4)
    a = ap.parse_args()
    sys.path.insert(0, a.sim)
    from slo_budget_sim.model import CallGraph, Span
    from slo_budget_sim.ttable import Calibrator

    cg = json.load(open(a.callgraph))
    graph = CallGraph({r: s for r, s in cg["root"].items()},
                      {(r, s): list(k) for r, ch in cg["children"].items() for s, k in ch.items()},
                      {(r, s): bool(v) for r, ch in cg.get("parallel", {}).items() for s, v in ch.items()})
    for r, by_svc in (cg.get("stages") or {}).items():      # fails on a simulator whose CallGraph has no stages
        graph.stages.update({(r, s): [list(g) for g in st] for s, st in by_svc.items()})
    spans = {}                     # span id -> (trace, parent, service, kind, start_ns, end_ns, ok, route)
    for line in lines(a.shadow):
        d = json.loads(line)
        for rs in d.get("resourceSpans", []):
            svc = "?"
            for at in rs.get("resource", {}).get("attributes", []):
                if at["key"] == "service.name":
                    svc = norm(at["value"].get("stringValue", "?"))
            for ss in rs.get("scopeSpans", []):
                for s in ss.get("spans", []):
                    route = "compose_post"
                    for at in s.get("attributes", []):
                        if at["key"] == "slo.root":
                            route = at["value"].get("stringValue", route)
                    spans[s["spanId"]] = (s["traceId"], s.get("parentSpanId", ""), svc, s.get("kind", 0),
                                          int(s["startTimeUnixNano"]), int(s["endTimeUnixNano"]),
                                          (s.get("status") or {}).get("code", 0) != 2, route)
    kids: dict[str, list] = {}
    for sid, (tid, par, svc, kind, st, en, ok, route) in spans.items():
        if par and kind == 3:      # client spans under their server span
            kids.setdefault(par, []).append((st, en))
    cal = Calibrator(graph, {"compose_post": a.slo_ms / 1000.0}, q=0.95, window=1, use_class=False)
    n = 0
    for sid, (tid, par, svc, kind, st, en, ok, route) in spans.items():
        if kind != 2 or not (a.start <= en / 1e9 <= a.end):
            continue
        dur = (en - st) / 1e9
        own = max(0.0, dur - union(list(kids.get(sid, []))) / 1e9)
        cal.observe(Span(n, 0, 0, -1 if not par else 0, svc, "h", st / 1e9, dur, ok, 0, route, route + "|any", 0, own=own), 1.0)
        n += 1
    cal.end_period()
    table: dict[str, dict[str, float]] = {}
    for (route, cls, svc), T in cal.table.items():
        if cls == "*":
            table.setdefault(route, {})[svc] = T
    json.dump(table, open(a.out, "w"), indent=1, sort_keys=True)
    print(f"{n} server spans -> {sum(len(v) for v in table.values())} entries:",
          {s: round(T * 1000, 2) for s, T in sorted(table.get("compose_post", {}).items())})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
