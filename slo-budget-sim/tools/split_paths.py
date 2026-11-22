"""How the square-root split treats parallel branches, and the own-time shares along every
root-to-leaf path against the SLO (Stage 2d A1).

  python tools/split_paths.py [--scenario steady] [--scenario steady+dr] [--seed 1] [--corpus]

The split gives every service a share b_i proportional to sqrt(own-time P95), builds the
inclusive T_i (b_i plus the callees' T: their sum under a sequential node, the largest under a
parallel fan-out) and scales every share by one factor so that T_root = SLO.  The shares are
therefore normalised over the critical structure under T (every callee of a sequential node,
the largest-T callee of a parallel node); parallel siblings each keep their full share.  The
normaliser W the code divides by first (over the longest-by-L callees) cancels in that scaling.

For each route and class key of the oracle table (q 0.95, class key) built on the scenario's
healthy reference (or, with --corpus, on the scenario's own corpus): the critical structure,
the sum of its shares over the SLO, the sum of all shares over the SLO, the largest
root-to-leaf share sum over the SLO and its path, and an independent re-implementation of the
split with the normaliser over every service (the shares it gives against the table's).
"""
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from slo_budget_sim.synthetic import build_scenario  # noqa: E402
from slo_budget_sim.ttable import EPS_OWN, oracle_table, own_from_inclusive  # noqa: E402


def paths(graph, route, s, acc=()):
    kids = graph.children.get((route, s), [])
    if not kids:
        yield acc + (s,)
    for c in kids:
        yield from paths(graph, route, c, acc + (s,))


def resplit(slo, own, graph, route):
    """The split with b ~ sqrt(own) normalised over every service, then scaled to T_root = SLO."""
    w = sum(math.sqrt(v) for v in own.values())
    b = {s: math.sqrt(v) / w for s, v in own.items()}
    T = {}

    def t(s):
        if s not in T:
            ks = [c for c in graph.children.get((route, s), []) if c in own and c != s]
            sub = [t(c) for c in ks]
            T[s] = b[s] + ((max(sub) if graph.parallel.get((route, s), False) else sum(sub)) if sub else 0.0)
        return T[s]

    scale = slo / t(graph.root[route])
    return {s: v * scale for s, v in b.items()}, {s: v * scale for s, v in T.items()}


def critical(graph, route, T):
    out, stack = [], [graph.root[route]]
    while stack:
        s = stack.pop()
        out.append(s)
        ks = [c for c in graph.children.get((route, s), []) if c in T and c != s]
        if ks:
            stack.extend([max(ks, key=lambda c: T[c])] if graph.parallel.get((route, s), False) else ks)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scenario", action="append")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--corpus", action="store_true",
                    help="build the table on the scenario's own corpus (e.g. the drifted deployment of steady+dr) "
                         "instead of its healthy reference (the drift variant's reference is the fitted workload)")
    a = ap.parse_args()
    for sc in a.scenario or ["steady", "steady+dr"]:
        corpus, ref = build_scenario(sc, seed=a.seed)
        if a.corpus:
            corpus.routes_slo = dict(ref.routes_slo)
            ref = corpus
        o = oracle_table(ref, q=0.95, use_class=True)
        g = ref.graph
        for route, slo in ref.routes_slo.items():
            print(f"== {sc}{' (own corpus)' if a.corpus else ''} route {route}: SLO {1000 * slo:.2f} ms, parallel nodes "
                  f"{sorted(s for (r, s), p in g.parallel.items() if r == route and p)}")
            for cls in sorted({c for (r, c, _) in o.b if r == route}):
                L = {s: v for (r, c, s), v in o.L.items() if r == route and c == cls}
                O = {s: v for (r, c, s), v in o.O.items() if r == route and c == cls}
                own = own_from_inclusive(L, g, route)
                for s in own:
                    if s in O:
                        own[s] = max(EPS_OWN, O[s])
                b = {s: o.lookup_b(route, cls, s) for s in own}
                T = {s: o.lookup(route, cls, s) for s in own}
                crit = critical(g, route, T)
                worst, wpath = 0.0, ()
                for pth in paths(g, route, g.root[route]):
                    tot = sum(b.get(s) or 0.0 for s in pth)
                    if tot > worst:
                        worst, wpath = tot, pth
                b2, _ = resplit(slo, own, g, route)
                dev = max(abs(b2[s] - b[s]) / b[s] for s in own)
                print(f"  class {cls}: critical structure {' > '.join(crit)} (shares sum {sum(b[s] for s in crit) / slo:.3f} x SLO); "
                      f"all shares {sum(b.values()) / slo:.2f} x SLO; largest root-to-leaf share sum {worst / slo:.3f} x SLO "
                      f"on {' > '.join(wpath)}; normaliser over every service: max |b' - b| / b = {dev:.1e}")
                if cls == "*":
                    order = sorted(b, key=lambda x: -b[x])
                    print("    b (ms): " + ", ".join(f"{s} {1000 * b[s]:.2f}" for s in order))
                    print("    T (ms): " + ", ".join(f"{s} {1000 * T[s]:.2f}" for s in order))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
