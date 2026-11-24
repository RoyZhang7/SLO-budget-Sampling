"""The oracle T table: the simulator's calibration run once on the shadow's complete
server spans of the healthy window (as `ttable.oracle_table` does on the simulator's
healthy reference corpus)."""
from __future__ import annotations

import json
import os
import sys

import numpy as np

from .spans import SERVER, SpanTable


def import_sim(sim_path: str):
    """Import slo_budget_sim from a checkout (the package directory's parent)."""
    sim_path = os.path.expanduser(sim_path)
    if sim_path not in sys.path:
        sys.path.insert(0, sim_path)
    from slo_budget_sim.model import CallGraph          # noqa: E402
    from slo_budget_sim.ttable import Calibrator         # noqa: E402
    return CallGraph, Calibrator


def load_callgraph(path: str, CallGraph):
    """deploy/callgraph.json: {"root": {route: svc}, "children": {route: {svc: [kids]}}, "parallel": {route: {svc: bool}}},
    optionally "stages": {route: {svc: [[callees of the first stage], ...]}} (read into CallGraph.stages)."""
    with open(path, encoding="utf-8") as f:
        g = json.load(f)
    children, parallel = {}, {}
    for route, ch in (g.get("children") or {}).items():
        for svc, kids in ch.items():
            children[(route, svc)] = list(kids)
    for route, par in (g.get("parallel") or {}).items():
        for svc, flag in par.items():
            parallel[(route, svc)] = bool(flag)
    for (route, svc) in children:
        parallel.setdefault((route, svc), False)
    cg = CallGraph(dict(g.get("root") or {}), children, parallel)
    stages = {(route, svc): [list(group) for group in st] for route, by_svc in (g.get("stages") or {}).items() for svc, st in by_svc.items()}
    if stages:
        if not hasattr(cg, "stages"):           # an older simulator would split on the parallel flags and say nothing
            raise ValueError(f"{path} has sequential stages, this simulator's CallGraph does not know them")
        cg.stages = stages
    return cg


class _Obs:
    """What Calibrator.observe reads from a span."""
    __slots__ = ("route", "req_class", "service", "duration", "own")

    def __init__(self, route, req_class, service, duration, own):
        self.route, self.req_class, self.service, self.duration, self.own = route, req_class, service, duration, own


def build_oracle(t: SpanTable, mask: np.ndarray, graph, Calibrator, routes_slo: dict[str, float],
                 q: float = 0.95, use_class: bool = True):
    """Calibrator fed once with every server span in `mask` (p = 1) and one end_period()."""
    cal = Calibrator(graph, routes_slo, q=q, window=1, use_class=use_class)
    idx = np.flatnonzero(mask & (t.kind == SERVER))
    dur = (t.end[idx] - t.start[idx]) / 1e9
    own = t.own[idx] / 1e9
    for k, i in enumerate(idx):
        cal.observe(_Obs(t.routes[t.route[i]], t.classes[t.cls[i]], t.services[t.svc[i]], float(dur[k]), float(own[k])), 1.0)
    cal.end_period()
    return cal


def lookup_arrays(t: SpanTable, cal, server_only: bool = True):
    """T and the stage budget b (seconds) per span from the oracle (nan where it has none)."""
    T = np.full(t.n, np.nan)
    B = np.full(t.n, np.nan)
    key = (t.route.astype(np.int64) * 4096 + t.cls.astype(np.int64)) * 4096 + t.svc.astype(np.int64)
    m = (t.kind == SERVER) if server_only else np.ones(t.n, dtype=bool)
    uk, inv = np.unique(key[m], return_inverse=True)
    tv = np.full(len(uk), np.nan)
    bv = np.full(len(uk), np.nan)
    for j, kk in enumerate(uk):
        svc = int(kk % 4096)
        cls = int((kk // 4096) % 4096)
        route = int(kk // (4096 * 4096))
        r, c, s = t.routes[route], t.classes[cls], t.services[svc]
        v = cal.lookup(r, c, s)
        if v is not None:
            tv[j] = v
        b = cal.lookup_b(r, c, s) if hasattr(cal, "lookup_b") else None
        if b is not None:
            bv[j] = b
    T[m] = tv[inv]
    B[m] = bv[inv]
    return T, B


def route_level_table(cal, route: str = "compose_post") -> dict[str, float]:
    """The oracle's route-level ("*") T per service, seconds."""
    return {svc: v for (r, c, svc), v in cal.table.items() if r == route and c == "*"}
