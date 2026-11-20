"""Derive the generator's workload (call trees, fan-out, own-time and transit
distributions) from real traces.

    python -m slo_budget_sim.derive corpora/wrk2api-200.json -o slo_budget_sim/workload_dsbsn.json

The output JSON is what `synthetic.py` loads.  Structure (which service calls
which, in what order, sequential or parallel, how many times) is taken from the
traces; timings are lognormal fits of the own time per service (median and
log-space standard deviation) and of the one-way transit per callee.  With a
few hundred traces the medians are usable and the tails are not: the JSON marks
the timings `provisional` below 5000 spans per service and `fitted` above
(the 10-minute export under load has 60000 per service).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

from .jaeger import load_jaeger
from .model import Corpus

ROUTE_NAME_RE = re.compile(r"^(?:[A-Za-z0-9]+Server_)?(?P<op>[A-Za-z0-9]+)$")


def route_name(operation: str) -> str:
    """`Wrk2APIServiceServer_ComposePost` -> `compose_post`."""
    m = ROUTE_NAME_RE.match(operation)
    op = m.group("op") if m else operation
    return re.sub(r"(?<!^)(?=[A-Z])", "_", op).lower()


def _lognormal_fit(values_s: list[float]) -> tuple[float, float]:
    v = np.array([x for x in values_s if x > 0], dtype=float)
    if len(v) == 0:
        return 0.0, 0.0
    lv = np.log(v)
    return float(math.exp(lv.mean()) * 1000.0), float(lv.std(ddof=1)) if len(v) > 1 else 0.0


def derive_workload(corpus: Corpus, source: str = "", min_route_share: float = 0.01) -> dict:
    spans = corpus.spans
    by_route_roots: Counter = Counter(spans[r].route for r in corpus.roots)
    total_roots = sum(by_route_roots.values())
    routes_out: dict[str, dict] = {}
    parallel: set[str] = set()
    for route, n_roots in by_route_roots.items():
        if n_roots / total_roots < min_route_share:
            continue
        # per parent instance: ordered callee services and whether they overlap
        kids_seq: dict[str, Counter] = defaultdict(Counter)
        overlap: dict[str, list[int]] = defaultdict(list)
        for s in spans:
            if s.route != route or not s.children:
                continue
            cs = sorted((spans[c] for c in s.children), key=lambda c: c.start)
            kids_seq[s.service][tuple(c.service for c in cs)] += 1
            if len(cs) >= 2:
                overlap[s.service].append(1 if any(cs[i + 1].start < cs[i].end for i in range(len(cs) - 1)) else 0)
        root_svc = spans[corpus.roots[0]].service
        for r in corpus.roots:
            if spans[r].route == route:
                root_svc = spans[r].service
                break

        def tree(svc: str, seen: tuple) -> list:
            if svc in seen or svc not in kids_seq:
                return [svc, []]
            seq = kids_seq[svc].most_common(1)[0][0]            # the modal callee sequence (with repeats)
            return [svc, [tree(c, seen + (svc,)) for c in seq]]

        for svc, ov in overlap.items():
            if sum(ov) * 2 > len(ov):
                parallel.add(svc)
        routes_out[route_name(route)] = {
            "operation": route, "share": round(n_roots / total_roots, 4), "requests": n_roots,
            "tree": tree(root_svc, ()),
            "spans_per_request": round(sum(1 for s in spans if s.route == route) / n_roots, 2),
            "root_p50_ms": round(float(np.quantile([spans[r].duration for r in corpus.roots if spans[r].route == route], 0.5)) * 1000, 3),
            "root_p95_ms": round(float(np.quantile([spans[r].duration for r in corpus.roots if spans[r].route == route], 0.95)) * 1000, 3),
        }
    services: dict[str, dict] = {}
    for svc in sorted({s.service for s in spans}):
        own = [s.own for s in spans if s.service == svc]
        dur = [s.duration for s in spans if s.service == svc]
        transit = [s.transit for s in spans if s.service == svc and s.parent_idx >= 0 and s.transit > 0]
        med, sig = _lognormal_fit(own)
        services[svc] = {
            "median_ms": round(med, 4), "sigma": round(sig, 3), "n": len(own),
            "own_p50_ms": round(float(np.quantile(own, 0.5)) * 1000, 4), "own_p95_ms": round(float(np.quantile(own, 0.95)) * 1000, 4),
            "dur_p50_ms": round(float(np.quantile(dur, 0.5)) * 1000, 4), "dur_p95_ms": round(float(np.quantile(dur, 0.95)) * 1000, 4),
            "transit_ms": round(float(np.median(transit)) * 1000, 4) if transit else None,
            "errors": int(sum(1 for s in spans if s.service == svc and not s.status_ok)),
        }
    hosts = sorted({s.host for s in spans})
    n_min = min((v["n"] for v in services.values()), default=0)
    span_s = corpus.t_end - corpus.t0
    timings = (f"fitted: lognormal fit of {n_min} spans per service over {span_s:.0f} s "
               f"at {len(corpus.roots) / span_s:.0f} requests/s" if n_min >= 5000 and span_s > 0
               else "provisional: lognormal fit of a few hundred spans per service")
    return {
        "source": source, "derived": dt.date.today().isoformat(), "traces": len(corpus.roots), "spans": len(spans),
        "span_s": round(corpus.t_end - corpus.t0, 3),
        "structure": "from real traces", "timings": timings,
        "routes": routes_out, "parallel": sorted(parallel), "services": services,
        "hosts_in_traces": hosts if not all(h.startswith("host:") for h in hosts) else [],
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("export", help="Jaeger JSON export")
    ap.add_argument("-o", "--out", default="slo_budget_sim/workload_dsbsn.json")
    ap.add_argument("--period", type=float, default=5.0)
    a = ap.parse_args(argv)
    corpus = load_jaeger(a.export, period=a.period)
    wl = derive_workload(corpus, source=Path(a.export).name)
    Path(a.out).write_text(json.dumps(wl, indent=1), encoding="utf-8")
    print(f"{a.export}: {wl['traces']} traces, {wl['spans']} spans over {wl['span_s']} s -> {a.out}")
    for r, info in wl["routes"].items():
        print(f"  route {r}: share {info['share']}, {info['spans_per_request']} spans/request, root p50/p95 "
              f"{info['root_p50_ms']}/{info['root_p95_ms']} ms")
        print("   ", json.dumps(info["tree"]))
    print("  parallel:", wl["parallel"])
    for svc, s in wl["services"].items():
        print(f"  {svc:14s} n={s['n']:4d} own median {s['median_ms']:7.3f} ms sigma {s['sigma']:.2f}  transit {s['transit_ms']} ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
