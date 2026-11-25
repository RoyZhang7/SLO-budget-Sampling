"""When requests reached their pods: the estimate every run's 100 % export gives, and the eBPF stamp of the runs that have it.

Estimate (the common truth of all arms): the kernel receive time is the caller's send time (its CLIENT span's start) plus
the edge's healthy median gap between that and the callee's handler start, so a call's wait is what its gap has over the
edge's healthy median, at least 0.  An edge is (caller service, callee service, caller host, callee host); its median
takes the two hosts' clock offset with it.  The within-service latency is the handler's own time plus the wait.

Stamp: slobudget.arrival_ns, attached in the tee in front of the fork (slobudget.arrival_source ebpf; `start` is a miss: the
span's own start).
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .spans import CLIENT, SERVER, SpanTable

MIN_CALLS = 20          # healthy calls an edge needs for its median


@dataclass
class Calls:
    v: np.ndarray           # the callees' server spans
    c: np.ndarray           # their CLIENT parents
    gap: np.ndarray         # ns from the caller's send to the handler's start
    edge: np.ndarray        # index into names
    names: list[str]        # "caller>callee [caller host->callee host]"


def calls(t: SpanTable, meta: dict) -> Calls:
    """Every server span called by a service: its CLIENT parent has a server span above it."""
    v = np.flatnonzero((t.kind == SERVER) & (t.pidx >= 0))
    v = v[t.kind[t.pidx[v]] == CLIENT]
    c = t.pidx[v]
    ok = t.pidx[c] >= 0
    v, c = v[ok], c[ok]
    place = meta.get("placement") or {}
    hosts = sorted(set(place.values()) | set(t.hosts) | {"?"})
    code = {h: i for i, h in enumerate(hosts)}
    svc_host = np.array([code[place.get(s, "?")] for s in t.services] or [0], dtype=np.int64)
    # a service on several nodes (the root's) is where the request's root ran
    many = {s for s, n in (meta.get("placement_nodes") or {}).items() if len(n) > 1} | {t.services[i] for i in np.unique(t.svc[t.is_root])}
    is_many = np.array([s in many for s in t.services] or [False])
    root_host = np.array([code[h] if h else code["?"] for h in t.hosts] or [0], dtype=np.int64)
    ch = svc_host[t.svc[c]]
    if len(t.rhost) and is_many.any():
        ch = np.where(is_many[t.svc[c]], root_host[t.rhost[c]], ch)
    vh = svc_host[t.svc[v]]
    nh, ns = len(hosts), max(1, len(t.services))
    key = ((t.svc[c].astype(np.int64) * ns + t.svc[v]) * nh + ch) * nh + vh
    uk, inv = np.unique(key, return_inverse=True)
    names = []
    for k in uk.tolist():
        k, b = divmod(k, nh)
        k, a = divmod(k, nh)
        s, d = divmod(k, ns)
        names.append(f"{t.services[s]}>{t.services[d]} [{hosts[a]}->{hosts[b]}]")
    return Calls(v, c, t.start[v] - t.start[c], inv, names)


def edge_medians(k: Calls, healthy: np.ndarray) -> np.ndarray:
    """ns per edge: the median gap of its healthy calls (nan under MIN_CALLS)."""
    out = np.full(len(k.names), np.nan)
    h = healthy[k.v]
    for e in range(len(k.names)):
        g = k.gap[(k.edge == e) & h]
        if len(g) >= MIN_CALLS:
            out[e] = float(np.median(g))
    return out


def estimated_wait(t: SpanTable, meta: dict, healthy: np.ndarray, k: Calls | None = None) -> np.ndarray:
    """ns per span: the estimated wait between the arrival at the pod and the handler's start (0: no edge, or an edge without a median)."""
    k = k or calls(t, meta)
    med = edge_medians(k, healthy)[k.edge] if len(k.v) else np.zeros(0)
    w = np.zeros(t.n, dtype=np.int64)
    w[k.v] = np.where(np.isfinite(med), np.maximum(0.0, k.gap - np.nan_to_num(med)), 0.0).astype(np.int64)
    return w


def stamped_wait(t: SpanTable) -> np.ndarray:
    """ns per span from the stamp, as the agents and the central read it (0 without one, or with a later arrival)."""
    w = np.where(t.arrival > 0, t.start - t.arrival, 0)
    return np.maximum(w, 0)


class Shifted:
    """A table of the same spans, counted from their arrivals: what build_oracle reads (duration and own time with the wait)."""

    def __init__(self, t: SpanTable, wait: np.ndarray, own: np.ndarray):
        self.__dict__.update({k: getattr(t, k) for k in ("kind", "route", "cls", "svc", "routes", "classes", "services", "end", "n")})
        self.start = t.start - wait
        self.own = own + wait


def within_service(t: SpanTable, wait: np.ndarray, own: np.ndarray, mask: np.ndarray, graph, Calibrator, routes_slo):
    """(within-service latency in s, its oracle share b in s) per span: the calibration on the same measure, from the healthy spans."""
    from .oracle import build_oracle, lookup_arrays
    s = Shifted(t, wait, own)
    cal = build_oracle(s, mask, graph, Calibrator, routes_slo)
    _, B = lookup_arrays(s, cal)
    return (own + wait) / 1e9, B


def quantiles(x: np.ndarray) -> dict:
    if not len(x):
        return {"n": 0}
    q = lambda p: float(np.quantile(x, p))
    return {"n": int(len(x)), "mean": float(x.mean()), "p5": q(.05), "p50": q(.5), "p95": q(.95), "p99": q(.99), "min": float(x.min()), "max": float(x.max())}


def compare(t: SpanTable, meta: dict, healthy: np.ndarray, own: np.ndarray, graph, Calibrator, routes_slo) -> dict:
    """The estimated kernel receive time against the stamp, per edge (a run with the stamp): ms, estimate minus stamp, in the healthy
    part and in the window (the fault, or the burst), and how often the two put a span over its threshold b differently (each with
    its own calibration).

    The estimate takes the callee's healthy median wait (arrival to handler start) for travel, so in healthy traffic it is
    later than the stamp by about that wait on every call of the edge, which its thresholds absorb; `around_it` is
    the difference without the edge's healthy median of it, what the estimate is wrong by from call to call."""
    k = calls(t, meta)
    med = edge_medians(k, healthy)
    f = meta.get("fault") or meta.get("burst") or {}
    s = t.start[k.v] / 1e9
    window = (s >= float(f["start"])) & (s < float(f["end"])) if f else np.zeros(len(k.v), bool)
    h = healthy[k.v]
    hit = t.arr_ebpf[k.v]
    ok = hit & np.isfinite(med[k.edge]) if len(k.v) else hit
    est = t.start[k.c] + np.nan_to_num(med[k.edge], nan=0.0) if len(k.v) else np.zeros(0)
    d = (est - t.arrival[k.v]) / 1e6
    usual = np.full(len(k.names), np.nan)           # ms: each edge's healthy median of the difference
    for e in range(len(k.names)):
        x = d[ok & h & (k.edge == e)]
        if len(x) >= MIN_CALLS:
            usual[e] = float(np.median(x))
    c = d - usual[k.edge] if len(k.v) else d
    wait_est, wait_real = estimated_wait(t, meta, healthy, k), stamped_wait(t)
    x_est, b_est = within_service(t, wait_est, own, healthy, graph, Calibrator, routes_slo)
    x_real, b_real = within_service(t, wait_real, own, healthy, graph, Calibrator, routes_slo)
    over_est, over_real = x_est > b_est, x_real > b_real

    def part(m):
        """of the calls in m; everything but the first two counts is over the stamped ones"""
        mm = m & ok
        vv = k.v[mm]
        a, b, cc = over_est[vv], over_real[vv], c[mm]
        cc = cc[np.isfinite(cc)]
        return {"calls": int(m.sum()), "stamped": int((m & hit).sum()), "estimate_minus_stamp_ms": quantiles(d[mm]), "around_it_ms": quantiles(cc),
                "share_off_by_over_1ms": float((np.abs(cc) > 1.0).mean()) if len(cc) else None,
                "wait_stamp_ms": quantiles(wait_real[vv] / 1e6), "wait_estimate_ms": quantiles(wait_est[vv] / 1e6),
                "over_by_both": int((a & b).sum()), "over_by_the_estimate_only": int((a & ~b).sum()), "over_by_the_stamp_only": int((~a & b).sum())}
    out = {"run_id": meta["run_id"], "scenario": meta["scenario"], "culprit": f.get("culprit"), "window": "fault" if meta.get("fault") else "burst" if f else None,
           "server_spans": int((t.kind == SERVER).sum()), "server_spans_with_the_attribute": int(((t.kind == SERVER) & (t.arrival > 0)).sum()),
           "calls": int(len(k.v)), "stamped_by_the_program": int(hit.sum()), "share_stamped": float(hit.mean()) if len(hit) else None,
           "edges": {}, "services": {}, "all": {"healthy": part(h), "window": part(window)}}
    for i in np.unique(t.svc[k.v]).tolist():            # a service is one pod here, the root's apart
        m = t.svc[k.v] == i
        out["services"][t.services[i]] = {"calls": int(m.sum()), "stamped": int((m & hit).sum())}
    for e, name in enumerate(k.names):
        m = k.edge == e
        out["edges"][name] = {"median_gap_ms": float(med[e]) / 1e6 if np.isfinite(med[e]) else None, "healthy": part(m & h), "window": part(m & window)}
    out["thresholds"] = {}
    for i, name in enumerate(t.services):           # the median over the service's healthy spans (the thresholds are per request class)
        j = healthy & (t.kind == SERVER) & (t.svc == i) & np.isfinite(b_est) & np.isfinite(b_real)
        if j.any():
            out["thresholds"][name] = {"b_estimate_ms": float(np.median(b_est[j]) * 1e3), "b_stamp_ms": float(np.median(b_real[j]) * 1e3)}
    return out
