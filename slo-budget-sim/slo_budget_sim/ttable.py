"""Per-service latency budgets T_i.

T_i comes from the Erms square-root split of the root's end-to-end SLO over the
own (self) time of each service along the route's call structure:

    O_i    = calibration quantile of the own (self) time of service i for
             (route, class): span duration minus the time spent in its outbound
             calls, which an agent computes locally from the server span and its
             client spans.  When O is unavailable it is derived from the
             inclusive quantiles L (L_i minus the callees' L), which is noisier.
    b_i    = SLO * sqrt(O_i) / W              W = sum of sqrt(O) over the contributing
                                              set (the root's critical structure; under
                                              a parallel fan-out only the callee with the
                                              largest inclusive L contributes)
    T_i    = b_i + sum/max of callees' T      inclusive budget; T_root = SLO after
                                              normalisation

A span is an outlier when its inclusive duration exceeds T_i (or its status is
not OK).  Tables are keyed by (route, class, service); a class with fewer than
`min_class_samples` kept spans falls back to the route-level ("*") entry.

Cold start: before a service's first calibrated period there is no T_i.  With
`prior="equal"` the calibrator answers from the equal-share split instead (the
same square-root split with every own time taken as equal, so the SLO is shared
equally over the stages of the root's critical structure); the prior is dropped
for a service as soon as it has been calibrated once.
"""
from __future__ import annotations

import math
from collections import deque

from .model import CallGraph, Corpus, Span

EPS_OWN = 1e-4          # 0.1 ms floor for an own-time estimate
MIN_SAMPLES = 8         # fewer kept samples than this gives no L at all


def weighted_quantile(values, weights, q: float) -> float:
    """q-quantile of values with positive weights (Horvitz-Thompson style)."""
    if not values:
        raise ValueError("no samples")
    pairs = sorted(zip(values, weights))
    total = sum(w for _, w in pairs)
    target = q * total
    acc = 0.0
    for v, w in pairs:
        acc += w
        if acc >= target:
            return v
    return pairs[-1][0]


def own_from_inclusive(L: dict[str, float], graph: CallGraph, route: str) -> dict[str, float]:
    own = {}
    for s, Ls in L.items():
        kids = [c for c in graph.children.get((route, s), []) if c in L and c != s]
        if not kids:
            cover = 0.0
        elif graph.parallel.get((route, s), False):
            cover = max(L[c] for c in kids)
        else:
            cover = sum(L[c] for c in kids)
        own[s] = max(EPS_OWN, Ls - cover)
    return own


def sqrt_split(slo: float, L: dict[str, float], graph: CallGraph, route: str,
               O: dict[str, float] | None = None) -> dict[str, float]:
    """Inclusive budgets T per service from the inclusive quantiles L and the
    own-time quantiles O (derived from L when not given)."""
    return split_budgets(slo, L, graph, route, O)[0]


def split_budgets(slo: float, L: dict[str, float], graph: CallGraph, route: str,
                  O: dict[str, float] | None = None, held_T: dict[str, float] | None = None,
                  held_b: dict[str, float] | None = None) -> tuple[dict[str, float], dict[str, float]]:
    """(inclusive T per service, stage budget b per service), both scaled so
    that T_root = SLO.  Services in `held_T` (frozen) keep that T; the other
    services' stage budgets are then scaled so that T_root = SLO over them."""
    root = graph.root.get(route)
    if root is None or root not in L:
        return {}, {}
    own = own_from_inclusive(L, graph, route)
    if O:
        for s in own:
            if s in O:
                own[s] = max(EPS_OWN, O[s])

    def kids_of(s):
        return [c for c in graph.children.get((route, s), []) if c in own and c != s]

    # contributing set: follow every callee of a sequential node, only the
    # longest callee of a parallel node
    contrib, stack = set(), [root]
    while stack:
        s = stack.pop()
        if s in contrib:
            continue
        contrib.add(s)
        ks = kids_of(s)
        if not ks:
            continue
        if graph.parallel.get((route, s), False):
            stack.append(max(ks, key=lambda c: L[c]))
        else:
            stack.extend(ks)
    W = sum(math.sqrt(own[s]) for s in sorted(contrib))      # fixed order: bit-identical across processes and the Go port
    b = {s: slo * math.sqrt(own[s]) / W for s in own}

    held = {s: v for s, v in (held_T or {}).items() if s in own and s != root}   # frozen services keep their T

    def t_with(k: float) -> dict[str, float]:
        """T per service with the unfrozen stage budgets scaled by k; held services keep their T."""
        T: dict[str, float] = {}
        visiting: set[str] = set()

        def t(s):
            if s in T:
                return T[s]
            if s in held:
                T[s] = held[s]
                return T[s]
            if s in visiting:          # cycle guard for odd graphs
                return k * b[s]
            visiting.add(s)
            ks = kids_of(s)
            if not ks:
                v = k * b[s]
            elif graph.parallel.get((route, s), False):
                v = k * b[s] + max(t(c) for c in ks)
            else:
                v = k * b[s] + sum(t(c) for c in ks)
            visiting.discard(s)
            T[s] = v
            return v

        for s in own:
            t(s)
        return T

    if not held or t_with(0.0)[root] >= slo:
        # no frozen service (or the frozen ones alone exceed the SLO): the plain split, T_root = SLO
        T = t_with(1.0)
        scale = slo / T[root]
        return {s: v * scale for s, v in T.items()}, {s: v * scale for s, v in b.items()}
    # frozen services keep their T; the unfrozen stage budgets are scaled by k so that
    # T_root = SLO (T_root is increasing and piecewise linear in k: bisection, 60 steps)
    lo, hi = 0.0, 1.0
    while t_with(hi)[root] < slo and hi < 1e6:
        hi *= 2.0
    for _ in range(60):
        mid = (lo + hi) / 2.0
        if t_with(mid)[root] < slo:
            lo = mid
        else:
            hi = mid
    k = hi
    T = t_with(k)
    bh = held_b or {}
    return T, {s: (bh[s] if s in held and s in bh else k * b[s]) for s in b}


def equal_share_prior(slo: float, graph: CallGraph, route: str) -> dict[str, float]:
    """Cold-start prior: the square-root split with every service's own time
    taken as equal.  Every stage of the root's critical structure gets the same
    share of the SLO (SLO / |contributing set|; under a parallel fan-out the
    first callee in callee order is followed), T adds up along the tree and
    T_root = SLO.  Needs only the call graph and the SLO."""
    one = {s: 1.0 for s in graph.services(route)}
    return split_budgets(slo, one, graph, route, one)[0]


PRIORS = ("none", "equal")


class Calibrator:
    """Builds the T table each period from the kept spans of the last `window`
    periods, reweighted by 1/p.  `lookup` is what the decider calls."""

    def __init__(self, graph: CallGraph, routes_slo: dict[str, float], q: float = 0.95,
                 min_class_samples: int = 20, window: int = 2, use_class: bool = True, prior: str = "none"):
        self.graph = graph
        self.routes_slo = routes_slo
        self.q = q
        self.min_class_samples = min_class_samples
        self.use_class = use_class
        if prior not in PRIORS:
            raise ValueError(f"unknown prior {prior}; choose from {PRIORS}")
        self.prior = prior
        self.prior_table: dict[tuple[str, str], float] = {}      # (route, service) -> T before the first calibration
        self.prior_b: dict[tuple[str, str], float] = {}          # (route, service) -> own-time share of the prior split
        if prior == "equal":
            for route, slo in routes_slo.items():
                if route in graph.root:
                    one = {s: 1.0 for s in graph.services(route)}
                    Tp, bp = split_budgets(slo, one, graph, route, one)       # equal_share_prior and its shares
                    for svc, T in Tp.items():
                        self.prior_table[(route, svc)] = T
                    for svc, bv in bp.items():
                        self.prior_b[(route, svc)] = bv
        self.calibrated: set[tuple[str, str]] = set()           # (route, service) calibrated at least once
        self.prior_hits = 0                                      # lookups answered by the prior
        self._buf: deque = deque(maxlen=max(1, window))
        self._cur: dict[tuple[str, str, str], tuple[list, list, list]] = {}
        self.table: dict[tuple[str, str, str], float] = {}
        self.b: dict[tuple[str, str, str], float] = {}      # stage budgets (own-time shares) of the last split
        self.L: dict[tuple[str, str, str], float] = {}
        self.O: dict[tuple[str, str, str], float] = {}
        self.freeze: set[str] = set()      # services whose previous L, O are reused at the next rebuild
        self.version = 0

    def observe(self, span: Span, p: float) -> None:
        cls = span.req_class if self.use_class else "*"
        d, o, w = self._cur.setdefault((span.route, cls, span.service), ([], [], []))
        d.append(span.duration)
        o.append(span.own if span.own > 0.0 else span.duration)
        w.append(1.0 / p if p > 0 else 1.0)

    def end_period(self, frozen: set[str] | None = None) -> None:
        """Close the period.  Samples of `frozen` services from this period are
        purged before they enter the window, so that when the service unfreezes
        the window holds no samples from its frozen periods."""
        if frozen is not None:
            self.freeze = set(frozen)
        if self.freeze:
            self._cur = {k: v for k, v in self._cur.items() if k[2] not in self.freeze}
        self._buf.append(self._cur)
        self._cur = {}
        merged: dict[tuple[str, str, str], tuple[list, list, list]] = {}
        for per in self._buf:
            for key, (d, o, w) in per.items():
                md, mo, mw = merged.setdefault(key, ([], [], []))
                md.extend(d)
                mo.extend(o)
                mw.extend(w)
        self.rebuild(merged)

    def rebuild(self, merged: dict[tuple[str, str, str], tuple[list, list, list]]) -> None:
        pooled: dict[tuple[str, str], tuple[list, list, list]] = {}
        for (route, cls, svc), (d, o, w) in merged.items():
            pd, po, pw = pooled.setdefault((route, svc), ([], [], []))
            pd.extend(d)
            po.extend(o)
            pw.extend(w)
        keys = {(r, c) for (r, c, _) in merged} | {(r, "*") for (r, _, _) in merged}
        # a frozen service whose samples were purged still needs its keys rebuilt
        keys |= {(r, c) for (r, c, _) in self.L}
        table, btab, Ltab, Otab = {}, {}, {}, {}
        empty = ([], [], [])
        for route, cls in keys:
            if route not in self.routes_slo or route not in self.graph.root:
                continue
            L, O = {}, {}
            held_T: dict[str, float] = {}
            held_b: dict[str, float] = {}
            for svc in self.graph.services(route):
                if svc in self.freeze:
                    # frozen source: the service is over N_need, keep what the
                    # last healthy-enough table said instead of chasing the fault:
                    # its L, O (for the split's shares) and its T itself (Stage 2c:
                    # the renormalisation to T_root = SLO runs over the unfrozen
                    # services only, so a frozen T no longer shrinks when others inflate)
                    pL = self.L.get((route, cls, svc), self.L.get((route, "*", svc)))
                    pO = self.O.get((route, cls, svc), self.O.get((route, "*", svc)))
                    if pL is not None and pO is not None:
                        L[svc], O[svc] = pL, pO
                        hT = self.table.get((route, cls, svc), self.table.get((route, "*", svc)))
                        if hT is not None:
                            held_T[svc] = hT
                            hb = self.b.get((route, cls, svc), self.b.get((route, "*", svc)))
                            if hb is not None:
                                held_b[svc] = hb
                        continue
                if cls != "*":
                    d, o, w = merged.get((route, cls, svc), empty)
                    if len(d) < self.min_class_samples:
                        d, o, w = pooled.get((route, svc), empty)
                else:
                    d, o, w = pooled.get((route, svc), empty)
                if len(d) >= MIN_SAMPLES:
                    L[svc] = weighted_quantile(d, w, self.q)
                    O[svc] = weighted_quantile(o, w, self.q)
            Tsplit, bsplit = split_budgets(self.routes_slo[route], L, self.graph, route, O, held_T, held_b)
            for svc, Tv in Tsplit.items():
                table[(route, cls, svc)] = Tv
                btab[(route, cls, svc)] = bsplit[svc]
            for svc in L:
                Ltab[(route, cls, svc)] = L[svc]
                Otab[(route, cls, svc)] = O[svc]
        self.table, self.b, self.L, self.O = table, btab, Ltab, Otab
        self.calibrated.update((r, s) for (r, _, s) in table)
        self.version += 1

    def lookup(self, route: str, cls: str, service: str) -> float | None:
        v = self.table.get((route, cls, service))
        if v is None:
            v = self.table.get((route, "*", service))
        if v is None and self.prior_table and (route, service) not in self.calibrated:
            v = self.prior_table.get((route, service))          # cold start: the prior until the first calibration
            if v is not None:
                self.prior_hits += 1
        return v

    def lookup_b(self, route: str, cls: str, service: str) -> float | None:
        """Stage budget (own-time share) of a service, same fallback as lookup
        (the class entry, else the route entry, else the prior's share before the
        service's first calibration)."""
        v = self.b.get((route, cls, service))
        if v is None:
            v = self.b.get((route, "*", service))
        if v is None and self.prior_b and (route, service) not in self.calibrated:
            v = self.prior_b.get((route, service))
            if v is not None:
                self.prior_hits += 1
        return v


def oracle_table(reference: Corpus, q: float = 0.95, use_class: bool = True) -> Calibrator:
    """The table the calibration procedure yields with complete (p = 1) data
    from a healthy reference workload: the ground-truth T_i."""
    cal = Calibrator(reference.graph, reference.routes_slo, q=q, min_class_samples=MIN_SAMPLES,
                     window=1, use_class=use_class)
    for s in reference.spans:
        cal.observe(s, 1.0)
    cal.end_period()
    return cal
