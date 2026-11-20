"""Baselines behind the same Sampler interface.

threshold      BudgetSampler with no budget, no pool and N_need = inf (sampler.make_budget_arm)
keep-parent    BudgetSampler that also keeps the parent of a kept outlier (sampler.make_budget_arm)
reservoir      weighted reservoir per host, top-K by size of miss, decided at period end
uniform-head   per-trace coin at the root, rate adapted to the volume target
canopy         per-service rate targets adapted multiplicatively at the collector
"""
from __future__ import annotations

import heapq
import math

from .model import Span
from .sampler import RequestState, RunContext, Sampler


class ReservoirSampler(Sampler):
    """Efraimidis-Spirakis weighted reservoir per host and period; weight is the
    size of the miss (duration - T)/T, so ordinary spans only fill spare slots.
    Uses the ground-truth table, which favours this baseline.  Decisions are
    final at period end, so that is also when evidence becomes available."""
    name = "reservoir"

    def __init__(self, eps: float = 1e-6, status_weight: float = 0.01, share_ewma: float = 0.5):
        self.eps = eps
        self.status_weight = status_weight
        self.share_ewma = share_ewma

    def start_run(self, ctx: RunContext) -> None:
        super().start_run(ctx)
        if ctx.oracle is None:
            raise ValueError("reservoir baseline needs the oracle table")
        self.hosts = list(ctx.corpus.hosts)
        self._share = {h: 1.0 / len(self.hosts) for h in self.hosts}
        self._K = {h: max(1, round(ctx.v_star * self._share[h])) for h in self.hosts}
        self._heap: dict[str, list] = {h: [] for h in self.hosts}
        self._spans_host: dict[str, int] = {}

    def on_end(self, span: Span, req: RequestState, t: float) -> tuple[bool, float, str]:
        h = span.host
        self._spans_host[h] = self._spans_host.get(h, 0) + 1
        T = self.ctx.oracle.lookup(span.route, span.req_class, span.service)
        if T is None:
            w = self.eps
        else:
            miss = (span.duration - T) / T
            if miss > 0:
                w = miss
            elif not span.status_ok:
                w = self.status_weight
            else:
                w = self.eps
        key = self.rng.random() ** (1.0 / w)
        heap = self._heap.setdefault(h, [])
        K = self._K.get(h, 1)
        if len(heap) < K:
            heapq.heappush(heap, (key, span.idx))
        elif key > heap[0][0]:
            heapq.heapreplace(heap, (key, span.idx))
        return False, 0.0, "reservoir_pending"

    def flush_period(self, period: int) -> list[tuple[int, float, str]]:
        out = [(idx, math.nan, "reservoir") for heap in self._heap.values() for _, idx in heap]
        self._heap = {h: [] for h in self.hosts}
        return out

    def on_period_end(self, period: int, collector_volume: int) -> None:
        total = sum(self._spans_host.values())
        if total:
            for h in self.hosts:
                share = self._spans_host.get(h, 0) / total
                self._share[h] = (1 - self.share_ewma) * self._share[h] + self.share_ewma * share
                self._K[h] = max(1, round(self.ctx.v_star * self._share[h]))
        self._diag.append({"period": period, "collector_volume": collector_volume, "K": dict(self._K)})
        self._spans_host = {}


class UniformHeadSampler(Sampler):
    """One coin per trace at the root; every span of a sampled trace is kept
    with p = r.  r is adapted each period to the volume target with the same
    damping as the refill loop."""
    name = "uniform-head"

    def __init__(self, initial_rate: float = 0.15, alpha: float = 0.5):
        self.r = initial_rate
        self.alpha = alpha

    def start_run(self, ctx: RunContext) -> None:
        super().start_run(ctx)
        self.r = min(1.0, max(1e-4, self.r))

    def on_root_start(self, span: Span, req: RequestState, t: float) -> None:
        req.head_keep = self.rng.random() < self.r
        req.head_p = self.r

    def on_end(self, span: Span, req: RequestState, t: float) -> tuple[bool, float, str]:
        if req.head_keep:
            return True, req.head_p, "head"
        return False, req.head_p, "head_coin"

    def on_period_end(self, period: int, collector_volume: int) -> None:
        raw = self.r * self.ctx.v_star / max(1.0, float(collector_volume))
        self.r = min(1.0, max(1e-4, (1 - self.alpha) * self.r + self.alpha * raw))
        self._diag.append({"period": period, "collector_volume": collector_volume, "r": self.r})


class CanopySampler(Sampler):
    """Canopy-style adaptive rate targets at the collector: every service gets
    an equal share of the volume target and its per-span rate is scaled by
    target/observed each period.  Blind to outliers; same cap as everyone."""
    name = "canopy"

    def __init__(self, initial_rate: float = 0.15, alpha: float = 0.5):
        self.p0 = initial_rate
        self.alpha = alpha

    def start_run(self, ctx: RunContext) -> None:
        super().start_run(ctx)
        self._p: dict[str, float] = {}
        self._kept: dict[str, int] = {}
        self._seen: dict[str, int] = {}

    def on_end(self, span: Span, req: RequestState, t: float) -> tuple[bool, float, str]:
        svc = span.service
        self._seen[svc] = self._seen.get(svc, 0) + 1
        p = self._p.get(svc, self.p0)
        if p < 1.0 and self.rng.random() >= p:
            return False, p, "rate_coin"
        self._kept[svc] = self._kept.get(svc, 0) + 1
        return True, p, "rate"

    def on_period_end(self, period: int, collector_volume: int) -> None:
        n = max(1, len(self._seen))
        target = self.ctx.v_star / n
        for svc in self._seen:
            p = self._p.get(svc, self.p0)
            raw = p * target / max(1.0, float(self._kept.get(svc, 0)))
            self._p[svc] = min(1.0, max(1e-4, (1 - self.alpha) * p + self.alpha * raw))
        self._diag.append({"period": period, "collector_volume": collector_volume,
                           "p_mean": sum(self._p.values()) / max(1, len(self._p))})
        self._kept, self._seen = {}, {}
