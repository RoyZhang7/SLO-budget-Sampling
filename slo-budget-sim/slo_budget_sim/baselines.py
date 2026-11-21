"""Baselines behind the same Sampler interface.

threshold      BudgetSampler with no budget, no pool and N_need = inf (sampler.make_budget_arm)
keep-parent    BudgetSampler that also keeps the parent of a kept outlier (sampler.make_budget_arm)
reservoir      weighted reservoir per host, top-K by size of miss, decided at period end
uniform-head   per-trace coin at the root, rate adapted to the volume target
canopy         per-service rate targets adapted multiplicatively at the collector
tail           tail-based whole-trace sampler: every span buffered at the collector until the
               root ends, the whole trace kept when the root misses its SLO or any span has an
               error status, else with the plan probability that brings the kept volume to V*;
               cap on whole traces, oldest decision first; no per-request or per-host bound
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
        self._buf_peak = 0
        self._buf_samples: list[int] = []

    def on_end(self, span: Span, req: RequestState, t: float) -> tuple[bool, float, str]:
        h = span.host
        self._spans_host[h] = self._spans_host.get(h, 0) + 1
        held = sum(len(x) for x in self._heap.values())          # the reservoir holds its candidates until period end
        self._buf_peak = max(self._buf_peak, held + 1)
        self._buf_samples.append(held + 1)
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
        self._diag.append({"period": period, "collector_volume": collector_volume, "K": dict(self._K),
                           "buffer_peak": self._buf_peak,
                           "buffer_mean": (sum(self._buf_samples) / len(self._buf_samples)) if self._buf_samples else 0.0})
        self._spans_host = {}
        self._buf_peak, self._buf_samples = 0, []


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


class _TraceBuf:
    __slots__ = ("idxs", "first_start", "nbytes", "has_error", "route")

    def __init__(self, route: str):
        self.idxs: list[int] = []
        self.first_start = math.inf
        self.nbytes = 0
        self.has_error = False
        self.route = route


class TailSampler(Sampler):
    """Tail-based whole-trace sampler, the canonical version.  Every span of a
    trace is buffered at the collector until the trace's root ends; then the
    whole trace is kept when the root exceeds its SLO or any span has an error
    status, otherwise with the plan probability r that brings the expected kept
    volume to V* (adapted each period from the slow-trace spans that are always
    kept and the ordinary spans seen).  The per-period cap is enforced on whole
    traces in decision order (oldest root end first).  There is no per-request
    or per-host bound.  A trace whose root has not ended `timeout` after its
    first span started is evicted and dropped (default 2 x the route's SLO).

    Cost reported per period: peak and mean buffered spans (sampled at every
    span end), peak buffered bytes, decision latency (root end minus the trace's
    first span start) of every decided trace, traces evicted by the timeout and
    traces refused by the cap."""
    name = "tail"

    def __init__(self, initial_rate: float = 0.15, alpha: float = 0.5, timeout_slo_mult: float = 2.0,
                 timeout_s: float | None = None):
        self.r0 = initial_rate
        self.alpha = alpha
        self.timeout_slo_mult = timeout_slo_mult
        self.timeout_s = timeout_s

    def start_run(self, ctx: RunContext) -> None:
        super().start_run(ctx)
        self.slo = ctx.corpus.routes_slo
        self.r = min(1.0, max(1e-4, self.r0))
        self._buf: dict[int, _TraceBuf] = {}
        self._gone: dict[int, float] = {}          # traces evicted by the timeout: their late spans are dropped, not re-buffered
        self._buffered = 0
        self._bytes = 0
        self._releases: list[tuple[int, bool, float, str]] = []
        self._cap_used = 0.0
        self._slow_exp: float | None = None
        self._ord_exp: float | None = None
        self._reset()

    def _reset(self) -> None:
        self._peak = 0
        self._peak_bytes = 0
        self._samples: list[int] = []
        self._lat: list[float] = []
        self._evicted = 0
        self._cap_dropped = 0
        self._slow_spans = 0
        self._ord_spans = 0
        self._slow_traces = 0

    def _timeout(self, route: str) -> float:
        if self.timeout_s is not None:
            return self.timeout_s
        return self.timeout_slo_mult * self.slo.get(route, 1.0)

    def _release(self, tb: _TraceBuf, keep: bool, p: float, reason: str) -> None:
        for idx in tb.idxs:
            self._releases.append((idx, keep, p, reason))
        self._buffered -= len(tb.idxs)
        self._bytes -= tb.nbytes

    def _evict(self, t: float) -> None:
        dead = [tid for tid, tb in self._buf.items() if t - tb.first_start > self._timeout(tb.route)]
        for tid in dead:
            tb = self._buf.pop(tid)
            self._evicted += 1
            self._gone[tid] = t
            self._release(tb, False, 0.0, "tail_timeout")
        if len(self._gone) > 10000:
            cut = t - 10.0 * max(self.slo.values(), default=1.0)
            self._gone = {tid: te for tid, te in self._gone.items() if te > cut}

    def on_end(self, span: Span, req: RequestState, t: float) -> tuple[bool, float, str]:
        if span.trace_id in self._gone:                 # a late span of a trace the timeout already evicted
            return False, 0.0, "tail_timeout"
        tb = self._buf.get(span.trace_id)
        if tb is None:
            tb = _TraceBuf(span.route)
            self._buf[span.trace_id] = tb
        tb.idxs.append(span.idx)
        tb.first_start = min(tb.first_start, span.start)
        tb.nbytes += span.nbytes
        tb.has_error = tb.has_error or not span.status_ok
        self._buffered += 1
        self._bytes += span.nbytes
        self._peak = max(self._peak, self._buffered)
        self._peak_bytes = max(self._peak_bytes, self._bytes)
        self._samples.append(self._buffered)
        self._evict(t)
        if span.parent_idx >= 0 or span.trace_id not in self._buf:
            return False, math.nan, "defer"
        # the root ended: decide the whole trace
        tb = self._buf.pop(span.trace_id)
        n = len(tb.idxs)
        self._lat.append(t - tb.first_start)
        slow = span.duration > self.slo.get(span.route, math.inf) or tb.has_error
        if slow:
            self._slow_spans += n
            self._slow_traces += 1
            keep, p, reason = True, 1.0, "tail_slow"
        else:
            self._ord_spans += n
            if self.rng.random() < self.r:
                keep, p, reason = True, self.r, "tail_plan"
            else:
                keep, p, reason = False, self.r, "tail_coin"
        if keep and self._cap_used + n > self.ctx.cap + 1e-9:
            self._cap_dropped += 1
            keep, reason = False, "tail_cap"
        if keep:
            self._cap_used += n
        self._release(tb, keep, p, reason)
        return False, p, "defer"

    def take_releases(self) -> list[tuple[int, bool, float, str]]:
        out, self._releases = self._releases, []
        return out

    def finish(self) -> list[tuple[int, bool, float, str]]:
        for tb in self._buf.values():
            self._evicted += 1
            self._release(tb, False, 0.0, "tail_timeout")
        self._buf = {}
        return self.take_releases()

    def on_period_end(self, period: int, collector_volume: float) -> None:
        a = self.alpha
        self._slow_exp = float(self._slow_spans) if self._slow_exp is None else (1 - a) * self._slow_exp + a * self._slow_spans
        self._ord_exp = float(self._ord_spans) if self._ord_exp is None else (1 - a) * self._ord_exp + a * self._ord_spans
        if self._ord_exp and self._ord_exp > 0:
            raw = (self.ctx.v_star - self._slow_exp) / self._ord_exp
            self.r = min(1.0, max(1e-4, (1 - a) * self.r + a * min(1.0, max(0.0, raw))))
        lat = sorted(self._lat)
        self._diag.append({"period": period, "collector_volume": collector_volume, "r": self.r,
                           "buffer_peak": self._peak, "buffer_peak_bytes": self._peak_bytes,
                           "buffer_mean": (sum(self._samples) / len(self._samples)) if self._samples else 0.0,
                           "latencies": lat, "evicted": self._evicted, "cap_dropped_traces": self._cap_dropped,
                           "slow_traces": self._slow_traces})
        self._cap_used = 0.0
        self._reset()
