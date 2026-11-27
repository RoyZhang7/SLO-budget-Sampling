"""Event replay: span START and END events in time order through one sampler.

Periods are wall-clock windows of `corpus.period` seconds.  The collector hard
cap is applied here (first come, first served within a period) so every arm is
subject to the same backstop.  Hole counts are derived at the end from the
kept set: a kept parent's holes = its direct children that were not kept, which
equals what a runtime dropped-child counter would record.

An arm may defer a span ("defer" from on_end, the tail-based baseline buffers
every span until the trace's root ends) and resolve it later through
`take_releases()`, drained after every on_end at the current time; `finish()`
resolves what is still deferred when the events run out.

Volume is counted in span units.  A full span costs 1; a compact root record
(reason "root_record", emitted by the record root policy) costs
`sampler.record_cost` spans and the same fraction of the span's bytes.  The cap
and the volume fed back to the sampler are in these units.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field

import numpy as np

from .model import Corpus
from .sampler import RequestState, RunContext, Sampler
from .ttable import Calibrator


@dataclass
class PeriodStats:
    period: int
    t_start: float
    t_end: float
    spans: int = 0
    requests: int = 0
    kept_pre: int = 0        # items the arm wanted to keep
    kept_post: int = 0       # items that passed the collector cap
    cost_pre: float = 0.0    # the same, in span units
    cost_post: float = 0.0
    bytes_post: float = 0.0
    cap: float = 0.0
    dropped: dict = field(default_factory=dict)


@dataclass
class RunResult:
    arm: str
    corpus_name: str
    v_star: float
    cap: float
    n_spans: int
    kept_idx: np.ndarray
    kept_p: np.ndarray
    kept_reason: list[str]
    kept_period: np.ndarray
    kept_time: np.ndarray
    kept_holes: np.ndarray
    kept_cost: np.ndarray
    periods: list[PeriodStats]
    drop_reason: list[str] = field(default_factory=list)    # per span: why it was not kept ("" when kept)
    diagnostics: list[dict] = field(default_factory=list)
    elapsed_s: float = 0.0

    @property
    def kept_mask(self) -> np.ndarray:
        """Spans whose duration reached the collector (full spans and root records)."""
        m = np.zeros(self.n_spans, dtype=bool)
        m[self.kept_idx] = True
        return m

    @property
    def full_mask(self) -> np.ndarray:
        """Spans kept in full (root records excluded)."""
        m = np.zeros(self.n_spans, dtype=bool)
        full = np.fromiter((r != "root_record" for r in self.kept_reason), dtype=bool, count=len(self.kept_reason))
        m[self.kept_idx[full]] = True
        return m

    def p_array(self) -> np.ndarray:
        """Stamped p per span (nan when not kept or when the arm stamps none)."""
        p = np.full(self.n_spans, np.nan)
        p[self.kept_idx] = self.kept_p
        return p


def replay(corpus: Corpus, sampler: Sampler, v_star: float, cap: float, seed: int = 0,
           oracle: Calibrator | None = None) -> RunResult:
    t_wall = time.perf_counter()
    ctx = RunContext(corpus, v_star, cap, oracle, seed)
    sampler.start_run(ctx)
    spans = corpus.spans
    period_len, t0 = corpus.period, corpus.t0
    record_cost = float(getattr(sampler, "record_cost", 0.1))

    events = []
    for s in spans:
        events.append((s.start, 1, s.depth, s.idx))
        events.append((s.start + s.duration, 0, -s.depth, s.idx))
    events.sort()

    reqs: dict[int, RequestState] = {}
    left: dict[int, int] = {}           # per trace: its spans that have not ended (the request is kept until the last one, as an agent does)
    for s in spans:
        left[s.trace_id] = left.get(s.trace_id, 0) + 1
    kept_flag = bytearray(len(spans))
    kept_idx: list[int] = []
    kept_p: list[float] = []
    kept_reason: list[str] = []
    kept_period: list[int] = []
    kept_time: list[float] = []
    kept_cost: list[float] = []
    drop_reason: list[str] = [""] * len(spans)
    periods: list[PeriodStats] = []

    cur = 0
    st = PeriodStats(0, t0, t0 + period_len, cap=cap)
    cost_in_period = 0.0

    def record(idx: int, p: float, reason: str, per: int, when: float, cost: float) -> None:
        nonlocal cost_in_period
        kept_flag[idx] = 1
        kept_idx.append(idx)
        kept_p.append(p)
        kept_reason.append(reason)
        kept_period.append(per)
        kept_time.append(when)
        kept_cost.append(cost)
        cost_in_period += cost
        st.kept_post += 1
        st.cost_post += cost
        st.bytes_post += spans[idx].nbytes * cost

    def admit(idx: int, p: float, reason: str, per: int, when: float) -> None:
        cost = record_cost if reason == "root_record" else 1.0
        st.kept_pre += 1
        st.cost_pre += cost
        if cost_in_period + cost > cap + 1e-9:          # the collector admits at most `cap` span units per period
            st.dropped["cap"] = st.dropped.get("cap", 0) + 1
            drop_reason[idx] = "cap:" + reason       # what the span would have been kept for
        else:
            record(idx, p, reason, per, when, cost)
            drop_reason[idx] = ""

    def finalize(per: int) -> None:
        nonlocal st, cost_in_period
        late = sampler.flush_period(per)
        t_end = t0 + (per + 1) * period_len
        for idx, p, reason in late:
            admit(idx, p, reason, per, t_end)
        take = getattr(sampler, "on_count_record", None)
        if take is not None:
            for svc, host, n in getattr(corpus, "count_records", {}).get(per, ()):
                take(svc, host, n)
        sampler.on_period_end(per, cost_in_period)
        periods.append(st)
        cost_in_period = 0.0
        st = PeriodStats(per + 1, t_end, t_end + period_len, cap=cap)

    for t, kind, _, idx in events:
        per = int((t - t0) // period_len)
        while per > cur:
            finalize(cur)
            cur += 1
        s = spans[idx]
        if kind == 1:
            if s.parent_idx < 0:
                req = RequestState(s.trace_id, s.route, s.req_class)
                reqs[s.trace_id] = req
                st.requests += 1
                sampler.on_root_start(s, req, t)
            else:
                req = reqs.get(s.trace_id)
                if req is None:                       # orphan (loaded corpora): treat as its own request
                    req = RequestState(s.trace_id, s.route, s.req_class)
                    reqs[s.trace_id] = req
                    sampler.on_root_start(s, req, t)
                sampler.on_start(s, req, t)
        else:
            req = reqs.get(s.trace_id)
            if req is None:
                req = RequestState(s.trace_id, s.route, s.req_class)
                reqs[s.trace_id] = req
            keep, p, reason = sampler.on_end(s, req, t)
            st.spans += 1
            if keep:
                admit(idx, p, reason, per, t)
            elif reason == "defer":
                drop_reason[idx] = "defer"
            else:
                st.dropped[reason] = st.dropped.get(reason, 0) + 1
                drop_reason[idx] = reason
            for ridx, rkeep, rp, rreason in sampler.take_releases():
                if rkeep:
                    admit(ridx, rp, rreason, per, t)
                else:
                    st.dropped[rreason] = st.dropped.get(rreason, 0) + 1
                    drop_reason[ridx] = rreason
            left[s.trace_id] -= 1
            if left[s.trace_id] == 0:
                reqs.pop(s.trace_id, None)
    t_last = events[-1][0] if events else t0
    for ridx, rkeep, rp, rreason in sampler.finish():
        if rkeep:
            admit(ridx, rp, rreason, cur, t_last)
        else:
            st.dropped[rreason] = st.dropped.get(rreason, 0) + 1
            drop_reason[ridx] = rreason
    finalize(cur)

    kept_arr = np.asarray(kept_idx, dtype=np.int64)
    holes = np.zeros(len(kept_arr), dtype=np.int32)
    for k, idx in enumerate(kept_idx):
        ch = spans[idx].children
        if ch:
            holes[k] = sum(1 for c in ch if not kept_flag[c])

    return RunResult(
        arm=sampler.name, corpus_name=corpus.name, v_star=v_star, cap=cap, n_spans=len(spans),
        kept_idx=kept_arr, kept_p=np.asarray(kept_p, dtype=float), kept_reason=kept_reason,
        kept_period=np.asarray(kept_period, dtype=np.int64), kept_time=np.asarray(kept_time, dtype=float),
        kept_holes=holes, kept_cost=np.asarray(kept_cost, dtype=float), periods=periods,
        drop_reason=drop_reason, diagnostics=sampler.diagnostics(), elapsed_s=time.perf_counter() - t_wall,
    )
