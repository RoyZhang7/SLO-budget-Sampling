"""Data model shared by the generator, the loader, the sampler and the metrics."""
from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass(slots=True)
class Span:
    idx: int                # dense index into Corpus.spans
    span_id: int
    trace_id: int
    parent_idx: int         # -1 for the root span
    service: str
    host: str
    start: float            # seconds, true (skew-free) clock
    duration: float         # seconds, inclusive of callees
    status_ok: bool
    depth: int              # root = 0
    route: str              # the request's route (root operation)
    req_class: str          # route + payload-size bucket, e.g. "compose_post|large"
    nbytes: int             # modeled serialized size
    transit: float = 0.0    # one-way network time of the call that created this span
    own: float = 0.0        # own (self) time, when known (generator / complete traces)
    children: list = field(default_factory=list)   # child span indexes
    wait: float = 0.0       # from the request's arrival at the pod to this span's start (0: no arrival known, or a later one)
    dropped_before: int = 0  # ordinary spans of the service an in-service filter dropped before this one (they count as seen)

    @property
    def end(self) -> float:
        return self.start + self.duration

    @property
    def is_root(self) -> bool:
        return self.parent_idx < 0


@dataclass
class CallGraph:
    """Service-level call structure per route, learned from complete traces or
    declared by the generator.  A service appearing at several positions of a
    route is one node; children are the union of its callees.

    `stages` gives, per (route, service), the service's callees as sequential
    stages: callees in one stage run concurrently, a stage starts when the one
    before it has finished.  A service without stages follows the parallel flag
    (one stage with every callee, or one stage per callee)."""
    root: dict[str, str]                               # route -> root service
    children: dict[tuple[str, str], list[str]]         # (route, service) -> callee services
    parallel: dict[tuple[str, str], bool]              # (route, service) -> callees run concurrently
    stages: dict[tuple[str, str], list[list[str]]] = field(default_factory=dict)     # (route, service) -> callees by stage

    def groups(self, route: str, service: str, kids: list[str]) -> list[list[str]]:
        """The callees `kids` of a service (a sublist, in the order given) as its sequential
        stages.  A callee that no stage names is a stage of its own, after the others."""
        st = self.stages.get((route, service))
        if st is None:
            return [list(kids)] if kids and self.parallel.get((route, service), False) else [[c] for c in kids]
        named = {c for group in st for c in group}
        out = [[c for c in kids if c in group] for group in st]
        return [g for g in out if g] + [[c] for c in kids if c not in named]

    def services(self, route: str) -> list[str]:
        out, seen, stack = [], set(), [self.root[route]]
        while stack:
            s = stack.pop()
            if s in seen:
                continue
            seen.add(s)
            out.append(s)
            stack.extend(self.children.get((route, s), []))
        return out

    def routes(self) -> list[str]:
        return list(self.root)

    @staticmethod
    def learn(spans: list["Span"]) -> "CallGraph":
        """Derive the graph from complete traces: edges from parent/child pairs,
        parallel flag when a parent's children overlap in time in most instances."""
        root: dict[str, str] = {}
        kids: dict[tuple[str, str], set[str]] = {}
        overlap: dict[tuple[str, str], list[int]] = {}
        for s in spans:
            if s.is_root:
                root.setdefault(s.route, s.service)
            if not s.children:
                continue
            key = (s.route, s.service)
            cs = [spans[c] for c in s.children]
            kids.setdefault(key, set()).update(c.service for c in cs)
            if len(cs) >= 2:
                cs.sort(key=lambda c: c.start)
                ov = any(cs[i + 1].start < cs[i].end for i in range(len(cs) - 1))
                overlap.setdefault(key, []).append(1 if ov else 0)
        children = {k: sorted(v) for k, v in kids.items()}
        parallel = {k: (sum(v) * 2 > len(v)) for k, v in overlap.items()}
        return CallGraph(root, children, parallel)


@dataclass
class FaultTruth:
    kind: str                       # saturate | cascade | late_all | none
    services: tuple[str, ...]       # services whose own time was inflated
    start: float                    # injection time (s)
    culprit: str | None             # the single root cause, None when systemic
    onsets: dict = field(default_factory=dict)     # service -> (onset, end) of its injected own-time factor (> 1)


@dataclass
class Corpus:
    name: str
    spans: list[Span]
    roots: list[int]
    routes_slo: dict[str, float]            # route -> end-to-end SLO (s)
    graph: CallGraph
    hosts: list[str]
    host_skew: dict[str, float]             # host clock offset (s), used by the deadline policy
    period: float                           # period length (s)
    t0: float
    fault: FaultTruth | None = None
    meta: dict = field(default_factory=dict)
    # an in-service filter's count records: period -> [(service, host, dropped)], handed to the sampler at that period's end
    count_records: dict[int, list[tuple[str, str, int]]] = field(default_factory=dict)

    @property
    def t_end(self) -> float:
        return max(s.end for s in self.spans) if self.spans else self.t0

    @property
    def n_periods(self) -> int:
        return int(math.floor((self.t_end - self.t0) / self.period)) + 1

    def period_of(self, t: float) -> int:
        return int((t - self.t0) // self.period)

    def services(self) -> list[str]:
        return sorted({s.service for s in self.spans})

    def finalize(self) -> None:
        """Fill children lists, depths and own times from parent links."""
        for s in self.spans:
            s.children = []
        for s in self.spans:
            if s.parent_idx >= 0:
                self.spans[s.parent_idx].children.append(s.idx)
        for r in self.roots:
            stack = [(r, 0)]
            while stack:
                i, d = stack.pop()
                sp = self.spans[i]
                sp.depth = d
                stack.extend((c, d + 1) for c in sp.children)
        for s in self.spans:
            if s.own > 0.0:
                continue                     # generator truth already present
            if s.children:
                s.own = max(0.0, s.duration - _union_length([(self.spans[c].start, self.spans[c].end) for c in s.children]))
            else:
                s.own = s.duration


def _union_length(intervals: list[tuple[float, float]]) -> float:
    intervals.sort()
    total, cur_s, cur_e = 0.0, intervals[0][0], intervals[0][1]
    for a, b in intervals[1:]:
        if a > cur_e:
            total += cur_e - cur_s
            cur_s, cur_e = a, b
        elif b > cur_e:
            cur_e = b
    return total + (cur_e - cur_s)
