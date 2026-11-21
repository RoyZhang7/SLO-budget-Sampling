"""Synthetic DSB-SN-like workloads for the scenarios the trace corpus lacks.

The workload (call trees, fan-out, own-time and transit distributions) comes
from `workload_dsbsn.json`, derived from real traces of the bridged deployment
by `derive.py`.  Structure is from the traces; timings are a lognormal fit per
service (the JSON's `timings` field says whether they are provisional, from the
200-trace pull, or fitted from the 10-minute export under load).  Routes the export did not
contain (the two read routes) are hand-written from the DSB-SN source and only
used when a scenario asks for them.

A request picks a route, a payload-size bucket and an arrival time.  Its spans
are instantiated from the route's call tree: own time per service is lognormal
(median and sigma per service), multiplied by the fault factors active at the
arrival time (saturation, cascade, late-everywhere) and by the size-bucket
factor.  Callees run sequentially except under the services the traces show
fanning out in parallel.  Every hop has a one-way transit time (median per
callee) and every host a clock skew (used only by the deadline escalation
policy).

The SLO per route is `slo_factor` times the healthy P95 end-to-end latency,
measured on a reference run of the same workload without faults or bursts.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from .model import CallGraph, Corpus, FaultTruth, Span
from .tailfit import draw as draw_dist, drawer as dist_drawer, lognormal_spec
from .ttable import Calibrator, split_budgets, weighted_quantile

# ---------------------------------------------------------------------------
# Workload: structure from real traces (derive.py), provisional timings
# ---------------------------------------------------------------------------
WORKLOAD_PATH = Path(__file__).with_name("workload_dsbsn.json")

# routes the export did not contain: DSB-SN source structure, timings reuse the
# per-service fits above; not in the default route mix
PROVISIONAL_ROUTES = {
    "read_home_timeline": ("wrk2api", [("hometimeline", [("poststorage", [])])]),
    "read_user_timeline": ("wrk2api", [("usertimeline", [("poststorage", [])])]),
}


def _tuple_tree(node) -> tuple:
    svc, kids = node
    return (svc, [_tuple_tree(k) for k in kids])


def load_workload(path: Path | str | None = None) -> dict:
    with open(path or WORKLOAD_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


WORKLOAD = load_workload()
# named variants of the workload: the same structure, other timings for some services; a
# service entry's `dist` object replaces its lognormal draw (tailfit.py).  A variant whose
# JSON names a `reference_variant` has its healthy reference (SLO, oracle T) generated from
# that workload instead of itself: the deployment drifted from what the SLO was derived from.
#   heavytail   usertimeline from the heavy-tailed refit of the 10-minute export (tailfit)
#   drift       the deployment as it ran during the Stage 2 cluster runs (tools/drift_workload.py
#               from run 3's plan-kept spans: every service's level, usertimeline's tail),
#               reference = fitted: the SLO and the oracle were derived before the drift
WORKLOAD_VARIANT_PATHS = {"heavytail": Path(__file__).with_name("workload_dsbsn_heavytail.json"),
                          "drift": Path(__file__).with_name("workload_dsbsn_drift.json")}
_VARIANTS: dict[str, dict] = {}


def workload_variant(name: str) -> dict:
    if name in ("", "fitted"):
        return WORKLOAD
    if name not in _VARIANTS:
        if name not in WORKLOAD_VARIANT_PATHS:
            raise KeyError(f"unknown workload variant {name}; choose from {tuple(WORKLOAD_VARIANT_PATHS)}")
        _VARIANTS[name] = load_workload(WORKLOAD_VARIANT_PATHS[name])
    return _VARIANTS[name]


def workload_id(wl: dict) -> str:
    return hashlib.sha1(json.dumps({k: wl[k] for k in ("routes", "parallel", "services")},
                                   sort_keys=True).encode()).hexdigest()[:8]


ROUTE_TREES: dict[str, tuple] = {r: _tuple_tree(info["tree"]) for r, info in WORKLOAD["routes"].items()}
for _r, _t in PROVISIONAL_ROUTES.items():
    ROUTE_TREES.setdefault(_r, _t)
DEFAULT_ROUTE_MIX: dict[str, float] = {r: info["share"] for r, info in WORKLOAD["routes"].items()}
MEDIAN_MS: dict[str, float] = {s: v["median_ms"] for s, v in WORKLOAD["services"].items()}
SIGMA: dict[str, float] = {s: v["sigma"] for s, v in WORKLOAD["services"].items()}
_transits = [v["transit_ms"] for v in WORKLOAD["services"].values() if v.get("transit_ms")]
DEFAULT_TRANSIT_MS: float = float(np.median(_transits)) if _transits else 0.5
TRANSIT_MS: dict[str, float] = {s: (v.get("transit_ms") or DEFAULT_TRANSIT_MS) for s, v in WORKLOAD["services"].items()}
PARALLEL = frozenset(WORKLOAD["parallel"])
# the paper's canonical placement (node-1..node-8 hold services, wrk2api is the gateway daemonset)
PLACEMENT = {
    "wrk2api": "node-gw", "composepost": "node-1", "hometimeline": "node-1", "socialgraph": "node-2",
    "uniqueid": "node-3", "usermention": "node-3", "usertimeline": "node-4", "media": "node-5",
    "text": "node-5", "userid": "node-5", "poststorage": "node-6", "urlshorten": "node-7", "user": "node-8",
}
ALL_SERVICES = tuple(MEDIAN_MS)
# identity of the workload the corpora are generated from (structure and timings, not the
# labels) and the version of each scenario's tuning; result rows carry both as `tag` so
# that a resumed experiment reruns the rows that no longer match
WORKLOAD_ID = workload_id(WORKLOAD)
SCENARIO_VERSION = {"late_everywhere": 2}      # pass 4: equal-rate tuning (v1: own-time P95 at 1.6 x stage budget)
SCENARIO_SUFFIX = {"ht": "heavytail", "dr": "drift"}         # `steady+ht` = steady on the heavytail variant


def split_scenario(name: str) -> tuple[str, str]:
    """`steady+ht` -> ("steady", "heavytail"); `steady` -> ("steady", "fitted")."""
    base, _, suffix = name.partition("+")
    if not suffix:
        return base, "fitted"
    if suffix not in SCENARIO_SUFFIX:
        raise KeyError(f"unknown scenario suffix +{suffix}; choose from {tuple(SCENARIO_SUFFIX)}")
    return base, SCENARIO_SUFFIX[suffix]


def scenario_tag(name: str) -> str:
    base, variant = split_scenario(name)
    wid = WORKLOAD_ID if variant == "fitted" else workload_id(workload_variant(variant))
    return f"{wid}/{name}v{SCENARIO_VERSION.get(base, 1)}"


@dataclass
class FaultSpec:
    kind: str                                   # saturate | cascade | late_all
    start: float
    entries: list[tuple[str, float, float]]     # (service, delay after start, own-time factor)
    end: float | None = None                    # the fault lifts at `end` (None: lasts to the end of the run)

    def factors(self, t: float) -> dict[str, float]:
        out: dict[str, float] = {}
        if self.end is not None and t >= self.end:
            return out
        for svc, delay, f in self.entries:
            if t >= self.start + delay:
                out[svc] = out.get(svc, 1.0) * f
        return out


@dataclass
class ScenarioConfig:
    name: str
    duration: float = 90.0
    rate: float = 250.0                         # requests per second
    period: float = 5.0
    route_mix: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_ROUTE_MIX))
    size_mix: dict[str, dict[str, float]] = field(default_factory=dict)         # route -> {bucket: prob}
    size_factor: dict[tuple[str, str], float] = field(default_factory=dict)     # (bucket, service) -> factor
    faults: list[FaultSpec] = field(default_factory=list)
    bursts: list[tuple[float, float, float]] = field(default_factory=list)      # (start, end, rate multiplier)
    burst_latency: float = 0.0                  # own-time factor per unit of rate multiple above 1
    skew_ms: float = 1.0                        # host clock offsets ~ U(-skew, +skew)
    transit_ms: float = DEFAULT_TRANSIT_MS      # fallback one-way transit for a callee without a fit
    transits_ms: dict[str, float] = field(default_factory=lambda: dict(TRANSIT_MS))   # per callee
    error_rate: float = 0.001                   # provisional: the export had no errors
    sigma: float | None = None                  # overrides every service's own-time sigma when set
    sigmas: dict[str, float] = field(default_factory=lambda: dict(SIGMA))
    slo_factor: float = 1.5
    late_share: float = 1.6                     # late_everywhere_v1: stage own-time P95 = late_share x stage budget
    late_tuning: str = "rate"                   # late_everywhere: rate (every service at late_rate) | share (v1)
    late_rate: float = 0.25                     # late_everywhere: truth outlier rate of every service during the fault
    nbytes: int = 550                           # modeled serialized size (export median 562 bytes)
    seed: int = 1
    culprit: str | None = None
    fault_kind: str = "none"
    medians_ms: dict[str, float] = field(default_factory=lambda: dict(MEDIAN_MS))
    workload: str = "fitted"                    # workload variant the timings come from
    dists: dict[str, dict] = field(default_factory=dict)    # service -> own-time `dist` spec (tailfit); lognormal when absent

    @property
    def mean_spans_per_request(self) -> float:
        return sum(p * _count(ROUTE_TREES[r]) for r, p in self.route_mix.items())


def _count(node) -> int:
    return 1 + sum(_count(c) for c in node[1])


def declared_graph(route_mix: dict[str, float]) -> CallGraph:
    root, children, parallel = {}, {}, {}
    for route in route_mix:
        tree = ROUTE_TREES[route]
        root[route] = tree[0]
        stack = [tree]
        while stack:
            svc, kids = stack.pop()
            key = (route, svc)
            children.setdefault(key, [])
            for k in kids:
                if k[0] not in children[key]:
                    children[key].append(k[0])
            parallel[key] = svc in PARALLEL
            stack.extend(kids)
        for key in children:
            children[key].sort()
    return CallGraph(root, children, parallel)


def _rate_mult(t: float, bursts) -> float:
    for a, b, m in bursts:
        if a <= t < b:
            return m
    return 1.0


class _Pool:
    """Cheap sequential access to pre-drawn random numbers."""
    def __init__(self, draw, n: int):
        self.draw, self.n = draw, n
        self.buf = draw(n)
        self.i = 0

    def next(self) -> float:
        if self.i >= len(self.buf):
            self.buf = self.draw(self.n)
            self.i = 0
        v = self.buf[self.i]
        self.i += 1
        return v


def generate(cfg: ScenarioConfig, routes_slo: dict[str, float] | None = None, healthy: bool = False,
             seed_offset: int = 0, name: str | None = None) -> Corpus:
    rng = np.random.default_rng(cfg.seed + seed_offset)
    faults = [] if healthy else cfg.faults
    bursts = [] if healthy else cfg.bursts

    # arrivals: Poisson with piecewise-constant rate
    slot, t, chunks = 0.25, 0.0, []
    while t < cfg.duration:
        n = rng.poisson(cfg.rate * _rate_mult(t, bursts) * slot)
        if n:
            chunks.append(t + rng.random(n) * slot)
        t += slot
    arrivals = np.sort(np.concatenate(chunks)) if chunks else np.zeros(0)
    N = len(arrivals)
    routes = list(cfg.route_mix)
    probs = np.array([cfg.route_mix[r] for r in routes], dtype=float)
    probs /= probs.sum()
    route_of = rng.choice(len(routes), size=N, p=probs)

    est = int(N * cfg.mean_spans_per_request * 1.2) + 1000
    z = _Pool(rng.standard_normal, est)
    trf = _Pool(lambda n: rng.lognormal(0.0, 0.3, n), est)       # transit = callee median x lognormal factor
    u = _Pool(rng.random, est)
    # uniforms for the non-lognormal own-time draws of a workload variant; created after the
    # three pools above and only when needed, so the fitted workload's corpora do not change
    ud = _Pool(rng.random, est // 8 + 1000) if cfg.dists else None

    skew_rng = np.random.default_rng(cfg.seed + 7)
    hosts = sorted(set(PLACEMENT.values()))
    skew = {h: float(skew_rng.uniform(-cfg.skew_ms, cfg.skew_ms) / 1000.0) for h in hosts}

    medians = {s: m / 1000.0 for s, m in cfg.medians_ms.items()}
    sig = {s: (cfg.sigma if cfg.sigma is not None else cfg.sigmas.get(s, 0.4)) for s in medians}
    # own-time draw per service (seconds): lognormal `median * exp(sigma * z)` unless the
    # workload variant gives the service a `dist` (cfg.sigma does not apply to those)
    own_of = {s: dist_drawer(cfg.dists.get(s) or lognormal_spec(cfg.medians_ms[s], sig[s]), z.next,
                             ud.next if ud is not None else None) for s in medians}
    transit = {s: cfg.transits_ms.get(s, cfg.transit_ms) / 1000.0 for s in medians}
    err = cfg.error_rate
    spans: list[Span] = []
    roots: list[int] = []
    next_id = 1

    def build(node, parent_idx, depth, start, trace_id, t_hop_in, route, cls, fac):
        nonlocal next_id
        service, kids = node
        own = own_of[service]() * fac.get(service, 1.0)
        idx = len(spans)
        sp = Span(idx, next_id, trace_id, parent_idx, service, PLACEMENT.get(service, "node-x"), start, 0.0,
                  u.next() >= err, depth, route, cls, cfg.nbytes + 2 * len(service), t_hop_in, own)
        next_id += 1
        spans.append(sp)
        pre = 0.3 * own
        if not kids:
            sp.duration = own
            return start + own
        if service in PARALLEL:
            end_max = start + pre
            for k in kids:
                t_hop = transit.get(k[0], cfg.transit_ms / 1000.0) * trf.next()
                c_end = build(k, idx, depth + 1, start + pre + t_hop, trace_id, t_hop, route, cls, fac)
                end_max = max(end_max, c_end + t_hop)
            sp.duration = own + (end_max - (start + pre))
        else:
            cur = start + pre
            for k in kids:
                t_hop = transit.get(k[0], cfg.transit_ms / 1000.0) * trf.next()
                c_end = build(k, idx, depth + 1, cur + t_hop, trace_id, t_hop, route, cls, fac)
                cur = c_end + t_hop
            sp.duration = own + (cur - (start + pre))
        return start + sp.duration

    for k in range(N):
        t_arr = float(arrivals[k])
        route = routes[route_of[k]]
        mix = cfg.size_mix.get(route)
        if mix:
            r, bucket = u.next(), None
            acc = 0.0
            for b, pb in mix.items():
                acc += pb
                bucket = b
                if r < acc:
                    break
        else:
            bucket = "any"
        fac: dict[str, float] = {}
        for fs in faults:
            for svc, f in fs.factors(t_arr).items():
                fac[svc] = fac.get(svc, 1.0) * f
        if bursts and cfg.burst_latency:
            m = _rate_mult(t_arr, bursts)
            if m > 1.0:
                g = 1.0 + cfg.burst_latency * (m - 1.0)
                for svc in ALL_SERVICES:
                    fac[svc] = fac.get(svc, 1.0) * g
        if mix:
            for (b, svc), f in cfg.size_factor.items():
                if b == bucket:
                    fac[svc] = fac.get(svc, 1.0) * f
        roots.append(len(spans))
        build(ROUTE_TREES[route], -1, 0, t_arr, k + 1, 0.0, route, f"{route}|{bucket}", fac)

    graph = declared_graph(cfg.route_mix)
    corpus = Corpus(name or cfg.name, spans, roots, {}, graph, hosts, skew, cfg.period, 0.0,
                    meta={"rate": cfg.rate, "duration": cfg.duration, "requests": N,
                          "mean_spans_per_request": cfg.mean_spans_per_request,
                          "workload": WORKLOAD.get("source", ""), "variant": cfg.workload,
                          "timings": workload_variant(cfg.workload).get("timings", "")})
    corpus.finalize()
    if routes_slo is None:
        routes_slo = {}
        for route in routes:
            d = np.array([spans[r].duration for r in roots if spans[r].route == route])
            routes_slo[route] = float(np.quantile(d, 0.95) * cfg.slo_factor) if len(d) else 1.0
    corpus.routes_slo = dict(routes_slo)
    if faults:
        services = tuple(sorted({svc for fs in faults for svc, _, _ in fs.entries}))
        corpus.fault = FaultTruth(cfg.fault_kind, services, min(fs.start for fs in faults), cfg.culprit)
    return corpus


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------
SCENARIOS = ("steady", "saturated", "cascade", "bursty", "cold_start", "late_everywhere", "mixed_size")
SCENARIO_VARIANTS = ("late_everywhere_v1",)      # named variants outside the default set
HEAVYTAIL_SCENARIOS = tuple(f"{s}+ht" for s in SCENARIOS)    # the default set on the heavytail workload variant
DRIFT_SCENARIOS = tuple(f"{s}+dr" for s in SCENARIOS)        # ... and on the drift variant
ALL_SCENARIOS = SCENARIOS + SCENARIO_VARIANTS + HEAVYTAIL_SCENARIOS + DRIFT_SCENARIOS


def scenario(name: str, seed: int = 1, **overrides) -> ScenarioConfig:
    base, variant = split_scenario(name)
    cfg = ScenarioConfig(name=name, seed=seed)
    if variant != "fitted":
        _apply_workload(cfg, variant)
    name = base
    if name == "steady":
        pass
    elif name == "saturated":
        cfg.faults = [FaultSpec("saturate", 30.0, [("socialgraph", 0.0, 5.0)])]
        cfg.culprit, cfg.fault_kind = "socialgraph", "saturate"
    elif name == "cascade":
        # the datastore slows first; its callers degrade a few seconds later (retries, waiting)
        # factors re-tuned to the real medians (poststorage 0.8 ms, its callers 2.7 / 5.1 / 4.6 ms): the
        # datastore adds the most latency, the callers' waiting is secondary
        cfg.faults = [FaultSpec("cascade", 30.0, [("poststorage", 0.0, 6.0), ("hometimeline", 4.0, 1.3),
                                                   ("usertimeline", 4.0, 1.3), ("composepost", 8.0, 1.2)])]
        cfg.culprit, cfg.fault_kind = "poststorage", "cascade"
    elif name == "bursty":
        cfg.bursts = [(20.0, 25.0, 3.0), (40.0, 45.0, 3.0), (60.0, 65.0, 3.0)]
    elif name == "cold_start":
        cfg.faults = [FaultSpec("saturate", 0.0, [("socialgraph", 0.0, 5.0)])]
        cfg.culprit, cfg.fault_kind = "socialgraph", "saturate"
    elif name in ("late_everywhere", "late_everywhere_v1"):
        # per-service factors are filled in by build_scenario from the healthy
        # reference.  late_everywhere (pass 4): every service's truth outlier rate
        # lands at late_rate, so no stage stands out and the oracle reads the fault
        # as systemic while requests miss the SLO.  late_everywhere_v1 (pass 3):
        # every stage's own-time P95 at late_share x its stage budget, which on the
        # real timings let the low-variance usertimeline stage dominate
        cfg.faults = [FaultSpec("late_all", 30.0, [(s, 0.0, 1.0) for s in ALL_SERVICES])]
        cfg.culprit, cfg.fault_kind = None, "late_all"
        cfg.late_tuning = "share" if name.endswith("_v1") else "rate"
    elif name == "mixed_size":
        cfg.size_mix = {"compose_post": {"small": 0.7, "large": 0.3}}
        cfg.size_factor = {("large", "text"): 3.0, ("large", "media"): 3.0, ("large", "poststorage"): 2.5}
        cfg.faults = [FaultSpec("saturate", 30.0, [("urlshorten", 0.0, 4.0)])]
        cfg.culprit, cfg.fault_kind = "urlshorten", "saturate"
    else:
        raise KeyError(f"unknown scenario {name}; choose from {ALL_SCENARIOS}")
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


LATE_SHARE = 1.6          # late_everywhere: own-time P95 of every stage = 1.6 x its stage budget


def late_everywhere_factors(reference: Corpus, route: str = "compose_post", share: float = LATE_SHARE,
                            q: float = 0.95) -> dict[str, float]:
    """Own-time factor per service that puts each stage's P95 own time at
    `share` times its square-root-split stage budget on the given route."""
    cal = Calibrator(reference.graph, reference.routes_slo, q=q, window=1, use_class=False)
    for s in reference.spans:
        cal.observe(s, 1.0)
    cal.end_period()
    L = {svc: v for (r, c, svc), v in cal.L.items() if r == route}
    O = {svc: v for (r, c, svc), v in cal.O.items() if r == route}
    T, b = split_budgets(reference.routes_slo[route], L, reference.graph, route, O)
    return {svc: max(1.0, share * b[svc] / O[svc]) for svc in b if svc in O and O[svc] > 0}


def late_everywhere_rate_factors(cfg: ScenarioConfig, reference: Corpus, route: str = "compose_post",
                                 target: float | None = None, n: int = 20000, seed: int = 11) -> dict[str, float]:
    """Own-time factor per service that puts every service's truth outlier rate
    (inclusive duration over its T_i from the healthy reference) at `target`
    during the fault, so the slowdown is spread evenly and the oracle reads it
    as systemic.  Solved bottom-up by bisection on a Monte-Carlo replica of the
    generator's duration arithmetic (own time x factor plus callees and
    transits, sum or max under a parallel fan-out); callees are solved before
    their caller and a factor never goes below 1 (a caller whose inflated
    callees alone push it over T_i keeps factor 1)."""
    target = cfg.late_rate if target is None else target
    cal = Calibrator(reference.graph, reference.routes_slo, q=0.95, window=1, use_class=False)
    for s in reference.spans:
        cal.observe(s, 1.0)
    cal.end_period()
    T = {svc: v for (r, c, svc), v in cal.table.items() if r == route}
    rng = np.random.default_rng(seed)
    med = {s: m / 1000.0 for s, m in cfg.medians_ms.items()}
    sig = {s: (cfg.sigma if cfg.sigma is not None else cfg.sigmas.get(s, 0.4)) for s in med}
    transit = {s: cfg.transits_ms.get(s, cfg.transit_ms) / 1000.0 for s in med}
    factors: dict[str, float] = {}

    def own_draw(svc: str) -> np.ndarray:
        spec = cfg.dists.get(svc)
        if spec is None:
            return med[svc] * np.exp(sig[svc] * rng.standard_normal(n))
        return draw_dist(spec, rng, n) / 1000.0

    def solve(node) -> np.ndarray:
        svc, kids = node
        parts = []
        for k in kids:
            d = solve(k)
            hop = transit.get(k[0], cfg.transit_ms / 1000.0) * rng.lognormal(0.0, 0.3, n)
            parts.append(2.0 * hop + d)
        rest = (np.max(parts, axis=0) if svc in PARALLEL else np.sum(parts, axis=0)) if parts else np.zeros(n)
        own = own_draw(svc)
        Ts = T.get(svc)
        if svc not in factors and Ts is not None:
            if np.mean(own + rest > Ts) >= target:
                factors[svc] = 1.0
            else:
                lo, hi = 0.0, math.log(1000.0)          # log factor; the rate is monotone in it
                for _ in range(40):
                    mid = 0.5 * (lo + hi)
                    if np.mean(own * math.exp(mid) + rest > Ts) < target:
                        lo = mid
                    else:
                        hi = mid
                factors[svc] = math.exp(0.5 * (lo + hi))
        return own * factors.get(svc, 1.0) + rest

    solve(ROUTE_TREES[route])
    return factors


def _apply_workload(cfg: ScenarioConfig, variant: str) -> ScenarioConfig:
    """Timings (medians, sigmas, transits, dist specs) of a workload variant into cfg."""
    wl = workload_variant(variant)
    cfg.workload = variant
    cfg.medians_ms = {s: v["median_ms"] for s, v in wl["services"].items()}
    cfg.sigmas = {s: v["sigma"] for s, v in wl["services"].items()}
    cfg.transits_ms = {s: (v.get("transit_ms") or DEFAULT_TRANSIT_MS) for s, v in wl["services"].items()}
    cfg.dists = {s: dict(v["dist"]) for s, v in wl["services"].items() if v.get("dist")}
    return cfg


def build_scenario(name: str, seed: int = 1, **overrides) -> tuple[Corpus, Corpus]:
    """(corpus, healthy reference).  The reference fixes the SLOs and is what
    the ground-truth T table is calibrated from.  A workload variant that names a
    `reference_variant` gets its reference from that workload (the SLO and the
    oracle were derived before the deployment drifted)."""
    cfg = scenario(name, seed, **overrides)
    ref_variant = workload_variant(cfg.workload).get("reference_variant", cfg.workload)
    ref_cfg = cfg
    if ref_variant != cfg.workload:
        ref_cfg = _apply_workload(scenario(name, seed, **overrides), ref_variant)
    reference = generate(ref_cfg, healthy=True, seed_offset=7919, name=f"{name}-reference")
    if cfg.fault_kind == "late_all" and all(f == 1.0 for fs in cfg.faults for _, _, f in fs.entries):
        if cfg.late_tuning == "share":
            factors = late_everywhere_factors(reference, share=cfg.late_share)
        else:
            factors = late_everywhere_rate_factors(cfg, reference, target=cfg.late_rate)
        for fs in cfg.faults:
            fs.entries = [(svc, delay, factors.get(svc, 1.0)) for svc, delay, _ in fs.entries]
    corpus = generate(cfg, routes_slo=reference.routes_slo, name=name)
    corpus.meta["tag"] = reference.meta["tag"] = scenario_tag(name)
    return corpus, reference
