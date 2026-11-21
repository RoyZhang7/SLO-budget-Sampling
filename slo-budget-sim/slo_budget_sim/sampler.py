"""The sampler interface and the chosen mechanism.

The interface is deliberately small so that a Go collector processor can mirror
it one to one:

    start_run(ctx)                         table, budget and pools initialised
    on_root_start(span, req, t)            budget issued into baggage, deadline set
    on_start(span, req, t)                 escalation check (reads baggage)
    on_end(span, req, t) -> keep, p, why   the decider
    flush_period(period) -> late keeps     only for arms that decide at period end
    on_period_end(period, volume)          refill, recalibrate, reset counters

`req` models the OTel baggage of one request: the integer budget, the root
service, the request class, the response-path flag ("a span of this request
was kept"), the credit spans used and (optionally) the deadline / escalate flag.

Pluggable parts (BudgetConfig):

  root_policy     pin        the root is always kept at p = 1, outside the budget
                  on_demand  the root is kept at p = 1 only if the flag arrived;
                             otherwise it is an ordinary span (outlier test, plan)
                  record     a compact record (trace id, root service, class,
                             start, duration, status) is always emitted at
                             `record_cost` of a span; the full root follows on_demand
                  plan       no pinning: the root is an ordinary span
  calib_source    kept       T_i from every kept span, 1/p-weighted
                  plan       T_i from plan-sampled ordinary spans (and roots kept at p = 1)
                  frozen     as kept, but a service whose outlier rate in the period
                             just ended is `freeze_rate_mult` x its healthy rate keeps
                             its previous calibration (L, O); its samples from frozen
                             periods never enter the window (`freeze_trigger=count`
                             is the pass-2 rule: over N_need)
                  oracle     the ground-truth table (ceiling)
  n_need_mode     fixed      N_need = n_need
                  relative   N_need = max(n_min, floor(n_need_frac x V* / services))
  cold_prior      none       no T_i before a service's first calibrated period: plan sampling only
                  equal      the equal-share square-root split of the SLO stands in for T_i until
                             the service has been calibrated once (ttable.equal_share_prior)
  outlier_margin  m          a span is an outlier only when its duration exceeds (1 + m) T_i (or
                             its status is not OK); spans between T_i and (1 + m) T_i are ordinary
                             (plan-sampled).  0 = the plain rule.  Pass 5 candidate rule (exp14)
  n_need_healthy  False/True a span over T_i is still an outlier for keeping, but the N_need
                             counter and the frozen trigger count only the misses beyond the
                             service's healthy over-T rate (the EWMA kept while not frozen):
                             n_eff = n_seen - rate x spans seen so far in the period, and the
                             trigger fires on (rate - healthy) instead of rate.  Pass 5 candidate rule
  calib_q         q          calibration percentile of the split (0.95; exp14 sweeps 0.975 and 0.99)
  outlier_credit  none       an outlier refused by the request budget or the host pool is dropped
                  credit     ... may be kept on credit while the host's credit for the
                             period (credit_frac x pool) lasts; the debt is repaid out of
                             the next period's plan sampling on that host (lower plan rate,
                             p recorded as usual); credit does not roll over
                  reserve    credit_frac x pool is set aside at the start of each period
                             for refused outliers; plan sampling and regular outlier keeps
                             draw from the remainder
                  eligibility, in order: the service is under N_need for the period;
                  below `credit_low_water` of the allowance a coin with p = miss ratio
                  clipped to [0.05, 1] (errors p = 1) is thrown and p recorded; at most
                  `credit_k` credit/reserve spans per request
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass

from .model import Corpus, Span
from .ttable import PRIORS, Calibrator

ROOT_POLICIES = ("pin", "on_demand", "record", "plan")
CALIB_SOURCES = ("kept", "plan", "frozen", "oracle")
CREDIT_POLICIES = ("none", "credit", "reserve")


@dataclass
class RunContext:
    corpus: Corpus
    v_star: float            # volume target: spans per period at the collector
    cap: float               # collector hard cap: spans per period
    oracle: Calibrator | None
    seed: int


class RequestState:
    """Per-request state that rides in baggage (plus a few bookkeeping fields)."""
    __slots__ = ("trace_id", "route", "cls", "budget", "budget0", "budget_by_host",
                 "deadline", "head_keep", "head_p", "flag", "credit_used")

    def __init__(self, trace_id: int, route: str, cls: str):
        self.trace_id = trace_id
        self.route = route
        self.cls = cls
        self.budget = 0
        self.budget0 = 0
        self.budget_by_host = None
        self.deadline = math.inf
        self.head_keep = False
        self.head_p = 1.0
        self.flag = False            # response-path flag: a span of this request was kept
        self.credit_used = 0         # credit / reserve spans this request has used


class Sampler:
    name = "base"
    record_cost = 0.1       # cost of a compact root record, in span units (budget arms set it from config)

    def start_run(self, ctx: RunContext) -> None:
        self.ctx = ctx
        self.rng = random.Random(ctx.seed)
        self._diag: list[dict] = []

    def on_root_start(self, span: Span, req: RequestState, t: float) -> None:
        pass

    def on_start(self, span: Span, req: RequestState, t: float) -> None:
        pass

    def on_end(self, span: Span, req: RequestState, t: float) -> tuple[bool, float, str]:
        raise NotImplementedError

    def flush_period(self, period: int) -> list[tuple[int, float, str]]:
        return []

    def take_releases(self) -> list[tuple[int, bool, float, str]]:
        """Decisions for spans the arm deferred ("defer" from on_end) that it has
        now resolved: (idx, keep, p, reason).  Drained by the replay after every
        on_end, at the current time (tail-based arms decide at the root's end)."""
        return []

    def finish(self) -> list[tuple[int, bool, float, str]]:
        """Resolve whatever is still deferred when the replay ends."""
        return []

    def on_period_end(self, period: int, collector_volume: float) -> None:
        pass

    def diagnostics(self) -> list[dict]:
        return self._diag


# ---------------------------------------------------------------------------
# The chosen mechanism
# ---------------------------------------------------------------------------

@dataclass
class BudgetConfig:
    n_need: float = 15.0            # fixed N_need: outliers kept at p = 1 per (service, period) before p decays
    n_need_mode: str = "fixed"      # fixed | relative  (relative: max(n_min, floor(n_need_frac V* / services)))
    n_need_frac: float = 0.2
    n_min: int = 3
    p_rule: str = "linear"          # const | linear | sqrt | quadratic   (growth with size of miss)
    alpha: float = 0.5              # refill damping
    calib_q: float = 0.95           # calibration percentile for T_i
    calib_window: int = 2           # periods of kept spans used for calibration
    min_class_samples: int = 20     # class key falls back to the route key below this
    use_class: bool = True          # key T_i by (service, root, class) rather than (service, root)
    plan: str = "uniform"           # uniform | sqrt   (ordinary-span plan weights)
    plan_fill: float = 0.5          # ordinary spend per request = plan_fill * (B - expected outlier spend)
    escalation: str = "none"        # none | deadline | duration_sum
    escalate_bypass_budget: bool = False   # escalated spans skip the request budget (host pool still applies)
    budget_scope: str = "shared"    # shared: one counter per request; local: per (request, host)
    keep_parent: bool = False       # also keep the parent of a kept outlier
    unbounded: bool = False         # threshold-only: no budget, no pool, N_need = inf
    calib_source: str = "kept"      # kept | plan | frozen | oracle   (see module docstring)
    table: str | None = None        # deprecated alias: "oracle" -> calib_source oracle, "calibrated" -> kept
    freeze_trigger: str = "rate"    # frozen source: rate (outlier rate vs healthy rate) | count (over N_need)
    freeze_rate_mult: float = 2.0   # freeze when the period's outlier rate >= mult x the service's healthy rate
    freeze_rate_floor: float = 0.01 # the healthy rate is taken as at least this
    freeze_ewma: float = 0.3        # smoothing of the healthy outlier rate (updated while not frozen)
    cold_prior: str = "none"        # none | equal  (T_i before the first calibrated period, see module docstring)
    outlier_margin: float = 0.0     # outlier only when duration > (1 + m) T_i (see module docstring)
    n_need_healthy: bool = False    # N_need and the frozen trigger count misses beyond the healthy over-T rate
    root_policy: str = "pin"        # pin | on_demand | record | plan  (see module docstring)
    root_flag_on: str = "outlier"   # outlier: outlier/escalate/parent keeps raise the flag; any: plan keeps too
    record_cost: float = 0.1        # a root record costs this fraction of a span
    outlier_credit: str = "none"    # none | credit | reserve  (see module docstring)
    credit_frac: float = 0.2        # c: fraction of the host's per-period pool
    credit_k: int = 2               # at most this many credit / reserve spans per request
    credit_low_water: float = 0.25  # below this fraction of the allowance the miss coin applies
    initial_budget: float = 3.0     # per-request budget before the first refill
    initial_plan_p: float = 0.15    # ordinary-span rate before the first plan
    pool_factor: float = 1.0        # host pool = pool_factor * V* * host share
    b_min: float = 0.05             # B below 1 means most requests get budget 0 (root only)
    b_max: float = 64.0
    rate_ewma: float = 0.5          # smoothing of expected requests / counts
    ordinary_floor: float = 0.05    # ordinary spend never below this fraction of B (of V* when unbounded)

    def __post_init__(self) -> None:
        if self.table is not None:
            self.calib_source = "oracle" if self.table == "oracle" else "kept"
        self.table = "oracle" if self.calib_source == "oracle" else "calibrated"
        if self.calib_source not in CALIB_SOURCES:
            raise ValueError(f"unknown calib_source {self.calib_source}; choose from {CALIB_SOURCES}")
        if self.root_policy not in ROOT_POLICIES:
            raise ValueError(f"unknown root_policy {self.root_policy}; choose from {ROOT_POLICIES}")
        if self.root_flag_on not in ("outlier", "any"):
            raise ValueError(f"unknown root_flag_on {self.root_flag_on}")
        if self.outlier_credit not in CREDIT_POLICIES:
            raise ValueError(f"unknown outlier_credit {self.outlier_credit}; choose from {CREDIT_POLICIES}")
        if self.n_need_mode not in ("fixed", "relative"):
            raise ValueError(f"unknown n_need_mode {self.n_need_mode}")
        if self.freeze_trigger not in ("rate", "count"):
            raise ValueError(f"unknown freeze_trigger {self.freeze_trigger}")
        if self.cold_prior not in PRIORS:
            raise ValueError(f"unknown cold_prior {self.cold_prior}; choose from {PRIORS}")
        if self.outlier_margin < 0.0:
            raise ValueError("outlier_margin must be >= 0")


def _growth(rule: str):
    if rule == "const":
        return lambda miss: 1.0
    if rule == "linear":
        return lambda miss: 1.0 + miss
    if rule == "sqrt":
        return lambda miss: 1.0 + math.sqrt(miss)
    if rule == "quadratic":
        return lambda miss: (1.0 + miss) ** 2
    raise ValueError(f"unknown p rule {rule}")


class BudgetSampler(Sampler):
    name = "chosen"

    def __init__(self, cfg: BudgetConfig | None = None, name: str | None = None):
        self.cfg = cfg or BudgetConfig()
        if name:
            self.name = name
        self._g = _growth(self.cfg.p_rule)

    # -- lifecycle ---------------------------------------------------------
    def start_run(self, ctx: RunContext) -> None:
        super().start_run(ctx)
        cfg, corpus = self.cfg, ctx.corpus
        self.calibrated = cfg.calib_source != "oracle"
        self.cal = Calibrator(corpus.graph, corpus.routes_slo, q=cfg.calib_q,
                              min_class_samples=cfg.min_class_samples,
                              window=cfg.calib_window, use_class=cfg.use_class,
                              prior=cfg.cold_prior if self.calibrated else "none")
        if not self.calibrated and ctx.oracle is None:
            raise ValueError("oracle table requested but none provided")
        self.record_cost = cfg.record_cost
        self._flag_reasons = ({"outlier", "escalate", "parent", "credit", "reserve"} if cfg.root_flag_on == "outlier"
                              else {"outlier", "escalate", "parent", "credit", "reserve", "plan"})
        n_svc = max(1, len(corpus.services()))
        if cfg.unbounded or (cfg.n_need_mode == "fixed" and math.isinf(cfg.n_need)):
            self.n_need = math.inf
        elif cfg.n_need_mode == "relative":
            self.n_need = float(max(cfg.n_min, math.floor(cfg.n_need_frac * ctx.v_star / n_svc)))
        else:
            self.n_need = float(cfg.n_need)
        self.hosts = list(corpus.hosts)
        self.skew = corpus.host_skew
        self.slo = corpus.routes_slo
        self.spans = corpus.spans
        self.B = cfg.initial_budget
        self.R_exp: float | None = None
        self.out_exp: float = 0.0
        self.N_exp: dict[str, float] = {}
        self._share = {h: 1.0 / max(1, len(self.hosts)) for h in self.hosts}
        self._pool_full = {h: math.inf if cfg.unbounded else max(1, round(cfg.pool_factor * ctx.v_star * self._share[h]))
                           for h in self.hosts}
        self._pool: dict[str, float] = {}
        self._allow: dict[str, float] = {}          # credit / reserve left per host this period
        self._allow_total: dict[str, float] = {}
        self._set_pools()
        self._plan_p: dict[str, float] = {}
        self._plan_scale: dict[str, float] = {}     # per host: plan-rate factor repaying last period's credit
        self._svc_host: dict[str, str] = {}
        self._esc: dict[int, bool] = {}
        self._elapsed: dict[int, float] = {}
        self._forced: set[int] = set()
        self._frozen: set[str] = set()
        self._healthy_rate: dict[str, float] = {}
        self._reset_counters()
        self._exhausted_reason = ""

    def _reset_counters(self) -> None:
        self._nseen: dict[str, int] = {}     # outliers counted by the N_need rule (kept or coin-dropped)
        self._ndet: dict[str, int] = {}      # outliers detected (including those refused by budget / pool)
        self._spans_svc: dict[str, int] = {}
        self._spans_host: dict[str, int] = {}
        self._requests = 0
        self._kept_outlier = 0
        self._kept_total = 0
        self._kept_by_reason: dict[str, int] = {}
        self._debt: dict[str, int] = {}

    def _set_pools(self) -> None:
        """Split each host's per-period pool into the main pool and the credit /
        reserve allowance according to the credit policy."""
        cfg = self.cfg
        for h, full in self._pool_full.items():
            if cfg.unbounded or not math.isfinite(full):
                self._pool[h], self._allow[h], self._allow_total[h] = math.inf, 0.0, 0.0
            elif cfg.outlier_credit == "reserve":
                res = cfg.credit_frac * full
                self._pool[h] = max(1, round(full - res))
                self._allow[h] = self._allow_total[h] = res
            elif cfg.outlier_credit == "credit":
                self._pool[h] = full
                self._allow[h] = self._allow_total[h] = cfg.credit_frac * full
            else:
                self._pool[h], self._allow[h], self._allow_total[h] = full, 0.0, 0.0

    # -- baggage -------------------------------------------------------------
    def on_root_start(self, span: Span, req: RequestState, t: float) -> None:
        self._requests += 1
        req.flag = False
        req.credit_used = 0
        if self.cfg.unbounded:
            req.budget = req.budget0 = 1 << 30
        else:
            b = int(self.B)
            if self.rng.random() < self.B - b:
                b += 1
            req.budget = req.budget0 = max(1, b)
        if self.cfg.budget_scope == "local":
            req.budget_by_host = {}
        if self.cfg.escalation == "deadline":
            req.deadline = span.start + self.skew.get(span.host, 0.0) + self.slo.get(span.route, math.inf)
        elif self.cfg.escalation == "duration_sum":
            self._elapsed[span.idx] = 0.0

    def on_start(self, span: Span, req: RequestState, t: float) -> None:
        pol = self.cfg.escalation
        if pol == "none" or span.parent_idx < 0:
            return
        parent = self.spans[span.parent_idx]
        if pol == "duration_sum":
            # elapsed time carried in baggage: the caller's elapsed at its own
            # start plus the time it spent (on its own clock) before this call
            el = self._elapsed.get(parent.idx, 0.0) + max(0.0, span.start - span.transit - parent.start)
            self._elapsed[span.idx] = el
        if self._esc.get(parent.idx):
            self._esc[span.idx] = True
            return
        T = self._lookup(span)
        if T is None:
            return
        if pol == "deadline":
            remaining = req.deadline - (span.start + self.skew.get(span.host, 0.0))
        else:
            remaining = self.slo.get(span.route, math.inf) - self._elapsed[span.idx]
        if remaining < T:
            self._esc[span.idx] = True

    # -- the decider -----------------------------------------------------------
    def _lookup_key(self, route: str, cls: str, service: str) -> float | None:
        if not self.calibrated:
            return self.ctx.oracle.lookup(route, cls, service)
        return self.cal.lookup(route, cls, service)

    def _lookup(self, span: Span) -> float | None:
        return self._lookup_key(span.route, span.req_class, span.service)

    def _observe(self, span: Span, p: float, reason: str) -> None:
        """Feed the calibrator according to the configured source.  `p` is the
        probability with which this observation was selected."""
        if not self.calibrated:
            return
        if self.cfg.calib_source == "plan" and reason not in ("plan", "root"):
            return
        self.cal.observe(span, p)

    def _exhausted(self, span: Span, req: RequestState, skip_budget: bool = False) -> bool:
        if self.cfg.unbounded:
            return False
        if not skip_budget:
            if self.cfg.budget_scope == "local":
                if req.budget_by_host.get(span.host, req.budget0) <= 0:
                    self._exhausted_reason = "budget"
                    return True
            elif req.budget <= 0:
                self._exhausted_reason = "budget"
                return True
        if self._pool.get(span.host, math.inf) <= 0:
            self._exhausted_reason = "pool"
            return True
        return False

    def _account(self, span: Span, req: RequestState, p: float, reason: str) -> tuple[bool, float, str]:
        """Bookkeeping shared by every keep: counters, calibration, flag, stamp."""
        self._kept_total += 1
        self._kept_by_reason[reason] = self._kept_by_reason.get(reason, 0) + 1
        if reason != "plan":
            self._kept_outlier += 1
        self._observe(span, p, "outlier" if reason in ("credit", "reserve") else reason)
        if span.parent_idx >= 0:
            if reason in self._flag_reasons:
                req.flag = True
            return True, p, reason
        # a root kept as an ordinary span: under the record policy its duration
        # reaches the collector either way (record or full span), so the stamp is 1
        return True, (1.0 if self.cfg.root_policy == "record" else p), reason

    def _spend(self, span: Span, req: RequestState, p: float, reason: str) -> tuple[bool, float, str]:
        bypass = reason == "escalate" and self.cfg.escalate_bypass_budget
        if self._exhausted(span, req, skip_budget=bypass):
            return False, p, self._exhausted_reason
        if not self.cfg.unbounded:
            if not bypass:
                if self.cfg.budget_scope == "local":
                    req.budget_by_host[span.host] = req.budget_by_host.get(span.host, req.budget0) - 1
                else:
                    req.budget -= 1
            self._pool[span.host] = self._pool.get(span.host, math.inf) - 1
        return self._account(span, req, p, reason)

    def _try_credit(self, span: Span, req: RequestState, miss: float, refusal: str) -> tuple[bool, float, str]:
        """An outlier refused by the request budget or the host pool: keep it on
        credit / from the reserve if eligible, else report the refusal."""
        cfg = self.cfg
        pol = cfg.outlier_credit
        svc = span.service
        if pol == "none" or self._nseen.get(svc, 0) >= self.n_need:        # (a) only while under N_need
            return False, 0.0, refusal
        if req.credit_used >= cfg.credit_k:                                   # (c) at most k per request
            return False, 0.0, "credit_k"
        left = self._allow.get(span.host, 0.0)
        if left < 1.0:
            return False, 0.0, f"{pol}_exhausted"
        p = 1.0
        if left < cfg.credit_low_water * self._allow_total.get(span.host, 0.0):   # (b) scarce: miss coin, errors first
            p = 1.0 if not span.status_ok else min(1.0, max(0.05, miss))
            if p < 1.0 and self.rng.random() >= p:
                return False, p, "credit_coin"
        self._allow[span.host] = left - 1.0
        req.credit_used += 1
        if pol == "credit":
            self._debt[span.host] = self._debt.get(span.host, 0) + 1
        self._nseen[svc] = self._nseen.get(svc, 0) + 1
        return self._account(span, req, p, pol)

    def _keep_root(self, span: Span) -> tuple[bool, float, str]:
        """Pinned root: kept at p = 1, outside the request budget and the host pool."""
        self._kept_total += 1
        self._kept_by_reason["root"] = self._kept_by_reason.get("root", 0) + 1
        self._observe(span, 1.0, "root")
        return True, 1.0, "root"

    def _decide_root(self, span: Span, req: RequestState) -> tuple[bool, float, str]:
        pol = self.cfg.root_policy
        if pol == "pin":
            return self._keep_root(span)
        self._spans_svc[span.service] = self._spans_svc.get(span.service, 0) + 1    # the root service gets a plan rate
        if pol in ("on_demand", "record") and req.flag:
            return self._keep_root(span)
        keep, p, reason = self._decide_span(span, req)
        if keep or pol != "record":
            return keep, p, reason
        # record policy: the full root was not kept, the compact record goes instead
        self._kept_by_reason["root_record"] = self._kept_by_reason.get("root_record", 0) + 1
        return True, 1.0, "root_record"

    def _decide_span(self, span: Span, req: RequestState) -> tuple[bool, float, str]:
        """The Decide(...) of the plan: reads the span, the baggage and the
        per-period counters only."""
        cfg = self.cfg
        svc = span.service
        if self._esc and self._esc.get(span.idx):
            return self._spend(span, req, 1.0, "escalate")
        if cfg.keep_parent and span.idx in self._forced:
            self._forced.discard(span.idx)
            return self._spend(span, req, 1.0, "parent")
        T = self._lookup(span)
        if T is None:                       # cold start: plan sampling only
            outlier, miss = False, 0.0
        else:
            miss = (span.duration - T) / T
            outlier = miss > cfg.outlier_margin or not span.status_ok      # margin 0: the plain over-T rule
            if miss < 0.0:
                miss = 0.0
        if outlier:
            self._ndet[svc] = self._ndet.get(svc, 0) + 1
            if self._exhausted(span, req):
                return self._try_credit(span, req, miss, self._exhausted_reason)
            n = self._nseen.get(svc, 0) + 1
            self._nseen[svc] = n
            if cfg.n_need_healthy:
                # misses beyond what the service's healthy over-T rate predicts for the spans seen so far
                h = self._healthy_rate.get(svc)
                if h is not None and h > 0.0:
                    n = max(1.0, n - h * self._spans_svc.get(svc, 0))
            p = 1.0 if n <= self.n_need else min(1.0, self.n_need / n * self._g(miss))
            if p < 1.0 and self.rng.random() >= p:
                return False, p, "outlier_coin"
            keep, p, reason = self._spend(span, req, p, "outlier")
            if keep and cfg.keep_parent and span.parent_idx >= 0:
                self._forced.add(span.parent_idx)
            return keep, p, reason
        p = self._plan_p.get(svc, cfg.initial_plan_p) * self._plan_scale.get(span.host, 1.0)
        if p <= 0.0 or (p < 1.0 and self.rng.random() >= p):
            return False, p, "plan_coin"
        return self._spend(span, req, p, "plan")

    def on_end(self, span: Span, req: RequestState, t: float) -> tuple[bool, float, str]:
        self._spans_host[span.host] = self._spans_host.get(span.host, 0) + 1
        self._svc_host[span.service] = span.host
        if span.parent_idx < 0:
            return self._decide_root(span, req)
        self._spans_svc[span.service] = self._spans_svc.get(span.service, 0) + 1
        return self._decide_span(span, req)

    # -- period boundary: calibration, refill, pools, plan -----------------------
    def _freeze_set(self) -> set[str]:
        """Services whose calibration is held for the next table.  `rate`: the
        period's outlier rate is >= freeze_rate_mult x the service's healthy
        rate (an EWMA updated only while not frozen, once a table exists);
        `count`: the pass-2 rule, more than N_need outliers in the period."""
        cfg = self.cfg
        out: set[str] = set()
        has_table = bool(self.cal.table)
        for svc, n_sp in self._spans_svc.items():
            n_out = self._ndet.get(svc, 0)
            if cfg.freeze_trigger == "count":
                if n_out > self.n_need:
                    out.add(svc)
                continue
            if not has_table or n_sp <= 0:
                continue
            rate = n_out / n_sp
            base = self._healthy_rate.get(svc)
            excess = rate - base if (cfg.n_need_healthy and base is not None) else rate     # n_need_healthy: beyond the healthy rate
            if base is not None and n_out >= cfg.n_min and excess >= cfg.freeze_rate_mult * max(base, cfg.freeze_rate_floor):
                out.add(svc)
            else:
                self._healthy_rate[svc] = rate if base is None else (1 - cfg.freeze_ewma) * base + cfg.freeze_ewma * rate
        return out

    def _track_healthy_rates(self) -> None:
        """The healthy over-T rate EWMA for `n_need_healthy` when the frozen
        source (which maintains it in `_freeze_set`) is not in use."""
        cfg = self.cfg
        if not self.cal.table:
            return
        for svc, n_sp in self._spans_svc.items():
            if n_sp <= 0:
                continue
            rate = self._ndet.get(svc, 0) / n_sp
            base = self._healthy_rate.get(svc)
            self._healthy_rate[svc] = rate if base is None else (1 - cfg.freeze_ewma) * base + cfg.freeze_ewma * rate

    def on_period_end(self, period: int, collector_volume: float) -> None:
        cfg, v_star = self.cfg, self.ctx.v_star
        if self.calibrated:
            if cfg.calib_source == "frozen":
                self._frozen = self._freeze_set()
                self.cal.end_period(self._frozen)
            else:
                if cfg.n_need_healthy:
                    self._track_healthy_rates()
                self.cal.end_period()
        R = self._requests
        if R > 0:
            self.R_exp = float(R) if self.R_exp is None else (1 - cfg.rate_ewma) * self.R_exp + cfg.rate_ewma * R
        b_raw = self.B
        if not cfg.unbounded and R > 0 and self.R_exp:
            K = collector_volume
            if K > 0:
                u = (K / R) / self.B                       # measured utilisation of the issued budget
                b_raw = v_star / (self.R_exp * u)
            else:
                b_raw = cfg.b_max
            b_raw = min(cfg.b_max, max(cfg.b_min, b_raw))
            self.B = (1 - cfg.alpha) * self.B + cfg.alpha * b_raw
        total_host = sum(self._spans_host.values())
        if total_host and not cfg.unbounded:
            for h in self.hosts:
                share = self._spans_host.get(h, 0) / total_host
                self._share[h] = (1 - cfg.rate_ewma) * self._share[h] + cfg.rate_ewma * share
                self._pool_full[h] = max(1, round(cfg.pool_factor * v_star * self._share[h]))
        self._set_pools()                                   # credit does not roll over
        self.out_exp = (1 - cfg.rate_ewma) * self.out_exp + cfg.rate_ewma * self._kept_outlier
        for svc, n in self._spans_svc.items():
            prev = self.N_exp.get(svc)
            self.N_exp[svc] = float(n) if prev is None else (1 - cfg.rate_ewma) * prev + cfg.rate_ewma * n
        self._replan(v_star)
        debt = dict(self._debt)
        self._plan_scale = self._repayment(debt) if cfg.outlier_credit == "credit" else {}
        prior_hits, self.cal.prior_hits = self.cal.prior_hits, 0        # the T_ratio diagnostic's lookups do not count
        self._diag.append({"period": period, "B": self.B, "requests": R, "collector_volume": collector_volume,
                           "kept": self._kept_total, "kept_outlier": self._kept_outlier, "n_need": self.n_need,
                           "plan_p_mean": (sum(self._plan_p.values()) / len(self._plan_p)) if self._plan_p else cfg.initial_plan_p,
                           "by_reason": dict(self._kept_by_reason), "debt": sum(debt.values()),
                           "plan_scale_min": min(self._plan_scale.values()) if self._plan_scale else 1.0,
                           "T_ratio": self._t_ratio(), "frozen": sorted(self._frozen), "prior_hits": prior_hits})
        self.cal.prior_hits = 0
        self._reset_counters()

    def _repayment(self, debt: dict[str, int]) -> dict[str, float]:
        """Plan-rate factor per host for the next period that removes the
        host's credit debt from its expected plan keeps.  Outlier keeps are
        never reduced."""
        if not debt:
            return {}
        exp_plan: dict[str, float] = {}
        for svc, n in self.N_exp.items():
            h = self._svc_host.get(svc)
            if h is not None:
                exp_plan[h] = exp_plan.get(h, 0.0) + n * self._plan_p.get(svc, 0.0)
        scale = {}
        for h, d in debt.items():
            e = exp_plan.get(h, 0.0)
            scale[h] = max(0.0, 1.0 - d / e) if e > 0 else 0.0
        return scale

    def _t_ratio(self) -> dict[str, float]:
        """Per service: mean over the oracle's (route, class) keys of T_used / T_oracle,
        i.e. how far the table in force for the next period is from the truth."""
        oracle = self.ctx.oracle
        if oracle is None:
            return {}
        acc: dict[str, list[float]] = {}
        for (route, cls, svc), T_or in oracle.table.items():
            if cls == "*" or T_or <= 0:
                continue
            T_c = self._lookup_key(route, cls, svc)
            if T_c is not None:
                acc.setdefault(svc, []).append(T_c / T_or)
        return {svc: sum(v) / len(v) for svc, v in acc.items()}

    def _replan(self, v_star: float) -> None:
        """Ordinary-span rates for the next period.  Budget arms spread the part
        of the per-request budget that outliers are not expected to use over
        the request's ordinary spans, so the refill loop (through B) is the only
        volume control.  The unbounded (threshold) arm has no budget and targets
        V* minus roots minus outliers directly."""
        cfg = self.cfg
        if not self.N_exp or not self.R_exp:
            return
        total = sum(self.N_exp.values())
        if total <= 0:
            return
        if cfg.unbounded:
            roots = self.R_exp if cfg.root_policy == "pin" else 0.0
            target = max(cfg.ordinary_floor * v_star, v_star - roots - self.out_exp)
        else:
            # a request keeps Binomial(spans, p) ordinary spans; aiming below B
            # keeps the budget from truncating the plan (which would bias 1/p)
            per_request = max(cfg.ordinary_floor * self.B, cfg.plan_fill * (self.B - self.out_exp / self.R_exp))
            target = per_request * self.R_exp
        if cfg.plan == "uniform":
            p = min(1.0, target / total)
            self._plan_p = {svc: p for svc in self.N_exp}
        elif cfg.plan == "sqrt":
            w = {svc: math.sqrt(n) for svc, n in self.N_exp.items()}
            wsum = sum(w.values())
            self._plan_p = {svc: min(1.0, (target * w[svc] / wsum) / max(1.0, n)) for svc, n in self.N_exp.items()}
        else:
            raise ValueError(f"unknown plan {cfg.plan}")


# What the follow-up experiments selected; `chosen-v2` and exp8 use them.
BEST_ROOT_POLICY = "on_demand"          # exp6
BEST_CALIB_SOURCE = "frozen"            # exp7 (rate trigger since pass 3)
BEST_N_NEED_FRAC = 0.2                  # exp10
BEST_CREDIT = {"outlier_credit": "none"}   # pass 4: credit / reserve off (exp9: no gain worth the extra rule); the switch stays
BEST_COLD_PRIOR = "none"                   # exp12 (pass 4), see results/REPORT-pass4.md


def make_budget_arm(name: str, **overrides) -> BudgetSampler:
    """Named variants of the chosen mechanism used by the experiments."""
    presets = {
        "chosen": {},
        "chosen-local": {"budget_scope": "local"},
        "chosen-oracleT": {"calib_source": "oracle"},
        "chosen-v2": {"budget_scope": "local", "plan": "uniform", "escalation": "none", "use_class": True,
                      "root_policy": BEST_ROOT_POLICY, "calib_source": BEST_CALIB_SOURCE,
                      "n_need_mode": "relative", "n_need_frac": BEST_N_NEED_FRAC, **BEST_CREDIT,
                      "cold_prior": BEST_COLD_PRIOR},
        "threshold": {"unbounded": True, "n_need": math.inf, "n_need_mode": "fixed"},
        "keep-parent": {"keep_parent": True},
    }
    # ablations of chosen-v2: the same mechanism with one part removed or replaced
    v2 = presets["chosen-v2"]
    presets["v2-shared"] = {**v2, "budget_scope": "shared"}        # one budget per request instead of per (request, host)
    presets["v2-oracleT"] = {**v2, "calib_source": "oracle"}      # the ground-truth table instead of calibration
    presets["v2-keep-parent"] = {**v2, "keep_parent": True}        # also keep the parent of a kept outlier
    if name not in presets:
        raise KeyError(name)
    cfg = BudgetConfig(**{**presets[name], **overrides})
    return BudgetSampler(cfg, name=name)
