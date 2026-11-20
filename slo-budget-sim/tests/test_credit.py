"""Outlier credit and reserve: eligibility, k cap, low-water coin, debt repayment,
reserve partition, and the refusal reasons the replay records."""
import math

import pytest

from slo_budget_sim.model import Span
from slo_budget_sim.replay import replay
from slo_budget_sim.sampler import BudgetConfig, BudgetSampler, RequestState, RunContext
from slo_budget_sim.ttable import oracle_table
from tests.conftest import chain_corpus


def _sampler(corpus, pool=10, v_star=None, **kw):
    """Oracle table, budget 1 per request, host pool `pool` (v_star chosen so that
    pool = v_star x share with a single host)."""
    cfg = BudgetConfig(table="oracle", initial_budget=1, b_min=1, b_max=1, initial_plan_p=0.0,
                       plan_fill=0.0, ordinary_floor=0.0, **kw)
    s = BudgetSampler(cfg)
    s.start_run(RunContext(corpus, v_star=float(pool if v_star is None else v_star), cap=1e9,
                           oracle=oracle_table(corpus), seed=1))
    return s


def _req(s, root):
    req = RequestState(root.trace_id, root.route, root.req_class)
    s.on_root_start(root, req, root.start)
    return req


def _outlier(k, svc="c", dur=0.009, ok=True):
    return Span(5000 + k, 5000 + k, 1, 0, svc, "h1", 0.0, dur, ok, 2, "r", "r|any", 1)


def test_refused_outlier_is_dropped_without_credit_and_kept_on_credit():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = _sampler(corpus, pool=10, outlier_credit="none")
    req = _req(s, corpus.spans[0])
    assert s.on_end(_outlier(1), req, 0.0)[0]                       # budget 1 spent
    keep, _, why = s.on_end(_outlier(2), req, 0.0)
    assert not keep and why == "budget"
    s = _sampler(corpus, pool=10, outlier_credit="credit", credit_frac=0.3, credit_k=2)
    assert s._allow["h1"] == pytest.approx(3.0) and s._pool["h1"] == 10
    req = _req(s, corpus.spans[0])
    assert s.on_end(_outlier(1), req, 0.0)[2] == "outlier"
    assert s.on_end(_outlier(2), req, 0.0)[2] == "credit"           # on credit, p = 1
    assert s.on_end(_outlier(3), req, 0.0)[2] == "credit"
    keep, _, why = s.on_end(_outlier(4), req, 0.0)
    assert not keep and why == "credit_k"                            # k = 2 per request
    assert s._debt["h1"] == 2 and s._allow["h1"] == pytest.approx(1.0)
    req2 = _req(s, corpus.spans[0])
    assert s.on_end(_outlier(5), req2, 0.0)[2] == "outlier"
    keep, _, why = s.on_end(_outlier(6), req2, 0.0)                  # the last credit unit: below low water
    assert keep and why == "credit"
    keep, _, why = s.on_end(_outlier(7), req2, 0.0)
    assert not keep and why == "credit_exhausted"


def test_credit_only_while_the_service_is_under_n_need():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = _sampler(corpus, pool=100, outlier_credit="credit", credit_frac=0.5, credit_k=5, n_need=2)
    req = _req(s, corpus.spans[0])
    assert s.on_end(_outlier(1), req, 0.0)[2] == "outlier"           # n = 1
    assert s.on_end(_outlier(2), req, 0.0)[2] == "credit"            # n = 2 (credit counts toward N_need)
    keep, _, why = s.on_end(_outlier(3), req, 0.0)
    assert not keep and why == "budget"                              # over N_need: plain refusal


def test_low_water_coin_is_proportional_to_miss_and_errors_go_first():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = _sampler(corpus, pool=1000, outlier_credit="credit", credit_frac=0.2, credit_k=9, credit_low_water=1.0)
    # low water = 100% of the allowance (200): with 1.9 left every credit keep goes through the coin;
    # the main pool (1000) never runs out, so the first outlier of each request is a regular keep
    ps = []
    for k in range(40):
        s._allow["h1"] = 1.9
        req = _req(s, corpus.spans[0])
        s.on_end(_outlier(100 + k), req, 0.0)
        keep, p, why = s.on_end(_outlier(200 + k, dur=0.0033), req, 0.0)   # miss = 0.1 -> p = 0.1
        ps.append((keep, p, why))
        if s._nseen.get("c", 0) >= 5:
            s._nseen["c"] = 0
    assert all(p == pytest.approx(0.1) for _, p, _ in ps)
    assert 0 < sum(1 for k, _, _ in ps if k) < 20
    assert {why for k, _, why in ps if not k} == {"credit_coin"}
    s._allow["h1"] = 1.9
    req = _req(s, corpus.spans[0])
    s.on_end(_outlier(300), req, 0.0)
    keep, p, why = s.on_end(_outlier(301, dur=0.0001, ok=False), req, 0.0)      # error status: p = 1
    assert keep and p == 1.0 and why == "credit"
    s._allow["h1"] = 1.9
    req = _req(s, corpus.spans[0])
    s.on_end(_outlier(302), req, 0.0)
    assert s.on_end(_outlier(303, dur=0.009), req, 0.0)[1] == 1.0                  # miss 2.0 clipped to 1


def test_debt_lowers_the_next_periods_plan_rate_on_that_host_only():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = _sampler(corpus, pool=1000, outlier_credit="credit", credit_frac=0.1, credit_k=1, n_need=100)
    s.cfg.plan_fill, s.cfg.ordinary_floor = 0.5, 0.05          # a non-zero plan target next period
    # budget 1: 10 requests with 2 outliers each -> 10 regular keeps, 10 credit keeps -> debt 10 on h1
    for k in range(10):
        req = _req(s, corpus.spans[0])
        s.on_end(_outlier(k), req, 0.0)
        s.on_end(_outlier(100 + k), req, 0.0)
        s.on_end(corpus.spans[1], req, 0.0)       # b: ordinary, plan p = 0 in this period
        s.on_end(corpus.spans[0], req, 0.0)
    assert s._debt["h1"] == 10
    s.on_period_end(0, 20.0)
    assert s._plan_p and all(p > 0 for p in s._plan_p.values())
    scale = s._plan_scale["h1"]
    assert 0.0 <= scale < 1.0                      # the plan on h1 is cut by the debt
    assert s._debt == {} and s._allow["h1"] == pytest.approx(0.1 * s._pool_full["h1"])   # no roll-over
    req = _req(s, corpus.spans[0])
    _, p, why = s.on_end(corpus.spans[1], req, 0.0)
    assert why in ("plan", "plan_coin") and p == pytest.approx(s._plan_p["b"] * scale)


def test_reserve_partitions_the_pool_and_serves_refused_outliers():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = _sampler(corpus, pool=10, outlier_credit="reserve", credit_frac=0.3, credit_k=3)
    assert s._pool["h1"] == 7 and s._allow["h1"] == pytest.approx(3.0)
    req = _req(s, corpus.spans[0])
    assert s.on_end(_outlier(1), req, 0.0)[2] == "outlier"           # main pool: 6 left
    assert s.on_end(_outlier(2), req, 0.0)[2] == "reserve"           # budget refused -> reserve
    assert s._pool["h1"] == 6 and s._allow["h1"] == pytest.approx(2.0)
    assert s._debt == {}                                             # the reserve is not repaid
    # exhaust the main pool with other requests: the reserve still serves eligible outliers
    for k in range(6):
        r = _req(s, corpus.spans[0])
        assert s.on_end(_outlier(10 + k), r, 0.0)[2] == "outlier"
    s._nseen["c"] = 0
    r = _req(s, corpus.spans[0])
    assert s.on_end(_outlier(50), r, 0.0)[2] == "reserve"            # pool refused -> reserve
    s._nseen["c"] = 0
    r = _req(s, corpus.spans[0])
    keep, _, why = s.on_end(_outlier(51), r, 0.0)                    # last unit, below low water: coin (miss 2 -> p 1)
    assert keep and why == "reserve"
    s._nseen["c"] = 0
    r = _req(s, corpus.spans[0])
    keep, _, why = s.on_end(_outlier(52), r, 0.0)
    assert not keep and why == "reserve_exhausted"


def test_replay_records_a_refusal_reason_per_dropped_span():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=40)
    for k, r in enumerate(corpus.roots):          # every request: b and c both outliers, budget 1
        corpus.spans[r + 2].duration = corpus.spans[r + 2].own = 0.0045
        corpus.spans[r + 1].duration = 0.0085
        corpus.spans[r].duration = 0.0119
    reference = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    cfg = BudgetConfig(table="oracle", initial_budget=1, b_min=1, b_max=1, initial_plan_p=0.0,
                       plan_fill=0.0, ordinary_floor=0.0, n_need=1000)
    res = replay(corpus, BudgetSampler(cfg), v_star=1e9, cap=1e9, seed=1, oracle=oracle_table(reference))
    reasons = {}
    for r in corpus.roots:
        reasons[res.drop_reason[r + 1]] = reasons.get(res.drop_reason[r + 1], 0) + 1   # b ends after c
    assert reasons == {"budget": 40}
    assert all(res.drop_reason[i] == "" for i in res.kept_idx)
