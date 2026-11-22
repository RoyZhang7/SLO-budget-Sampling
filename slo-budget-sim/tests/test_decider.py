"""The N_need rule, budget and pool accounting, hole counting, root pinning."""
import math

import pytest

from slo_budget_sim.model import CallGraph, Span
from slo_budget_sim.replay import replay
from slo_budget_sim.sampler import BudgetConfig, BudgetSampler, RequestState, RunContext
from slo_budget_sim.ttable import oracle_table
from tests.conftest import chain_corpus


def _sampler(corpus, **kw):
    cfg = BudgetConfig(outlier_trigger="duration", table="oracle", **kw)
    s = BudgetSampler(cfg)
    s.start_run(RunContext(corpus, v_star=1e9, cap=1e9, oracle=oracle_table(corpus), seed=1))
    return s


def _req(s, span):
    req = RequestState(span.trace_id, span.route, span.req_class)
    s.on_root_start(span, req, span.start)
    return req


def test_n_need_rule_p_is_one_then_decays():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=400)
    s = _sampler(corpus, n_need=15, p_rule="const", initial_budget=1000, pool_factor=1e9)
    root = corpus.spans[0]
    # 100 outliers of service c (T = 3 ms, duration 9 ms) in one period
    ps = []
    for k in range(100):
        sp = Span(10_000 + k, 10_000 + k, 1, 0, "c", "h1", 0.0, 0.009, True, 2, "r", "r|any", 1)
        req = _req(s, root)
        keep, p, why = s.on_end(sp, req, 0.0)
        ps.append(p)
    assert ps[:15] == [1.0] * 15
    assert ps[15] == pytest.approx(15 / 16)
    assert ps[99] == pytest.approx(15 / 100)
    # the first 15 were kept for sure, outliers were counted even when dropped by the coin
    assert s._nseen["c"] == 100


def test_p_rises_with_size_of_miss():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=10)
    s = _sampler(corpus, n_need=1, p_rule="linear", initial_budget=1000, pool_factor=1e9, outlier_margin=0.0)   # the plain over-T rule
    root = corpus.spans[0]
    req = _req(s, root)
    s.on_end(Span(1, 1, 1, 0, "c", "h1", 0.0, 0.0031, True, 2, "r", "r|any", 1), req, 0.0)   # n = 1, p = 1
    req = _req(s, root)
    _, p_small, _ = s.on_end(Span(2, 2, 1, 0, "c", "h1", 0.0, 0.0033, True, 2, "r", "r|any", 1), req, 0.0)
    req = _req(s, root)
    _, p_big, _ = s.on_end(Span(3, 3, 1, 0, "c", "h1", 0.0, 0.0090, True, 2, "r", "r|any", 1), req, 0.0)
    assert p_small == pytest.approx(0.5 * (1 + 0.1), rel=1e-6)      # N_need/n * (1 + miss) with miss = 0.1
    assert p_big == pytest.approx(min(1.0, (1 / 3) * (1 + 2.0)))    # miss = 2.0 -> p = 1


def test_status_not_ok_is_an_outlier():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=10)
    s = _sampler(corpus, initial_budget=1000, pool_factor=1e9)
    req = _req(s, corpus.spans[0])
    keep, p, why = s.on_end(Span(1, 1, 1, 0, "c", "h1", 0.0, 0.0001, False, 2, "r", "r|any", 1), req, 0.0)
    assert keep and p == 1.0 and why == "outlier"


def test_request_budget_and_host_pool_are_decremented_and_block():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=10)
    s = _sampler(corpus, initial_budget=2, pool_factor=1e9)
    req = _req(s, corpus.spans[0])
    assert req.budget == 2
    for k in range(2):
        keep, _, why = s.on_end(Span(k + 1, k + 1, 1, 0, "c", "h1", 0.0, 0.009, True, 2, "r", "r|any", 1), req, 0.0)
        assert keep and why == "outlier"
    keep, _, why = s.on_end(Span(9, 9, 1, 0, "c", "h1", 0.0, 0.009, True, 2, "r", "r|any", 1), req, 0.0)
    assert not keep and why == "budget"
    # host pool
    s2 = _sampler(corpus, initial_budget=100)
    s2._pool["h1"] = 1
    req = _req(s2, corpus.spans[0])
    assert s2.on_end(Span(1, 1, 1, 0, "c", "h1", 0.0, 0.009, True, 2, "r", "r|any", 1), req, 0.0)[0]
    keep, _, why = s2.on_end(Span(2, 2, 1, 0, "c", "h1", 0.0, 0.009, True, 2, "r", "r|any", 1), req, 0.0)
    assert not keep and why == "pool"


def test_local_budget_scope_is_per_host():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=10)
    s = _sampler(corpus, initial_budget=1, budget_scope="local", pool_factor=1e9)
    req = _req(s, corpus.spans[0])
    assert s.on_end(Span(1, 1, 1, 0, "c", "hA", 0.0, 0.009, True, 2, "r", "r|any", 1), req, 0.0)[0]
    assert not s.on_end(Span(2, 2, 1, 0, "c", "hA", 0.0, 0.009, True, 2, "r", "r|any", 1), req, 0.0)[0]
    assert s.on_end(Span(3, 3, 1, 0, "c", "hB", 0.0, 0.009, True, 2, "r", "r|any", 1), req, 0.0)[0]


def test_root_is_always_kept_and_holes_are_counted():
    # chain a -> b -> c, budget 0 for every request: only roots survive, each with one hole
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = BudgetSampler(BudgetConfig(outlier_trigger="duration", table="oracle", initial_budget=1, initial_plan_p=1.0, b_min=1))
    res = replay(corpus, s, v_star=1e9, cap=1e9, seed=3, oracle=oracle_table(corpus))
    kept = {int(i): r for i, r in zip(res.kept_idx, res.kept_reason)}
    roots_kept = [i for i in corpus.roots if i in kept]
    assert len(roots_kept) == len(corpus.roots)
    assert all(kept[i] == "root" for i in corpus.roots)
    # with budget 1 and plan p = 1, c (ends first) is kept and b is dropped ->
    # root has 1 hole (b), c has no children
    holes = dict(zip(res.kept_idx.tolist(), res.kept_holes.tolist()))
    for r in corpus.roots:
        assert holes[r] == 1
        assert r + 2 in kept and kept[r + 2] == "plan"
        assert r + 1 not in kept


def test_cold_start_is_plan_sampling_only():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=10)
    s = BudgetSampler(BudgetConfig(outlier_trigger="duration", table="calibrated", initial_plan_p=0.0, initial_budget=10))
    s.start_run(RunContext(corpus, 1e9, 1e9, None, 1))
    req = _req(s, corpus.spans[0])
    keep, p, why = s.on_end(Span(1, 1, 1, 0, "c", "h1", 0.0, 0.5, True, 2, "r", "r|any", 1), req, 0.0)
    assert not keep and why == "plan_coin"      # a huge span is not an outlier without a table


def test_collector_cap_is_a_hard_backstop():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=200, period=10.0)
    s = BudgetSampler(BudgetConfig(outlier_trigger="duration", table="oracle", initial_plan_p=1.0, initial_budget=10, unbounded=True))
    res = replay(corpus, s, v_star=50, cap=60, seed=1, oracle=oracle_table(corpus))
    assert res.periods[0].kept_post == 60
    assert res.periods[0].kept_pre == 600
