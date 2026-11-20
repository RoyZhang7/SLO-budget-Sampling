"""Root policies: pin / on_demand / record / plan, the response-path flag, and
record cost accounting in the replay."""
import math

import pytest

from slo_budget_sim.model import Span
from slo_budget_sim.replay import replay
from slo_budget_sim.sampler import BudgetConfig, BudgetSampler, RequestState, RunContext
from slo_budget_sim.ttable import oracle_table
from tests.conftest import chain_corpus


def _run(corpus, seed=3, v_star=1e9, cap=1e9, **kw):
    """The oracle comes from a clean chain (6, 2, 1 ms): T = (12, 6, 3 ms)."""
    cfg = BudgetConfig(table="oracle", initial_budget=10, b_min=10, b_max=10, **kw)
    reference = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    return replay(corpus, BudgetSampler(cfg), v_star=v_star, cap=cap, seed=seed, oracle=oracle_table(reference))


def _kept(res):
    return {int(i): (r, p, c) for i, r, p, c in zip(res.kept_idx, res.kept_reason, res.kept_p, res.kept_cost)}


def _slow_chain(n_traces=40):
    """Chain a -> b -> c; every other trace has a 4.5 ms c span (T_c = 3 ms),
    so half the requests carry an outlier; its parents stretch with it but
    stay under their budgets (T_b = 6, T_a = 12 ms)."""
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=n_traces)
    for k, r in enumerate(corpus.roots):
        if k % 2 == 1:
            corpus.spans[r + 2].duration = corpus.spans[r + 2].own = 0.0045
            corpus.spans[r + 1].duration = 0.0055
            corpus.spans[r].duration = 0.0095
    return corpus


def test_pin_keeps_every_root_outside_the_budget():
    corpus = _slow_chain()
    res = _run(corpus, root_policy="pin", initial_plan_p=0.0)
    kept = _kept(res)
    assert all(kept[r][0] == "root" for r in corpus.roots)
    assert all(c == 1.0 for _, _, c in kept.values())


def test_on_demand_keeps_the_root_only_when_a_kept_outlier_raised_the_flag():
    corpus = _slow_chain()
    res = _run(corpus, root_policy="on_demand", initial_plan_p=0.0)     # no plan keeps: flags only
    kept = _kept(res)
    for k, r in enumerate(corpus.roots):
        if k % 2 == 1:
            assert kept[r + 2][0] == "outlier"
            assert kept[r][0] == "root" and kept[r][1] == 1.0
        else:
            assert r not in kept and (r + 2) not in kept


def test_on_demand_flag_any_is_raised_by_plan_keeps_too():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=30)              # no outliers at all
    res_out = _run(corpus, root_policy="on_demand", root_flag_on="outlier", initial_plan_p=1.0)
    res_any = _run(corpus, root_policy="on_demand", root_flag_on="any", initial_plan_p=1.0)
    k_out, k_any = _kept(res_out), _kept(res_any)
    # with p = 1 plan sampling every child is kept; "any" pulls every root in at p = 1
    assert all(k_any[r][0] == "root" and k_any[r][1] == 1.0 for r in corpus.roots)
    # with "outlier" flags only, the root goes through the plan coin (p = 1 here -> kept as "plan")
    assert all(k_out[r][0] == "plan" for r in corpus.roots)


def test_record_policy_emits_a_record_when_the_full_root_is_not_kept():
    corpus = _slow_chain()
    res = _run(corpus, root_policy="record", initial_plan_p=0.0, record_cost=0.1)
    kept = _kept(res)
    for k, r in enumerate(corpus.roots):
        reason, p, cost = kept[r]
        if k % 2 == 1:
            assert reason == "root" and cost == 1.0
        else:
            assert reason == "root_record" and cost == pytest.approx(0.1) and p == 1.0
    n_rec = sum(1 for r in corpus.roots if kept[r][0] == "root_record")
    assert n_rec == len(corpus.roots) // 2
    # volume is counted in span units: 20 full roots + 20 outliers + 20 records * 0.1
    total_cost = sum(p.cost_post for p in res.periods)
    assert total_cost == pytest.approx(20 + 20 + 20 * 0.1)


def test_record_policy_stamps_p_one_on_plan_kept_roots():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=30)
    res = _run(corpus, root_policy="record", initial_plan_p=0.5)
    kept = _kept(res)
    roots = [kept[r] for r in corpus.roots]
    assert {r for r, _, _ in roots} <= {"plan", "root_record"}
    assert all(p == 1.0 for _, p, _ in roots)           # the root duration always reaches the collector


def test_plan_policy_treats_the_root_as_an_ordinary_span():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=40)
    # every other root violates the SLO (12 ms): kept as an outlier; the rest go through the plan coin (p = 0)
    for k, r in enumerate(corpus.roots):
        if k % 2 == 1:
            corpus.spans[r].duration = 0.020
    res = _run(corpus, root_policy="plan", initial_plan_p=0.0)
    kept = _kept(res)
    for k, r in enumerate(corpus.roots):
        if k % 2 == 1:
            assert kept[r][0] == "outlier"
        else:
            assert r not in kept


def test_plan_policy_root_pays_from_the_budget():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=10)
    cfg = BudgetConfig(table="oracle", root_policy="plan", initial_plan_p=1.0, initial_budget=1, b_min=1, b_max=1)
    s = BudgetSampler(cfg)
    s.start_run(RunContext(corpus, 1e9, 1e9, oracle_table(corpus), 1))
    root = corpus.spans[0]
    req = RequestState(root.trace_id, root.route, root.req_class)
    s.on_root_start(root, req, root.start)
    assert s.on_end(corpus.spans[2], req, 0.0)[0]            # c spends the single budget unit
    keep, _, why = s.on_end(root, req, 0.006)
    assert not keep and why == "budget"


def test_collector_cap_counts_records_at_their_cost():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=200, period=10.0)
    res = _run(corpus, root_policy="record", initial_plan_p=0.0, record_cost=0.1, cap=5.05, v_star=5.0)
    st = res.periods[0]
    assert st.kept_post == 50                                  # 50 records fit under 5.05 span units
    assert st.cost_post == pytest.approx(5.0)
    assert st.dropped["cap"] == 150
