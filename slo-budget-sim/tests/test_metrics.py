"""The headline metrics on a corpus whose answers are known."""
import math

import pytest

from slo_budget_sim.metrics import CorpusArrays, compute
from slo_budget_sim.replay import replay
from slo_budget_sim.sampler import BudgetConfig, BudgetSampler
from slo_budget_sim.ttable import oracle_table
from tests.conftest import chain_corpus


def _corpus():
    """Chain a -> b -> c over 4 periods of 1 s; in periods >= 1 every other
    request has a 4.5 ms c span (an outlier against T_c = 3 ms); its parents
    stretch with it but stay under their budgets (T_b = 6, T_a = 12 ms)."""
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=400)
    for k, r in enumerate(corpus.roots):
        if k % 2 == 1 and corpus.spans[r].start >= 1.0:
            corpus.spans[r + 2].duration = corpus.spans[r + 2].own = 0.0045
            corpus.spans[r + 1].duration = 0.0055
            corpus.spans[r].duration = 0.0095
    reference = chain_corpus([6.0, 2.0, 1.0], n_traces=400)
    return corpus, oracle_table(reference)


def _run(corpus, oracle, **kw):
    cfg = BudgetConfig(outlier_trigger="duration", table="oracle", initial_budget=10, b_min=10, b_max=10, n_need=1000, **kw)
    if cfg.initial_plan_p == 0.0:
        cfg.plan_fill = cfg.ordinary_floor = 0.0          # no plan keeps after the first replan either
    return replay(corpus, BudgetSampler(cfg), v_star=1e9, cap=1e9, seed=1, oracle=oracle)


def test_outlier_share_precision_and_context_with_pinned_roots():
    corpus, oracle = _corpus()
    A = CorpusArrays(corpus, oracle)
    res = _run(corpus, oracle, root_policy="pin", initial_plan_p=0.0)
    m = compute(corpus, A, res)
    # kept non-root spans in warm periods are exactly the outliers of c
    assert m["outlier_share"] == pytest.approx(1.0)
    assert m["outlier_precision"] == pytest.approx(1.0)
    assert m["ctx_root_kept"] == pytest.approx(1.0)
    assert m["outlier_no_root"] == pytest.approx(0.0)
    assert m["ctx_sufficient"] == pytest.approx(1.0)           # c is a leaf: no holes
    assert m["max_per_request"] == pytest.approx(2.0)          # root + c
    assert m["root_share"] == pytest.approx(300 / (300 + 150))  # 300 roots, 150 outliers in periods 1..3


def test_context_is_lost_without_roots_and_records_restore_it():
    corpus, oracle = _corpus()
    A = CorpusArrays(corpus, oracle)
    res = _run(corpus, oracle, root_policy="plan", initial_plan_p=0.0)
    m = compute(corpus, A, res)
    assert m["outlier_no_root"] == pytest.approx(1.0)
    assert m["ctx_sufficient"] == pytest.approx(0.0)
    assert m["root_share"] == pytest.approx(0.0)
    res = _run(corpus, oracle, root_policy="record", initial_plan_p=0.0, root_flag_on="any")
    m = compute(corpus, A, res)
    # flagged requests keep the full root, the others a 0.1 record: context is complete
    assert m["ctx_root_kept"] == pytest.approx(1.0)
    assert m["record_share"] == pytest.approx(150 * 0.1 / (150 * 0.1 + 150 + 150))
    assert m["records_total"] == 150 + 100                         # plus the cold-start period's 100 roots
    assert m["p95_err_root"] == pytest.approx(0.0)                 # root durations all reach the collector


def test_holes_break_context_sufficiency():
    corpus, oracle = _corpus()
    A = CorpusArrays(corpus, oracle)
    # make b the outlier instead of c, keep c out: b has one hole
    for k, r in enumerate(corpus.roots):
        if k % 2 == 1 and corpus.spans[r].start >= 1.0:
            corpus.spans[r + 2].duration = 0.001
            corpus.spans[r + 1].duration = 0.0065
            corpus.spans[r].duration = 0.0105
    res = _run(corpus, oracle, root_policy="pin", initial_plan_p=0.0, outlier_margin=0.0)   # b's miss is small: the plain rule
    A2 = CorpusArrays(corpus, oracle)
    m = compute(corpus, A2, res)
    assert m["ctx_root_kept"] == pytest.approx(1.0)
    assert m["ctx_sufficient"] == pytest.approx(0.0)
    assert m["holes_per_kept_parent"] > 0


def test_per_service_p95_error_is_zero_with_complete_data():
    corpus, oracle = _corpus()
    A = CorpusArrays(corpus, oracle)
    res = _run(corpus, oracle, root_policy="pin", initial_plan_p=1.0)
    m = compute(corpus, A, res)
    assert m["p95_err_svc_mean"] == pytest.approx(0.0)
    assert m["p95_svc_n"] == 3
    assert m["max_per_host_period"] == pytest.approx(300.0)        # every span of a period on one host


def test_refusal_split_and_attributability():
    """Chain a -> b -> c, budget 1 per request, oracle T = (12, 6, 3 ms), stage
    budgets b = (6, 3, 3 ms).  Odd requests: c = 4.5 ms (outlier, kept first) and
    b = 8.5 ms (outlier, refused by the budget).  The kept c is a leaf with own
    time 4.5 > 3 ms: explained by (a).  Even requests in warm periods: b = 9 ms
    with c = 1 ms kept by nothing -> b is kept (budget free), its hole c is
    ordinary -> explained by (c)."""
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=400)
    for k, r in enumerate(corpus.roots):
        if corpus.spans[r].start < 1.0:
            continue
        if k % 2 == 1:
            corpus.spans[r + 2].duration = corpus.spans[r + 2].own = 0.0045
            corpus.spans[r + 1].duration = 0.0085
            corpus.spans[r].duration = 0.0119
        else:
            corpus.spans[r + 1].duration = 0.009
            corpus.spans[r].duration = 0.0119
    corpus.finalize()
    reference = chain_corpus([6.0, 2.0, 1.0], n_traces=400)
    oracle = oracle_table(reference)
    A = CorpusArrays(corpus, oracle)
    cfg = BudgetConfig(outlier_trigger="duration", table="oracle", initial_budget=1, b_min=1, b_max=1, n_need=1000, initial_plan_p=0.0,
                       plan_fill=0.0, ordinary_floor=0.0)
    res = replay(corpus, BudgetSampler(cfg), v_star=1e9, cap=1e9, seed=1, oracle=oracle)
    m = compute(corpus, A, res)
    # 150 odd requests lose their b to the budget
    assert m["lost_outliers"] == 150
    assert m["ref_budget"] == pytest.approx(1.0)
    assert m["refusal_split"] == "budget:100%"
    # kept outliers: 150 c (own 4.5 > b_c 3 ms -> explained by own) + 150 even b (hole c ordinary -> clean)
    assert m["kept_outliers"] == 300
    assert m["attrib_own"] == pytest.approx(0.5)
    assert m["attrib_clean"] == pytest.approx(0.5)
    assert m["attrib_explained"] == pytest.approx(1.0)
    assert m["attrib_loss"] == pytest.approx(0.0)


def test_true_loss_is_split_by_the_dropped_childs_refusal_reason():
    """Odd requests: b = 9 ms with own time 4.5 ms (under its 3 ms share? no: 4.5 > 3).
    Use b own = 2.5 ms (< 3 ms share) and c = 6.5 ms: b's excess sits in c; c is
    refused by the budget (b ends... no, c ends first).  Make c lose the N_need coin
    instead: n_need = 1 and a huge count."""
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=400)
    for k, r in enumerate(corpus.roots):
        if corpus.spans[r].start < 1.0:
            continue
        corpus.spans[r + 2].duration = corpus.spans[r + 2].own = 0.0065          # c outlier (T 3 ms)
        corpus.spans[r + 1].duration = 0.009                                     # b outlier (T 6), own 2.5 < 3
        corpus.spans[r].duration = 0.0119
    corpus.finalize()
    reference = chain_corpus([6.0, 2.0, 1.0], n_traces=400)
    oracle = oracle_table(reference)
    A = CorpusArrays(corpus, oracle)
    # c is pinned to lose: p_rule const, n_need 1 -> after the first c per period p = 1/n, most c are coin-dropped;
    # b (ends after c) finds the budget free and is kept
    cfg = BudgetConfig(outlier_trigger="duration", table="oracle", initial_budget=1, b_min=1, b_max=1, n_need=1, p_rule="const",
                       initial_plan_p=0.0, plan_fill=0.0, ordinary_floor=0.0)
    res = replay(corpus, BudgetSampler(cfg), v_star=1e9, cap=1e9, seed=1, oracle=oracle)
    m = compute(corpus, A, res)
    assert m["ref_n_need"] > 0.5                                   # most c were lost to the N_need coin
    assert m["attrib_loss"] > 0.5                                  # ... and their parents cannot be explained
    assert m["loss_n_need"] == pytest.approx(m["attrib_loss"])    # every loss is attributed to that reason
    assert m["attrib_explained"] + m["attrib_loss"] == pytest.approx(1.0)
