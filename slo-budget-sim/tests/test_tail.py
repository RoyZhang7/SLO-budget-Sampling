"""The tail-based whole-trace baseline: decisions at the root's end, whole traces,
the volume loop, the whole-trace cap and the buffer timeout."""
import math

import pytest

from slo_budget_sim.baselines import TailSampler
from slo_budget_sim.experiments import ABLATION_ARMS, ALL_ARMS, COMPETITOR_ARMS, MECHANISM_ARMS, make_arm
from slo_budget_sim.metrics import CorpusArrays, compute
from slo_budget_sim.replay import replay
from slo_budget_sim.ttable import oracle_table
from tests.conftest import chain_corpus


def _corpus(n=400):
    """Chain a -> b -> c; odd requests in periods >= 1 have a slow root (13 ms > SLO 12 ms)."""
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=n)
    for k, r in enumerate(corpus.roots):
        if k % 2 == 1 and corpus.spans[r].start >= 1.0:
            corpus.spans[r].duration = 0.013
    return corpus, oracle_table(chain_corpus([6.0, 2.0, 1.0], n_traces=n))


def test_whole_traces_are_kept_at_root_end_and_slow_traces_always():
    corpus, oracle = _corpus()
    res = replay(corpus, TailSampler(), v_star=150.0, cap=1e9, seed=1, oracle=oracle)
    kept = set(res.kept_idx.tolist())
    # every slow request is kept whole (3 spans); an ordinary request is kept whole or not at all
    for k, r in enumerate(corpus.roots):
        whole = {r, r + 1, r + 2}
        if corpus.spans[r].duration > 0.012:
            assert whole <= kept
        else:
            assert whole <= kept or not (whole & kept)
    reasons = set(res.kept_reason)
    assert reasons == {"tail_slow", "tail_plan"}
    for idx, why in zip(res.kept_idx.tolist(), res.kept_reason):
        if why == "tail_plan":
            assert corpus.spans[corpus.roots[idx // 3]].duration <= 0.012
    # the decision is stamped at the root's end: kept_time of a child equals the root's end
    i = res.kept_idx.tolist().index(corpus.roots[201] + 2)
    assert res.kept_time[i] == pytest.approx(corpus.spans[corpus.roots[201]].end)
    assert all(d != "defer" for d in res.drop_reason)
    m = compute(corpus, CorpusArrays(corpus, oracle), res)
    assert m["max_per_request"] == 3.0
    # decided traces are half slow (13 ms roots) and half ordinary (6 ms roots): mean latency 9.5 ms
    assert m["buffer_peak_spans"] >= 3 and m["decision_latency_mean"] == pytest.approx(0.0095, rel=0.05)
    assert m["decision_latency_max"] == pytest.approx(0.013, rel=0.01)
    assert m["evicted_traces"] == 0


def test_rate_adapts_to_the_volume_target_and_cap_is_whole_traces():
    corpus, oracle = _corpus(n=1000)
    # 100 requests per period of 1 s, 300 spans; V* = 150 = the slow traces alone, so the plan rate must fall to zero
    res = replay(corpus, TailSampler(), v_star=150.0, cap=1e9, seed=1, oracle=oracle)
    m = compute(corpus, CorpusArrays(corpus, oracle), res)
    assert 0.9 < m["vol_ratio"] < 1.25
    assert res.periods[-2].cost_post <= 165                        # settled: the slow traces plus a few plan keeps
    assert m["max_per_request"] == 3.0
    for per in res.periods:
        kept = [i for i, k in enumerate(res.kept_idx.tolist()) if res.kept_period[i] == per.period]
        assert per.cost_post % 3 == 0 or per.period == res.periods[-1].period      # whole traces only
    # a tight cap refuses whole traces, never splits one
    res = replay(corpus, TailSampler(initial_rate=1.0), v_star=1e9, cap=30.0, seed=1, oracle=oracle)
    for per in res.periods:
        assert per.cost_post <= 30.0 + 1e-9 and per.cost_post % 3 == 0
    m = compute(corpus, CorpusArrays(corpus, oracle), res)
    assert m["cap_dropped_traces"] > 0 and m["over_cap_periods"] == 0


def test_buffer_timeout_evicts_unfinished_traces():
    corpus, oracle = _corpus(n=50)
    # cut the roots of 10 traces short of ending: give them a huge duration so they never end within the replay
    for r in corpus.roots[:10]:
        corpus.spans[r].duration = 100.0
    res = replay(corpus, TailSampler(initial_rate=1.0, timeout_s=0.05), v_star=1e9, cap=1e9, seed=1, oracle=oracle)
    m = compute(corpus, CorpusArrays(corpus, oracle), res)
    assert m["evicted_traces"] == 10
    kept = set(res.kept_idx.tolist())
    assert not any(corpus.roots[k] + 1 in kept for k in range(10))        # their children were dropped
    assert all(res.drop_reason[corpus.roots[k] + 1] == "tail_timeout" for k in range(10))


def test_arm_groups_cover_all_arms():
    assert set(MECHANISM_ARMS) | set(COMPETITOR_ARMS) | set(ABLATION_ARMS) == set(ALL_ARMS)
    assert not (set(COMPETITOR_ARMS) & set(ABLATION_ARMS))
    for a in ("tail", "v2-shared", "v2-oracleT", "v2-keep-parent"):
        make_arm(a, {})
    assert make_arm("v2-shared", {}).cfg.budget_scope == "shared"
    assert make_arm("v2-oracleT", {}).cfg.calib_source == "oracle"
    assert make_arm("v2-keep-parent", {}).cfg.keep_parent is True
    assert math.isinf(make_arm("threshold", {}).cfg.n_need)
