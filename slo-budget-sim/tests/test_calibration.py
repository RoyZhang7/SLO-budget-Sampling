"""T_i calibration sources: kept / plan / frozen / oracle, and the drift diagnostics."""
import math

import pytest

from slo_budget_sim.model import Span
from slo_budget_sim.sampler import BudgetConfig, BudgetSampler, RequestState, RunContext
from slo_budget_sim.ttable import Calibrator, oracle_table
from tests.conftest import chain_corpus


def _sampler(corpus, **kw):
    cfg = BudgetConfig(outlier_trigger="duration", initial_budget=1000, pool_factor=1e9, **kw)
    s = BudgetSampler(cfg)
    s.start_run(RunContext(corpus, v_star=1e9, cap=1e9, oracle=oracle_table(corpus), seed=1))
    return s


def _req(s, root):
    req = RequestState(root.trace_id, root.route, root.req_class)
    s.on_root_start(root, req, root.start)
    return req


def _warm(s, corpus):
    """One full period of complete data so a table exists."""
    for r in corpus.roots:
        req = _req(s, corpus.spans[r])
        for i in (r + 2, r + 1):
            s.on_end(corpus.spans[i], req, 0.0)
        s.on_end(corpus.spans[r], req, 0.0)
    s.on_period_end(0, 1.0)


def test_plan_source_ignores_outlier_keeps():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s_kept = _sampler(corpus, calib_source="kept", initial_plan_p=1.0)
    s_plan = _sampler(corpus, calib_source="plan", initial_plan_p=1.0)
    for s in (s_kept, s_plan):
        _warm(s, corpus)
    root = corpus.spans[0]
    for s in (s_kept, s_plan):
        req = _req(s, root)
        keep, _, why = s.on_end(Span(901, 901, 1, 0, "c", "h1", 0.0, 0.009, True, 2, "r", "r|any", 1), req, 0.0)
        assert keep and why == "outlier"
    assert len(s_kept.cal._cur.get(("r", "r|any", "c"), ([], [], []))[0]) == 1
    assert ("r", "r|any", "c") not in s_plan.cal._cur       # plan source did not record the outlier
    # but it does record plan keeps and pinned roots
    req = _req(s_plan, root)
    assert s_plan.on_end(root, req, 0.0)[2] == "root"
    assert len(s_plan.cal._cur[("r", "r|any", "a")][0]) == 1


def test_frozen_source_holds_a_service_over_n_need():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = _sampler(corpus, calib_source="frozen", freeze_trigger="count", n_need=5, initial_plan_p=1.0)
    _warm(s, corpus)
    T_before = s.cal.lookup("r", "r|any", "c")
    O_before = s.cal.O[("r", "r|any", "c")]
    # a period in which c is slow: 40 outliers (> N_need) at 9 ms, plus complete data for a and b
    for k in range(40):
        req = _req(s, corpus.spans[0])
        s.on_end(Span(2000 + k, 2000 + k, 1, 0, "c", "h1", 0.0, 0.009, True, 2, "r", "r|any", 1), req, 0.0)
        s.on_end(corpus.spans[1], req, 0.0)
        s.on_end(corpus.spans[0], req, 0.0)
    s.on_period_end(1, 1.0)
    assert s._frozen == {"c"}
    assert s.cal.O[("r", "r|any", "c")] == pytest.approx(O_before)
    assert s.cal.lookup("r", "r|any", "c") == pytest.approx(T_before, rel=0.05)
    # the same period with the kept source moves T_c up
    s2 = _sampler(corpus, calib_source="kept", n_need=5, initial_plan_p=1.0)
    _warm(s2, corpus)
    for k in range(40):
        req = _req(s2, corpus.spans[0])
        s2.on_end(Span(2000 + k, 2000 + k, 1, 0, "c", "h1", 0.0, 0.009, True, 2, "r", "r|any", 1), req, 0.0)
        s2.on_end(corpus.spans[1], req, 0.0)
        s2.on_end(corpus.spans[0], req, 0.0)
    s2.on_period_end(1, 1.0)
    assert s2.cal.lookup("r", "r|any", "c") > 1.5 * T_before


def test_frozen_releases_when_the_service_calms_down():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = _sampler(corpus, calib_source="frozen", freeze_trigger="count", n_need=5, initial_plan_p=1.0, calib_window=1)
    _warm(s, corpus)
    for k in range(40):
        req = _req(s, corpus.spans[0])
        s.on_end(Span(2000 + k, 2000 + k, 1, 0, "c", "h1", 0.0, 0.009, True, 2, "r", "r|any", 1), req, 0.0)
        s.on_end(corpus.spans[1], req, 0.0)
        s.on_end(corpus.spans[0], req, 0.0)
    s.on_period_end(1, 1.0)
    assert s._frozen == {"c"}
    _warm(s, corpus)                                   # a quiet period: no outliers
    assert s._frozen == set()
    assert s.cal.lookup("r", "r|any", "c") == pytest.approx(0.003, rel=0.05)


def test_oracle_source_has_zero_drift_and_kept_source_reports_drift():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = _sampler(corpus, calib_source="oracle")
    _warm(s, corpus)
    assert all(v == pytest.approx(1.0) for v in s.diagnostics()[-1]["T_ratio"].values())
    s2 = _sampler(corpus, calib_source="kept", initial_plan_p=1.0)
    _warm(s2, corpus)
    ratios = s2.diagnostics()[-1]["T_ratio"]
    assert set(ratios) == {"a", "b", "c"}
    assert all(v == pytest.approx(1.0, rel=1e-6) for v in ratios.values())     # complete data reproduces the oracle


def test_table_alias_maps_to_calib_source():
    assert BudgetConfig(outlier_trigger="duration", table="oracle").calib_source == "oracle"
    assert BudgetConfig(outlier_trigger="duration", table="calibrated").calib_source == "kept"
    assert BudgetConfig(outlier_trigger="duration", calib_source="frozen").table == "calibrated"
    with pytest.raises(ValueError):
        BudgetConfig(outlier_trigger="duration", calib_source="nope")
    with pytest.raises(ValueError):
        BudgetConfig(outlier_trigger="duration", root_policy="nope")


def _fault_period(s, corpus, n_out=40, dur=0.009):
    """One period in which c is slow (n_out outliers) while a and b stay normal."""
    for k in range(n_out):
        req = _req(s, corpus.spans[0])
        s.on_end(Span(2000 + k, 2000 + k, 1, 0, "c", "h1", 0.0, dur, True, 2, "r", "r|any", 1), req, 0.0)
        s.on_end(corpus.spans[1], req, 0.0)
        s.on_end(corpus.spans[0], req, 0.0)
    s.on_period_end(1, 1.0)


def test_rate_trigger_freezes_at_twice_the_healthy_rate_and_purges_the_window():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = _sampler(corpus, calib_source="frozen", freeze_trigger="rate", initial_plan_p=1.0, freeze_release=1)   # Stage 2c release
    _warm(s, corpus)                      # period 0: builds the first table, no baseline yet
    assert s._frozen == set() and s._healthy_rate == {}
    _warm(s, corpus)                      # period 1: healthy rates (0 outliers) become the baseline
    assert s._frozen == set()
    assert s._healthy_rate["c"] == 0.0
    T_before = s.cal.lookup("r", "r|any", "c")
    _fault_period(s, corpus)              # 40 of 40 c spans are outliers: rate 1.0 >= 2 x max(0, floor)
    assert s._frozen == {"c"}
    assert s.cal.lookup("r", "r|any", "c") == pytest.approx(T_before, rel=0.05)
    assert all(k[2] != "c" for k in s.cal._buf[-1])          # the frozen period's c samples were purged
    assert s._healthy_rate["c"] == 0.0                         # the baseline is not updated while frozen
    _warm(s, corpus)                      # quiet period: unfreezes, window holds only clean c samples
    assert s._frozen == set()
    assert s.cal.lookup("r", "r|any", "c") == pytest.approx(0.003, rel=0.05)


def test_rate_trigger_releases_at_the_third_consecutive_period_below_it():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = _sampler(corpus, calib_source="frozen", freeze_trigger="rate", initial_plan_p=1.0)    # freeze_release 3
    _warm(s, corpus)
    _warm(s, corpus)                      # healthy baseline 0
    T_before = s.cal.lookup("r", "r|any", "c")
    _fault_period(s, corpus)
    assert s._frozen == {"c"}
    _fault_period(s, corpus, n_out=2)     # below the trigger (n_min): held, the baseline is not updated
    assert s._frozen == {"c"} and s._quiet["c"] == 1 and s._healthy_rate["c"] == 0.0
    _warm(s, corpus)                      # second period below it: still held
    assert s._frozen == {"c"}
    assert s.cal.lookup("r", "r|any", "c") == pytest.approx(T_before, rel=0.05)
    _fault_period(s, corpus)              # the trigger fires again: the count restarts
    assert s._frozen == {"c"} and s._quiet["c"] == 0
    _warm(s, corpus)
    _warm(s, corpus)
    assert s._frozen == {"c"}
    _warm(s, corpus)                      # third consecutive period below it: released, clean samples enter
    assert s._frozen == set() and "c" not in s._quiet
    assert s.cal.lookup("r", "r|any", "c") == pytest.approx(0.003, rel=0.05)
    with pytest.raises(ValueError):
        BudgetConfig(outlier_trigger="duration", freeze_release=0)


def test_rate_trigger_stays_quiet_in_steady_state_and_ignores_small_counts():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = _sampler(corpus, calib_source="frozen", freeze_trigger="rate", initial_plan_p=1.0)
    for _ in range(5):
        _warm(s, corpus)
        assert s._frozen == set()
    # two outliers in a period are below n_min and do not freeze
    _fault_period(s, corpus, n_out=2)
    assert s._frozen == set()


def test_relative_n_need_scales_with_the_volume_target():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)          # 3 services
    s = BudgetSampler(BudgetConfig(outlier_trigger="duration", table="oracle", n_need_mode="relative", n_need_frac=0.2, n_min=3))
    s.start_run(RunContext(corpus, v_star=300.0, cap=1e9, oracle=oracle_table(corpus), seed=1))
    assert s.n_need == 20.0                                       # floor(0.2 * 300 / 3)
    s.start_run(RunContext(corpus, v_star=10.0, cap=1e9, oracle=oracle_table(corpus), seed=1))
    assert s.n_need == 3.0                                        # N_min
    s2 = BudgetSampler(BudgetConfig(outlier_trigger="duration", table="oracle", n_need_mode="relative", unbounded=True))
    s2.start_run(RunContext(corpus, v_star=300.0, cap=1e9, oracle=oracle_table(corpus), seed=1))
    assert s2.n_need == math.inf
