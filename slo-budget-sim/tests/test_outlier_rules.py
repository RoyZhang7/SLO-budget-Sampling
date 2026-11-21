"""Pass 5: the healthy-tail outlier rules (miss margin, calibration percentile,
healthy-rate N_need), the workload variants and the result-row hash."""
import numpy as np
import pytest

from slo_budget_sim.experiments import EXP14_SCENARIOS, RULE_VARIANTS, _config_hash_impl, exp14_jobs
from slo_budget_sim.model import Span
from slo_budget_sim.sampler import BudgetConfig, BudgetSampler, RequestState, RunContext
from slo_budget_sim.synthetic import (ALL_SCENARIOS, WORKLOAD_ID, build_scenario, scenario, scenario_tag,
                                      split_scenario, workload_variant)
from slo_budget_sim.ttable import oracle_table
from tests.conftest import chain_corpus


def _sampler(corpus, **kw):
    cfg = BudgetConfig(initial_budget=1000, pool_factor=1e9, **kw)
    s = BudgetSampler(cfg)
    s.start_run(RunContext(corpus, v_star=1e9, cap=1e9, oracle=oracle_table(corpus), seed=1))
    return s


def _req(s, root):
    req = RequestState(root.trace_id, root.route, root.req_class)
    s.on_root_start(root, req, root.start)
    return req


def _warm(s, corpus, period=0):
    for r in corpus.roots:
        req = _req(s, corpus.spans[r])
        for i in (r + 2, r + 1):
            s.on_end(corpus.spans[i], req, 0.0)
        s.on_end(corpus.spans[r], req, 0.0)
    s.on_period_end(period, 1.0)


def _c_span(k, dur, ok=True):
    return Span(900 + k, 900 + k, 1, 0, "c", "h1", 0.0, dur, ok, 2, "r", "r|any", 1)


def test_defaults_are_the_plain_rule_and_do_not_change_the_row_hash():
    cfg = BudgetConfig()
    assert cfg.outlier_margin == 0.0 and cfg.n_need_healthy is False
    assert _config_hash_impl("chosen-v2", {}) == _config_hash_impl("chosen-v2", {"outlier_margin": 0.0, "n_need_healthy": False})
    assert _config_hash_impl("chosen-v2", {}) != _config_hash_impl("chosen-v2", {"outlier_margin": 0.1})
    assert _config_hash_impl("chosen-v2", {}) != _config_hash_impl("chosen-v2", {"n_need_healthy": True})
    with pytest.raises(ValueError):
        BudgetConfig(outlier_margin=-0.1)


def test_margin_plan_samples_the_band_between_T_and_one_plus_m_T():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    plain, margin = _sampler(corpus, initial_plan_p=1.0), _sampler(corpus, outlier_margin=0.3, initial_plan_p=1.0)
    for s in (plain, margin):
        _warm(s, corpus)
    T = plain.cal.lookup("r", "r|any", "c")
    assert margin.cal.lookup("r", "r|any", "c") == pytest.approx(T)
    root = corpus.spans[0]

    def decide(s, k, dur, ok=True):
        return s.on_end(_c_span(k, dur, ok), _req(s, root), 0.0)

    assert decide(plain, 1, 1.2 * T)[2] == "outlier"
    assert decide(margin, 1, 1.2 * T)[2] in ("plan", "plan_coin")     # inside the band: an ordinary span
    assert decide(margin, 2, 1.31 * T)[2] == "outlier"                 # beyond (1 + m) T
    assert decide(margin, 3, 0.5 * T, ok=False)[2] == "outlier"         # a bad status is an outlier regardless
    assert plain._ndet["c"] == 1 and margin._ndet["c"] == 2             # detections follow the margin rule


def test_percentile_is_passed_to_the_calibrator():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = _sampler(corpus, calib_q=0.99)
    assert s.cal.q == 0.99
    labels = {l: ov for l, ov in RULE_VARIANTS}
    assert labels["P99"] == {"calib_q": 0.99} and labels["P97.5"] == {"calib_q": 0.975}
    assert labels["baseline"] == {} and labels["healthy-rate N_need"] == {"n_need_healthy": True}
    assert [ov["outlier_margin"] for l, ov in RULE_VARIANTS if l.startswith("margin")] == [0.1, 0.2, 0.3, 0.5]


def _healthy_tail_period(s, corpus, base, n_pairs=20, period=2):
    """One period in which half of c's spans are over T while the EWMA says the
    healthy over-T rate is `base`; returns the number of outliers kept at p = 1."""
    s._healthy_rate["c"] = base
    T = s.cal.lookup("r", "r|any", "c")
    root = corpus.spans[0]
    full = 0
    for k in range(n_pairs):
        keep, p, why = s.on_end(_c_span(2 * k, 1.5 * T), _req(s, root), 0.0)
        full += int(keep and p == 1.0 and why == "outlier")
        s.on_end(_c_span(2 * k + 1, 0.5 * T), _req(s, root), 0.0)
    s.on_period_end(period, 1.0)
    return full


def test_healthy_rate_counts_only_the_excess_for_n_need_and_the_trigger():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    kw = dict(calib_source="frozen", freeze_trigger="rate", n_need=5, p_rule="const", initial_plan_p=1.0)
    plain = _sampler(corpus, **kw)
    healthy = _sampler(corpus, n_need_healthy=True, **kw)
    for s in (plain, healthy):
        _warm(s, corpus, 0)
        _warm(s, corpus, 1)
    # the service's healthy over-T rate is 20%; this period it runs at 50%
    n_plain = _healthy_tail_period(plain, corpus, 0.2)
    n_healthy = _healthy_tail_period(healthy, corpus, 0.2)
    assert n_plain == 5                       # N_need counts every miss: p = 1 for the first five
    assert n_healthy == 8                     # excess = k - 0.2 x 2k = 0.6 k <= 5 until k = 8
    assert plain._frozen == {"c"}             # 0.5 >= 2 x 0.2
    assert healthy._frozen == set()           # 0.5 - 0.2 < 2 x 0.2
    # at 70% over T the excess (0.5) crosses 2 x 0.2 and the healthy-rate trigger fires too
    s = _sampler(corpus, calib_source="frozen", freeze_trigger="rate", n_need=5, n_need_healthy=True, initial_plan_p=1.0)
    _warm(s, corpus, 0)
    _warm(s, corpus, 1)
    s._healthy_rate["c"] = 0.2
    T = s.cal.lookup("r", "r|any", "c")
    for k in range(20):
        s.on_end(_c_span(k, 1.5 * T if k % 10 < 7 else 0.5 * T), _req(s, corpus.spans[0]), 0.0)
    s.on_period_end(2, 1.0)
    assert s._frozen == {"c"}


def test_healthy_rate_tracks_without_the_frozen_source():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = _sampler(corpus, calib_source="kept", n_need_healthy=True, initial_plan_p=1.0)
    _warm(s, corpus, 0)
    assert s._healthy_rate == {}              # no table yet when period 0 closed
    _warm(s, corpus, 1)
    assert s._healthy_rate["c"] == 0.0        # complete healthy data: nothing over T


def test_workload_variants_and_tags():
    assert WORKLOAD_ID == "6a9c4224"          # the fitted workload is untouched (result rows stay valid)
    assert split_scenario("steady") == ("steady", "fitted")
    assert split_scenario("steady+dr") == ("steady", "drift")
    assert split_scenario("cascade+ht") == ("cascade", "heavytail")
    with pytest.raises(KeyError):
        split_scenario("steady+zz")
    assert scenario_tag("steady") == f"{WORKLOAD_ID}/steadyv1"
    assert scenario_tag("late_everywhere") == f"{WORKLOAD_ID}/late_everywherev2"
    tags = {scenario_tag(s) for s in ("steady", "steady+dr", "steady+ht")}
    assert len(tags) == 3
    cfg = scenario("steady+dr")
    assert cfg.workload == "drift" and cfg.dists["usertimeline"]["type"] in ("lognormal", "lognormal_pareto", "loglogistic")
    assert set(cfg.dists) == {"usertimeline"}
    assert workload_variant("drift")["reference_variant"] == "fitted"
    assert scenario("steady").dists == {} and scenario("steady").workload == "fitted"
    assert all(s in ALL_SCENARIOS for s in EXP14_SCENARIOS)
    assert len(exp14_jobs([1, 2, 3])) == len(EXP14_SCENARIOS) * 3 * len(RULE_VARIANTS) * 3


def test_fitted_corpus_is_unchanged_and_the_drift_variant_keeps_the_fitted_reference():
    corpus, reference = build_scenario("steady", 1)
    assert corpus.spans[5].duration == 0.0032236835040020465            # pass-4 value (the replay dumps)
    small = dict(duration=12.0, rate=60.0)
    c_ut, r_ut = build_scenario("steady+dr", 1, **small)
    c_f, r_f = build_scenario("steady", 1, **small)
    ut = lambda c: np.array([s.duration for s in c.spans if s.service == "usertimeline"])     # noqa: E731
    assert np.median(ut(r_ut)) == pytest.approx(np.median(ut(r_f)), rel=0.02)   # reference from the fitted workload
    assert np.median(ut(c_ut)) > 1.2 * np.median(ut(c_f))                      # the corpus runs usertimeline hotter
    assert c_ut.routes_slo == r_ut.routes_slo
    assert c_ut.meta["tag"] == scenario_tag("steady+dr") and c_ut.meta["variant"] == "drift"
    # the heavy-tail refit of the 10-minute export changes usertimeline by less than the seed noise
    c_ht, _ = build_scenario("steady+ht", 1, **small)
    assert np.median(ut(c_ht)) == pytest.approx(np.median(ut(c_f)), rel=0.03)
