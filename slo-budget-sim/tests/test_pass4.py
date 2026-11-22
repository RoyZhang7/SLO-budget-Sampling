"""Pass 4: the cold-start prior, period-0 metrics, the late-everywhere retune
and resumable experiments."""
import numpy as np
import pytest

from slo_budget_sim import experiments as ex
from slo_budget_sim.experiments import Job, run_experiment
from slo_budget_sim.metrics import CorpusArrays, compute
from slo_budget_sim.model import CallGraph, Span
from slo_budget_sim.replay import replay
from slo_budget_sim.sampler import BudgetConfig, BudgetSampler
from slo_budget_sim.synthetic import (ALL_SCENARIOS, SCENARIOS, build_scenario, scenario, scenario_tag)
from slo_budget_sim.ttable import Calibrator, equal_share_prior, oracle_table
from tests.conftest import chain_corpus
from tests.test_calibration import _req, _sampler, _warm


# -- cold-start prior ---------------------------------------------------------------------
def test_equal_share_prior_on_the_chain(chain):
    # a -> b -> c sequential: three equal stages of SLO / 3, T adds up along the chain
    T = equal_share_prior(0.012, chain.graph, "r")
    assert T["a"] == pytest.approx(0.012)
    assert T["b"] == pytest.approx(0.008)
    assert T["c"] == pytest.approx(0.004)
    # a parallel fan-out: one branch contributes, both callees get the same share
    g = CallGraph({"r": "a"}, {("r", "a"): ["b", "c"], ("r", "b"): [], ("r", "c"): []},
                  {("r", "a"): True, ("r", "b"): False, ("r", "c"): False})
    T = equal_share_prior(0.010, g, "r")
    assert T["a"] == pytest.approx(0.010) and T["b"] == pytest.approx(0.005) and T["c"] == pytest.approx(0.005)


def test_calibrator_prior_answers_until_the_first_calibration(chain):
    cal = Calibrator(chain.graph, chain.routes_slo, window=1, prior="equal")
    assert cal.lookup("r", "r|any", "c") == pytest.approx(0.004)        # cold start: the prior
    assert cal.lookup("r", "r|any", "nope") is None                      # unknown service: nothing
    assert cal.prior_hits == 1
    big = chain_corpus([6.0, 2.0, 1.0], n_traces=50)
    for s in big.spans:
        cal.observe(s, 1.0)
    cal.end_period()
    assert cal.lookup("r", "r|any", "c") == pytest.approx(0.003)        # calibrated: the table
    cal.end_period()                                                     # an empty period: c leaves the window
    assert cal.lookup("r", "r|any", "c") is None                         # ... and the prior does not come back
    assert Calibrator(chain.graph, chain.routes_slo, window=1).lookup("r", "r|any", "c") is None
    with pytest.raises(ValueError):
        Calibrator(chain.graph, chain.routes_slo, prior="nope")
    with pytest.raises(ValueError):
        BudgetConfig(outlier_trigger="duration", cold_prior="nope")


def test_cold_prior_flags_outliers_before_the_first_table():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = _sampler(corpus, cold_prior="equal", initial_plan_p=0.0)
    req = _req(s, corpus.spans[0])
    # prior T_c = SLO / 3 = 4 ms: a 9 ms c span is an outlier in period 0, the 1 ms one is not
    keep, p, why = s.on_end(Span(901, 901, 1, 0, "c", "h1", 0.0, 0.009, True, 2, "r", "r|any", 1), req, 0.0)
    assert keep and why == "outlier" and p == 1.0
    assert s.on_end(corpus.spans[2], req, 0.0)[2] == "plan_coin"
    s_none = _sampler(corpus, cold_prior="none", initial_plan_p=0.0)
    req = _req(s_none, corpus.spans[0])
    assert s_none.on_end(Span(901, 901, 1, 0, "c", "h1", 0.0, 0.009, True, 2, "r", "r|any", 1), req, 0.0)[2] == "plan_coin"
    s.cfg.initial_plan_p = 1.0                 # keep every span from here on so that b and c get calibrated
    _warm(s, corpus)
    assert s.diagnostics()[-1]["prior_hits"] >= 2
    # after the first calibrated period the table answers and the prior is out
    assert s.cal.lookup("r", "r|any", "c") == pytest.approx(0.003, rel=0.05)
    _warm(s, corpus)
    assert s.diagnostics()[-1]["prior_hits"] == 0
    # the oracle arm never uses a prior
    s_or = _sampler(corpus, calib_source="oracle", cold_prior="equal")
    assert s_or.cal.prior_table == {}


# -- period-0 metrics and the fault shape --------------------------------------------------
def _p0_corpus():
    """Chain a -> b -> c over 4 periods of 1 s; every other request has a 4.5 ms
    c span (an outlier against T_c = 3 ms) in every period, period 0 included."""
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=400)
    for k, r in enumerate(corpus.roots):
        if k % 2 == 1:
            corpus.spans[r + 2].duration = corpus.spans[r + 2].own = 0.0045
            corpus.spans[r + 1].duration = 0.0055
            corpus.spans[r].duration = 0.0095
    return corpus, oracle_table(chain_corpus([6.0, 2.0, 1.0], n_traces=400))


def _run(corpus, oracle, **kw):
    cfg = BudgetConfig(outlier_trigger="duration", initial_budget=10, b_min=10, b_max=10, n_need=1000, initial_plan_p=0.0,
                       plan_fill=0.0, ordinary_floor=0.0, **kw)
    return replay(corpus, BudgetSampler(cfg), v_star=1e9, cap=1e9, seed=1, oracle=oracle)


def test_period_zero_metrics_and_truth_rate_shape():
    corpus, oracle = _p0_corpus()
    A = CorpusArrays(corpus, oracle)
    m = compute(corpus, A, _run(corpus, oracle, table="oracle"))          # the oracle flags in period 0 too
    assert m["truth_outliers_p0"] == 50 and m["recall_p0"] == pytest.approx(1.0)
    assert m["outlier_share_p0"] == pytest.approx(1.0) and m["outlier_precision_p0"] == pytest.approx(1.0)
    assert m["lost_outliers_p0"] == 0 and m["prior_periods"] == 0
    # a calibrated arm without a prior has no table in period 0: every period-0 outlier is lost, not flagged
    m = compute(corpus, A, _run(corpus, oracle, calib_source="kept"))
    assert m["recall_p0"] == pytest.approx(0.0) and m["lost_outliers_p0"] == 50
    assert m["ref_not_flagged_p0"] == pytest.approx(1.0) and m["refusal_split_p0"] == "not_flagged:100%"
    # with the prior (T_c = 4 ms) the period-0 outliers are kept (b, never kept with plan p = 0, stays on the prior)
    m = compute(corpus, A, _run(corpus, oracle, calib_source="kept", cold_prior="equal"))
    assert m["recall_p0"] == pytest.approx(1.0) and m["lost_outliers_p0"] == 0
    assert m["outlier_precision_p0"] == pytest.approx(1.0) and m["prior_periods"] >= 1
    # the fault shape on complete data: only c has outliers -> concentrated, not systemic
    assert m["truth_rate_max"] == pytest.approx(0.5, abs=0.01)
    assert m["truth_culprit_est"] == "c" and m["truth_systemic"] is False
    assert m["truth_rate_by_svc"].startswith("c:0.50")


# -- late-everywhere retune ----------------------------------------------------------------
def _rates(corpus, oracle):
    A = CorpusArrays(corpus, oracle)
    window = A.end >= corpus.fault.start
    cnt = np.bincount(A.svc[A.truth & window], minlength=len(A.services))
    vol = np.bincount(A.svc[window], minlength=len(A.services))
    return dict(zip(A.services, cnt / np.maximum(vol, 1)))


def test_late_everywhere_rates_are_even_and_v1_is_not():
    corpus, reference = build_scenario("late_everywhere", seed=1, duration=50.0, rate=120.0)
    rates = _rates(corpus, oracle_table(reference))
    vals = np.array(list(rates.values()))
    assert vals.max() <= 1.5 * np.median(vals), rates
    assert abs(np.median(vals) - 0.25) < 0.06, rates
    assert corpus.fault.culprit is None and corpus.meta["tag"] == scenario_tag("late_everywhere")
    m = compute(corpus, CorpusArrays(corpus, oracle_table(reference)),
                _run(corpus, oracle_table(reference), table="oracle"))
    assert m["truth_systemic"] is True and m["truth_culprit_est"] == "none"
    v1, ref1 = build_scenario("late_everywhere_v1", seed=1, duration=50.0, rate=120.0)
    r1 = _rates(v1, oracle_table(ref1))
    assert max(r1.values()) > 1.5 * np.median(list(r1.values())), r1    # the pass-3 tuning: one stage dominates
    assert v1.meta["tag"] == scenario_tag("late_everywhere_v1") != corpus.meta["tag"]


def test_scenario_variants_and_tags():
    assert "late_everywhere_v1" in ALL_SCENARIOS and "late_everywhere_v1" not in SCENARIOS
    assert scenario("late_everywhere").late_tuning == "rate"
    assert scenario("late_everywhere_v1").late_tuning == "share"
    assert scenario_tag("steady").endswith("/steadyv1") and scenario_tag("late_everywhere").endswith("v2")
    with pytest.raises(KeyError):
        scenario("nope")


# -- resumable experiments ------------------------------------------------------------------
def _tiny(seeds, extra=None):
    return [Job("tiny", "steady", sd, arm, arm, {"n_need": 5} if arm == "chosen" else {}, 0.25, 1.25,
                {"duration": 15.0, "rate": 40.0}) for sd in seeds for arm in ("chosen", "uniform-head")]


def test_run_experiment_resumes_and_reruns_what_changed(tmp_path, monkeypatch, capsys):
    monkeypatch.setitem(ex.EXPERIMENTS, "tiny", _tiny)
    rows = run_experiment("tiny", tmp_path, [1, 2])
    assert len(rows) == 4 and (tmp_path / "tiny.csv").exists() and (tmp_path / "tiny.md").exists()
    assert "0 of 4 runs already" in capsys.readouterr().out
    rows2 = run_experiment("tiny", tmp_path, [1, 2])                     # nothing to do: the rows come back
    assert "4 of 4 runs already" in capsys.readouterr().out
    assert [(r["seed"], r["arm"]) for r in rows2] == [(r["seed"], r["arm"]) for r in rows]
    assert all(r["tag"] == scenario_tag("steady") for r in rows2)
    assert all(len(r["config"]) == 10 for r in rows2 if r["arm"] == "chosen") and rows2[1]["config"] == "truth=own"
    # a changed preset (or default) invalidates the rows even with empty overrides
    monkeypatch.setattr(ex, "_config_hash", lambda arm, ov: "changed" if arm == "chosen" else ex._config_hash_impl(arm, ov))
    run_experiment("tiny", tmp_path, [1, 2])
    assert "2 of 4 runs already" in capsys.readouterr().out
    monkeypatch.setattr(ex, "_config_hash", ex._config_hash_impl)
    # a third seed and changed overrides for chosen: 2 uniform-head rows reused, 4 runs
    monkeypatch.setitem(ex.EXPERIMENTS, "tiny", lambda seeds, extra=None: [
        Job(j.exp, j.scenario, j.seed, j.arm, j.label, {"n_need": 7} if j.arm == "chosen" else {}, j.volume_frac,
            j.cap_factor, j.scenario_overrides) for j in _tiny(seeds)])
    rows3 = run_experiment("tiny", tmp_path, [1, 2, 3])
    assert "2 of 6 runs already" in capsys.readouterr().out and len(rows3) == 6
    assert all(r["overrides"] == "n_need=7" for r in rows3 if r["arm"] == "chosen")
    # another workload or scenario tuning (a different tag) reruns everything
    monkeypatch.setattr(ex, "scenario_tag", lambda name: "other/" + name)
    run_experiment("tiny", tmp_path, [1, 2, 3])
    assert "0 of 6 runs already" in capsys.readouterr().out
    run_experiment("tiny", tmp_path, [1, 2, 3], fresh=True)              # --fresh ignores the file
    assert "0 of 6 runs already" in capsys.readouterr().out


def test_rows_without_a_tag_count_as_the_current_workload_v1():
    r = {"scenario": "steady", "seed": 1, "arm": "chosen", "label": "chosen", "volume_frac": 0.25, "overrides": ""}
    assert ex._row_key(r) == ex._job_key(Job("x", "steady", 1, "chosen", "chosen"))
    r["scenario"] = "late_everywhere"
    assert ex._row_key(r) != ex._job_key(Job("x", "late_everywhere", 1, "chosen", "chosen"))   # v2 now: rerun
