"""Stage 2c: a frozen service keeps its T while the others drift (the renormalisation to
T_root = SLO runs over the unfrozen services only), and the freeze releases once the
service's outlier rate is healthy again (drift workload, transient fault)."""
import pytest

from slo_budget_sim.replay import replay
from slo_budget_sim.sampler import make_budget_arm
from slo_budget_sim.synthetic import FaultSpec, build_scenario
from slo_budget_sim.ttable import Calibrator, oracle_table
from tests.conftest import chain_corpus

SLO = 12.0  # ms, route "r" of the chain corpus


def _table(cal, corpus, frozen):
    for s in corpus.spans:
        cal.observe(s, 1.0)
    cal.end_period(frozen)
    return {svc: cal.table[("r", "*", svc)] for svc in ("a", "b", "c")}


def test_frozen_service_keeps_its_T_while_others_drift():
    healthy = chain_corpus([6.0, 2.0, 1.0], n_traces=60)
    drifted = chain_corpus([9.0, 2.0, 1.0], n_traces=60)     # a's own time 4 -> 7 ms, b and c unchanged
    cal = Calibrator(healthy.graph, healthy.routes_slo, q=0.95, window=1, use_class=False)
    t1 = _table(cal, healthy, set())
    assert t1["a"] == pytest.approx(SLO / 1000.0)
    # b frozen while a drifts: b's T is held bit for bit, the root still gets the SLO, c is renormalised
    t2 = _table(cal, drifted, {"b"})
    assert t2["b"] == t1["b"]
    assert t2["a"] == pytest.approx(SLO / 1000.0, abs=1e-12)
    assert t2["c"] != t1["c"]
    # a second frozen period holds it still
    t3 = _table(cal, drifted, {"b"})
    assert t3["b"] == t1["b"]
    # released: b is calibrated again and the plain renormalisation moves it with a's drift
    t4 = _table(cal, drifted, set())
    assert t4["b"] != t1["b"]
    assert t4["a"] == pytest.approx(SLO / 1000.0, abs=1e-12)


def test_held_T_survives_the_class_key_and_a_missing_previous_table():
    healthy = chain_corpus([6.0, 2.0, 1.0], n_traces=60)
    cal = Calibrator(healthy.graph, healthy.routes_slo, q=0.95, window=1, use_class=True)
    # frozen before any table exists: nothing to hold and b's samples are purged, so the
    # split runs over a and c (b has no entry yet); the root still gets the SLO
    for sp in healthy.spans:
        cal.observe(sp, 1.0)
    cal.end_period({"b"})
    assert cal.table[("r", "*", "a")] == pytest.approx(SLO / 1000.0, abs=1e-12)
    assert ("r", "*", "b") not in cal.table
    t1 = _table(cal, healthy, set())
    drifted = chain_corpus([9.0, 2.0, 1.0], n_traces=60)
    t2 = _table(cal, drifted, {"b"})
    assert t2["b"] == t1["b"]
    assert cal.table[("r", "r|any", "b")] == t1["b"]


def test_freeze_releases_after_a_transient_fault_on_the_drift_workload():
    fault = FaultSpec("saturate", 30.0, [("socialgraph", 0.0, 5.0)], end=60.0)
    corpus, reference = build_scenario("saturated+dr", seed=1, duration=100.0, rate=120.0, faults=[fault])
    periods = 100.0 / corpus.period
    v_star = 0.12 * len(corpus.spans) / periods
    sampler = make_budget_arm("chosen-v2")
    res = replay(corpus, sampler, v_star, 1.25 * v_star, seed=1, oracle=oracle_table(reference))
    frozen = sorted(d["period"] for d in res.diagnostics if "socialgraph" in d["frozen"])
    fault_periods = range(int(30.0 / corpus.period), int(60.0 / corpus.period) + 1)
    assert any(p in fault_periods for p in frozen), f"socialgraph never froze during the fault: {frozen}"
    late = [p for p in frozen if p > int(60.0 / corpus.period) + 1]
    assert not late, f"socialgraph still frozen after the fault ended: {late}"
    # while frozen, socialgraph's T did not move (held), the root kept the SLO
    held = [d["T_ratio"] for d in res.diagnostics if "socialgraph" in d["frozen"]]
    assert held
