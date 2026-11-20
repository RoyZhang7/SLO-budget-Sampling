import math

import pytest

from slo_budget_sim.model import CallGraph, Span
from slo_budget_sim.ttable import Calibrator, oracle_table, sqrt_split, weighted_quantile
from tests.conftest import chain_corpus


def test_weighted_quantile_matches_unweighted():
    vals = list(range(1, 101))
    assert weighted_quantile(vals, [1.0] * 100, 0.95) == 95
    # doubling every weight changes nothing
    assert weighted_quantile(vals, [2.0] * 100, 0.95) == 95


def test_weighted_quantile_uses_weights():
    # one heavy sample at 10 outweighs ten light ones at 1
    assert weighted_quantile([1] * 10 + [10], [1.0] * 10 + [100.0], 0.5) == 10


def test_sqrt_split_chain():
    # inclusive L = (6, 2, 1) ms -> own = (4, 1, 1) -> sqrt weights (2, 1, 1)
    # SLO 12 -> stage budgets (6, 3, 3) -> inclusive T = (12, 6, 3)
    g = CallGraph({"r": "a"}, {("r", "a"): ["b"], ("r", "b"): ["c"], ("r", "c"): []},
                  {("r", "a"): False, ("r", "b"): False, ("r", "c"): False})
    T = sqrt_split(0.012, {"a": 0.006, "b": 0.002, "c": 0.001}, g, "r")
    assert T["a"] == pytest.approx(0.012)
    assert T["b"] == pytest.approx(0.006)
    assert T["c"] == pytest.approx(0.003)


def test_sqrt_split_parallel_fanout():
    # root a calls b and c in parallel; only the longer branch contributes to W
    g = CallGraph({"r": "a"}, {("r", "a"): ["b", "c"], ("r", "b"): [], ("r", "c"): []},
                  {("r", "a"): True, ("r", "b"): False, ("r", "c"): False})
    # own times given directly: a 1, b 4, c 1; only a and b (the longer branch) form W = 1 + 2
    T = sqrt_split(0.010, {"a": 0.005, "b": 0.004, "c": 0.001}, g, "r", O={"a": 0.001, "b": 0.004, "c": 0.001})
    assert T["a"] == pytest.approx(0.010)
    assert T["b"] == pytest.approx(0.010 * 2 / 3)
    assert T["c"] == pytest.approx(0.010 / 3)
    # the small branch gets relatively more slack than the big one (sqrt)
    assert T["c"] / 0.001 > T["b"] / 0.004
    # without O the own times are derived from L (a: 5 - max(4, 1) = 1) and give the same split
    assert sqrt_split(0.010, {"a": 0.005, "b": 0.004, "c": 0.001}, g, "r")["b"] == pytest.approx(T["b"])


def test_sqrt_split_root_is_slo_regardless_of_scale():
    g = CallGraph({"r": "a"}, {("r", "a"): ["b"], ("r", "b"): []}, {("r", "a"): False, ("r", "b"): False})
    for slo in (0.005, 0.05, 0.5):
        T = sqrt_split(slo, {"a": 0.010, "b": 0.004}, g, "r")
        assert T["a"] == pytest.approx(slo)
        assert T["b"] < slo


def test_calibrator_class_fallback_and_cold_start(chain):
    cal = Calibrator(chain.graph, chain.routes_slo, q=0.95, min_class_samples=20, window=1)
    assert cal.lookup("r", "r|any", "a") is None           # cold start: no table
    big = chain_corpus([6.0, 2.0, 1.0], n_traces=50)
    for s in big.spans:
        s.req_class = "r|small" if s.trace_id % 10 else "r|large"   # 5 large traces: too few for a class key
        cal.observe(s, 1.0)
    cal.end_period()
    assert cal.lookup("r", "r|small", "a") == pytest.approx(0.012)
    assert cal.lookup("r", "r|large", "b") == pytest.approx(cal.lookup("r", "*", "b"))   # fell back to the route key
    assert cal.lookup("r", "r|unknown", "c") == pytest.approx(0.003)                     # unknown class -> route key


def test_calibrator_reweights_by_p():
    """A service whose slow spans were kept at p = 0.1 must still get the same
    P95 as with complete data."""
    cal_full = Calibrator(CallGraph({"r": "a"}, {("r", "a"): []}, {("r", "a"): False}), {"r": 1.0}, window=1)
    cal_sub = Calibrator(CallGraph({"r": "a"}, {("r", "a"): []}, {("r", "a"): False}), {"r": 1.0}, window=1)
    for i in range(1, 1001):
        d = i / 1000.0
        sp = Span(i, i, i, -1, "a", "h", 0.0, d, True, 0, "r", "r|any", 1)
        cal_full.observe(sp, 1.0)
        if d > 0.9:
            if i % 10 == 0:              # keep 1 in 10 of the slow ones, stamped p = 0.1
                cal_sub.observe(sp, 0.1)
        else:
            cal_sub.observe(sp, 1.0)
    cal_full.end_period()
    cal_sub.end_period()
    assert cal_sub.L[("r", "*", "a")] == pytest.approx(cal_full.L[("r", "*", "a")], rel=0.02)


def test_oracle_table_from_reference():
    orc = oracle_table(chain_corpus([6.0, 2.0, 1.0], n_traces=10))
    assert orc.lookup("r", "r|any", "a") == pytest.approx(0.012)
    assert orc.lookup("r", "r|any", "c") == pytest.approx(0.003)
