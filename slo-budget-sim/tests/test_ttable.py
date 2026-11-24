import math
import random

import pytest

from slo_budget_sim.model import CallGraph, Span
from slo_budget_sim.ttable import Calibrator, oracle_table, split_budgets, sqrt_split, weighted_quantile
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


def _stage_graph(stages=None, parallel=False):
    g = CallGraph({"r": "a"}, {("r", "a"): ["b", "c", "d"], ("r", "b"): [], ("r", "c"): [], ("r", "d"): []},
                  {("r", "a"): parallel})
    if stages is not None:
        g.stages[("r", "a")] = stages
    return g


@pytest.mark.parametrize("stages, parallel, want", [
    (None, False, [["b"], ["c"], ["d"]]),                          # the sequential flag: a stage per callee
    (None, True, [["b", "c", "d"]]),                               # the parallel flag: one stage
    ([["b", "c"], ["d"]], True, [["b", "c"], ["d"]]),
    ([["d"], ["c", "b"]], True, [["d"], ["b", "c"]]),              # the stage order is kept, the callees' order within a stage is the graph's
    ([["d"]], True, [["d"], ["b"], ["c"]]),                        # callees no stage names: stages of their own, last
    ([["x"], ["d"]], True, [["d"], ["b"], ["c"]]),                 # a stage of callees that are not there is dropped
])
def test_groups_by_stage_flag_and_left_over(stages, parallel, want):
    assert _stage_graph(stages, parallel).groups("r", "a", ["b", "c", "d"]) == want


def test_sqrt_split_with_stages():
    # a calls {b, c} together, then d: own times 1, 4, 1, 9 ms -> weights 1, 2, 1, 3 -> root = 1 + max(2, 1) + 3 = 6
    L = {"a": 0.014, "b": 0.004, "c": 0.001, "d": 0.009}
    O = {"a": 0.001, "b": 0.004, "c": 0.001, "d": 0.009}
    T, b = split_budgets(0.012, L, _stage_graph([["b", "c"], ["d"]], True), "r", O)
    assert b == pytest.approx({"a": 0.002, "b": 0.004, "c": 0.002, "d": 0.006})
    assert T == pytest.approx({"a": 0.012, "b": 0.004, "c": 0.002, "d": 0.006})
    # the one-stage flag counts only the longest branch, so every share is larger than the two-stage truth
    _, bp = split_budgets(0.012, L, _stage_graph(None, True), "r", O)
    assert all(bp[s] > b[s] for s in b)


def test_stages_equal_to_the_flag_change_nothing():
    rng = random.Random(3)
    for _ in range(300):
        kids = {"a": ["b", "c", "d"], "b": ["e"], "c": [], "d": ["e", "f"], "e": [], "f": []}
        par = {s: rng.random() < 0.5 for s in kids}
        L = {s: 0.001 + 0.01 * rng.random() for s in kids}
        O = {s: 0.0001 + 0.005 * rng.random() for s in kids}
        held = {s: 0.001 + 0.004 * rng.random() for s in kids if s != "a" and rng.random() < 0.2}
        heldb = {s: 0.0005 + 0.002 * rng.random() for s in held}
        g = CallGraph({"r": "a"}, {("r", s): ks for s, ks in kids.items()}, {("r", s): v for s, v in par.items()})
        gs = CallGraph(g.root, g.children, g.parallel, {("r", s): ([ks] if par[s] else [[c] for c in ks]) for s, ks in kids.items() if ks})
        assert split_budgets(0.0234, L, g, "r", O, held, heldb) == split_budgets(0.0234, L, gs, "r", O, held, heldb)


def test_discard_period_leaves_only_what_is_closed(chain):
    cal = Calibrator(chain.graph, chain.routes_slo, q=0.95, window=2)
    big = chain_corpus([6.0, 2.0, 1.0], n_traces=40)
    for s in big.spans:
        cal.observe(s, 1.0)
    cal.discard_period()
    cal.end_period()
    assert not cal.table                                     # nothing of the discarded period calibrated
    for s in big.spans:
        cal.observe(s, 1.0)
    cal.end_period()
    assert cal.lookup("r", "r|any", "c") == pytest.approx(0.003)   # the next period calibrates as before


def _stage_path_sums(g, route, b, s):
    """Sum of the shares along every path a request can take: one callee of every stage of every service on it."""
    outs = [b[s]]
    for group in g.groups(route, s, [c for c in g.children.get((route, s), []) if c in b]):
        outs = [x + y for x in outs for c in group for y in _stage_path_sums(g, route, b, c)]
    return outs


def test_shares_along_every_stage_path_sum_to_at_most_the_slo():
    rng = random.Random(11)
    for _ in range(300):
        names = ["a", "b", "c", "d", "e", "f", "g", "h"]
        kids = {"a": ["b", "c", "d", "e"], "b": ["f", "g"], "c": [], "d": ["h"], "e": [], "f": [], "g": [], "h": []}
        stages = {}
        for s in ("a", "b"):
            ks = kids[s][:]
            rng.shuffle(ks)
            cut = rng.randint(1, len(ks))
            stages[("r", s)] = [g for g in (ks[:cut], ks[cut:]) if g]
        g = CallGraph({"r": "a"}, {("r", s): ks for s, ks in kids.items()}, {("r", s): rng.random() < 0.5 for s in kids}, stages)
        O = {s: 0.0001 + 0.005 * rng.random() for s in names}
        L = {s: 0.001 + 0.01 * rng.random() for s in names}
        slo = 0.0234
        T, b = split_budgets(slo, L, g, "r", O)
        sums = _stage_path_sums(g, "r", b, "a")
        assert max(sums) == pytest.approx(slo)                    # the critical path's shares are the SLO, no other path's exceed it
        assert T["a"] == pytest.approx(slo)
