"""own_measure: the own trigger and its calibration on the within-service latency (arrival at the pod to
the handler's end, minus the downstream calls) instead of the handler's own time."""
import json
import random

import pytest

from slo_budget_sim.dump import add_waits, dump_replay
from slo_budget_sim.experiments import _config_hash_impl
from slo_budget_sim.model import Span
from slo_budget_sim.replay import replay
from slo_budget_sim.sampler import BudgetConfig, BudgetSampler, RequestState, RunContext, make_budget_arm
from slo_budget_sim.synthetic import build_scenario
from slo_budget_sim.ttable import Calibrator, oracle_table
from tests.conftest import chain_corpus

# the chain a -> b -> c (6, 2, 1 ms inclusive; own 4, 1, 1 ms), SLO 12 ms: shares b = 6, 3, 3 ms


def _sampler(corpus, within=False, **kw):
    cfg = BudgetConfig(table="oracle", initial_budget=1000, pool_factor=1e9, initial_plan_p=0.0,
                       own_measure="within_service" if within else "handler", **kw)
    s = BudgetSampler(cfg)
    s.start_run(RunContext(corpus, v_star=1e9, cap=1e9, oracle=oracle_table(corpus, within=within), seed=1))
    return s


def _span(k, svc, own_ms, wait_ms, dur_ms=None):
    return Span(900 + k, 900 + k, 1, 0, svc, "h1", 0.0, (dur_ms or own_ms) / 1000.0, True, 2, "r", "r|any", 1, 0.0, own_ms / 1000.0,
                wait=wait_ms / 1000.0)


def test_the_wait_counts_only_with_the_switch():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=50)        # no waits: both measures calibrate b = 3 ms for c
    off, on = _sampler(corpus), _sampler(corpus, within=True)
    root = corpus.spans[0]
    for k, (own, wait, want_off, want_on) in enumerate([
            (1.0, 0.0, False, False), (1.0, 2.2, False, False),        # 3.2 ms is inside the margin (3.3)
            (1.0, 2.4, False, True),                                   # 3.4 ms: late only from its arrival
            (3.4, 0.0, True, True), (3.2, 0.2, False, True)]):
        for s, want in ((off, want_off), (on, want_on)):
            req = RequestState(root.trace_id, root.route, root.req_class)
            s.on_root_start(root, req, 0.0)
            assert (s.on_end(_span(k, "c", own, wait), req, 0.0)[2] == "outlier") == want, (own, wait, s.cfg.own_measure)


def test_the_miss_is_of_the_same_measure():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=50)
    s = _sampler(corpus, within=True)
    out, miss = s._outlier_test(_span(1, "c", 4.0, 2.0))
    assert out and miss == pytest.approx((0.006 - 0.003) / 0.003)
    assert s._outlier_test(_span(2, "c", 1.0, 0.0)) == (False, 0.0)


def test_thresholds_are_calibrated_on_the_same_measure():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=60)
    for sp in corpus.spans:                                     # 3 ms in c's wait, none elsewhere
        sp.wait = 0.003 if sp.service == "c" else 0.0
    h = oracle_table(corpus)
    w = oracle_table(corpus, within=True)
    assert h.O[("r", "*", "c")] == pytest.approx(0.001) and w.O[("r", "*", "c")] == pytest.approx(0.004)
    assert h.L[("r", "*", "c")] == pytest.approx(0.001) and w.L[("r", "*", "c")] == pytest.approx(0.004)
    # own quantiles 4, 1, 4 ms: shares 12 x sqrt(own) / (2 + 1 + 2)
    assert [w.lookup_b("r", "r|any", s) for s in "abc"] == pytest.approx([0.0048, 0.0024, 0.0048])
    assert [h.lookup_b("r", "r|any", s) for s in "abc"] == pytest.approx([0.006, 0.003, 0.003])
    # a span without an arrival, and one whose own time is not known, are taken as they are
    cal = Calibrator(corpus.graph, corpus.routes_slo, use_class=False, within=True)
    cal.observe(_span(1, "c", 2.0, 0.0), 1.0)
    cal.observe(Span(1, 1, 1, 0, "c", "h1", 0.0, 0.005, True, 2, "r", "r|any", 1, 0.0, 0.0, wait=0.001), 1.0)
    d, o, _ = cal._cur[("r", "*", "c")]
    assert d == pytest.approx([0.002, 0.006]) and o == pytest.approx([0.002, 0.006])


def test_default_is_the_handlers_own_time():
    assert BudgetConfig().own_measure == "handler"
    corpus, reference = build_scenario("saturated", seed=2, duration=40.0, rate=50.0, period=2.0)
    run = lambda **kw: replay(corpus, make_budget_arm("chosen-v2", **kw), 400.0, 500.0, seed=1002, oracle=oracle_table(reference))
    plain = run()
    add_waits(corpus, 1)                                        # arrivals are there, the switch is off
    off = run()
    assert (plain.kept_idx == off.kept_idx).all() and (plain.kept_p == off.kept_p).all()
    on = run(own_measure="within_service")
    assert len(on.kept_idx) != len(off.kept_idx) or (on.kept_idx != off.kept_idx).any()
    for sp in corpus.spans:                                     # and without arrivals the switch changes nothing
        sp.wait = 0.0
    none = run(own_measure="within_service")
    assert (plain.kept_idx == none.kept_idx).all() and (plain.kept_p == none.kept_p).all()


def test_validation_and_hash():
    with pytest.raises(ValueError):
        BudgetConfig(own_measure="arrival")
    with pytest.raises(ValueError):
        BudgetConfig(own_measure="within_service", outlier_trigger="duration")
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=20)
    s = BudgetSampler(BudgetConfig(table="oracle", own_measure="within_service"))
    with pytest.raises(ValueError):
        s.start_run(RunContext(corpus, 1e9, 1e9, oracle_table(corpus), 1))
    assert _config_hash_impl("chosen-v2", {}) == _config_hash_impl("chosen-v2", {"own_measure": "handler"})
    assert _config_hash_impl("chosen-v2", {}) != _config_hash_impl("chosen-v2", {"own_measure": "within_service"})


def test_dump_carries_the_waits(tmp_path):
    ev, _ = dump_replay("steady", 1, 0.12, tmp_path, overrides={"own_measure": "within_service"}, waits=True)
    d = json.load(open(ev, encoding="utf-8"))
    w = d["spans"]["wait"]
    assert d["config"]["own_measure"] == "within_service" and len(w) == len(d["spans"]["start"])
    assert all((x == 0.0) == (p < 0) for x, p in zip(w, d["spans"]["parent_idx"]))
    assert 0.0002 < sorted(w)[len(w) // 2] < 0.0004
    ev2, _ = dump_replay("steady", 1, 0.12, tmp_path / "plain")
    assert "wait" not in json.load(open(ev2, encoding="utf-8"))["spans"]


def test_add_waits_follows_the_fault():
    corpus, _ = build_scenario("saturated", seed=2, duration=40.0, rate=50.0, period=2.0)
    add_waits(corpus, 3)
    svc, (lo, hi) = next(iter(corpus.fault.onsets.items()))
    inside = [s.wait for s in corpus.spans if s.service == svc and lo <= s.start < hi]
    outside = [s.wait for s in corpus.spans if s.service == svc and s.start < lo]
    assert min(len(inside), len(outside)) > 50
    assert sorted(inside)[len(inside) // 2] > 10 * sorted(outside)[len(outside) // 2]
    assert random.Random(3).gauss(0.0, 1.0) == random.Random(3).gauss(0.0, 1.0)


def test_a_span_that_ends_after_its_root_has_its_requests_budget():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=40)
    for k in range(0, 40, 4):                                   # c of every fourth trace ends 1 ms after the root
        c = corpus.spans[3 * k + 2]
        c.duration = corpus.spans[3 * k].end + 0.001 - c.start
        c.own = c.duration
    res = replay(corpus, make_budget_arm("chosen-v2", initial_plan_p=1.0), 1e6, 1e6, seed=1, oracle=oracle_table(corpus))
    late = [res.drop_reason[3 * k + 2] for k in range(0, 40, 4)]
    assert set(late) == {""}                                    # kept: b and a have spent two of the three
