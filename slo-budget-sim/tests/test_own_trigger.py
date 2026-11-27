"""Stage 2d: the own-time outlier trigger (own time > (1 + m) x the own-time share b of the
square-root split), the own-time oracle truth, the fault onsets, the trace-consistent plan
coin, decaying N_need counts, hash compatibility and the shares along root-to-leaf paths."""
import math

import numpy as np
import pytest

from slo_budget_sim.experiments import _config_hash_impl
from slo_budget_sim.metrics import CorpusArrays, compute
from slo_budget_sim.model import Span, _union_length
from slo_budget_sim.replay import replay
from slo_budget_sim.sampler import BudgetConfig, BudgetSampler, RequestState, RunContext, make_budget_arm
from slo_budget_sim.synthetic import build_scenario
from slo_budget_sim.ttable import oracle_table
from tests.conftest import chain_corpus

# the chain a -> b -> c (6, 2, 1 ms inclusive; own 4, 1, 1 ms), SLO 12 ms: the oracle split gives
# W = sqrt(4) + sqrt(1) + sqrt(1) = 4, shares b = 12 x sqrt(own) / 4 = 6, 3, 3 ms, T = 12, 6, 3 ms


def _sampler(corpus, **kw):
    cfg = BudgetConfig(table="oracle", initial_budget=1000, pool_factor=1e9, initial_plan_p=0.0, **kw)
    s = BudgetSampler(cfg)
    s.start_run(RunContext(corpus, v_star=1e9, cap=1e9, oracle=oracle_table(corpus), seed=1))
    return s


def _req(s, root):
    req = RequestState(root.trace_id, root.route, root.req_class)
    s.on_root_start(root, req, root.start)
    return req


def _span(k, svc, dur_ms, own_ms, ok=True, depth=2):
    return Span(900 + k, 900 + k, 1, 0, svc, "h1", 0.0, dur_ms / 1000.0, ok, depth, "r", "r|any", 1, 0.0, own_ms / 1000.0)


def test_oracle_shares_on_the_chain():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=50)
    o = oracle_table(corpus)
    assert o.lookup_b("r", "r|any", "a") == pytest.approx(0.006)
    assert o.lookup_b("r", "r|any", "b") == pytest.approx(0.003)
    assert o.lookup_b("r", "r|any", "c") == pytest.approx(0.003)
    assert o.lookup("r", "r|any", "b") == pytest.approx(0.006)


def test_own_trigger_flags_own_time_and_leaves_callers_of_a_slow_callee_ordinary():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=50)
    root = corpus.spans[0]
    own = _sampler(corpus, outlier_trigger="own", own_margin=0.1)
    dur = _sampler(corpus, outlier_trigger="duration", outlier_margin=0.1)
    cases = [  # (service, duration ms, own ms) -> (own trigger outlier, duration trigger outlier)
        (("c", 3.4, 3.4), (True, True)),       # a leaf: own = duration, over 1.1 x 3 ms under both
        (("b", 9.0, 1.0), (False, True)),      # a caller above a slow callee: ordinary under the own trigger
        (("b", 4.0, 3.2), (False, False)),     # own in the band (3, 3.3] ms: plan-sampled
        (("b", 5.0, 3.4), (True, False)),      # own over 3.3 ms while the duration is within 1.1 x T
        (("a", 7.0, 2.0, False), (True, True)),  # an error is an outlier under both
    ]
    for k, (args, (want_own, want_dur)) in enumerate(cases):
        svc, d, o = args[:3]
        ok = args[3] if len(args) > 3 else True
        for s, want in ((own, want_own), (dur, want_dur)):
            before = s._ndet.get(svc, 0)
            keep, p, why = s.on_end(_span(k, svc, d, o, ok), _req(s, root), 0.0)
            flagged = s._ndet.get(svc, 0) > before
            assert flagged == want, (svc, d, o, s.cfg.outlier_trigger, why)
            assert (why == "outlier") == want


def test_own_margin_is_separate_from_the_duration_margin():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=50)
    root = corpus.spans[0]
    s = _sampler(corpus, outlier_trigger="own", own_margin=0.3, outlier_margin=0.0)
    keep, _, why = s.on_end(_span(1, "c", 3.6, 3.6), _req(s, root), 0.0)      # 3.6 < 1.3 x 3 = 3.9
    assert why == "plan_coin"
    keep, _, why = s.on_end(_span(2, "c", 4.0, 4.0), _req(s, root), 0.0)
    assert why == "outlier"
    with pytest.raises(ValueError):
        BudgetConfig(own_margin=-0.1)
    with pytest.raises(ValueError):
        BudgetConfig(outlier_trigger="nope")


def test_defaults_and_hash_compatibility():
    cfg = BudgetConfig()
    assert cfg.outlier_trigger == "own" and cfg.plan_coin == "span" and cfg.n_need_decay == 0.0
    # a duration-trigger configuration hashes as it did before the Stage 2d fields existed,
    # so the result rows computed with the Stage 2c rule stay valid (and the Stage 2c freeze
    # release, before freeze_release existed)
    pre_2d = {("chosen-v2", ()): "1c831dfa20", ("chosen-v2", (("outlier_margin", 0.2),)): "2c1b88020a",
              ("threshold", ()): "b32db50535", ("v2-keep-parent", ()): "693867f75f", ("chosen", ()): "513a407c67"}
    for (arm, ov), h in pre_2d.items():
        assert _config_hash_impl(arm, {"outlier_trigger": "duration", "freeze_release": 1, "table_smooth": 1, **dict(ov)}) == h
    assert _config_hash_impl("chosen-v2", {"outlier_trigger": "duration"}) != pre_2d[("chosen-v2", ())]   # frozen source: release 3
    assert _config_hash_impl("threshold", {"outlier_trigger": "duration", "table_smooth": 1}) == pre_2d[("threshold", ())]   # kept source: no effect
    assert _config_hash_impl("chosen-v2", {}) != pre_2d[("chosen-v2", ())]
    assert _config_hash_impl("chosen-v2", {"own_margin": 0.2}) != _config_hash_impl("chosen-v2", {})


def test_generator_own_time_is_duration_minus_the_client_intervals():
    corpus, _ = build_scenario("steady", seed=2, duration=4.0, rate=40.0)
    worst = 0.0
    for s in corpus.spans:
        if not s.children:
            assert s.own == pytest.approx(s.duration, abs=1e-12)
            continue
        iv = [(corpus.spans[c].start - corpus.spans[c].transit, corpus.spans[c].end + corpus.spans[c].transit) for c in s.children]
        worst = max(worst, abs(s.own - (s.duration - _union_length(iv))))
    assert worst < 1e-9


def test_own_truth_and_the_metric_blocks():
    corpus, reference = build_scenario("saturated", seed=3, duration=40.0, rate=50.0, period=2.0)
    oracle = oracle_table(reference, q=0.95, use_class=True)
    A = CorpusArrays(corpus, oracle)
    B = A.B
    want = (~np.isnan(B) & (A.own > B)) | ~np.array([s.status_ok for s in corpus.spans])
    assert np.array_equal(A.truth_own, want)
    # leaves: own = duration and b = T, so both truths agree there
    leaf = np.array([not s.children for s in corpus.spans])
    assert np.array_equal(A.truth_own[leaf], A.truth[leaf])
    s = make_budget_arm("chosen-v2")
    res = replay(corpus, s, 400.0, 500.0, seed=5, oracle=oracle)
    m_own = compute(corpus, A, res, truth="own")
    m_dur = compute(corpus, A, res, truth="duration")
    for k in ("outlier_share", "outlier_precision", "recall_culprit", "identified", "orphans_per_kept_outlier"):
        assert m_own[k] == m_own[k + "_own"] or (isinstance(m_own[k], float) and math.isnan(m_own[k]))
        assert m_dur[k] == m_dur[k + "_dur"] or (isinstance(m_dur[k], float) and math.isnan(m_dur[k]))
    assert m_own["truth"] == "own" and m_dur["truth"] == "duration"
    # the fault: socialgraph x5 from t = 30 s, applied per request at its arrival
    assert corpus.fault.onsets == {"socialgraph": (30.0, math.inf)}
    root_start = np.array([corpus.spans[r].start for r in A.root_idx])
    sg = np.array([s.service == "socialgraph" for s in corpus.spans])
    assert np.array_equal(A.faulty, sg & (root_start >= 30.0))
    assert m_own["faulty_spans"] == int((A.faulty & (A.period >= 1)).sum()) > 0
    assert 0.0 < m_own["faulty_cov"] <= 1.0


def test_trace_plan_coin_keeps_a_requests_ordinary_spans_together():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=10)
    cfg = BudgetConfig(table="oracle", initial_budget=1000, pool_factor=1e9, initial_plan_p=0.5, plan_coin="trace")
    s = BudgetSampler(cfg)
    s.start_run(RunContext(corpus, v_star=1e9, cap=1e9, oracle=oracle_table(corpus), seed=7))
    root = corpus.spans[0]
    kept = []
    for k in range(400):
        req = _req(s, root)
        a = s.on_end(_span(2 * k, "b", 2.0, 1.0), req, 0.0)[0]       # ordinary spans of the same request
        b = s.on_end(_span(2 * k + 1, "c", 1.0, 1.0), req, 0.0)[0]
        assert a == b
        kept.append(a)
    assert 0.4 < np.mean(kept) < 0.6
    # the span coin decides them independently
    cfg2 = BudgetConfig(table="oracle", initial_budget=1000, pool_factor=1e9, initial_plan_p=0.5)
    s2 = BudgetSampler(cfg2)
    s2.start_run(RunContext(corpus, v_star=1e9, cap=1e9, oracle=oracle_table(corpus), seed=7))
    differ = 0
    for k in range(400):
        req = _req(s2, root)
        differ += s2.on_end(_span(2 * k, "b", 2.0, 1.0), req, 0.0)[0] != s2.on_end(_span(2 * k + 1, "c", 1.0, 1.0), req, 0.0)[0]
    assert differ > 100


def test_n_need_counts_decay_instead_of_resetting():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=50)
    root = corpus.spans[0]
    for decay, want in ((0.0, None), (0.5, 2.0)):
        s = _sampler(corpus, outlier_trigger="own", n_need_decay=decay)
        for k in range(4):
            s.on_end(_span(k, "c", 4.0, 4.0), _req(s, root), 0.0)
        assert s._nseen["c"] == 4
        s.on_period_end(0, 1.0)
        assert s._nseen.get("c") == want
    with pytest.raises(ValueError):
        BudgetConfig(n_need_decay=1.0)


@pytest.mark.parametrize("scenario", ["steady", "steady+dr"])
def test_shares_along_every_root_to_leaf_path_fit_in_the_slo(scenario):
    _, reference = build_scenario(scenario, seed=1, duration=30.0)
    o = oracle_table(reference, q=0.95, use_class=True)
    g = reference.graph
    for route, slo in reference.routes_slo.items():
        root = g.root[route]
        classes = {c for (r, c, _) in o.b if r == route}
        for cls in classes:
            def paths(s, acc):
                kids = g.children.get((route, s), [])
                if not kids:
                    yield acc + [s]
                for c in kids:
                    yield from paths(c, acc + [s])
            for path in paths(root, []):
                total = sum(o.lookup_b(route, cls, s) or 0.0 for s in path)
                assert total <= slo * (1 + 1e-9), (route, cls, path, total / slo)
            # the shares are normalised over the critical structure under T (every callee of a
            # sequential node, the largest-T callee of a parallel fan-out): they sum to the SLO there
            T = {s: o.lookup(route, cls, s) for s in g.services(route) if o.lookup(route, cls, s) is not None}
            crit, stack = [], [root]
            while stack:
                s = stack.pop()
                crit.append(s)
                ks = [c for c in g.children.get((route, s), []) if c in T and c != s]
                if ks:
                    stack.extend([max(ks, key=lambda c: T[c])] if g.parallel.get((route, s), False) else ks)
            assert sum(o.lookup_b(route, cls, s) for s in crit) == pytest.approx(slo, rel=1e-9)


def test_pre_2d_duration_rows_stay_valid_and_baselines_follow_the_default_truth():
    from slo_budget_sim.experiments import _job_key, _row_key, exp14_jobs
    # exp14 (the Stage 2c duration-rule sweep) pins the duration trigger; a row it wrote
    # before the trigger field existed still matches the pinned job
    job = [j for j in exp14_jobs([1]) if j.label == "margin m=0.2"][0]
    assert job.overrides == {"outlier_trigger": "duration", "freeze_release": 1, "table_smooth": 1, "outlier_margin": 0.2}
    row = {"scenario": job.scenario, "seed": 1, "arm": "chosen-v2", "label": job.label, "volume_frac": job.volume_frac,
           "overrides": "outlier_margin=0.2", "config": "2c1b88020a", "tag": _job_key(job)[-1]}
    assert _row_key(row) == _job_key(job)
    # the same overrides on the own trigger hash differently, so that row is not reused for it
    assert _row_key(dict(row, config=_config_hash_impl("chosen-v2", {"outlier_margin": 0.2}))) != _job_key(job)
    # the baselines are judged under the default trigger's truth; their pre-2d rows ("") are recomputed
    assert _config_hash_impl("tail", {}) == "truth=own"
