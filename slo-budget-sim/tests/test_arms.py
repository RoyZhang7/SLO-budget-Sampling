"""Every arm runs on a small scenario, respects the cap, and the metrics are sane."""
import math

import pytest

from slo_budget_sim.experiments import ALL_ARMS, make_arm
from slo_budget_sim.metrics import CorpusArrays, compute
from slo_budget_sim.replay import replay
from slo_budget_sim.synthetic import FaultSpec, build_scenario
from slo_budget_sim.ttable import oracle_table


@pytest.fixture(scope="module")
def small():
    corpus, reference = build_scenario("saturated", seed=11, duration=20.0, rate=60.0, period=2.0,
                                       faults=[FaultSpec("saturate", 10.0, [("socialgraph", 0.0, 5.0)])])
    assert corpus.fault.start == 10.0
    oracle = oracle_table(reference)
    return corpus, oracle, CorpusArrays(corpus, oracle)


@pytest.mark.parametrize("arm", ALL_ARMS)
def test_arm_runs_and_respects_cap(small, arm):
    corpus, oracle, arrays = small
    v_star = 0.25 * len(corpus.spans) / corpus.n_periods
    cap = 1.25 * v_star
    sampler = make_arm(arm, {})
    res = replay(corpus, sampler, v_star, cap, seed=1, oracle=oracle)
    assert all(p.cost_post <= cap + 1e-9 for p in res.periods)
    assert len(res.kept_idx) == len(set(res.kept_idx.tolist()))
    m = compute(corpus, arrays, res)
    assert 0 <= m["recall"] <= 1
    assert 0 <= m["outlier_share"] <= 1
    assert m["kept_total"] > 0
    if getattr(sampler, "cfg", None) is not None and sampler.cfg.root_policy == "pin":
        # roots are pinned by the budget arms, except when the collector cap cuts a period
        kept = set(res.kept_idx.tolist())
        capped = {p.period for p in res.periods if p.kept_pre > cap}
        for r in corpus.roots:
            if corpus.period_of(corpus.spans[r].end) not in capped:
                assert r in kept


@pytest.mark.parametrize("policy", ["pin", "on_demand", "record", "plan"])
@pytest.mark.parametrize("source", ["kept", "plan", "frozen", "oracle"])
def test_root_policies_and_calibration_sources_run(small, policy, source):
    corpus, oracle, arrays = small
    v_star = 0.12 * len(corpus.spans) / corpus.n_periods
    res = replay(corpus, make_arm("chosen-local", {"root_policy": policy, "calib_source": source}),
                 v_star, 1.25 * v_star, seed=1, oracle=oracle)
    m = compute(corpus, arrays, res)
    assert all(p.cost_post <= 1.25 * v_star + 1e-9 for p in res.periods)
    assert 0.3 < m["vol_ratio"] < 2.0
    if policy == "record":
        assert m["records_total"] > 0 and m["ctx_root_kept"] > 0.8
    if policy == "pin":
        assert m["root_share"] > 0.3                     # roots are ~9% of spans, V* is 12%
    if source == "oracle":
        assert m["t_drift_culprit"] == pytest.approx(0.0)


def test_escalation_policies_run(small):
    corpus, oracle, arrays = small
    v_star = 0.25 * len(corpus.spans) / corpus.n_periods
    for pol in ("deadline", "duration_sum"):
        res = replay(corpus, make_arm("chosen", {"escalation": pol}), v_star, 1.25 * v_star, seed=1, oracle=oracle)
        m = compute(corpus, arrays, res)
        assert "escalate" in m["kept_by_reason"] or m["kept_total"] > 0


def test_saturation_is_detected_by_the_chosen_arm(small):
    corpus, oracle, arrays = small
    v_star = 0.25 * len(corpus.spans) / corpus.n_periods
    res = replay(corpus, make_arm("chosen", {}), v_star, 1.25 * v_star, seed=2, oracle=oracle)
    m = compute(corpus, arrays, res)
    assert m["recall_culprit"] > 0.05
    assert not math.isnan(m["evidence_s"]) and m["evidence_s"] < 2.0
    assert m["culprit_est"] == "socialgraph"
