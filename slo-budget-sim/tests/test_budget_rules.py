"""2026-11-26: the budget-rule study options (outlier_policy, pool_follow_demand), the cap drop's reason, the smoothing default and
the freeze under smoothing."""
import math

import pytest

from slo_budget_sim.metrics import _group
from slo_budget_sim.replay import replay
from slo_budget_sim.sampler import BudgetConfig, RunContext, make_budget_arm
from slo_budget_sim.synthetic import build_scenario
from slo_budget_sim.ttable import Calibrator
from tests.conftest import chain_corpus

VOL = 0.12


@pytest.fixture(scope="module")
def saturated():
    corpus, _ = build_scenario("saturated", 1)
    nper = max(1, int(math.ceil((max(s.start + s.duration for s in corpus.spans) - corpus.t0) / corpus.period)))
    return corpus, VOL * len(corpus.spans) / nper


def _reasons(res):
    out = {}
    for r in res.drop_reason:
        out[r] = out.get(r, 0) + 1
    return out


def test_defaults_and_validation():
    cfg = BudgetConfig()
    assert cfg.table_smooth == 6 and cfg.outlier_policy == "nneed" and not cfg.pool_follow_demand
    for bad in ({"outlier_policy": "nope"}, {"pool_demand_floor": 1.0}, {"pool_demand_floor": -0.1}, {"table_smooth": 0}):
        with pytest.raises(ValueError):
            BudgetConfig(**bad)


def test_outliers_first_skips_n_need_and_keeps_more_outliers(saturated):
    corpus, v_star = saturated
    today = replay(corpus, make_budget_arm("chosen-v2"), v_star, 1.25 * v_star, seed=7)
    first = replay(corpus, make_budget_arm("chosen-v2", outlier_policy="first"), v_star, 1.25 * v_star, seed=7)
    assert _reasons(first).get("outlier_coin", 0) == 0                       # no N_need coin
    assert first.kept_reason.count("outlier") > today.kept_reason.count("outlier")
    assert sum(first.kept_reason.count(r) for r in ("outlier", "plan", "root")) <= len(first.kept_reason)


def test_n_need_only_when_ahead_drops_no_more_than_today(saturated):
    corpus, v_star = saturated
    today = replay(corpus, make_budget_arm("chosen-v2"), v_star, 1.25 * v_star, seed=7)
    ahead = replay(corpus, make_budget_arm("chosen-v2", outlier_policy="nneed_when_ahead"), v_star, 1.25 * v_star, seed=7)
    assert _reasons(ahead).get("outlier_coin", 0) <= _reasons(today).get("outlier_coin", 0)


def test_pools_follow_the_outlier_demand():
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=60, hosts={"a": "h1", "b": "h2", "c": "h3"})
    s = make_budget_arm("chosen-v2", pool_follow_demand=True)
    s.start_run(RunContext(corpus, 100.0, 125.0, None, 1))
    s._spans_host = {"h1": 60, "h2": 60, "h3": 60}
    s._ndet_host = {"h1": 90, "h2": 10}                       # last period's outliers: 90 % on h1, none on h3
    s.on_period_end(0, 50.0)
    # each host keeps a 5 % floor share, the rest follows the demand: 0.05 + 0.85 x (0.9, 0.1, 0) of V* = 100
    assert s._pool_full == {"h1": round(81.5), "h2": round(13.5), "h3": 5}


def test_a_cap_drop_records_what_it_would_have_been_kept_for(saturated):
    corpus, v_star = saturated
    res = replay(corpus, make_budget_arm("chosen-v2"), v_star, 0.3 * v_star, seed=7)      # a cap well below the target
    capped = [r for r in res.drop_reason if r.startswith("cap")]
    assert capped and all(r.startswith("cap:") and r[4:] in ("outlier", "plan", "root", "root_record") for r in capped)
    assert sum(ps.dropped.get("cap", 0) for ps in res.periods) == len(capped)
    assert _group("cap:plan") == "cap" and _group("cap") == "cap" and _group("pool") == "pool"


def test_a_frozen_service_keeps_its_b_under_smoothing():
    healthy = chain_corpus([6.0, 2.0, 1.0], n_traces=60)
    drifted = chain_corpus([6.0, 4.0, 1.0], n_traces=60)          # b's own time 2 -> 3 ms
    cal = Calibrator(healthy.graph, healthy.routes_slo, q=0.95, window=1, use_class=False, smooth=3)

    def period(corpus, frozen):
        for sp in corpus.spans:
            cal.observe(sp, 1.0)
        cal.end_period(frozen)
        return cal.b[("r", "*", "b")]

    b0 = [period(healthy, set()) for _ in range(3)][-1]
    b1 = period(drifted, set())                                    # one raw drift enters the 3-period mean
    assert b1 != b0
    held = [period(drifted, {"b"}) for _ in range(4)]             # frozen: published exactly as when the freeze began
    assert all(x == b1 for x in held)
    released = [period(drifted, set()) for _ in range(3)]          # released: smoothing resumes towards the drift
    assert released[0] != b1
