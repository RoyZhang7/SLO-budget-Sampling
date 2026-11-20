"""Refill loop: B_next = (1 - alpha) B + alpha * V* / (R_exp * u), u = kept per request / B."""
import pytest

from slo_budget_sim.sampler import BudgetConfig, BudgetSampler, RunContext
from slo_budget_sim.ttable import oracle_table
from tests.conftest import chain_corpus


def _sampler(v_star, alpha=1.0, B0=4.0):
    corpus = chain_corpus([6.0, 2.0, 1.0], n_traces=10)
    s = BudgetSampler(BudgetConfig(table="oracle", alpha=alpha, initial_budget=B0, rate_ewma=1.0))
    s.start_run(RunContext(corpus, v_star=v_star, cap=1e9, oracle=oracle_table(corpus), seed=1))
    return s


def test_fixed_point_when_volume_on_target():
    s = _sampler(v_star=1000.0, alpha=1.0, B0=4.0)
    s._requests = 500
    s.on_period_end(0, collector_volume=1000)     # u = (1000/500)/4 = 0.5 -> V*/(R u) = 4
    assert s.B == pytest.approx(4.0)


def test_budget_grows_when_under_and_shrinks_when_over():
    s = _sampler(v_star=1000.0, alpha=1.0, B0=4.0)
    s._requests = 500
    s.on_period_end(0, collector_volume=500)      # half the target -> B doubles
    assert s.B == pytest.approx(8.0)
    s._requests = 500
    s.on_period_end(1, collector_volume=2000)     # twice the target -> B halves
    assert s.B == pytest.approx(4.0)


def test_damping():
    s = _sampler(v_star=1000.0, alpha=0.5, B0=4.0)
    s._requests = 500
    s.on_period_end(0, collector_volume=500)      # raw 8, damped half way
    assert s.B == pytest.approx(6.0)


def test_clamped_to_bounds():
    s = _sampler(v_star=1000.0, alpha=1.0, B0=4.0)
    s.cfg.b_max = 16
    s._requests = 500
    s.on_period_end(0, collector_volume=1)        # raw would be 2000
    assert s.B == pytest.approx(16.0)
    s._requests = 500
    s.cfg.b_min = 1.0
    s.on_period_end(1, collector_volume=10_000_000)
    assert s.B == pytest.approx(1.0)


def test_loop_converges_in_closed_form_simulation():
    """Iterate the controller against a toy plant: kept = R * min(B, demand)."""
    s = _sampler(v_star=1500.0, alpha=0.5, B0=1.0)
    R, demand = 1000, 2.2
    for per in range(40):
        s._requests = R
        kept = int(R * min(s.B, demand))
        s.on_period_end(per, kept)
    # the plant saturates at demand = 2.2 spans per request (2200 > V*), so B settles at V*/R = 1.5
    assert s.B == pytest.approx(1.5, rel=0.05)
