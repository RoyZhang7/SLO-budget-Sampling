"""tailfit: distribution functions, draws and fits of the own-time families."""
import math

import numpy as np
import pytest

from slo_budget_sim import tailfit as tf


@pytest.mark.parametrize("spec", [
    tf.lognormal_spec(5.65, 0.16),
    {"type": "loglogistic", "scale_ms": 5.0, "shape": 12.0},
    {"type": "lognormal_pareto", "median_ms": 5.65, "sigma": 0.16, "tail_q": 0.95, "tail_alpha": 3.0},
])
def test_quantile_cdf_and_density_are_consistent(spec):
    p = np.array([0.01, 0.1, 0.5, 0.9, 0.95, 0.96, 0.99, 0.999])
    assert np.abs(tf.cdf(spec, tf.quantile(spec, p)) - p).max() < 1e-9
    x = np.linspace(0.5, 300.0, 300000)
    assert np.trapezoid(np.exp(tf.logpdf(spec, x)), x) == pytest.approx(1.0, abs=1e-3)
    rng = np.random.default_rng(1)
    d = tf.draw(spec, rng, 50000)
    assert abs((d > tf.quantile(spec, 0.95)).mean() - 0.05) < 0.005


def test_fit_recovers_the_tail_index_and_picks_the_right_family():
    rng = np.random.default_rng(3)
    true = {"type": "lognormal_pareto", "median_ms": 5.65, "sigma": 0.16, "tail_q": 0.95, "tail_alpha": 3.0}
    x = tf.draw(true, rng, 60000)
    res = tf.fit_candidates(x)
    assert res["chosen"] == "lognormal_pareto@0.95"
    spec = res["candidates"][res["chosen"]]["spec"]
    assert spec["tail_alpha"] == pytest.approx(3.0, rel=0.1)
    assert spec["median_ms"] == pytest.approx(5.65, rel=0.01)
    ll = {"type": "loglogistic", "scale_ms": 5.0, "shape": 12.0}
    fit = tf.fit_loglogistic(tf.draw(ll, rng, 60000))
    assert fit["scale_ms"] == pytest.approx(5.0, rel=0.01) and fit["shape"] == pytest.approx(12.0, rel=0.05)
    # a plain lognormal sample is read as lognormal (or as a spliced fit with a tail too light to matter)
    res2 = tf.fit_candidates(tf.draw(tf.lognormal_spec(5.65, 0.16), rng, 60000))
    chosen = res2["candidates"][res2["chosen"]]["spec"]
    assert chosen["type"] == "lognormal" or chosen["tail_alpha"] > 8


def test_generator_drawer_keeps_the_lognormal_arithmetic():
    zs = iter([0.3, -1.2, 2.0])
    us = iter([0.5, 0.99])
    f = tf.drawer(tf.lognormal_spec(5.6513, 0.159), lambda: next(zs), lambda: next(us))
    med, sig = 5.6513 / 1000.0, 0.159
    assert f() == med * math.exp(sig * 0.3)
    assert f() == med * math.exp(sig * -1.2)
    g = tf.drawer({"type": "lognormal_pareto", "median_ms": 5.6513, "sigma": 0.159, "tail_q": 0.95, "tail_alpha": 3.0},
                  lambda: next(zs), lambda: next(us))
    xq = tf.splice_point_ms({"type": "lognormal_pareto", "median_ms": 5.6513, "sigma": 0.159, "tail_q": 0.95, "tail_alpha": 3.0})
    v = g()                                   # z = 2.0 is above the splice: a Pareto draw with u = 0.5
    assert v == pytest.approx(xq / 1000.0 * (1.0 - 0.5) ** (-1.0 / 3.0))
    with pytest.raises(ValueError):
        tf.cdf({"type": "weibull"}, 1.0)
