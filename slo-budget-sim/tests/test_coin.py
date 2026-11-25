"""The hash plan coin (plan_coin: hash) and the spans a filter dropped before a span."""
import random

from slo_budget_sim.sampler import coin_u

GOLDEN = [(0x1, 0.3381666012719897), (0x2, 0.8586764687735633), (0x7FFFFFFFFFFFFFFF, 0.35315197670697474), (0xDEADBEEFCAFEF00D, 0.09790485404703486), (0xFFFFFFFFFFFFFFFF, 0.7063039534139496), (1234567890123456789, 0.7820656816100434)]


def test_coin_golden():
    for i, u in GOLDEN:
        assert coin_u(i) == u


def test_coin_is_uniform_enough():
    rng = random.Random(3)
    us = [coin_u(rng.getrandbits(64)) for _ in range(200_000)]
    assert all(0.0 <= u < 1.0 for u in us)
    assert abs(sum(us) / len(us) - 0.5) < 0.005
    assert abs(sum(u < 0.12 for u in us) / len(us) - 0.12) < 0.004
