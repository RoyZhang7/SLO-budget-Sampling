"""coin_source = span: every draw of the sampler is a function of the deciding span's id and the draw's number; the same values in the Go
port (slobudget.SpanCoin, coin_test.go); the count record counts into the service's spans."""
from slo_budget_sim.sampler import BudgetConfig, BudgetSampler, RunContext, coin_u, span_coin
from tests.conftest import chain_corpus


def test_span_coin_golden():
    assert span_coin(1, 1) == 0.8939429202831845
    assert span_coin(1, 2) == 0.7457817572627011
    assert span_coin(0xDEADBEEFCAFEF00D, 1) == 0.01856730578732535
    assert span_coin(1234567890123456789, 3) == 0.6982905724364573
    assert span_coin(1, 1) != coin_u(1)                        # never the plan coin's own value


def test_span_coins_are_the_spans_own_and_a_count_record_counts():
    corpus = chain_corpus()
    cfg = BudgetConfig(coin_source="span", plan_coin="hash")
    s = BudgetSampler(cfg, name="x")
    s.start_run(RunContext(corpus, 100.0, 125.0, None, 7))
    root = next(sp for sp in corpus.spans if sp.parent_idx < 0)
    s._cur, s._draw = root.span_id, 0
    assert s.rng.random() == span_coin(root.span_id, 1)
    assert s.rng.random() == span_coin(root.span_id, 2)
    s._cur, s._draw = root.span_id, 0
    assert s.rng.random() == span_coin(root.span_id, 1)        # the same span: the same draws again
    before = s._spans_svc.get("b", 0)
    s.on_count_record("b", "h1", 40)
    assert s._spans_svc["b"] == before + 40 and s._spans_host["h1"] >= 40


def test_rng_is_the_default():
    s = BudgetSampler(BudgetConfig(), name="x")
    s.start_run(RunContext(chain_corpus(), 100.0, 125.0, None, 7))
    assert type(s.rng).__name__ == "Random"
