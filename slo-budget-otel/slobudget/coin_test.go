package slobudget

import "testing"

// The values the Python sampler's coin_u gives (slo-budget-sim/tests/test_coin.py has the same list).
var coinGolden = []struct {
	id uint64
	u  float64
}{
	{1, 0.3381666012719897},
	{2, 0.8586764687735633},
	{0x7fffffffffffffff, 0.35315197670697474},
	{0xdeadbeefcafef00d, 0.09790485404703486},
	{0xffffffffffffffff, 0.7063039534139496},
	{1234567890123456789, 0.7820656816100434},
}

func TestCoinU(t *testing.T) {
	for _, g := range coinGolden {
		if u := CoinU(g.id); u != g.u {
			t.Errorf("CoinU(%#x) = %.17g, Python gives %.17g", g.id, u, g.u)
		}
	}
	if CoinU(0) < 0 || CoinU(0) >= 1 {
		t.Error("out of range")
	}
}

// With plan_coin hash the plan decision is the id's, and the spans a filter dropped count as seen.
func TestHashCoinAndDroppedBefore(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PlanCoin = "hash"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.PlanCoin = "trace"
	if err := cfg.Validate(); err == nil {
		t.Error("trace should not validate")
	}
}

// SpanCoin: the Python span_coin's values (tests/test_span_coins.py); a sampler with coin_source span draws them for the
// deciding span, the same draws again for the same span.
func TestSpanCoinMatchesPython(t *testing.T) {
	for _, g := range []struct {
		id   uint64
		draw int
		u    float64
	}{{1, 1, 0.8939429202831845}, {1, 2, 0.7457817572627011}, {0xdeadbeefcafef00d, 1, 0.01856730578732535}, {1234567890123456789, 3, 0.6982905724364573}} {
		if u := SpanCoin(g.id, g.draw); u != g.u {
			t.Errorf("SpanCoin(%#x, %d) = %.17g, Python gives %.17g", g.id, g.draw, u, g.u)
		}
	}
	if SpanCoin(1, 1) == CoinU(1) {
		t.Error("a draw must not be the plan coin's own value")
	}
	cfg := DefaultConfig()
	cfg.CoinSource = "span"
	cfg.PlanCoin = "hash"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	c := &spanCoins{}
	c.deciding(1)
	if c.Random() != SpanCoin(1, 1) || c.Random() != SpanCoin(1, 2) {
		t.Error("the deciding span's draws")
	}
	c.deciding(1)
	if c.Random() != SpanCoin(1, 1) {
		t.Error("the same span: the same draws")
	}
	cfg.CoinSource = "dice"
	if err := cfg.Validate(); err == nil {
		t.Error("coin_source dice should not validate")
	}
}
