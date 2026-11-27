package slobudgetprocessor

import (
	"testing"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
)

// The detail log's threshold string: "route|class=ms" per table key, keys sorted, three decimals of a millisecond.
func TestBStringSortsKeysAndPrintsMilliseconds(t *testing.T) {
	b := map[slobudget.TableKey]float64{
		{Route: "compose_post", Class: "compose_post|small", Service: "c"}: 0.0041,
		{Route: "compose_post", Class: "compose_post|any", Service: "c"}:   0.00395,
	}
	got := bString(b)
	want := "compose_post|compose_post|any=3.950;compose_post|compose_post|small=4.100"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if bString(nil) != "" {
		t.Fatalf("an empty table prints nothing")
	}
}
