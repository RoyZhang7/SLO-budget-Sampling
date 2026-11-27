package slobudget

import (
	"math"
	"testing"
)

// Stage 2c: a frozen service keeps its T while the others drift; the renormalisation to
// T_root = SLO runs over the unfrozen services only.
func chainGraph() *CallGraph {
	return &CallGraph{
		Root:     map[string]string{"r": "a"},
		Children: map[string]map[string][]string{"r": {"a": {"b"}, "b": {"c"}, "c": {}}},
		Parallel: map[string]map[string]bool{"r": {"a": false, "b": false, "c": false}},
	}
}

// feed one period of a chain a -> b -> c with the given inclusive durations (ms), own = duration - callee
func feedChain(c *Calibrator, durMs [3]float64, n int, frozen map[string]bool) map[string]float64 {
	svcs := [3]string{"a", "b", "c"}
	for i := 0; i < n; i++ {
		for k, svc := range svcs {
			own := durMs[k]
			if k < 2 {
				own = durMs[k] - durMs[k+1]
			}
			sp := &Span{Service: svc, Route: "r", ReqClass: "r|any", Duration: durMs[k] / 1000, Own: own / 1000, StatusOK: true, Depth: k}
			c.Observe(sp, 1.0)
		}
	}
	c.EndPeriod(frozen)
	out := map[string]float64{}
	for _, svc := range svcs {
		out[svc] = c.Table[TableKey{"r", "*", svc}]
	}
	return out
}

func TestFrozenServiceKeepsItsT(t *testing.T) {
	c, err := NewCalibrator(chainGraph(), map[string]float64{"r": 0.012}, 0.95, 20, 1, false, "none", 1)
	if err != nil {
		t.Fatal(err)
	}
	t1 := feedChain(c, [3]float64{6, 2, 1}, 60, map[string]bool{})
	if math.Abs(t1["a"]-0.012) > 1e-12 {
		t.Fatalf("root T %v", t1["a"])
	}
	t2 := feedChain(c, [3]float64{9, 2, 1}, 60, map[string]bool{"b": true}) // a drifts, b frozen
	if t2["b"] != t1["b"] {
		t.Fatalf("frozen b moved: %v -> %v", t1["b"], t2["b"])
	}
	if math.Abs(t2["a"]-0.012) > 1e-12 {
		t.Fatalf("root T with a held service %v", t2["a"])
	}
	if t2["c"] == t1["c"] {
		t.Fatalf("c should be renormalised: %v", t2["c"])
	}
	t3 := feedChain(c, [3]float64{9, 2, 1}, 60, map[string]bool{"b": true})
	if t3["b"] != t1["b"] {
		t.Fatalf("frozen b moved in the second frozen period: %v", t3["b"])
	}
	t4 := feedChain(c, [3]float64{9, 2, 1}, 60, map[string]bool{})
	if t4["b"] == t1["b"] {
		t.Fatalf("released b should be recalibrated with the drift: %v", t4["b"])
	}
	if math.Abs(t4["a"]-0.012) > 1e-12 {
		t.Fatalf("root T after release %v", t4["a"])
	}
}

func TestFrozenBeforeAnyTableFallsBackToThePlainSplit(t *testing.T) {
	c, _ := NewCalibrator(chainGraph(), map[string]float64{"r": 0.012}, 0.95, 20, 1, true, "none", 1)
	t0 := feedChain(c, [3]float64{6, 2, 1}, 60, map[string]bool{"b": true})
	if math.Abs(t0["a"]-0.012) > 1e-12 {
		t.Fatalf("root T %v", t0["a"])
	}
	t1 := feedChain(c, [3]float64{6, 2, 1}, 60, map[string]bool{})
	t2 := feedChain(c, [3]float64{9, 2, 1}, 60, map[string]bool{"b": true})
	if t2["b"] != t1["b"] {
		t.Fatalf("frozen b moved: %v -> %v", t1["b"], t2["b"])
	}
	if v := c.Table[TableKey{"r", "r|any", "b"}]; v != t1["b"] {
		t.Fatalf("class entry of the frozen b moved: %v", v)
	}
}
