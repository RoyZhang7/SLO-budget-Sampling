package slobudget

import (
	"math"
	"testing"
)

// Threshold smoothing (table_smooth, 2026-11-26): the published own-time share B of a key is the mean of its last K raw
// calibrations (the latest alone when K = 1), the raw history never grows past K, and a key's published value follows a
// drift only gradually.
func TestTableSmoothPublishesTheMeanOfTheLastKRawTables(t *testing.T) {
	for _, K := range []int{1, 3} {
		c, err := NewCalibrator(chainGraph(), map[string]float64{"r": 0.012}, 0.95, 20, 1, false, "none", K)
		if err != nil {
			t.Fatal(err)
		}
		if c.Smooth != K {
			t.Fatalf("K=%d: the calibrator got smooth %d", K, c.Smooth)
		}
		drifts := [][3]float64{{6, 2, 1}, {6, 2, 1}, {9, 2, 1}, {9, 2, 1}, {9, 2, 1}, {6, 2, 1}}
		var prev map[TableKey]float64
		for i, d := range drifts {
			feedChain(c, d, 60, map[string]bool{})
			want := i + 1
			if want > K {
				want = K
			}
			if len(c.braw) != want {
				t.Fatalf("K=%d period %d: %d raw tables kept", K, i, len(c.braw))
			}
			latest := c.braw[len(c.braw)-1]
			if len(c.B) != len(latest) {
				t.Fatalf("K=%d: B has %d keys, the latest raw table %d", K, len(c.B), len(latest))
			}
			for k, b := range c.B {
				sum, n := 0.0, 0
				for _, h := range c.braw {
					if v, ok := h[k]; ok {
						sum += v
						n++
					}
				}
				if n == 0 || math.Abs(b-sum/float64(n)) > 1e-12 {
					t.Fatalf("K=%d period %d: B[%v] = %g, mean of the last %d raw values %g", K, i, k, b, n, sum/float64(n))
				}
				if K == 1 && b != latest[k] {
					t.Fatalf("K=1: B[%v] = %g differs from the raw %g", k, b, latest[k])
				}
			}
			// the drift of a at period 2: with K = 3 the published value moves by a third of the raw step, with K = 1 all of it
			if i == 2 {
				ka := TableKey{"r", "*", "a"}
				raw, pub := latest[ka]-prev[ka], c.B[ka]-prev[ka]
				if K == 1 && math.Abs(pub-raw) > 1e-12 {
					t.Fatalf("K=1: the published step %g is not the raw step %g", pub, raw)
				}
				if K == 3 && (math.Abs(pub) >= math.Abs(raw)*0.5 || pub*raw <= 0) {
					t.Fatalf("K=3: the published step %g should be about a third of the raw step %g", pub, raw)
				}
			}
			prev = map[TableKey]float64{}
			for k, v := range c.B {
				prev[k] = v
			}
		}
	}
}

func TestTableSmoothMustBeAtLeastOne(t *testing.T) {
	cfg := ChosenV2()
	cfg.TableSmooth = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("table_smooth 0 accepted")
	}
	if DefaultConfig().TableSmooth != 6 {
		t.Fatalf("the default K is %d, not 6", DefaultConfig().TableSmooth)
	}
}

// The freeze is kept under smoothing: a frozen service's published b stays exactly where it was when the freeze began, and
// smoothing resumes when it is released.
func TestTableSmoothKeepsAFrozenServicesB(t *testing.T) {
	c, err := NewCalibrator(chainGraph(), map[string]float64{"r": 0.012}, 0.95, 20, 1, false, "none", 3)
	if err != nil {
		t.Fatal(err)
	}
	kb := TableKey{"r", "*", "b"}
	for i := 0; i < 3; i++ {
		feedChain(c, [3]float64{6, 2, 1}, 60, map[string]bool{})
	}
	b0 := c.B[kb]
	feedChain(c, [3]float64{6, 4, 1}, 60, map[string]bool{}) // b's own time 1 -> 3 ms enters the 3-period mean
	b1 := c.B[kb]
	if b1 == b0 {
		t.Fatalf("the drift did not move b: %v", b1)
	}
	for i := 0; i < 4; i++ {
		feedChain(c, [3]float64{6, 4, 1}, 60, map[string]bool{"b": true})
		if c.B[kb] != b1 {
			t.Fatalf("frozen period %d: b moved %v -> %v", i, b1, c.B[kb])
		}
	}
	feedChain(c, [3]float64{6, 4, 1}, 60, map[string]bool{})
	if c.B[kb] == b1 {
		t.Fatalf("released b should be smoothed towards the drift again: %v", c.B[kb])
	}
}
