package slobudget

import "testing"

// one period end of the frozen source for service c: nSpans spans, nOut detected outliers
func freezePeriod(s *Sampler, nSpans, nOut int) bool {
	s.spansSvc = newOrderedMap()
	s.spansSvc.set("c", float64(nSpans))
	s.ndet = map[string]int{"c": nOut}
	s.frozen = s.freezeSet()
	return s.frozen["c"]
}

// Stage 3: a service the rate trigger froze is released only at its FreezeRelease-th
// consecutive period below the trigger (1 = the Stage 2c rule, released at the first).
func TestFreezeReleaseAfterConsecutivePeriodsBelowTheTrigger(t *testing.T) {
	c := chainCorpus([3]float64{6, 2, 1}, 20)
	// periods: trigger, below it (n_out < n_min), quiet, trigger, quiet, quiet, quiet
	periods := [][2]int{{40, 40}, {2, 2}, {40, 0}, {40, 40}, {40, 0}, {40, 0}, {40, 0}}
	for _, tc := range []struct {
		release int
		want    []bool
	}{
		{1, []bool{true, false, false, true, false, false, false}},
		{3, []bool{true, true, true, true, true, true, false}},
	} {
		cfg := ChosenV2()
		cfg.FreezeRelease = tc.release
		s, err := NewSampler(cfg, c.Graph, c.RoutesSLO, c.Hosts, len(c.Services), 300.0, 1001, nil)
		if err != nil {
			t.Fatal(err)
		}
		if s.Cal.Table == nil {
			s.Cal.Table = map[TableKey]float64{}
		}
		s.Cal.Table[TableKey{"r", "*", "c"}] = 0.003 // a table exists
		s.healthyRate["c"] = 0                       // healthy baseline
		for i, p := range periods {
			if got := freezePeriod(s, p[0], p[1]); got != tc.want[i] {
				t.Fatalf("release %d, period %d: frozen %v, want %v", tc.release, i, got, tc.want[i])
			}
			if tc.release == 3 && i == 1 && s.healthyRate["c"] != 0 {
				t.Fatalf("the healthy rate moved while c was held: %v", s.healthyRate["c"])
			}
		}
		if _, held := s.quiet["c"]; held {
			t.Fatalf("release %d: c released but its quiet count is kept", tc.release)
		}
	}
	cfg := ChosenV2()
	cfg.FreezeRelease = 0
	if _, err := NewSampler(cfg, c.Graph, c.RoutesSLO, c.Hosts, len(c.Services), 300.0, 1001, nil); err == nil {
		t.Fatal("freeze_release 0 should be rejected")
	}
}
