package slobudget

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
)

func orderedOf(kv ...any) *orderedMap {
	m := newOrderedMap()
	for i := 0; i+1 < len(kv); i += 2 {
		m.set(kv[i].(string), kv[i+1].(float64))
	}
	return m
}

// a calls {b, c} together, then d: a 1 ms, b 4 ms, c 1 ms, d 9 ms of own time.
func stageGraph(stages [][]string, parallel bool) *CallGraph {
	g := &CallGraph{Root: map[string]string{"r": "a"},
		Children: map[string]map[string][]string{"r": {"a": {"b", "c", "d"}, "b": {}, "c": {}, "d": {}}},
		Parallel: map[string]map[string]bool{"r": {"a": parallel}}}
	if stages != nil {
		g.Stages = map[string]map[string][][]string{"r": {"a": stages}}
	}
	return g
}

func TestGroupsByStageFlagAndLeftOver(t *testing.T) {
	kids := []string{"b", "c", "d"}
	cases := []struct {
		name string
		g    *CallGraph
		want [][]string
	}{
		{"sequential flag", stageGraph(nil, false), [][]string{{"b"}, {"c"}, {"d"}}},
		{"parallel flag", stageGraph(nil, true), [][]string{{"b", "c", "d"}}},
		{"stages", stageGraph([][]string{{"b", "c"}, {"d"}}, true), [][]string{{"b", "c"}, {"d"}}},
		{"stage order is kept", stageGraph([][]string{{"d"}, {"c", "b"}}, true), [][]string{{"d"}, {"b", "c"}}},
		{"a callee no stage names", stageGraph([][]string{{"d"}}, true), [][]string{{"d"}, {"b"}, {"c"}}},
		{"a stage naming an absent callee", stageGraph([][]string{{"x"}, {"d"}}, true), [][]string{{"d"}, {"b"}, {"c"}}},
	}
	for _, tc := range cases {
		if got := tc.g.groups("r", "a", kids); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
	if got := stageGraph([][]string{{"b", "c"}, {"d"}}, false).groups("r", "a", []string{"c", "d"}); !reflect.DeepEqual(got, [][]string{{"c"}, {"d"}}) {
		t.Errorf("callees without calibration drop out of their stage: %v", got)
	}
}

// the same numbers as the Python test test_sqrt_split_with_stages
func TestSplitWithStages(t *testing.T) {
	L := orderedOf("a", 0.014, "b", 0.004, "c", 0.001, "d", 0.009)
	O := orderedOf("a", 0.001, "b", 0.004, "c", 0.001, "d", 0.009)
	g := stageGraph([][]string{{"b", "c"}, {"d"}}, true)
	T, b := SplitBudgets(0.012, L, g, "r", O, nil, nil)
	for s, want := range map[string]float64{"a": 0.002, "b": 0.004, "c": 0.002, "d": 0.006} {
		if math.Abs(b.vals[s]-want) > 1e-12 {
			t.Errorf("b[%s] = %v, want %v", s, b.vals[s], want)
		}
	}
	for s, want := range map[string]float64{"a": 0.012, "b": 0.004, "c": 0.002, "d": 0.006} {
		if math.Abs(T.vals[s]-want) > 1e-12 {
			t.Errorf("T[%s] = %v, want %v", s, T.vals[s], want)
		}
	}
	// the one-stage flag puts d in parallel with b and c: the longest (d) alone counts, so the shares are larger
	_, bp := SplitBudgets(0.012, L, stageGraph(nil, true), "r", O, nil, nil)
	if !(bp.vals["a"] > b.vals["a"] && bp.vals["d"] > b.vals["d"]) {
		t.Errorf("the parallel flag should give larger shares: %v against %v", bp.vals, b.vals)
	}
}

// Without stages the groups are the flag's, so the split must be the pre-stage one; with stages
// equal to the flag's grouping it must be the same too, held services included.
func TestSplitStagesEqualToTheFlagChangeNothing(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	names := []string{"a", "b", "c", "d", "e", "f"}
	for it := 0; it < 2000; it++ {
		children := map[string][]string{"a": {"b", "c", "d"}, "b": {"e"}, "c": {}, "d": {"e", "f"}, "e": {}, "f": {}}
		parallel := map[string]bool{}
		flagStages := map[string][][]string{}
		for s, ks := range children {
			parallel[s] = rng.Intn(2) == 0
			if len(ks) == 0 {
				continue
			}
			if parallel[s] {
				flagStages[s] = [][]string{ks}
			} else {
				for _, k := range ks {
					flagStages[s] = append(flagStages[s], []string{k})
				}
			}
		}
		L, O := newOrderedMap(), newOrderedMap()
		heldT, heldB := newOrderedMap(), newOrderedMap()
		for _, s := range names {
			L.set(s, 0.001+rng.Float64()*0.01)
			O.set(s, 0.0001+rng.Float64()*0.005)
			if s != "a" && rng.Intn(5) == 0 {
				heldT.set(s, 0.001+rng.Float64()*0.004)
				heldB.set(s, 0.0005+rng.Float64()*0.002)
			}
		}
		mk := func(st map[string][][]string) *CallGraph {
			g := &CallGraph{Root: map[string]string{"r": "a"}, Children: map[string]map[string][]string{"r": children}, Parallel: map[string]map[string]bool{"r": parallel}}
			if st != nil {
				g.Stages = map[string]map[string][][]string{"r": st}
			}
			return g
		}
		T1, b1 := SplitBudgets(0.0234, L, mk(nil), "r", O, heldT, heldB)
		T2, b2 := SplitBudgets(0.0234, L, mk(flagStages), "r", O, heldT, heldB)
		for _, s := range names {
			if T1.vals[s] != T2.vals[s] || b1.vals[s] != b2.vals[s] {
				t.Fatalf("iteration %d, %s: T %v / %v, b %v / %v", it, s, T1.vals[s], T2.vals[s], b1.vals[s], b2.vals[s])
			}
		}
	}
}

func TestDiscardPeriodLeavesOnlyWhatIsClosed(t *testing.T) {
	c := chainCorpus([3]float64{6, 2, 1}, 40)
	cal, err := NewCalibrator(c.Graph, c.RoutesSLO, 0.95, 20, 2, true, "none")
	if err != nil {
		t.Fatal(err)
	}
	for i := range c.Spans {
		cal.Observe(&c.Spans[i], 1.0)
	}
	cal.DiscardPeriod()
	cal.EndPeriod(nil)
	if len(cal.Table) != 0 {
		t.Fatalf("the discarded period still calibrated: %v", cal.Table)
	}
	for i := range c.Spans {
		cal.Observe(&c.Spans[i], 1.0)
	}
	cal.EndPeriod(nil)
	if v, ok := cal.Lookup("r", "r|any", "c"); !ok || !approx(v, 0.003, 1e-9) {
		t.Fatalf("the next period calibrates as before: %v %v", v, ok)
	}
}

// An agent's sampler never closes a period; ResetPeriod must bound what it holds, and change
// nothing it decides with.
func TestResetPeriodDropsTheAgentsSamples(t *testing.T) {
	c := chainCorpus([3]float64{6, 2, 1}, 40)
	cfg := ChosenV2()
	cfg.CalibSource = "frozen"
	s, err := NewSampler(cfg, c.Graph, c.RoutesSLO, c.Hosts, len(c.Services), 300.0, 1001, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Cal.Table = map[TableKey]float64{{"r", "*", "a"}: 0.012}
	count := func() int {
		n := 0
		for _, x := range s.Cal.cur {
			n += len(x.d)
		}
		return n
	}
	for period := 0; period < 5; period++ {
		for i := range c.Spans {
			s.observe(&c.Spans[i], 1.0, "plan")
		}
		if count() == 0 {
			t.Fatal("the samples did not accumulate within the period")
		}
		s.ResetPeriod()
		if count() != 0 || len(s.Cal.buf) != 0 {
			t.Fatalf("period %d: %d samples and %d buffered periods after the reset", period, count(), len(s.Cal.buf))
		}
	}
	if got, ok := s.Cal.Lookup("r", "r|any", "a"); !ok || got != 0.012 {
		t.Fatalf("the published table changed: %v %v", got, ok)
	}
}
