package slobudget

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// chainCorpus is tests/conftest.py's chain a -> b -> c: inclusive durations per
// service for every trace, traces 10 ms apart, SLO 12 ms, one period of 1 s.
func chainCorpus(durMs [3]float64, nTraces int) *Corpus {
	services := []string{"a", "b", "c"}
	c := &Corpus{Name: "chain", RoutesSLO: map[string]float64{"r": 0.012}, Hosts: []string{"h1"},
		HostSkew: map[string]float64{"h1": 0}, Period: 1.0, Services: services}
	c.Graph = &CallGraph{Root: map[string]string{"r": "a"},
		Children: map[string]map[string][]string{"r": {"a": {"b"}, "b": {"c"}, "c": {}}},
		Parallel: map[string]map[string]bool{"r": {"a": false, "b": false, "c": false}}}
	for k := 0; k < nTraces; k++ {
		t0 := 0.010 * float64(k)
		parent := -1
		start := t0
		for depth, svc := range services {
			idx := len(c.Spans)
			c.Spans = append(c.Spans, Span{Idx: idx, SpanID: uint64(idx + 1), TraceID: uint64(k + 1), ParentIdx: parent,
				IsRoot: parent < 0, Service: svc, Host: "h1", Start: start, Duration: durMs[depth] / 1000.0, StatusOK: true,
				Depth: depth, Route: "r", ReqClass: "r|any", NBytes: 300, Own: durMs[depth] / 1000.0})
			if parent < 0 {
				c.Roots = append(c.Roots, idx)
			}
			parent = idx
			start += 0.0002
		}
	}
	// own time of a parent = duration minus the child's duration (finalize() in the Python model)
	for i := range c.Spans {
		if c.Spans[i].ParentIdx >= 0 {
			p := &c.Spans[c.Spans[i].ParentIdx]
			p.Own = math.Max(0, p.Duration-c.Spans[i].Duration)
		}
	}
	return c
}

func approx(a, b, rel float64) bool { return math.Abs(a-b) <= rel*math.Abs(b) }

func TestSqrtSplitAndPriorOnTheChain(t *testing.T) {
	c := chainCorpus([3]float64{6, 2, 1}, 10)
	orc := OracleTable(c, 0.95, true)
	// inclusive L = (6, 2, 1) ms -> own (4, 1, 1) -> stage budgets (6, 3, 3) -> T = (12, 6, 3) ms
	for svc, want := range map[string]float64{"a": 0.012, "b": 0.006, "c": 0.003} {
		got, ok := orc.Lookup("r", "r|any", svc)
		if !ok || !approx(got, want, 1e-9) {
			t.Fatalf("oracle T[%s] = %v (%v), want %v", svc, got, ok, want)
		}
	}
	prior := EqualSharePrior(0.012, c.Graph, "r")
	for svc, want := range map[string]float64{"a": 0.012, "b": 0.008, "c": 0.004} {
		if !approx(prior[svc], want, 1e-9) {
			t.Fatalf("prior T[%s] = %v, want %v", svc, prior[svc], want)
		}
	}
	cal, _ := NewCalibrator(c.Graph, c.RoutesSLO, 0.95, 20, 1, true, "equal", 1)
	if v, ok := cal.Lookup("r", "r|any", "c"); !ok || !approx(v, 0.004, 1e-9) || cal.PriorHits != 1 {
		t.Fatalf("prior lookup: %v %v hits %d", v, ok, cal.PriorHits)
	}
	for i := range c.Spans {
		cal.Observe(&c.Spans[i], 1.0)
	}
	cal.EndPeriod(nil)
	if v, _ := cal.Lookup("r", "r|any", "c"); !approx(v, 0.003, 1e-9) {
		t.Fatalf("calibrated T[c] = %v", v)
	}
	cal.EndPeriod(nil) // empty window: c leaves the table and the prior does not come back
	if _, ok := cal.Lookup("r", "r|any", "c"); ok {
		t.Fatal("prior came back after the first calibration")
	}
}

func TestInclusiveOwnTimeFallbackMatchesOnTheChain(t *testing.T) {
	// on a sequential chain own = L minus the child's L exactly, so both sources give the same split
	c := chainCorpus([3]float64{6, 2, 1}, 20)
	cal, _ := NewCalibrator(c.Graph, c.RoutesSLO, 0.95, 20, 1, true, "none", 1)
	cal.UseOwn = false
	for i := range c.Spans {
		cal.Observe(&c.Spans[i], 1.0)
	}
	cal.EndPeriod(nil)
	for svc, want := range map[string]float64{"a": 0.012, "b": 0.006, "c": 0.003} {
		if got, ok := cal.Lookup("r", "r|any", svc); !ok || !approx(got, want, 1e-9) {
			t.Fatalf("T[%s] = %v (%v), want %v", svc, got, ok, want)
		}
	}
}

func TestRelativeNNeedAndBudgetIssue(t *testing.T) {
	c := chainCorpus([3]float64{6, 2, 1}, 20)
	cfg := ChosenV2()
	s, err := NewSampler(cfg, c.Graph, c.RoutesSLO, c.Hosts, len(c.Services), 300.0, 1001, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.NNeed != 20 { // floor(0.2 * 300 / 3)
		t.Fatalf("N_need %v", s.NNeed)
	}
	s2, _ := NewSampler(cfg, c.Graph, c.RoutesSLO, c.Hosts, len(c.Services), 10.0, 1001, nil)
	if s2.NNeed != 3 {
		t.Fatalf("N_min: %v", s2.NNeed)
	}
	cfg.OutlierCredit = "credit"
	if _, err := NewSampler(cfg, c.Graph, c.RoutesSLO, c.Hosts, 3, 300, 1, nil); err == nil {
		t.Fatal("credit should be rejected")
	}
}

func TestReplayKeepsOutliersWithTheOracleTable(t *testing.T) {
	c := chainCorpus([3]float64{6, 2, 1}, 400)
	// every other request has a 4.5 ms c span (an outlier against T_c = 3 ms)
	for k, r := range c.Roots {
		if k%2 == 1 {
			c.Spans[r+2].Duration, c.Spans[r+2].Own = 0.0045, 0.0045
			c.Spans[r+1].Duration = 0.0055
			c.Spans[r].Duration = 0.0095
		}
	}
	orc := OracleTable(chainCorpus([3]float64{6, 2, 1}, 400), 0.95, true)
	cfg := DefaultConfig()
	cfg.CalibSource = "oracle"
	cfg.RootPolicy = "pin"
	cfg.InitialBudget, cfg.BMin, cfg.BMax = 10, 10, 10
	cfg.NNeed = 1000
	cfg.InitialPlanP, cfg.PlanFill, cfg.OrdinaryFloor = 0, 0, 0
	res, _, err := Replay(c, cfg, 1e9, 1e9, 1, orc)
	if err != nil {
		t.Fatal(err)
	}
	kept, outl := 0, 0
	for i, d := range res.Decisions {
		if d.Keep && !c.Spans[i].IsRoot {
			kept++
			if d.Reason == "outlier" && c.Spans[i].Service == "c" {
				outl++
			}
		}
	}
	if kept != 200 || outl != 200 {
		t.Fatalf("kept non-root %d, outlier keeps of c %d (want 200, 200)", kept, outl)
	}
	if len(res.Periods) != 4 {
		t.Fatalf("periods %d", len(res.Periods))
	}
}

// TestDifferentialAgainstPythonDumps replays the dumps written by
// `python -m slo_budget_sim.cli dump` and demands bit-identical decisions.
func TestDifferentialAgainstPythonDumps(t *testing.T) {
	dir := os.Getenv("SLOBUDGET_DUMPS")
	if dir == "" {
		t.Skip("set SLOBUDGET_DUMPS to a directory of *.events.json / *.decisions.json dumps")
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.events.json"))
	if len(files) == 0 {
		t.Fatalf("no dumps in %s", dir)
	}
	for _, ev := range files {
		name := strings.TrimSuffix(filepath.Base(ev), ".events.json")
		t.Run(name, func(t *testing.T) {
			d, c, err := LoadEvents(ev)
			if err != nil {
				t.Fatal(err)
			}
			py, err := LoadDecisions(strings.TrimSuffix(ev, ".events.json") + ".decisions.json")
			if err != nil {
				t.Fatal(err)
			}
			oracle := OracleFromDump(d, c.Graph)
			res, s, err := Replay(c, d.Config, d.VStar, d.Cap, d.RNGSeed, oracle)
			if err != nil {
				t.Fatal(err)
			}
			pyN := py.NNeed
			if pyN >= 1e300 {
				pyN = math.Inf(1)
			}
			if s.NNeed != pyN {
				t.Fatalf("N_need: go %v python %v", s.NNeed, pyN)
			}
			if err := Compare(c, res, py); err != nil {
				t.Fatal(err)
			}
			kept := 0
			for _, dcs := range res.Decisions {
				if dcs.Keep {
					kept++
				}
			}
			t.Logf("%s: %d spans, %d kept, %d periods: identical", name, len(c.Spans), kept, len(res.Periods))
		})
	}
}
