package slobudget

import "testing"

// tests/test_own_measure.py on the same chain: b of c is 3 ms, the margin 0.1.
func TestOwnMeasure(t *testing.T) {
	c := chainCorpus([3]float64{6, 2, 1}, 50)
	orc := OracleTable(c, 0.95, true)
	sampler := func(measure string) *Sampler {
		cfg := DefaultConfig()
		cfg.CalibSource, cfg.OwnMeasure = "oracle", measure
		cfg.InitialBudget, cfg.PoolFactor, cfg.InitialPlanP = 1000, 1e9, 0
		o := *orc
		o.Within = measure == "within_service"
		s, err := NewSampler(cfg, c.Graph, c.RoutesSLO, c.Hosts, len(c.Services), 1e9, 1, &o)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	off, on := sampler("handler"), sampler("within_service")
	for _, x := range []struct {
		own, wait float64 // ms
		wOff, wOn bool
	}{{1, 0, false, false}, {1, 2.2, false, false}, {1, 2.4, false, true}, {3.4, 0, true, true}, {3.2, 0.2, false, true}} {
		sp := Span{Service: "c", Route: "r", ReqClass: "r|any", StatusOK: true, Duration: x.own / 1000, Own: x.own / 1000, Wait: x.wait / 1000}
		if out, _ := off.outlierTest(&sp); out != x.wOff {
			t.Errorf("handler, own %v wait %v: %v", x.own, x.wait, out)
		}
		if out, _ := on.outlierTest(&sp); out != x.wOn {
			t.Errorf("within_service, own %v wait %v: %v", x.own, x.wait, out)
		}
	}
	sp := Span{Service: "c", Route: "r", ReqClass: "r|any", StatusOK: true, Own: 0.004, Duration: 0.004, Wait: 0.002}
	if out, miss := on.outlierTest(&sp); !out || !approx(miss, 1, 1e-9) {
		t.Errorf("miss %v", miss)
	}
	cfg := DefaultConfig()
	cfg.CalibSource, cfg.OwnMeasure = "oracle", "within_service"
	if _, err := NewSampler(cfg, c.Graph, c.RoutesSLO, c.Hosts, 3, 1e9, 1, orc); err == nil {
		t.Error("an oracle table on the other measure")
	}
	cfg = DefaultConfig()
	cfg.OwnMeasure, cfg.OutlierTrigger = "within_service", "duration"
	if cfg.Validate() == nil {
		t.Error("within_service with the duration trigger")
	}
	cfg = DefaultConfig()
	cfg.OwnMeasure = "arrival"
	if cfg.Validate() == nil {
		t.Error("an unknown measure")
	}
}

func TestArrivalWait(t *testing.T) {
	for _, x := range []struct {
		start, arrival int64
		want           float64
	}{{2_000_000_000, 1_500_000_000, 0.5}, {2, 2, 0}, {2, 3, 0}, {2, 0, 0}, {2, -1, 0}} {
		if got := ArrivalWait(x.start, x.arrival); got != x.want {
			t.Errorf("%+v: %v", x, got)
		}
	}
}
