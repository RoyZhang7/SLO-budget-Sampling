package filter

import (
	"testing"
	"time"
)

func poll(v int, b float64, p float64) Poll {
	return Poll{Version: v, Period: int64(v), BTable: map[string]map[string]map[string]float64{"r": {"*": {"svc": b}, "r|m": {"svc": b * 0.9}}},
		PlanP: map[string]float64{"svc": p}}
}

func TestUnion(t *testing.T) {
	if Union(nil) != 0 || Union([]Interval{{10, 20}, {15, 30}, {40, 50}}) != 30 || Union([]Interval{{40, 50}, {10, 20}}) != 20 {
		t.Fatal("union")
	}
}

func TestCoinMatchesTheSampler(t *testing.T) {
	// the Python sampler's coin_u (slo-budget-sim/tests/test_coin.py) and slobudget.CoinU
	for id, u := range map[uint64]float64{1: 0.3381666012719897, 0xdeadbeefcafef00d: 0.09790485404703486, 1234567890123456789: 0.7820656816100434} {
		if CoinU(id) != u {
			t.Errorf("CoinU(%#x) = %.17g, want %.17g", id, CoinU(id), u)
		}
	}
}

func TestBeforeAnyPollEverythingIsExported(t *testing.T) {
	s := New("svc")
	now := time.Now()
	for i, d := range []Decision{s.ServerEnded(7, false, "r", "r|m", 100, 200, 90, false, now), s.ClientEnded(8, 7, 100, 150)} {
		if !d.Export {
			t.Errorf("%d: not exported before a poll: %+v", i, d)
		}
	}
}

func TestServerDecisions(t *testing.T) {
	s := New("svc")
	now := time.Now()
	s.Update(poll(1, 0.005, 0.0), now) // b 5 ms (4.5 ms for class m), no plan keeps
	// roots always; a client span, dropped at rate 0, is counted and goes into the next export's dropped_before
	if d := s.ClientEnded(21, 20, 1_000_000, 2_000_000); d.Export {
		t.Fatal("client at rate 0 exported")
	}
	// the server span: 10 ms from arrival, 1 ms downstream -> within 9 ms > 4.5 ms: an outlier, dropped_before 1
	d := s.ServerEnded(20, false, "r", "r|m", 500_000, 10_000_000, 0, false, now)
	if !d.Export || d.Reason != "no_arrival" || d.Dropped != 1 {
		t.Fatalf("no arrival must export, with the dropped client counted: %+v", d)
	}
	s.ClientEnded(31, 30, 1_000_000, 2_000_000)
	d = s.ServerEnded(30, false, "r", "r|m", 500_000, 10_000_000, 400_000, false, now)
	if !d.Export || d.Reason != "outlier" || d.WithinNs != 10_000_000-400_000-1_000_000 || d.Downstream != 1_000_000 || d.Dropped != 1 {
		t.Fatalf("outlier: %+v", d)
	}
	// under the threshold without the margin: 4.6 ms is over b 4.5 (an agent with the margin would not flag it; the filter must pass it)
	d = s.ServerEnded(40, false, "r", "r|m", 10, 4_600_000, 1, false, now)
	if !d.Export || d.Reason != "outlier" {
		t.Fatalf("over b without margin: %+v", d)
	}
	d = s.ServerEnded(41, false, "r", "r|m", 10, 4_400_000, 1, false, now)
	if d.Export || d.Reason != "dropped" {
		t.Fatalf("under b, rate 0: %+v", d)
	}
	d = s.ServerEnded(42, false, "r", "r|m", 10, 4_400_000, 1, true, now)
	if !d.Export || d.Reason != "error" || d.Dropped != 1 {
		t.Fatalf("error: %+v", d)
	}
	d = s.ServerEnded(43, true, "r", "r|m", 0, 100, 0, false, now)
	if !d.Export || d.Reason != "root" {
		t.Fatalf("root: %+v", d)
	}
	// an unknown class falls back to the route's "*"; an unknown route exports
	if d := s.ServerEnded(44, false, "r", "r|x", 10, 5_200_000, 1, false, now); !d.Export || d.Reason != "outlier" {
		t.Fatalf("class fallback: %+v", d)
	}
	if d := s.ServerEnded(45, false, "other", "other|m", 10, 100, 1, false, now); !d.Export || d.Reason != "no_threshold" {
		t.Fatalf("unknown route: %+v", d)
	}
	st := s.Stats()
	if st.Servers != 7 || st.Outliers != 3 || st.Errors != 1 || st.Dropped != 1 || st.Roots != 1 || s.Pending() != 0 {
		t.Fatalf("stats %+v pending %d", st, s.Pending())
	}
}

func TestPlanCoinIsTheIdsAndTheRateTheHigherOfTwo(t *testing.T) {
	s := New("svc")
	now := time.Now()
	s.Update(poll(3, 1.0, 0.1), now)
	s.Update(poll(4, 1.0, 0.3), now.Add(5*time.Second))
	s.Update(poll(4, 1.0, 0.9), now.Add(6*time.Second)) // the same version again: ignored
	kept := 0
	for id := uint64(1); id <= 20000; id++ {
		d := s.ServerEnded(id, false, "r", "r|m", 10, 1000, 1, false, now.Add(7*time.Second))
		if d.Export != (CoinU(id) < 0.3*RateHeadroom) {
			t.Fatalf("id %d: %+v against the coin %.3f", id, d, CoinU(id))
		}
		if d.Export {
			kept++
		}
	}
	if kept < 6400 || kept > 6800 { // 0.3 x 1.10 x 20000 = 6600 (6300 at the old headroom 1.05)
		t.Fatalf("%d of 20000 kept at the higher rate 0.3 x the headroom %.2f", kept, RateHeadroom)
	}
	s.Update(poll(5, 1.0, 0.05), now.Add(10*time.Second)) // the rate fell: the previous period's 0.3 still rules
	if d := s.ServerEnded(99991, false, "r", "r|m", 10, 1000, 1, false, now.Add(11*time.Second)); d.Export != (CoinU(99991) < 0.3*RateHeadroom) {
		t.Fatalf("falling rate: %+v", d)
	}
	if st := s.Stats(); st.Staleness != time.Second {
		t.Fatalf("staleness %v", st.Staleness)
	}
	// a service the central has no rate for: exported
	s.Update(Poll{Version: 6, PlanP: map[string]float64{"x": 0.0}, BTable: poll(6, 1.0, 0).BTable}, now)
	if d := s.ServerEnded(5, false, "r", "r|m", 10, 1000, 1, false, now); !d.Export {
		t.Fatal("no rate for the service: must export")
	}
}

// The period-end count record: nothing before a poll with period_s; the first call sets the period; a later period with
// dropped spans uncounted yields the record once and resets the count; an export in between takes the count instead.
func TestPeriodEndCountRecord(t *testing.T) {
	s := New("svc")
	t0 := time.Unix(1000, 0)
	if _, ok := s.PeriodEnded(t0); ok {
		t.Fatal("a record before any poll")
	}
	p := poll(1, 1.0, 0.0)
	p.PeriodS = 5
	s.Update(p, t0)
	if _, ok := s.PeriodEnded(t0); ok {
		t.Fatal("the first call only sets the period")
	}
	s.ClientEnded(21, 20, 1, 2) // dropped at rate 0
	s.ClientEnded(22, 20, 1, 2)
	if _, ok := s.PeriodEnded(t0.Add(4 * time.Second)); ok {
		t.Fatal("the period has not ended")
	}
	rec, ok := s.PeriodEnded(t0.Add(5 * time.Second))
	if !ok || rec.Period != 200 || rec.Dropped != 2 {
		t.Fatalf("record %+v %v", rec, ok)
	}
	if _, ok := s.PeriodEnded(t0.Add(6 * time.Second)); ok {
		t.Fatal("a second record for the same period")
	}
	if d := s.ServerEnded(30, true, "r", "r|m", 1, 2, 0, false, t0.Add(7*time.Second)); d.Dropped != 0 {
		t.Fatalf("the count was sent already, the root carries %d", d.Dropped)
	}
	s.ClientEnded(23, 20, 1, 2)
	if d := s.ServerEnded(31, true, "r", "r|m", 1, 2, 0, false, t0.Add(8*time.Second)); d.Dropped != 1 {
		t.Fatalf("the export carries the count: %d", d.Dropped)
	}
	if _, ok := s.PeriodEnded(t0.Add(10 * time.Second)); ok {
		t.Fatal("nothing uncounted at this period's end")
	}
	if st := s.Stats(); st.CountRecords != 1 {
		t.Fatalf("count records %d", st.CountRecords)
	}
}
