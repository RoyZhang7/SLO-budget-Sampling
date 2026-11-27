package slobudget

import (
	"math"
	"testing"
)

// The agent's period_detail_log (2026-11-26): PeriodDetail reports, per service seen in the period, the thresholds b in force
// for its table keys, what the N_need rule counted and what its coin dropped, and the freeze; PoolFull / PoolLeft the host pool;
// all of it read before ResetPeriod, which clears the counters and refills the pools.
func TestPeriodDetailReportsThresholdsCoinDropsAndPools(t *testing.T) {
	c := chainCorpus([3]float64{6, 2, 1}, 20)
	cfg := ChosenV2()
	s, err := NewSampler(cfg, c.Graph, c.RoutesSLO, c.Hosts, len(c.Services), 300.0, 1001, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.spansSvc = newOrderedMap()
	s.spansSvc.set("b", 40)
	s.spansSvc.set("c", 30)
	s.nseen = map[string]int{"c": 7}
	s.coinDropped = map[string]int{"c": 4}
	s.frozen = map[string]bool{"c": true}
	s.Cal.B = map[TableKey]float64{{"r", "r|any", "c"}: 0.004, {"r", "r|large", "c"}: 0.005, {"r", "r|any", "b"}: 0.002}
	s.poolFull = map[string]float64{c.Hosts[0]: 120}
	s.setPools()                 // the period starts with 120
	s.poolFull[c.Hosts[0]] = 200 // an update of the central sets the next period's pool (ApplyCentral)
	s.pool[c.Hosts[0]] = 15      // 105 spent

	d := s.PeriodDetail()
	if len(d) != 2 || d[0].Service != "b" || d[1].Service != "c" {
		t.Fatalf("services in the order seen: %+v", d)
	}
	if d[1].NSeen != 7 || d[1].CoinDropped != 4 || !d[1].Frozen || len(d[1].B) != 2 || d[1].B[TableKey{"r", "r|large", "c"}] != 0.005 {
		t.Fatalf("c's detail: %+v", d[1])
	}
	if d[0].CoinDropped != 0 || d[0].Frozen || len(d[0].B) != 1 {
		t.Fatalf("b's detail: %+v", d[0])
	}
	if s.PoolFull(c.Hosts[0]) != 120 || s.PoolLeft(c.Hosts[0]) != 15 { // the pool it started with, not the next period's
		t.Fatalf("pool %v of %v", s.PoolLeft(c.Hosts[0]), s.PoolFull(c.Hosts[0]))
	}
	if !math.IsInf(s.PoolFull("no-such-host"), 1) {
		t.Fatalf("an unknown host's pool is +Inf")
	}
	s.ResetPeriod()
	if len(s.coinDropped) != 0 || s.PoolLeft(c.Hosts[0]) != 200 || s.PoolFull(c.Hosts[0]) != 200 {
		t.Fatalf("after the reset: coinDropped %v, pool left %v", s.coinDropped, s.PoolLeft(c.Hosts[0]))
	}
}
