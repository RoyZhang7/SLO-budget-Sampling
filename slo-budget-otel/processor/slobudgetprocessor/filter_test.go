package slobudgetprocessor

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
)

// A server span with the in-service filter's stamps is decided at once, from its stamped downstream time, and its
// dropped_before count the service's spans; the SDK's poll serves the thresholds and the plan rates.
func TestFilterStamps(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := testConfig(t)
	cfg.Period = time.Hour
	cfg.ServerHold = time.Hour // a span without the stamps would wait here
	cfg.Sampler.OutlierTrigger = "own"
	cfg.Sampler.OwnMeasure = "within_service"
	cfg.Sampler.InitialPlanP = 0
	cfg.Sampler.InitialBudget = 100
	cfg.Sampler.PlanCoin = "hash"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	a, put := arrivalAgent(t, cfg, sink)
	u := slobudget.CentralUpdate{Version: 1, Period: 7, Table: map[string]map[string]map[string]float64{"compose_post": {"*": {"socialgraph": 0.003}}},
		BTable: map[string]map[string]map[string]float64{"compose_post": {"*": {"socialgraph": 0.003}}}, PlanP: map[string]float64{"socialgraph": 0.2}}
	a.mu.Lock()
	a.s.ApplyCentral(&u, a.host)
	a.last, a.version = u, u.Version
	a.mu.Unlock()
	base := time.Now().Add(-100 * time.Millisecond)
	put(4, base.Add(-1500*time.Microsecond)) // span 5's request arrived 1.5 ms before its handler
	td := buildTraces(1, base, []spanSpec{
		{"socialgraph", "GetFollowers", 5, 4, 0, 10 * time.Millisecond, ptrace.SpanKindServer}, // 10 ms, 8 ms downstream by its stamp: own 2 ms, with 1.5 ms wait over 1.1 x 3 ms
		{"socialgraph", "GetFollowers", 6, 9, 0, 10 * time.Millisecond, ptrace.SpanKindServer}, // the same without stamps: held
	})
	s5 := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	s5.Attributes().PutInt(AttrWithinUS, 3500)
	s5.Attributes().PutInt(AttrDownstreamUS, 8000)
	s5.Attributes().PutInt(AttrDroppedBefore, 40)
	if err := a.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	kept := map[byte]bool{}
	for _, x := range append(sink.AllTraces(), td) {         // the decided spans stay in the batch, the held ones leave it
		for i := 0; i < x.ResourceSpans().Len(); i++ {
			s := x.ResourceSpans().At(i).ScopeSpans().At(0).Spans().At(0)
			kept[s.SpanID()[7]] = true
			if v, ok := s.Attributes().Get(AttrOwnUS); !ok || v.Int() < 1990 || v.Int() > 2010 { // 10 ms less the stamped 8 ms downstream
				t.Errorf("own_us %v", v.AsRaw())
			}
		}
	}
	if !kept[5] || kept[6] {
		t.Fatalf("kept %v: the stamped span at once, the other one held", kept)
	}
	a.mu.Lock()
	seen, _ := a.s.SpansSvc("socialgraph")
	a.mu.Unlock()
	if seen != 41 { // itself and the 40 it says were dropped before it
		t.Errorf("spans seen by the service: %v", seen)
	}
	// the SDK's poll
	rec := httptest.NewRecorder()
	a.handleSDK(rec, httptest.NewRequest("GET", "/slobudget/sdk", nil))
	var got struct {
		Version int                                      `json:"version"`
		Period  int64                                    `json:"period"`
		BTable  map[string]map[string]map[string]float64 `json:"btable"`
		PlanP   map[string]float64                       `json:"plan_p"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 || got.Period != 7 || got.BTable["compose_post"]["*"]["socialgraph"] != 0.003 || got.PlanP["socialgraph"] != 0.2 {
		t.Errorf("poll: %s", rec.Body.String())
	}
}

// The filter's period-end count record is counted into the service's spans and never kept.
func TestCountRecordIsCountedAndDropped(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := testConfig(t)
	cfg.Period = time.Hour
	cfg.Sampler.InitialPlanP = 1
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	a, _ := arrivalAgent(t, cfg, sink)
	base := time.Now().Add(-100 * time.Millisecond)
	td := buildTraces(2, base, []spanSpec{{"socialgraph", "slobudget.count", 7, 0, 0, time.Microsecond, ptrace.SpanKindInternal}})
	s7 := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	s7.Attributes().PutBool(AttrCountRecord, true)
	s7.Attributes().PutInt(AttrCountPeriod, 1)
	s7.Attributes().PutInt(AttrDroppedBefore, 25)
	if err := a.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if n := sink.SpanCount(); n != 0 {
		t.Fatalf("the record was exported: %d spans", n)
	}
	a.mu.Lock()
	seen, _ := a.s.SpansSvc("socialgraph")
	rec, late := a.stats.countRecords, a.stats.lateCountRecords
	a.mu.Unlock()
	if seen != 25 || rec != 1 || late != 1 {
		t.Errorf("seen %v, records %d, late %d", seen, rec, late)
	}
}
