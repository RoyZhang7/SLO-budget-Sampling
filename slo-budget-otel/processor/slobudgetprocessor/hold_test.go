package slobudgetprocessor

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
)

// Stage 2d: a server span is held for server_hold, so its client child that arrives in the
// next batch (the SDK's low-priority chunk) is subtracted from its own time; a client span
// that arrives after the release is counted as late.
func TestServerHoldWaitsForTheClientChildren(t *testing.T) {
	cfg := testConfig(t)
	cfg.ServerHold = 100 * time.Millisecond
	cfg.Period = time.Hour // no period boundary (and counter reset) during the test
	cfg.ClientSpans = "plan"
	cfg.Sampler.InitialPlanP = 0 // only outliers and roots are kept
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	sink := new(consumertest.TracesSink)
	a, err := newAgent(processortest.NewNopSettings(typeStr), cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Shutdown(context.Background()) }()
	base := time.Now().Add(-100 * time.Millisecond)
	// the server span first (high-priority chunk), its client child 20 ms later
	srv := buildTraces(1, base, []spanSpec{{"composepost", "ComposePost", 2, 1, time.Millisecond, 30 * time.Millisecond, ptrace.SpanKindServer}})
	if err := a.ConsumeTraces(context.Background(), srv); err != nil {
		t.Fatal(err)
	}
	if srv.SpanCount() != 0 {
		t.Fatalf("the server span should be held, batch has %d spans", srv.SpanCount())
	}
	time.Sleep(20 * time.Millisecond)
	cli := buildTraces(1, base, []spanSpec{{"composepost", "hometimeline.call", 4, 2, 2 * time.Millisecond, 12 * time.Millisecond, ptrace.SpanKindClient}})
	if err := a.ConsumeTraces(context.Background(), cli); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for sink.SpanCount() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	var own int64 = -1
	for _, td := range sink.AllTraces() {
		for i := 0; i < td.ResourceSpans().Len(); i++ {
			sp := td.ResourceSpans().At(i).ScopeSpans().At(0).Spans().At(0)
			if sp.Name() == "ComposePost" {
				if v, ok := sp.Attributes().Get(AttrOwnUS); ok {
					own = v.Int()
				}
			}
		}
	}
	if own < 17000 || own > 19000 { // 30 ms minus the 12 ms client child that arrived during the hold
		t.Fatalf("own time %d us, want about 18000", own)
	}
	// a client child that arrives after the release is late
	late := buildTraces(1, base, []spanSpec{{"composepost", "media.call", 5, 2, 15 * time.Millisecond, 5 * time.Millisecond, ptrace.SpanKindClient}})
	if err := a.ConsumeTraces(context.Background(), late); err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	n, held := a.stats.srvLateClients, a.stats.srvHeld
	a.mu.Unlock()
	if n != 1 || held != 1 {
		t.Fatalf("late clients %d (want 1), held %d (want 1)", n, held)
	}
}
