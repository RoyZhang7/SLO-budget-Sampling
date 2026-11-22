package capgateprocessor

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
)

func batch(n int, kind ptrace.SpanKind, first byte) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "unknown_service:composepost_service_pb_es_proc")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < n; i++ {
		s := ss.Spans().AppendEmpty()
		s.SetTraceID(pcommon.TraceID{first})
		s.SetSpanID(pcommon.SpanID{first, byte(i + 1)})
		s.SetKind(kind)
		s.SetName("x")
	}
	return td
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func TestCapBindsMidPeriodRollsOverAndStamps(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.VStar = 8 // cap = 10 span units
	cfg.StatsLog = false
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	sink := new(consumertest.TracesSink)
	g, err := newGate(processortest.NewNopSettings(typeStr), cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	period := cfg.Period
	base := time.Unix(1_800_000_000, 0).Truncate(period) // a period boundary
	ck := &clock{base.Add(time.Second)}
	g.now = ck.now
	g.emitDue(ck.t) // starts the accounting at this period
	ctx := context.Background()
	if err := g.ConsumeTraces(ctx, batch(7, ptrace.SpanKindServer, 1)); err != nil {
		t.Fatal(err)
	}
	ck.t = base.Add(2 * time.Second)
	if err := g.ConsumeTraces(ctx, batch(6, ptrace.SpanKindClient, 2)); err != nil {
		t.Fatal(err)
	}
	if got := sink.SpanCount(); got != 10 {
		t.Fatalf("admitted %d, want 10 (cap 10)", got)
	}
	// every admitted span carries the stamp of its admission
	for _, td := range sink.AllTraces() {
		rss := td.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			spans := rss.At(i).ScopeSpans().At(0).Spans()
			for k := 0; k < spans.Len(); k++ {
				v, ok := spans.At(k).Attributes().Get(AttrAdmitNs)
				if !ok || v.Int() < base.UnixNano() || v.Int() >= base.Add(period).UnixNano() {
					t.Fatalf("missing or wrong admit stamp: %v %v", ok, v)
				}
			}
		}
	}
	// the next period starts with a fresh cap
	ck.t = base.Add(period + 100*time.Millisecond)
	if err := g.ConsumeTraces(ctx, batch(4, ptrace.SpanKindServer, 3)); err != nil {
		t.Fatal(err)
	}
	if got := sink.SpanCount(); got != 14 {
		t.Fatalf("after roll-over %d, want 14", got)
	}
	// the first period's record is emitted only after its end plus the grace
	g.emitDue(base.Add(period + 200*time.Millisecond))
	if len(g.emitted) != 0 {
		t.Fatalf("emitted before the grace: %+v", g.emitted)
	}
	g.emitDue(base.Add(period + cfg.Grace))
	if len(g.emitted) != 1 {
		t.Fatalf("records %d, want 1", len(g.emitted))
	}
	r := g.emitted[0]
	if r.Received != 13 || r.Admitted != 10 || r.CapDropped != 3 || r.Server != 7 || r.Client != 3 || r.BytesAdmitted <= 0 {
		t.Fatalf("first period record %+v", r)
	}
	// a batch timestamped in the emitted period is late: counted in the oldest open period
	ck.t = base.Add(4 * time.Second)
	if err := g.ConsumeTraces(ctx, batch(2, ptrace.SpanKindServer, 4)); err != nil {
		t.Fatal(err)
	}
	// an empty period still gets a record
	g.emitDue(base.Add(3*period + cfg.Grace))
	if len(g.emitted) != 3 {
		t.Fatalf("records %d, want 3", len(g.emitted))
	}
	r2 := g.emitted[1]
	if r2.Received != 6 || r2.Admitted != 6 || r2.Late != 2 {
		t.Fatalf("second period record %+v", r2)
	}
	if e := g.emitted[2]; e.Received != 0 || e.Admitted != 0 {
		t.Fatalf("empty period record %+v", e)
	}
}

func TestNoCapStillStampsAndCounts(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.StatsLog = false
	sink := new(consumertest.TracesSink)
	g, _ := newGate(processortest.NewNopSettings(typeStr), cfg, sink)
	if err := g.ConsumeTraces(context.Background(), batch(50, ptrace.SpanKindInternal, 5)); err != nil {
		t.Fatal(err)
	}
	if sink.SpanCount() != 50 {
		t.Fatalf("admitted %d", sink.SpanCount())
	}
	for _, r := range g.open {
		if r.Other != 50 || r.CapDropped != 0 {
			t.Fatalf("record %+v", r)
		}
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Grace = cfg.Period
	if err := cfg.Validate(); err == nil {
		t.Fatal("grace >= period accepted")
	}
	cfg = createDefaultConfig().(*Config)
	cfg.VStar, cfg.CapFactor = 100, 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("cap_factor 0 accepted")
	}
}
