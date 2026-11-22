package reservoirprocessor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

// spans of socialgraph with the given durations (ms); error marks the status
func batch(first byte, durMs []float64, errIdx int, start time.Time) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "unknown_service:socialgraph_service_pb_es_proc")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("socialgraph")
	for i, d := range durMs {
		s := ss.Spans().AppendEmpty()
		s.SetTraceID(pcommon.TraceID{first, byte(i)})
		s.SetSpanID(pcommon.SpanID{first, byte(i), 7})
		s.SetKind(ptrace.SpanKindServer)
		s.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
		s.SetEndTimestamp(pcommon.NewTimestampFromTime(start.Add(time.Duration(d * float64(time.Millisecond)))))
		s.Attributes().PutStr("slo.root", "compose_post")
		if i == errIdx {
			s.Status().SetCode(ptrace.StatusCodeError)
		}
	}
	return td
}

func TestTopKByMissAndEmissionAtPeriodEnd(t *testing.T) {
	dir := t.TempDir()
	tt := filepath.Join(dir, "t.json")
	if err := os.WriteFile(tt, []byte(`{"compose_post": {"socialgraph": 0.003}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := createDefaultConfig().(*Config)
	cfg.VStar, cfg.TTableFile, cfg.Host, cfg.StatsLog = 10, tt, "node-a", false
	cfg.Shares = map[string]float64{"node-a": 0.5, "node-b": 0.5}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	sink := new(consumertest.TracesSink)
	r, err := newReservoir(processortest.NewNopSettings(typeStr), cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if r.K != 5 {
		t.Fatalf("K %d, want 5", r.K)
	}
	base := time.Unix(1_800_000_000, 0).Truncate(cfg.Period)
	ck := &clock{base.Add(time.Second)}
	r.now = ck.now
	r.emitDue(context.Background(), ck.t)
	ordinary := make([]float64, 20)
	for i := range ordinary {
		ordinary[i] = 1.0 // under T = 3 ms
	}
	ctx := context.Background()
	if err := r.ConsumeTraces(ctx, batch(1, ordinary[:10], 3, base)); err != nil { // one error span, no miss
		t.Fatal(err)
	}
	if err := r.ConsumeTraces(ctx, batch(2, []float64{9, 1, 12, 1, 30}, -1, base)); err != nil { // three misses
		t.Fatal(err)
	}
	if err := r.ConsumeTraces(ctx, batch(3, ordinary[10:], -1, base)); err != nil {
		t.Fatal(err)
	}
	if sink.SpanCount() != 0 {
		t.Fatal("spans released before the period end")
	}
	r.emitDue(ctx, base.Add(cfg.Period+cfg.Grace/2))
	if sink.SpanCount() != 0 {
		t.Fatal("spans released before the grace")
	}
	r.emitDue(ctx, base.Add(cfg.Period+cfg.Grace))
	if sink.SpanCount() != 5 {
		t.Fatalf("emitted %d, want K = 5", sink.SpanCount())
	}
	misses, errs := 0, 0
	for _, td := range sink.AllTraces() {
		rss := td.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			spans := rss.At(i).ScopeSpans().At(0).Spans()
			for k := 0; k < spans.Len(); k++ {
				sp := spans.At(k)
				w, _ := sp.Attributes().Get(attrWeight)
				reason, _ := sp.Attributes().Get(attrReason)
				if reason.Str() != "reservoir" {
					t.Fatalf("reason %q", reason.Str())
				}
				if w.Double() > 1 {
					misses++
				}
				if sp.Status().Code() == ptrace.StatusCodeError {
					errs++
				}
			}
		}
	}
	if misses != 3 {
		t.Fatalf("kept misses %d, want all 3", misses)
	}
	if errs != 1 {
		t.Fatalf("kept error spans %d, want 1 (weight 0.01 beats eps)", errs)
	}
	rec := r.history[len(r.history)-1]
	if rec.Seen != 25 || rec.Emitted != 5 || rec.Peak != 5 || rec.K != 5 {
		t.Fatalf("record %+v", rec)
	}
	// the next period starts empty and emits nothing without traffic
	r.emitDue(ctx, base.Add(2*cfg.Period+cfg.Grace))
	if sink.SpanCount() != 5 || len(r.history) != 2 || r.history[1].Emitted != 0 {
		t.Fatalf("second period: spans %d history %+v", sink.SpanCount(), r.history)
	}
}

func TestMissingHostShareFallsBackToAnEqualShare(t *testing.T) {
	dir := t.TempDir()
	tt := filepath.Join(dir, "t.json")
	_ = os.WriteFile(tt, []byte(`{}`), 0o644)
	cfg := createDefaultConfig().(*Config)
	cfg.VStar, cfg.TTableFile, cfg.Host, cfg.StatsLog = 9, tt, "node-z", false
	cfg.Shares = map[string]float64{"node-a": 0.5, "node-b": 0.25, "node-c": 0.25}
	r, err := newReservoir(processortest.NewNopSettings(typeStr), cfg, new(consumertest.TracesSink))
	if err != nil {
		t.Fatal(err)
	}
	if r.K != 3 {
		t.Fatalf("K %d, want round(9/3) = 3", r.K)
	}
}
