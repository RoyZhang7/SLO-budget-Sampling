package canopyprocessor

import (
	"context"
	"math"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
)

func batch(svc string, n int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "unknown_service:"+svc+"_service_pb_es_proc")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < n; i++ {
		s := ss.Spans().AppendEmpty()
		s.SetTraceID(pcommon.TraceID{byte(i), byte(i >> 8)})
		s.SetSpanID(pcommon.SpanID{byte(i), byte(i >> 8), 1})
	}
	return td
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

// Two services, 2000 spans each per period, V* = 400: each service's target is 200, so
// the rates adapt from 0.15 toward 0.1 and the kept volume toward V*.
func TestRatesAdaptTowardTheEqualShareTarget(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.VStar = 400
	cfg.StatsLog = false
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	sink := new(consumertest.TracesSink)
	c, _ := newCanopy(processortest.NewNopSettings(typeStr), cfg, sink)
	base := time.Unix(1_800_000_000, 0).Truncate(cfg.Period)
	ck := &clock{base.Add(time.Second)}
	c.now = ck.now
	ctx := context.Background()
	keptLast := 0
	for per := 0; per < 12; per++ {
		ck.t = base.Add(time.Duration(per)*cfg.Period + time.Second)
		before := sink.SpanCount()
		for k := 0; k < 4; k++ {
			if err := c.ConsumeTraces(ctx, batch("composepost", 500)); err != nil {
				t.Fatal(err)
			}
			if err := c.ConsumeTraces(ctx, batch("text", 500)); err != nil {
				t.Fatal(err)
			}
		}
		keptLast = sink.SpanCount() - before
	}
	ck.t = base.Add(12*cfg.Period + time.Second)
	c.mu.Lock()
	c.rollTo(c.periodOf(ck.t))
	p1, p2 := c.p["composepost"], c.p["text"]
	c.mu.Unlock()
	if math.Abs(p1-0.1) > 0.02 || math.Abs(p2-0.1) > 0.02 {
		t.Fatalf("rates %.4f %.4f, want about 0.1", p1, p2)
	}
	if keptLast < 300 || keptLast > 500 {
		t.Fatalf("kept in the last period %d, want about 400", keptLast)
	}
	// kept spans carry their p and reason
	td := sink.AllTraces()[len(sink.AllTraces())-1]
	sp := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	if v, ok := sp.Attributes().Get(attrSamplingP); !ok || v.Double() <= 0 || v.Double() > 1 {
		t.Fatalf("sampling.p %v %v", ok, v)
	}
	if v, _ := sp.Attributes().Get(attrReason); v.Str() != "rate" {
		t.Fatalf("reason %q", v.Str())
	}
	if len(c.history) < 12 {
		t.Fatalf("history %d", len(c.history))
	}
}

// A first update by hand: p0 0.15, kept 300 of a target 200 -> p = 0.5*0.15 + 0.5*0.15*200/300 = 0.125.
func TestUpdateRuleMatchesThePythonFormula(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.VStar = 200
	cfg.StatsLog = false
	c, _ := newCanopy(processortest.NewNopSettings(typeStr), cfg, new(consumertest.TracesSink))
	c.started = true
	c.seen["a"], c.kept["a"] = 2000, 300
	c.endPeriod()
	if math.Abs(c.p["a"]-0.125) > 1e-12 {
		t.Fatalf("p %.6f", c.p["a"])
	}
	c.seen["a"], c.kept["a"] = 10, 0 // nothing kept: kept counts as 1, the rate grows, capped at 1
	c.endPeriod()
	if c.p["a"] != 1.0 {
		t.Fatalf("p after a period without keeps %.6f", c.p["a"])
	}
}
