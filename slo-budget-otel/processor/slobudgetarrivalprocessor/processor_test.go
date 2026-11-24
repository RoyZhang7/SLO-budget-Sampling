package slobudgetarrivalprocessor

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrival"
	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/processor/internal/arrivalstamp"
)

const lag = time.Hour // the monotonic clock behind the wall clock

func started(t *testing.T, cfg *Config, log *zap.Logger) (*stamp, *consumertest.TracesSink, func(id byte, at time.Time)) {
	t.Helper()
	sink := new(consumertest.TracesSink)
	set := processortest.NewNopSettings(typeStr)
	if log != nil {
		set.Logger = log
	}
	cfg.ArrivalTable = t.TempDir() // nothing pinned there
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p := newStamp(set, cfg, sink)
	if err := p.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	mem := make([]byte, 256*arrival.SlotSize+8)
	for i := 0; i < 8; i++ {
		if tab, err := arrival.NewTable(mem[i:i+256*arrival.SlotSize], nil); err == nil {
			mem = mem[i : i+256*arrival.SlotSize]
			p.st.Use(tab, arrival.NewClock(func() (int64, int64) { n := time.Now().UnixNano(); return n, n - int64(lag) }))
			break
		}
	}
	return p, sink, func(id byte, at time.Time) {
		s := mem[int(id)*arrival.SlotSize:]
		binary.LittleEndian.PutUint64(s[arrival.OffID:], uint64(id))
		binary.LittleEndian.PutUint64(s[arrival.OffTime:], uint64(at.Add(-lag).UnixNano()))
	}
}

func span(td ptrace.Traces, id, parent byte, kind ptrace.SpanKind, start time.Time) ptrace.Span {
	s := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	s.SetSpanID(pcommon.SpanID{7: id})
	if parent != 0 {
		s.SetParentSpanID(pcommon.SpanID{7: parent})
	}
	s.SetKind(kind)
	s.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
	s.SetEndTimestamp(pcommon.NewTimestampFromTime(start.Add(time.Millisecond)))
	s.Attributes().PutStr("own", "stays")
	return s
}

func stampOf(s ptrace.Span) (int64, string) {
	at, ok := s.Attributes().Get(arrivalstamp.Attr)
	src, ok2 := s.Attributes().Get(arrivalstamp.AttrSource)
	if !ok || !ok2 {
		return 0, ""
	}
	return at.Int(), src.Str()
}

// Every server span gets its arrival, the program's or its own start; nothing else is touched and nothing is dropped.
func TestStamp(t *testing.T) {
	p, sink, put := started(t, &Config{Period: time.Hour}, nil)
	base := time.Now().Add(-time.Second)
	put(1, base.Add(-2*time.Millisecond))
	td := ptrace.NewTraces()
	span(td, 2, 1, ptrace.SpanKindServer, base)   // found
	span(td, 3, 9, ptrace.SpanKindServer, base)   // not in the table
	span(td, 4, 0, ptrace.SpanKindServer, base)   // a root
	span(td, 5, 2, ptrace.SpanKindClient, base)   // not a server span
	span(td, 6, 2, ptrace.SpanKindInternal, base) // nor this
	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	if sink.SpanCount() != 5 {
		t.Fatalf("%d spans passed on", sink.SpanCount())
	}
	out := sink.AllTraces()[0]
	for i := 0; i < out.ResourceSpans().Len(); i++ {
		s := out.ResourceSpans().At(i).ScopeSpans().At(0).Spans().At(0)
		at, src := stampOf(s)
		wantAt, wantSrc := int64(s.StartTimestamp()), "start"
		switch s.SpanID()[7] {
		case 2:
			wantAt, wantSrc = base.Add(-2*time.Millisecond).UnixNano(), "ebpf"
		case 5, 6:
			wantAt, wantSrc = 0, ""
		}
		if d := at - wantAt; src != wantSrc || d > 1e6 || d < -1e6 {
			t.Errorf("span %d: %s, %d ns off", s.SpanID()[7], src, d)
		}
		if v, ok := s.Attributes().Get("own"); !ok || v.Str() != "stays" {
			t.Errorf("span %d lost an attribute", s.SpanID()[7])
		}
	}
	n := p.st.Counts()
	if n[arrival.Hit] != 1 || n[arrival.MissEmpty] != 1 || n[arrival.MissRoot] != 1 {
		t.Errorf("outcomes %v (%v)", n, arrival.CauseNames)
	}
}

// The period line has the outcomes, and the table's absence is said at the start.
func TestPeriodLine(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	p, _, put := started(t, &Config{Period: 200 * time.Millisecond, StatsLog: true}, zap.New(core))
	if logs.FilterMessage("arrival table not open").Len() == 0 {
		t.Error("nothing said about the missing table")
	}
	put(1, time.Now().Add(-10*time.Millisecond))
	td := ptrace.NewTraces()
	span(td, 2, 1, ptrace.SpanKindServer, time.Now())
	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	hits := int64(0)
	for end := time.Now().Add(3 * time.Second); hits == 0 && time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		for _, e := range logs.FilterMessage("slobudget arrival").All() {
			hits += e.ContextMap()["hit"].(int64)
		}
	}
	if hits != 1 {
		t.Errorf("%d hits in the period lines", hits)
	}
}

// Batches come on several connections at once.
func TestConcurrent(t *testing.T) {
	p, sink, put := started(t, &Config{Period: 50 * time.Millisecond, StatsLog: false}, nil)
	put(1, time.Now().Add(-time.Second))
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				td := ptrace.NewTraces()
				span(td, 2, 1, ptrace.SpanKindServer, time.Now())
				_ = p.ConsumeTraces(context.Background(), td)
			}
		}()
	}
	wg.Wait()
	if sink.SpanCount() != 1600 {
		t.Fatalf("%d spans", sink.SpanCount())
	}
}

func TestConfig(t *testing.T) {
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	if cfg.Period != 5*time.Second || !cfg.StatsLog || cfg.Validate() == nil {
		t.Errorf("%+v", cfg)
	}
	cfg.ArrivalTable = "/x"
	if err := cfg.Validate(); err != nil {
		t.Error(err)
	}
	cfg.Period = 0
	if cfg.Validate() == nil {
		t.Error("no period accepted")
	}
}
