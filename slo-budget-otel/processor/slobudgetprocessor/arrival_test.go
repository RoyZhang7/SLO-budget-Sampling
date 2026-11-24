package slobudgetprocessor

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrival"
	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
)

// arrivalAgent is an agent with a table of 256 slots in memory (spanSpec ids are one byte: a
// slot each) and a clock 1 h behind the wall clock.
func arrivalAgent(t *testing.T, cfg *Config, sink *consumertest.TracesSink) (*agent, func(id byte, at time.Time)) {
	t.Helper()
	cfg.ArrivalTable = t.TempDir() // nothing pinned there: the agent starts without a table
	a, err := newAgent(processortest.NewNopSettings(typeStr), cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Shutdown(context.Background()) })
	if a.arr.Opened() {
		t.Fatal("a table from an empty directory")
	}
	const lag = time.Hour
	mem := make([]byte, 256*arrival.SlotSize+8)
	for i := 0; i < 8; i++ {
		if tab, err := arrival.NewTable(mem[i:i+256*arrival.SlotSize], nil); err == nil {
			mem = mem[i : i+256*arrival.SlotSize]
			a.arr.Use(tab, arrival.NewClock(func() (int64, int64) {
				n := time.Now().UnixNano()
				return n, n - int64(lag)
			}))
			break
		}
	}
	return a, func(id byte, at time.Time) {
		s := mem[int(id)*arrival.SlotSize:]
		binary.LittleEndian.PutUint64(s[arrival.OffID:], uint64(id))
		binary.LittleEndian.PutUint64(s[arrival.OffTime:], uint64(at.Add(-lag).UnixNano()))
	}
}

func arrivalOf(t *testing.T, span ptrace.Span) (int64, string) {
	t.Helper()
	at, ok := span.Attributes().Get(AttrArrival)
	if !ok {
		return 0, ""
	}
	src, ok := span.Attributes().Get(AttrArrivalSource)
	if !ok {
		t.Fatal("an arrival without its source")
	}
	return at.Int(), src.Str()
}

// A server span gets its request's arrival by its parent span id, in its own clock; a miss gets
// the span's start; client spans get nothing; every outcome is counted.
func TestArrivalIsAttached(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := testConfig(t)
	cfg.Period = time.Hour
	cfg.ClientSpans = "decide"
	cfg.Sampler.InitialBudget = 100
	a, put := arrivalAgent(t, cfg, sink)
	base := time.Now().Add(-100 * time.Millisecond)
	arrived := base.Add(1500 * time.Microsecond)
	put(1, arrived)                       // the request of span 2 (its parent is span 1)
	put(4, base.Add(40*time.Millisecond)) // after span 5's start
	// every span is an outlier (T 3 to 17.6 ms) and kept
	td := buildTraces(1, base, []spanSpec{
		{"composepost", "ComposePost", 2, 1, 2 * time.Millisecond, 30 * time.Millisecond, ptrace.SpanKindServer},
		{"socialgraph", "socialgraph.call", 4, 2, 3 * time.Millisecond, 12 * time.Millisecond, ptrace.SpanKindClient},
		{"socialgraph", "GetFollowers", 5, 4, 4 * time.Millisecond, 10 * time.Millisecond, ptrace.SpanKindServer},
		{"hometimeline", "WriteHomeTimeline", 7, 9, 4 * time.Millisecond, 15 * time.Millisecond, ptrace.SpanKindServer},
	})
	if err := a.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	spans := map[byte]ptrace.Span{}
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		s := td.ResourceSpans().At(i).ScopeSpans().At(0).Spans().At(0)
		spans[s.SpanID()[7]] = s
	}
	if len(spans) != 4 {
		t.Fatalf("%d spans kept", len(spans))
	}
	for id, want := range map[byte]struct {
		at  time.Time
		src string
	}{2: {arrived, "ebpf"}, 5: {base.Add(4 * time.Millisecond), "start"}, 7: {base.Add(4 * time.Millisecond), "start"}} {
		at, src := arrivalOf(t, spans[id])
		if d := time.Duration(at - want.at.UnixNano()); src != want.src || d > time.Millisecond || d < -time.Millisecond {
			t.Errorf("span %d: %s, %v off", id, src, d)
		}
	}
	if at, src := arrivalOf(t, spans[4]); src != "" {
		t.Errorf("a client span got an arrival (%d, %s)", at, src)
	}
	n := a.arr.Counts()
	if n[arrival.Hit] != 1 || n[arrival.MissLate] != 1 || n[arrival.MissEmpty] != 1 {
		t.Errorf("outcomes %v (%v)", n, arrival.CauseNames)
	}
}

// The lookup is made when the span arrives, not when its hold ends: the slot may be another's
// by then.  Roots have no parent and get their own start.
func TestArrivalIsLookedUpBeforeTheHold(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := testConfig(t)
	cfg.Period = time.Hour
	cfg.ServerHold = 60 * time.Millisecond
	cfg.Sampler.InitialPlanP = 1
	a, put := arrivalAgent(t, cfg, sink)
	base := time.Now().Add(-100 * time.Millisecond)
	put(1, base.Add(1500*time.Microsecond))
	td := buildTraces(1, base, []spanSpec{
		{"composepost", "ComposePost", 2, 1, 2 * time.Millisecond, 30 * time.Millisecond, ptrace.SpanKindServer},
		{"wrk2api", "Wrk2APIServiceServer_ComposePost", 1, 0, 0, 33 * time.Millisecond, ptrace.SpanKindServer},
	})
	if err := a.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	put(1, time.Now().Add(time.Hour)) // the slot is taken while the span waits
	for end := time.Now().Add(2 * time.Second); sink.SpanCount() < 2 && time.Now().Before(end); {
		time.Sleep(10 * time.Millisecond)
	}
	if sink.SpanCount() != 2 {
		t.Fatalf("%d spans out", sink.SpanCount())
	}
	for _, td := range sink.AllTraces() {
		for i := 0; i < td.ResourceSpans().Len(); i++ {
			s := td.ResourceSpans().At(i).ScopeSpans().At(0).Spans().At(0)
			at, src := arrivalOf(t, s)
			switch s.SpanID()[7] {
			case 2:
				if d := time.Duration(at - base.Add(1500*time.Microsecond).UnixNano()); src != "ebpf" || d > time.Millisecond || d < -time.Millisecond {
					t.Errorf("held span: %s, %v off", src, d)
				}
			case 1:
				if src != "start" || at != int64(s.StartTimestamp()) {
					t.Errorf("root: %s %d", src, at)
				}
			}
		}
	}
	if n := a.arr.Counts(); n[arrival.MissRoot] != 1 || n[arrival.Hit] != 1 {
		t.Errorf("outcomes %v", n)
	}
}

// Without arrival_table nothing is attached; with it, and no table, every span falls back and
// the decisions are the same.
func TestArrivalDoesNotChangeDecisions(t *testing.T) {
	run := func(table string) map[byte]bool {
		sink := new(consumertest.TracesSink)
		cfg := testConfig(t)
		cfg.Period = time.Hour
		cfg.ArrivalTable = table
		cfg.Sampler.InitialPlanP = 0 // outliers only
		a, err := newAgent(processortest.NewNopSettings(typeStr), cfg, sink)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Start(context.Background(), componenttest.NewNopHost()); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = a.Shutdown(context.Background()) }()
		td := buildTraces(1, time.Now().Add(-100*time.Millisecond), []spanSpec{
			{"socialgraph", "GetFollowers", 3, 2, 2 * time.Millisecond, 10 * time.Millisecond, ptrace.SpanKindServer},
			{"socialgraph", "GetFollowers", 6, 2, 2 * time.Millisecond, 1 * time.Millisecond, ptrace.SpanKindServer},
			{"composepost", "ComposePost", 2, 1, time.Millisecond, 30 * time.Millisecond, ptrace.SpanKindServer},
		})
		if err := a.ConsumeTraces(context.Background(), td); err != nil {
			t.Fatal(err)
		}
		kept := map[byte]bool{}
		for i := 0; i < td.ResourceSpans().Len(); i++ {
			s := td.ResourceSpans().At(i).ScopeSpans().At(0).Spans().At(0)
			kept[s.SpanID()[7]] = true
			if _, src := arrivalOf(t, s); (src != "") != (table != "") || table != "" && src != "start" {
				t.Errorf("table %q: source %q", table, src)
			}
		}
		if table != "" {
			if n := a.arr.Counts(); n[arrival.MissNoTable] != 3 {
				t.Errorf("outcomes %v", n)
			}
			if f := a.arr.Take(); len(f) != int(arrival.NumCauses)+arrival.NumCounters+2 {
				t.Errorf("%d log fields", len(f))
			}
		}
		return kept
	}
	off, on := run(""), run(t.TempDir())
	if len(off) == 0 || len(off) != len(on) {
		t.Fatalf("kept %v without, %v with", off, on)
	}
	for id := range off {
		if !on[id] {
			t.Fatalf("kept %v without, %v with", off, on)
		}
	}
}

// own_measure within_service: the outlier test counts from the arrival (the stamp, else the span's
// start), also when the span was held; own_measure handler does not.
func TestOwnMeasure(t *testing.T) {
	for _, hold := range []time.Duration{0, 30 * time.Millisecond} {
		for measure, want := range map[string][]byte{"handler": {7}, "within_service": {5, 7}} {
			sink := new(consumertest.TracesSink)
			cfg := testConfig(t)
			cfg.Period = time.Hour
			cfg.ServerHold = hold
			cfg.Sampler.OutlierTrigger = "own"
			cfg.Sampler.OwnMeasure = measure
			cfg.Sampler.InitialPlanP = 0 // outliers only
			cfg.Sampler.InitialBudget = 100
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			a, put := arrivalAgent(t, cfg, sink)
			a.mu.Lock()
			a.s.ApplyCentral(&slobudget.CentralUpdate{Version: 1, Table: map[string]map[string]map[string]float64{"compose_post": {"*": {"socialgraph": 0.003}}},
				BTable: map[string]map[string]map[string]float64{"compose_post": {"*": {"socialgraph": 0.003}}}}, a.host)
			a.mu.Unlock()
			base := time.Now().Add(-100 * time.Millisecond)
			put(4, base.Add(1500*time.Microsecond)) // span 5's request: 2.5 ms before its handler starts
			td := buildTraces(1, base, []spanSpec{
				{"socialgraph", "GetFollowers", 5, 4, 4 * time.Millisecond, time.Millisecond, ptrace.SpanKindServer},     // own 1 ms, 3.5 ms from its arrival
				{"socialgraph", "GetFollowers", 6, 9, 4 * time.Millisecond, time.Millisecond, ptrace.SpanKindServer},     // no arrival
				{"socialgraph", "GetFollowers", 7, 9, 4 * time.Millisecond, 4 * time.Millisecond, ptrace.SpanKindServer}, // own 4 ms
			})
			if err := a.ConsumeTraces(context.Background(), td); err != nil {
				t.Fatal(err)
			}
			time.Sleep(3*hold + 20*time.Millisecond)
			kept := map[byte]bool{}
			collect := func(td ptrace.Traces) {
				for i := 0; i < td.ResourceSpans().Len(); i++ {
					s := td.ResourceSpans().At(i).ScopeSpans().At(0).Spans().At(0)
					kept[s.SpanID()[7]] = true
					if v, ok := s.Attributes().Get(AttrOwnUS); !ok || v.Int() > 4000 { // the stamp stays the handler's own time
						t.Errorf("span %d: own_us %v", s.SpanID()[7], v.AsRaw())
					}
				}
			}
			if hold == 0 {
				collect(td)
			}
			for _, x := range sink.AllTraces() {
				collect(x)
			}
			ok := len(kept) == len(want)
			for _, id := range want {
				ok = ok && kept[id]
			}
			if !ok {
				t.Errorf("hold %v, %s: kept %v, want %v", hold, measure, kept, want)
			}
		}
	}
}

// Behind the stamp in the tee the agent has no table: it reads the attribute the spans bring and
// leaves it alone.
func TestArrivalFromTheTee(t *testing.T) {
	for measure, want := range map[string]int{"handler": 0, "within_service": 1} {
		sink := new(consumertest.TracesSink)
		cfg := testConfig(t)
		cfg.Period = time.Hour
		cfg.Sampler.OutlierTrigger = "own"
		cfg.Sampler.OwnMeasure = measure
		cfg.Sampler.InitialPlanP = 0 // outliers only
		a, err := newAgent(processortest.NewNopSettings(typeStr), cfg, sink)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Start(context.Background(), componenttest.NewNopHost()); err != nil {
			t.Fatal(err)
		}
		a.mu.Lock()
		a.s.ApplyCentral(&slobudget.CentralUpdate{Version: 1, Table: map[string]map[string]map[string]float64{"compose_post": {"*": {"socialgraph": 0.003}}},
			BTable: map[string]map[string]map[string]float64{"compose_post": {"*": {"socialgraph": 0.003}}}}, a.host)
		a.mu.Unlock()
		base := time.Now().Add(-100 * time.Millisecond)
		td := buildTraces(1, base, []spanSpec{{"socialgraph", "GetFollowers", 5, 4, 4 * time.Millisecond, time.Millisecond, ptrace.SpanKindServer}})
		sp := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		at := base.Add(1500 * time.Microsecond).UnixNano() // own 1 ms, 3.5 ms from its arrival
		sp.Attributes().PutInt(AttrArrival, at)
		sp.Attributes().PutStr(AttrArrivalSource, "ebpf")
		if err := a.ConsumeTraces(context.Background(), td); err != nil {
			t.Fatal(err)
		}
		if td.SpanCount() != want {
			t.Errorf("%s: %d spans kept, want %d", measure, td.SpanCount(), want)
		}
		if want == 1 {
			if got, src := arrivalOf(t, td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)); got != at || src != "ebpf" {
				t.Errorf("the stamp became %d, %s", got, src)
			}
		}
		_ = a.Shutdown(context.Background())
	}
}

func TestSpanIDIsBigEndian(t *testing.T) {
	// the program stores the digits as written; the table's key is the same number
	id := pcommon.SpanID{0x96, 0x32, 0xf8, 0x65, 0x18, 0xe1, 0x53, 0x43}
	if spanIDToU64(id) != 0x9632f86518e15343 {
		t.Fatalf("%x", spanIDToU64(id))
	}
}
