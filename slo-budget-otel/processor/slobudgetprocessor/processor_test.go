package slobudgetprocessor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
)

func seedFile(t *testing.T) string {
	t.Helper()
	tab := map[string]map[string]float64{"compose_post": {"wrk2api": 0.0234, "composepost": 0.0176, "socialgraph": 0.003, "hometimeline": 0.0105}}
	raw, _ := json.Marshal(tab)
	p := filepath.Join(t.TempDir(), "ttable.json")
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func testConfig(t *testing.T) *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.StandaloneVStar = 1000
	cfg.Host = "node-test"
	cfg.RootHold = 50 * time.Millisecond
	cfg.NoticeListen = "127.0.0.1:0"
	cfg.ClientSpans = "drop"
	cfg.Sampler.ColdPrior = "file"
	cfg.TTableFile = seedFile(t)
	cfg.StatsLog = false
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

type spanSpec struct {
	service, name string
	id, parent    uint64
	start, dur    time.Duration
	kind          ptrace.SpanKind
}

func buildTraces(tid byte, base time.Time, specs []spanSpec) ptrace.Traces {
	td := ptrace.NewTraces()
	for _, s := range specs {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "unknown_service:"+s.service+"_service_pb_es_proc")
		span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetName(s.name)
		var t pcommon.TraceID
		t[15] = tid
		span.SetTraceID(t)
		var sid pcommon.SpanID
		sid[7] = byte(s.id)
		span.SetSpanID(sid)
		if s.parent != 0 {
			var pid pcommon.SpanID
			pid[7] = byte(s.parent)
			span.SetParentSpanID(pid)
		}
		span.SetKind(s.kind)
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(base.Add(s.start)))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(base.Add(s.start + s.dur)))
	}
	return td
}

func TestAgentDecidesOutliersHoldsAndPinsRoots(t *testing.T) {
	sink := new(consumertest.TracesSink)
	a, err := newAgent(processortest.NewNopSettings(typeStr), testConfig(t), sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Shutdown(context.Background()) }()
	base := time.Now().Add(-100 * time.Millisecond)
	// trace 1: socialgraph 10 ms (T = 3 ms: outlier), composepost 30 ms (T = 17.6: outlier) with a client span
	// (dropped by config) and the server child; the root arrives last and must be pinned by the local notice
	td := buildTraces(1, base, []spanSpec{
		{"socialgraph", "GetFollowers", 3, 2, 2 * time.Millisecond, 10 * time.Millisecond, ptrace.SpanKindServer},
		{"composepost", "socialgraph.call", 4, 2, time.Millisecond, 12 * time.Millisecond, ptrace.SpanKindClient},
		{"composepost", "ComposePost", 2, 1, time.Millisecond, 30 * time.Millisecond, ptrace.SpanKindServer},
		{"wrk2api", "Wrk2APIServiceServer_ComposePost", 1, 0, 0, 33 * time.Millisecond, ptrace.SpanKindServer},
	})
	if err := a.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	// the batch keeps the two outliers; the root was released from the hold at once (flagged) via a goroutine
	if td.SpanCount() != 2 {
		t.Fatalf("batch after decisions: %d spans", td.SpanCount())
	}
	deadline := time.Now().Add(2 * time.Second)
	for sink.SpanCount() < 3 && time.Now().Before(deadline) { // the batch's 2 spans plus the root, forwarded asynchronously
		time.Sleep(10 * time.Millisecond)
	}
	got := map[string]map[string]any{}
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		span := td.ResourceSpans().At(i).ScopeSpans().At(0).Spans().At(0)
		got[span.Name()] = span.Attributes().AsRaw()
	}
	for _, rs := range sink.AllTraces() {
		for i := 0; i < rs.ResourceSpans().Len(); i++ {
			span := rs.ResourceSpans().At(i).ScopeSpans().At(0).Spans().At(0)
			got[span.Name()] = span.Attributes().AsRaw()
		}
	}
	sg := got["GetFollowers"]
	if sg == nil || sg[AttrSamplingP] != 1.0 || sg[AttrReason] != "outlier" {
		t.Fatalf("socialgraph: %v", sg)
	}
	cp := got["ComposePost"]
	if cp == nil || cp[AttrReason] != "outlier" || cp[AttrChildren] != int64(2) || cp[AttrDropped] != int64(1) {
		t.Fatalf("composepost: %v", cp)
	}
	if own := cp[AttrOwnUS].(int64); own < 17000 || own > 19000 { // 30 ms minus the 12 ms client span
		t.Fatalf("composepost own time %d us", own)
	}
	root := got["Wrk2APIServiceServer_ComposePost"]
	if root == nil || root[AttrRootPinned] != true || root[AttrSamplingP] != 1.0 || root[AttrReason] != "root" {
		names := []string{}
		for n := range got {
			names = append(names, n)
		}
		t.Fatalf("root: %v (sink spans %d, names %v)", root, sink.SpanCount(), names)
	}
	a.mu.Lock()
	st := a.s.Snapshot(a.host, 0)
	pending := len(a.pending)
	a.mu.Unlock()
	if st.Requests != 1 || st.Ndet["socialgraph"] != 1 || st.Ndet["composepost"] != 1 || st.SpansSvc["socialgraph"] != 1 || pending != 0 {
		t.Fatalf("stats %+v pending %d", st, pending)
	}
	// trace 2: nothing is slow; the root is held for root_hold and then plan-sampled (p = initial_plan_p)
	td2 := buildTraces(2, base, []spanSpec{
		{"socialgraph", "GetFollowers", 3, 2, 2 * time.Millisecond, time.Millisecond, ptrace.SpanKindServer},
		{"wrk2api", "Wrk2APIServiceServer_ComposePost", 1, 0, 0, 12 * time.Millisecond, ptrace.SpanKindServer},
	})
	before := sink.SpanCount()
	_ = a.ConsumeTraces(context.Background(), td2)
	a.mu.Lock()
	held := len(a.pending)
	a.mu.Unlock()
	if held != 1 {
		t.Fatalf("root of trace 2 should be held, pending %d", held)
	}
	time.Sleep(250 * time.Millisecond)
	a.mu.Lock()
	held = len(a.pending)
	a.mu.Unlock()
	if held != 0 {
		t.Fatal("hold did not expire")
	}
	if n := sink.SpanCount() - before; n > 1 {
		t.Fatalf("trace 2 forwarded %d spans", n)
	}
}

func TestKeptNoticeReleasesARootHeldElsewhere(t *testing.T) {
	// two agents on one machine: B keeps a span and notices A, which holds the root
	sinkA, sinkB := new(consumertest.TracesSink), new(consumertest.TracesSink)
	cfgA := testConfig(t)
	cfgA.RootHold = 5 * time.Second
	cfgA.NoticeListen = "127.0.0.1:0"
	a, err := newAgent(processortest.NewNopSettings(typeStr), cfgA, sinkA)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Shutdown(context.Background()) }()
	cfgB := testConfig(t)
	cfgB.Host = "node-b"
	cfgB.NoticePeers = []string{a.notices.conn.LocalAddr().String()}
	b, err := newAgent(processortest.NewNopSettings(typeStr), cfgB, sinkB)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Shutdown(context.Background()) }()
	base := time.Now().Add(-50 * time.Millisecond)
	_ = a.ConsumeTraces(context.Background(), buildTraces(7, base, []spanSpec{
		{"wrk2api", "Wrk2APIServiceServer_ComposePost", 1, 0, 0, 40 * time.Millisecond, ptrace.SpanKindServer}}))
	a.mu.Lock()
	held := len(a.pending)
	a.mu.Unlock()
	if held != 1 {
		t.Fatal("root not held on A")
	}
	_ = b.ConsumeTraces(context.Background(), buildTraces(7, base, []spanSpec{
		{"socialgraph", "GetFollowers", 3, 2, 2 * time.Millisecond, 10 * time.Millisecond, ptrace.SpanKindServer}}))
	deadline := time.Now().Add(2 * time.Second)
	for sinkA.SpanCount() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if sinkA.SpanCount() != 1 {
		t.Fatal("root on A was not released by B's notice")
	}
	span := sinkA.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	if v, _ := span.Attributes().Get(AttrRootPinned); !v.Bool() {
		t.Fatal("root should be pinned")
	}
}
