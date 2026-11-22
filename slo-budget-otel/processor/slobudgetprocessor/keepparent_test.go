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
)

func keepParentConfig(t *testing.T, host string) *Config {
	cfg := testConfig(t)
	cfg.Host = host
	cfg.ClientSpans = "plan"
	cfg.Sampler.KeepParent = true
	cfg.Sampler.InitialPlanP = 0 // ordinary spans are never plan-kept: only forces keep them
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func brBytes(depth uint64, anchor uint64) []byte {
	b := binary.AppendUvarint(nil, depth)
	var a [8]byte
	binary.BigEndian.PutUint64(a[:], anchor)
	b = append(b, a[:]...)
	return append(b, 0, 0, 0)
}

func reasonsOf(sink *consumertest.TracesSink) map[uint64]string {
	out := map[uint64]string{}
	for _, td := range sink.AllTraces() {
		rss := td.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			sss := rss.At(i).ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				spans := sss.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					v, _ := spans.At(k).Attributes().Get(AttrReason)
					out[spanIDToU64(spans.At(k).SpanID())] = v.Str()
				}
			}
		}
	}
	return out
}

// A keeps an outlier (socialgraph server span 5, parent client span 4, `_br` anchor 3 =
// hometimeline's server span); its force-notices reach B, which sees spans 4 and 3 and
// keeps both at p = 1 with reason "parent".
func TestForceNoticeKeepsTheParentAndTheAnchorOnAnotherAgent(t *testing.T) {
	sinkA, sinkB := new(consumertest.TracesSink), new(consumertest.TracesSink)
	a, err := newAgent(processortest.NewNopSettings(typeStr), keepParentConfig(t, "node-a"), sinkA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := newAgent(processortest.NewNopSettings(typeStr), keepParentConfig(t, "node-b"), sinkB)
	if err != nil {
		t.Fatal(err)
	}
	for _, ag := range []*agent{a, b} {
		if err := ag.Start(context.Background(), componenttest.NewNopHost()); err != nil {
			t.Fatal(err)
		}
	}
	defer func() { _ = a.Shutdown(context.Background()); _ = b.Shutdown(context.Background()) }()
	a.notices.mu.Lock()
	a.notices.peers = []string{b.notices.conn.LocalAddr().String(), a.notices.conn.LocalAddr().String()}
	a.notices.mu.Unlock()

	base := time.Now().Add(-100 * time.Millisecond)
	tdA := buildTraces(9, base, []spanSpec{
		{"socialgraph", "SocialGraphServiceServer_GetFollowers", 5, 4, 4 * time.Millisecond, 30 * time.Millisecond, ptrace.SpanKindServer}})
	tdA.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().PutEmptyBytes(AttrBR).FromRaw(brBytes(4, 3))
	if err := a.ConsumeTraces(context.Background(), tdA); err != nil {
		t.Fatal(err)
	}
	if got := reasonsOf(sinkA)[5]; got != "outlier" {
		t.Fatalf("A kept span 5 as %q, want outlier", got)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		b.mu.Lock()
		_, f4 := b.forcedAt[4]
		_, f3 := b.forcedAt[3]
		b.mu.Unlock()
		if f4 && f3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("force-notices did not reach B: parent %v anchor %v", f4, f3)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// A ignored its own broadcast echo: the forces it made are its own entries
	a.mu.Lock()
	if _, ok := a.sentForce[4]; !ok {
		t.Fatal("A did not record its broadcast")
	}
	a.mu.Unlock()
	tdB := buildTraces(9, base, []spanSpec{
		{"hometimeline", "SocialGraphServiceClient_GetFollowers", 4, 3, 3 * time.Millisecond, 32 * time.Millisecond, ptrace.SpanKindClient},
		{"hometimeline", "HomeTimelineServiceServer_WriteHomeTimeline", 3, 2, 2 * time.Millisecond, 5 * time.Millisecond, ptrace.SpanKindServer}})
	if err := b.ConsumeTraces(context.Background(), tdB); err != nil {
		t.Fatal(err)
	}
	got := reasonsOf(sinkB)
	if got[4] != "parent" || got[3] != "parent" {
		t.Fatalf("B kept %v, want spans 4 and 3 as parent", got)
	}
	b.mu.Lock()
	kept, late := b.stats.forcedKept, b.stats.forceLate+b.stats.forceLateKept
	b.mu.Unlock()
	if kept != 2 || late != 0 {
		t.Fatalf("B forced kept %d late %d", kept, late)
	}
}

// A force for a span the agent already decided is late: counted, not kept.
func TestLateForceIsCountedNotHonoured(t *testing.T) {
	sink := new(consumertest.TracesSink)
	b, err := newAgent(processortest.NewNopSettings(typeStr), keepParentConfig(t, "node-b"), sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Shutdown(context.Background()) }()
	base := time.Now().Add(-100 * time.Millisecond)
	_ = b.ConsumeTraces(context.Background(), buildTraces(8, base, []spanSpec{
		{"hometimeline", "SocialGraphServiceClient_GetFollowers", 6, 3, time.Millisecond, 2 * time.Millisecond, ptrace.SpanKindClient}}))
	if sink.SpanCount() != 0 {
		t.Fatal("the ordinary client span should be dropped (plan rate 0)")
	}
	var sid pcommon.SpanID
	sid[7] = 6
	b.onForce([16]byte{}, [8]byte(sid))
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stats.forceLate != 1 || b.s.IsForced(6) {
		t.Fatalf("late %d forced %v", b.stats.forceLate, b.s.IsForced(6))
	}
}

// Without keep_parent nothing is forced, broadcast or remembered.
func TestKeepParentOffLeavesTheAgentUnchanged(t *testing.T) {
	sink := new(consumertest.TracesSink)
	cfg := testConfig(t)
	a, err := newAgent(processortest.NewNopSettings(typeStr), cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().Add(-100 * time.Millisecond)
	td := buildTraces(7, base, []spanSpec{
		{"socialgraph", "SocialGraphServiceServer_GetFollowers", 5, 4, 4 * time.Millisecond, 30 * time.Millisecond, ptrace.SpanKindServer}})
	td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().PutEmptyBytes(AttrBR).FromRaw(brBytes(4, 3))
	if err := a.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Shutdown(context.Background()) }()
	_ = a.ConsumeTraces(context.Background(), td)
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.forcedAt)+len(a.sentForce)+len(a.decided) != 0 || a.s.IsForced(4) || a.s.IsForced(3) {
		t.Fatal("keep_parent bookkeeping ran with keep_parent off")
	}
}

func TestBrAnchorDecoding(t *testing.T) {
	m := pcommon.NewMap()
	m.PutEmptyBytes(AttrBR).FromRaw(brBytes(300, 0x0102030405060708))
	id, ok := brAnchor(m)
	if !ok || spanIDToU64(id) != 0x0102030405060708 {
		t.Fatalf("anchor %v %v", id, ok)
	}
	m.PutEmptyBytes(AttrBR).FromRaw(brBytes(0, 0))
	if _, ok := brAnchor(m); ok {
		t.Fatal("an all-zero anchor (the root's) must not count")
	}
	m.PutEmptyBytes(AttrBR).FromRaw([]byte{2, 1})
	if _, ok := brAnchor(m); ok {
		t.Fatal("a short payload must not decode")
	}
}
