//go:build slobudget_recon

package slobudgetreconprocessor

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
)

const graphJSON = `{"root": {"compose_post": "wrk2api"},
 "children": {"compose_post": {
    "wrk2api": ["composepost"],
    "composepost": ["hometimeline", "media", "poststorage", "text", "uniqueid", "user", "usertimeline"],
    "hometimeline": ["socialgraph"], "text": ["urlshorten", "usermention"],
    "socialgraph": [], "media": [], "poststorage": [], "uniqueid": [], "user": [], "usertimeline": [], "urlshorten": [], "usermention": []}}}`

func sid(n byte) pcommon.SpanID { var s pcommon.SpanID; s[7] = n; return s }

func br(depth int, anchor pcommon.SpanID) []byte {
	buf := binary.AppendUvarint(nil, uint64(depth))
	buf = append(buf, anchor[:]...)
	return append(buf, 0, 0, 0)
}

func addSpan(td ptrace.Traces, svc, name string, kind ptrace.SpanKind, id, parent pcommon.SpanID, start, end int64, attrs func(pcommon.Map)) {
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "unknown_service:"+svc+"_service_pb_es_proc")
	s := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	s.SetTraceID(pcommon.TraceID{1})
	s.SetSpanID(id)
	s.SetParentSpanID(parent)
	s.SetName(name)
	s.SetKind(kind)
	s.SetStartTimestamp(pcommon.Timestamp(start))
	s.SetEndTimestamp(pcommon.Timestamp(end))
	if attrs != nil {
		attrs(s.Attributes())
	}
}

// Kept: usertimeline server (depth 4, parent = composepost's client 0x31, anchor = composepost
// server 0x20) and hometimeline's client span to socialgraph (depth 5, parent = hometimeline
// server 0x42).  Dropped: everything else.  Expected: one tree under one synthetic root.
func TestReconstructOneTree(t *testing.T) {
	dir := t.TempDir()
	gf := filepath.Join(dir, "g.json")
	if err := os.WriteFile(gf, []byte(graphJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := createDefaultConfig().(*Config)
	cfg.CallGraphFile = gf
	cfg.Hold = 50 * time.Millisecond
	sink := new(consumertest.TracesSink)
	p, err := newRecon(processortest.NewNopSettings(typeStr), cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	td := ptrace.NewTraces()
	addSpan(td, "usertimeline", "UserTimelineServiceServer_WriteUserTimeline", ptrace.SpanKindServer, sid(0x40), sid(0x31), 1000, 1500,
		func(m pcommon.Map) { m.PutEmptyBytes("_br").FromRaw(br(4, sid(0x20))); m.PutStr("slo.root", "compose_post") })
	addSpan(td, "hometimeline", "SocialGraphServiceClient_GetFollowers", ptrace.SpanKindClient, sid(0x50), sid(0x42), 1200, 1900,
		func(m pcommon.Map) { m.PutEmptyBytes("_d").FromRaw(binary.AppendUvarint(nil, 5)) })
	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for sink.SpanCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	out := sink.AllTraces()
	if len(out) != 1 {
		t.Fatalf("traces out: %d", len(out))
	}
	spans := map[pcommon.SpanID]ptrace.Span{}
	synth := 0
	roots := 0
	rss := out[0].ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j).Spans()
			for k := 0; k < ss.Len(); k++ {
				s := ss.At(k)
				spans[s.SpanID()] = s
				if v, ok := s.Attributes().Get("synthetic"); ok && v.Bool() {
					synth++
				}
				if s.ParentSpanID().IsEmpty() {
					roots++
				}
			}
		}
	}
	// 2 kept + placeholders: 0x31 client, 0x20 composepost server, wrk2api client, root,
	// 0x42 hometimeline server, composepost's client to hometimeline = 6
	if synth != 6 {
		t.Fatalf("placeholders: %d (spans %d)", synth, len(spans))
	}
	if roots != 1 {
		t.Fatalf("roots: %d", roots)
	}
	for id, s := range spans {
		if pid := s.ParentSpanID(); !pid.IsEmpty() {
			if _, ok := spans[pid]; !ok {
				t.Fatalf("span %v: parent %v missing", id, pid)
			}
		}
	}
	// bounds: the composepost server placeholder covers both kept spans
	cp := spans[sid(0x20)]
	if cp.StartTimestamp() != 1000 || cp.EndTimestamp() != 1900 {
		t.Fatalf("composepost placeholder bounds %d-%d", cp.StartTimestamp(), cp.EndTimestamp())
	}
	if cp.Kind() != ptrace.SpanKindServer {
		t.Fatalf("composepost placeholder kind %v", cp.Kind())
	}
	if v, _ := rss.At(0).Resource().Attributes().Get("service.name"); v.AsString() == "" {
		t.Fatal("resource without service.name")
	}
	// the hometimeline server placeholder has the kept client's parent id
	if hs, ok := spans[sid(0x42)]; !ok || hs.Kind() != ptrace.SpanKindServer {
		t.Fatalf("hometimeline server placeholder missing or wrong kind")
	}
	// root placeholder named after the route
	for _, s := range spans {
		if s.ParentSpanID().IsEmpty() && s.Name() != "compose_post root [synthetic]" {
			t.Fatalf("root name %q", s.Name())
		}
	}
	// late span after the flush passes through unchanged
	late := ptrace.NewTraces()
	addSpan(late, "media", "MediaServiceServer_UploadMedia", ptrace.SpanKindServer, sid(0x44), sid(0x33), 1300, 1400, nil)
	sink2 := new(consumertest.TracesSink)
	p2, _ := newRecon(processortest.NewNopSettings(typeStr), cfg, sink2)
	p2.done[pcommon.TraceID{1}] = time.Now()
	if err := p2.ConsumeTraces(context.Background(), late); err != nil {
		t.Fatal(err)
	}
	if sink2.SpanCount() != 1 {
		t.Fatalf("late span not passed through: %d", sink2.SpanCount())
	}
}
