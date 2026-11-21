package recon

import (
	"encoding/binary"
	"testing"
)

const graphJSON = `{"root": {"compose_post": "wrk2api"},
 "children": {"compose_post": {
    "wrk2api": ["composepost"],
    "composepost": ["hometimeline", "media", "poststorage", "text", "uniqueid", "user", "usertimeline"],
    "hometimeline": ["socialgraph"], "text": ["urlshorten", "usermention"],
    "socialgraph": [], "media": [], "poststorage": [], "uniqueid": [], "user": [], "usertimeline": [], "urlshorten": [], "usermention": []}}}`

func sid(n byte) SpanID { var s SpanID; s[7] = n; return s }

func br(depth int, anchor SpanID) []byte {
	buf := binary.AppendUvarint(nil, uint64(depth))
	buf = append(buf, anchor[:]...)
	return append(buf, 0, 0, 0)
}

func dd(depth int) []byte { return binary.AppendUvarint(nil, uint64(depth)) }

func graph(t *testing.T) *CallGraph {
	g, err := ParseCallGraph([]byte(graphJSON))
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func index(spans []Span, ph []Placeholder) map[SpanID]Placeholder {
	m := map[SpanID]Placeholder{}
	for _, p := range ph {
		m[p.ID] = p
	}
	return m
}

// Kept: usertimeline server (depth 4, parent = composepost's client 0x31, anchor = composepost
// server 0x20) and hometimeline's client span to socialgraph (depth 5, parent = hometimeline
// server 0x42).  Everything else dropped: one tree under one synthetic root, the composepost
// server shared by both chains (slot merging), bounds covering both kept spans.
func TestReconstructOneTree(t *testing.T) {
	g := graph(t)
	spans := []Span{
		{ID: sid(0x40), Parent: sid(0x31), Service: "usertimeline", Operation: "UserTimelineServiceServer_WriteUserTimeline", Kind: KindServer, Start: 1000, End: 1500, BR: br(4, sid(0x20))},
		{ID: sid(0x50), Parent: sid(0x42), Service: "hometimeline", Operation: "SocialGraphServiceClient_GetFollowers", Kind: KindClient, Start: 1200, End: 1900, D: dd(5)},
	}
	res := Reconstruct(TraceID{1}, "compose_post", spans, g, Options{SynthesizeIDs: true})
	// 0x31 client, 0x20 composepost server, wrk2api client, root, 0x42 hometimeline server,
	// composepost's client to hometimeline = 6
	if len(res.Placeholders) != 6 {
		for _, p := range res.Placeholders {
			t.Logf("%v depth %d %s %s parent %v", p.ID, p.Depth, p.Service, p.Operation, p.Parent)
		}
		t.Fatalf("placeholders: %d", len(res.Placeholders))
	}
	if !OneTree(spans, res.Placeholders) {
		t.Fatal("not one tree")
	}
	ph := index(spans, res.Placeholders)
	cp, ok := ph[sid(0x20)]
	if !ok || cp.Kind != KindServer || cp.Service != "composepost" || !cp.RealID {
		t.Fatalf("composepost server placeholder: %+v", cp)
	}
	if cp.Start != 1000 || cp.End != 1900 {
		t.Fatalf("composepost bounds %d-%d", cp.Start, cp.End)
	}
	hs, ok := ph[sid(0x42)]
	if !ok || hs.Kind != KindServer || hs.Service != "hometimeline" {
		t.Fatalf("hometimeline server placeholder: %+v", hs)
	}
	// hometimeline's chain reaches the composepost server through the slot, not a second one
	cl := ph[hs.Parent]
	if cl.Service != "composepost" || cl.Kind != KindClient || cl.Parent != sid(0x20) {
		t.Fatalf("composepost client to hometimeline: %+v", cl)
	}
	roots := 0
	for _, p := range res.Placeholders {
		if p.Depth == 0 {
			roots++
			if p.Operation != "compose_post root [synthetic]" || p.Service != "wrk2api" || p.RealID {
				t.Fatalf("root placeholder: %+v", p)
			}
		}
		if p.Operation[len(p.Operation)-len("[synthetic]"):] != "[synthetic]" {
			t.Fatalf("operation without the marker: %q", p.Operation)
		}
	}
	if roots != 1 {
		t.Fatalf("root placeholders: %d", roots)
	}
	// coverage: the server span reaches a missing grandparent at depth 2 -> open with hole 2;
	// the client span's parent is missing and nothing above is known -> open with hole 1
	if len(res.Orphans) != 2 {
		t.Fatalf("orphans: %d", len(res.Orphans))
	}
	for _, o := range res.Orphans {
		switch o.Span {
		case sid(0x40):
			if o.Class != "open" || o.Hole != 2 || o.HoleFull != 4 {
				t.Fatalf("usertimeline placement %+v", o)
			}
		case sid(0x50):
			if o.Class != "open" || o.Hole != 1 || o.HoleFull != 5 {
				t.Fatalf("hometimeline client placement %+v", o)
			}
		}
	}
	// deterministic and trace-specific synthetic ids
	again := Reconstruct(TraceID{1}, "compose_post", spans, g, Options{SynthesizeIDs: true})
	other := Reconstruct(TraceID{2}, "compose_post", spans, g, Options{SynthesizeIDs: true})
	for i := range res.Placeholders {
		if res.Placeholders[i].ID != again.Placeholders[i].ID {
			t.Fatal("ids not deterministic")
		}
	}
	rootID := func(r Result) SpanID {
		for _, p := range r.Placeholders {
			if p.Depth == 0 {
				return p.ID
			}
		}
		return SpanID{}
	}
	if rootID(res) == rootID(other) {
		t.Fatal("synthetic root id equal across traces")
	}
}

// Without synthesised ids the chains stop at the first unknown id: the composepost server
// (known through the anchor) stays, its parent is open, and the trace is not one tree.
func TestReconstructKnownIDsOnly(t *testing.T) {
	g := graph(t)
	spans := []Span{
		{ID: sid(0x40), Parent: sid(0x31), Service: "usertimeline", Operation: "UserTimelineServiceServer_WriteUserTimeline", Kind: KindServer, Start: 1000, End: 1500, BR: br(4, sid(0x20))},
	}
	res := Reconstruct(TraceID{1}, "compose_post", spans, g, Options{})
	if len(res.Placeholders) != 2 {
		t.Fatalf("placeholders: %d", len(res.Placeholders))
	}
	ph := index(spans, res.Placeholders)
	if !ph[sid(0x31)].RealID || ph[sid(0x31)].Parent != sid(0x20) {
		t.Fatalf("client placeholder %+v", ph[sid(0x31)])
	}
	if !ph[sid(0x20)].Open || !ph[sid(0x20)].Parent.IsZero() {
		t.Fatalf("composepost server should be open: %+v", ph[sid(0x20)])
	}
	if OneTree(spans, res.Placeholders) {
		t.Fatal("open chain must not count as one tree")
	}
}

// A kept span whose grandparent is kept: hole 1 to a kept ancestor, placeholder bounded by the
// child; a depth-1 client span whose root is missing: hole 1 to the root slot.
func TestClassifyAndBounds(t *testing.T) {
	g := graph(t)
	spans := []Span{
		{ID: sid(0x20), Parent: sid(0x11), Service: "composepost", Operation: "ComposePostServiceServer_ComposePost", Kind: KindServer, Start: 100, End: 900, BR: br(2, sid(0x01))},
		{ID: sid(0x40), Parent: sid(0x31), Service: "usertimeline", Operation: "UserTimelineServiceServer_WriteUserTimeline", Kind: KindServer, Start: 300, End: 600, BR: br(4, sid(0x20))},
		{ID: sid(0x11), Parent: sid(0x01), Service: "wrk2api", Operation: "ComposePostServiceClient_ComposePost", Kind: KindClient, Start: 90, End: 950, D: dd(1)},
	}
	res := Reconstruct(TraceID{3}, "compose_post", spans, g, Options{SynthesizeIDs: true})
	ph := index(spans, res.Placeholders)
	if len(res.Placeholders) != 2 { // composepost's client to usertimeline (0x31) and the root (0x01)
		t.Fatalf("placeholders %d", len(res.Placeholders))
	}
	if p := ph[sid(0x31)]; p.Start != 300 || p.End != 600 || p.Parent != sid(0x20) || p.Operation != "UserTimelineServiceClient_WriteUserTimeline [synthetic]" {
		t.Fatalf("client placeholder %+v", p)
	}
	if p := ph[sid(0x01)]; p.Depth != 0 || p.Start != 90 || p.End != 950 || !p.RealID || p.Reason != "root-missing" {
		t.Fatalf("root placeholder %+v", p)
	}
	for _, o := range res.Orphans {
		switch o.Span {
		case sid(0x40):
			if o.Class != "kept" || o.Hole != 1 || o.HoleFull != 1 {
				t.Fatalf("usertimeline %+v", o)
			}
		case sid(0x11):
			if o.Class != "root" || o.Hole != 1 || o.HoleFull != 1 {
				t.Fatalf("wrk2api client %+v", o)
			}
		default:
			t.Fatalf("unexpected orphan %v", o.Span)
		}
	}
	if !OneTree(spans, res.Placeholders) {
		t.Fatal("not one tree")
	}
}

func TestDecodeBridge(t *testing.T) {
	d, a := DecodeBridge(br(4, sid(0x20)), nil)
	if d != 4 || a != sid(0x20) {
		t.Fatalf("br decode %d %v", d, a)
	}
	d, a = DecodeBridge(nil, dd(5))
	if d != 5 || !a.IsZero() {
		t.Fatalf("d decode %d %v", d, a)
	}
	d, _ = DecodeBridge(nil, nil)
	if d != -1 {
		t.Fatalf("absent decode %d", d)
	}
	if ClientNameOf("UserServiceServer_Login") != "UserServiceClient_Login" {
		t.Fatal("client name")
	}
}
