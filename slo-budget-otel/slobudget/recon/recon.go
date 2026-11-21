// Package recon reconstructs the shed structure of a trace: for every kept span whose parent
// was dropped it inserts placeholder ancestors up to the nearest kept ancestor or the root,
// from the span's own parent id, the Path Bridge's `_br` checkpoint anchor and depth, and
// the call graph.  It is pdata-free and shared by the offline tool (tools/recon) and the
// collector processor (processor/slobudgetreconprocessor, build tag slobudget_recon).
//
// Path Bridge fields (cpd 2 in DSB-SN): `_br` = uvarint(absolute depth, root = 0) ||
// 8-byte span id of the nearest checkpoint ancestor strictly above (every even depth is a
// checkpoint) || bloom of the ancestors strictly between the two; it is emitted on
// checkpoints and leaves, i.e. on every server span.  `_d` = uvarint(depth) on interior
// non-checkpoints, i.e. on every client span.  For a server span at even depth d the anchor
// is therefore its grandparent (depth d-2), the caller's server span.
package recon

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"strings"
)

// Span kinds.
const (
	KindServer = "server"
	KindClient = "client"
)

// TraceID and SpanID are the raw ids.
type (
	TraceID [16]byte
	SpanID  [8]byte
)

// IsZero reports an absent id.
func (s SpanID) IsZero() bool { return s == SpanID{} }

// String is the hex form Jaeger uses.
func (s SpanID) String() string { return fmt.Sprintf("%016x", binary.BigEndian.Uint64(s[:])) }

// Span is a kept span as the reconstruction sees it.
type Span struct {
	ID, Parent SpanID
	Service    string // short service name (slobudget.NormalizeService)
	Operation  string
	Kind       string // KindServer or KindClient
	Start, End uint64 // ns
	BR, D      []byte // the Path Bridge fields, nil when absent
	Synthetic  bool   // an earlier reconstruction's placeholder: ignored as a source
}

// Placeholder is a reconstructed ancestor.
type Placeholder struct {
	ID, Parent SpanID // Parent zero: the root, or an open end when ids are not synthesised
	Depth      int
	Service    string
	Kind       string
	Operation  string
	Callee     string // client placeholders: the callee service (the slot key)
	Start, End uint64
	RealID     bool   // the id is a kept span's parent id or `_br` anchor
	Reason     string // parent-missing | root-missing
	Open       bool   // its own parent is unknown and ids were not synthesised
}

// Placement classifies one kept span whose parent is missing, by known ids only.
type Placement struct {
	Span  SpanID
	Kind  string
	Depth int
	// Class: "kept" (a kept ancestor is reached), "root" (the root slot is reached), "open"
	// (the chain needs ids nobody kept).
	Class string
	// Hole: missing spans between the span and the kept ancestor / root slot it reaches; for
	// "open" the known missing links (1, or 2 when the anchor names a missing grandparent).
	Hole int
	// HoleFull: placeholders between the span and the nearest real ancestor or the root in the
	// full reconstruction (synthesised ids included).
	HoleFull int
}

// Options of Reconstruct.
type Options struct {
	// SynthesizeIDs lets a chain continue past ids nobody kept with deterministic ids hashed
	// from the trace id and the slot; false stops the chain there (the placeholder is Open).
	SynthesizeIDs bool
}

// Result of one trace.
type Result struct {
	Placeholders []Placeholder
	Orphans      []Placement // one per kept span whose parent is missing
}

// CallGraph is the subset of the simulator's call graph the reconstruction needs.
type CallGraph struct {
	Root     map[string]string              `json:"root"`     // route -> root service
	Children map[string]map[string][]string `json:"children"` // route -> service -> callees
	caller   map[string]map[string]string
}

// LoadCallGraph reads the call graph file; "" gives an empty graph.
func LoadCallGraph(file string) (*CallGraph, error) {
	if file == "" {
		return ParseCallGraph(nil)
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return ParseCallGraph(raw)
}

// ParseCallGraph parses the JSON form; nil gives an empty graph.
func ParseCallGraph(raw []byte) (*CallGraph, error) {
	g := &CallGraph{Root: map[string]string{}, Children: map[string]map[string][]string{}, caller: map[string]map[string]string{}}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, g); err != nil {
			return nil, err
		}
	}
	for route, ch := range g.Children {
		m := map[string]string{}
		for parent, kids := range ch {
			for _, k := range kids {
				m[k] = parent
			}
		}
		g.caller[route] = m
	}
	return g, nil
}

// CallerOf names the service that calls svc on route (any route that knows svc as a
// fallback), "" when unknown.
func (g *CallGraph) CallerOf(route, svc string) string {
	if m := g.caller[route]; m != nil {
		if c, ok := m[svc]; ok {
			return c
		}
	}
	keys := make([]string, 0, len(g.caller))
	for r := range g.caller {
		keys = append(keys, r)
	}
	sort.Strings(keys)
	for _, r := range keys {
		if c, ok := g.caller[r][svc]; ok {
			return c
		}
	}
	return ""
}

// RootOf names the root service of route (any route's root as a fallback).
func (g *CallGraph) RootOf(route string) string {
	if r, ok := g.Root[route]; ok {
		return r
	}
	keys := make([]string, 0, len(g.Root))
	for r := range g.Root {
		keys = append(keys, r)
	}
	sort.Strings(keys)
	if len(keys) > 0 {
		return g.Root[keys[0]]
	}
	return ""
}

// DecodeBridge reads the depth and the checkpoint anchor from the Path Bridge fields:
// `_br` = uvarint(depth) || 8-byte anchor || bloom, else `_d` = uvarint(depth).  depth is -1
// when neither is present; anchor is zero when `_br` is absent or too short.
func DecodeBridge(br, d []byte) (depth int, anchor SpanID) {
	depth = -1
	if len(br) > 0 {
		v, n := binary.Uvarint(br)
		if n > 0 {
			depth = int(v)
			if len(br) >= n+8 {
				copy(anchor[:], br[n:n+8])
			}
		}
		return
	}
	if len(d) > 0 {
		if v, n := binary.Uvarint(d); n > 0 {
			depth = int(v)
		}
	}
	return
}

type keptSpan struct {
	Span
	depth  int
	anchor SpanID
}

// slot identifies an ancestor position in the trace: two kept spans whose chains pass
// through the same slot share one placeholder.  Client slots carry the callee so that a
// caller's client spans to different callees stay apart.
type slot struct {
	depth         int
	service, kind string
	callee        string
}

// Reconstruct builds the missing ancestors of the kept spans of one trace.  Real ids first:
// every kept span names its parent, and a server span names its grandparent through the
// `_br` anchor; slots nobody named get a synthetic id (or stay open).  Chains from
// different kept spans merge on the slot.  Placeholders bound their kept descendants.
func Reconstruct(tid TraceID, route string, spans []Span, g *CallGraph, opt Options) Result {
	if g == nil {
		g, _ = ParseCallGraph(nil)
	}
	kept := map[SpanID]*keptSpan{}
	var order []*keptSpan
	for _, s := range spans {
		if s.Synthetic {
			continue
		}
		ks := &keptSpan{Span: s}
		ks.depth, ks.anchor = DecodeBridge(s.BR, s.D)
		if s.Parent.IsZero() {
			ks.depth = 0
		}
		if ks.Kind != KindClient {
			ks.Kind = KindServer
		}
		kept[s.ID] = ks
		order = append(order, ks)
	}
	ph := map[SpanID]*Placeholder{}
	var out []*Placeholder
	pos := map[slot]SpanID{}
	known := map[SpanID]bool{}
	parentSlot := func(depth int, svc, kind string) (slot, string) {
		if kind == KindServer {
			caller := g.CallerOf(route, svc)
			if caller == "" {
				caller = "unknown"
			}
			return slot{depth - 1, caller, KindClient, svc}, caller
		}
		return slot{depth - 1, svc, KindServer, ""}, svc
	}
	// seed the slots with every real id the kept spans know
	for _, ks := range order {
		if ks.depth >= 0 {
			callee := ""
			if ks.Kind == KindClient {
				callee = calleeOf(ks.Operation)
			}
			pos[slot{ks.depth, ks.Service, ks.Kind, callee}] = ks.ID
		}
	}
	for _, ks := range order {
		if ks.depth < 1 || ks.Parent.IsZero() {
			continue
		}
		ps, _ := parentSlot(ks.depth, ks.Service, ks.Kind)
		if _, ok := pos[ps]; !ok {
			pos[ps] = ks.Parent
		}
		known[ks.Parent] = true
		if ks.Kind == KindServer && !ks.anchor.IsZero() && ks.depth >= 2 {
			if caller := g.CallerOf(route, ks.Service); caller != "" {
				gs := slot{ks.depth - 2, caller, KindServer, ""}
				if _, ok := pos[gs]; !ok {
					pos[gs] = ks.anchor
				}
			}
			known[ks.anchor] = true
		}
	}
	rootSvc := g.RootOf(route)
	children := map[SpanID][]SpanID{}
	attach := func(parent, child SpanID) { children[parent] = append(children[parent], child) }
	var orphans []Placement
	for _, ks := range order {
		if ks.Parent.IsZero() {
			continue
		}
		if _, ok := kept[ks.Parent]; ok {
			attach(ks.Parent, ks.ID)
			continue
		}
		orphans = append(orphans, classify(ks, kept))
		curID, curParent, curDepth, curSvc, curKind, curName := ks.ID, ks.Parent, ks.depth, ks.Service, ks.Kind, ks.Operation
		curAnchor := ks.anchor
		curStart, curEnd := ks.Start, ks.End
		for !curParent.IsZero() {
			if _, ok := kept[curParent]; ok {
				attach(curParent, curID)
				break
			}
			if p, ok := ph[curParent]; ok {
				attach(curParent, curID)
				if curStart < p.Start || p.Start == 0 {
					p.Start = curStart
				}
				if curEnd > p.End {
					p.End = curEnd
				}
				break
			}
			p := &Placeholder{ID: curParent, RealID: known[curParent], Start: curStart, End: curEnd, Reason: "parent-missing"}
			if curDepth > 0 {
				p.Depth = curDepth - 1
			} else {
				p.Depth = -1
			}
			ps, psvc := parentSlot(curDepth, curSvc, curKind)
			p.Service = psvc
			if curKind == KindServer {
				p.Kind = KindClient
				p.Callee = curSvc
				if strings.Contains(curName, "[synthetic]") {
					p.Operation = psvc + " client to " + curSvc + " [synthetic]"
				} else {
					p.Operation = ClientNameOf(curName) + " [synthetic]"
				}
			} else {
				p.Kind = KindServer
				p.Operation = curSvc + " server [synthetic]"
			}
			if p.Depth == 0 || (p.Depth < 0 && p.Kind == KindServer && p.Service == rootSvc) {
				p.Depth = 0
				p.Kind = KindServer
				p.Callee = ""
				if rootSvc != "" {
					p.Service = rootSvc
				}
				p.Operation = route + " root [synthetic]"
				p.Reason = "root-missing"
				ps = slot{0, p.Service, KindServer, ""}
			}
			if _, ok := pos[ps]; !ok && p.Depth >= 0 {
				pos[ps] = p.ID
			}
			var next SpanID
			switch {
			case p.Depth == 0:
			case curKind == KindServer && !curAnchor.IsZero() && curDepth >= 2:
				next = curAnchor // the checkpoint two levels up names the grandparent
			default:
				ns, _ := parentSlot(p.Depth, p.Service, p.Kind)
				if id, ok := pos[ns]; ok {
					next = id
				} else if opt.SynthesizeIDs {
					next = SynthID(tid, ns.depth, ns.service, ns.kind, ns.callee)
					pos[ns] = next
				} else {
					p.Open = true
				}
			}
			p.Parent = next
			ph[p.ID] = p
			out = append(out, p)
			attach(curParent, curID)
			curID, curParent, curDepth, curSvc, curKind, curName = p.ID, p.Parent, p.Depth, p.Service, p.Kind, p.Operation
			curAnchor = SpanID{}
			if curDepth < 0 {
				break
			}
		}
	}
	// bounds bottom-up so parents cover their children
	sort.Slice(out, func(i, j int) bool { return out[i].Depth > out[j].Depth })
	for _, p := range out {
		for _, c := range children[p.ID] {
			var cs, ce uint64
			if k, ok := kept[c]; ok {
				cs, ce = k.Start, k.End
			} else if q, ok := ph[c]; ok {
				cs, ce = q.Start, q.End
			} else {
				continue
			}
			if p.Start == 0 || cs < p.Start {
				p.Start = cs
			}
			if ce > p.End {
				p.End = ce
			}
		}
	}
	// the full hole of every orphan: placeholders up to the nearest kept ancestor or the root
	for i := range orphans {
		n := 0
		cur := kept[orphans[i].Span].Parent
		for !cur.IsZero() {
			if _, ok := kept[cur]; ok {
				break
			}
			p, ok := ph[cur]
			if !ok {
				break
			}
			n++
			cur = p.Parent
		}
		orphans[i].HoleFull = n
	}
	res := Result{Orphans: orphans, Placeholders: make([]Placeholder, 0, len(out))}
	for _, p := range out {
		res.Placeholders = append(res.Placeholders, *p)
	}
	return res
}

// classify places one orphan by known ids only: its parent id, then (server spans) the
// `_br` anchor two levels up.
func classify(ks *keptSpan, kept map[SpanID]*keptSpan) Placement {
	pl := Placement{Span: ks.ID, Kind: ks.Kind, Depth: ks.depth, Class: "open", Hole: 1}
	switch {
	case ks.depth == 1:
		pl.Class, pl.Hole = "root", 1
	case ks.Kind == KindServer && !ks.anchor.IsZero() && ks.depth >= 2:
		if _, ok := kept[ks.anchor]; ok {
			pl.Class, pl.Hole = "kept", 1
		} else if ks.depth == 2 {
			pl.Class, pl.Hole = "root", 2
		} else {
			pl.Class, pl.Hole = "open", 2
		}
	}
	return pl
}

// ClientNameOf turns "<Svc>Server_<Method>" into "<Svc>Client_<Method>".
func ClientNameOf(serverName string) string {
	if i := strings.Index(serverName, "Server_"); i >= 0 {
		return serverName[:i] + "Client_" + serverName[i+len("Server_"):]
	}
	return serverName + " client"
}

// calleeOf extracts the callee interface from "<Svc>Client_<Method>" (slot key only).
func calleeOf(clientName string) string {
	if i := strings.Index(clientName, "Client_"); i >= 0 {
		return strings.ToLower(strings.TrimSuffix(clientName[:i], "Service"))
	}
	return clientName
}

// SynthID is the deterministic id of an ancestor slot nobody kept: the same for every kept
// span of the trace whose chain passes through the slot, different across traces.
func SynthID(tid TraceID, depth int, service, kind, callee string) SpanID {
	h := fnv.New64a()
	_, _ = h.Write(tid[:])
	_, _ = h.Write([]byte(fmt.Sprintf("%d|%s|%s|%s", depth, service, kind, callee)))
	var id SpanID
	binary.BigEndian.PutUint64(id[:], h.Sum64()|1)
	return id
}

// OneTree reports whether the spans plus placeholders form a single tree: exactly one
// parentless span and every parent present.
func OneTree(spans []Span, ph []Placeholder) bool {
	ids := map[SpanID]bool{}
	for _, s := range spans {
		ids[s.ID] = true
	}
	for _, p := range ph {
		ids[p.ID] = true
	}
	roots := 0
	for _, s := range spans {
		if s.Parent.IsZero() {
			roots++
		} else if !ids[s.Parent] {
			return false
		}
	}
	for _, p := range ph {
		if p.Open { // its parent exists but nobody knows its id
			return false
		}
		if p.Parent.IsZero() {
			roots++
		} else if !ids[p.Parent] {
			return false
		}
	}
	return roots == 1
}
