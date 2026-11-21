package slobudgetreconprocessor

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
)

// Attributes read and written.
const (
	attrBR      = "_br"      // path bridge: varint(depth) || 8-byte checkpoint anchor || bloom
	attrD       = "_d"       // path bridge: varint(depth)
	attrRoot    = "slo.root" // the route (from the SDK's baggage)
	attrRecon   = "slobudget.recon"
	attrDepth   = "slobudget.depth"
	scopeName   = "slobudgetrecon"
	kindServer  = "server"
	kindClient  = "client"
	traceHistTTL = 20 * time.Second
)

// callGraph is the subset of the simulator's call graph the reconstruction needs.
type callGraph struct {
	Root     map[string]string              `json:"root"`     // route -> root service
	Children map[string]map[string][]string `json:"children"` // route -> service -> callees
	caller   map[string]map[string]string   // route -> service -> caller
}

func loadCallGraph(file string) (*callGraph, error) {
	g := &callGraph{Root: map[string]string{}, Children: map[string]map[string][]string{}, caller: map[string]map[string]string{}}
	if file == "" {
		return g, nil
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, g); err != nil {
		return nil, err
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

func (g *callGraph) callerOf(route, svc string) string {
	if m := g.caller[route]; m != nil {
		if c, ok := m[svc]; ok {
			return c
		}
	}
	for _, m := range g.caller { // any route that knows the service
		if c, ok := m[svc]; ok {
			return c
		}
	}
	return ""
}

func (g *callGraph) rootOf(route string) string {
	if r, ok := g.Root[route]; ok {
		return r
	}
	for _, r := range g.Root {
		return r
	}
	return ""
}

// keptSpan is what the reconstruction knows about a kept span.
type keptSpan struct {
	id, parent pcommon.SpanID
	anchor     pcommon.SpanID // `_br` checkpoint anchor (server spans), zero when unknown
	depth      int            // absolute depth from `_br` / `_d`, -1 when unknown
	service    string         // short service name
	kind       string
	name       string
	start, end pcommon.Timestamp
}

// placeholder is a reconstructed ancestor.
type placeholder struct {
	id, parent pcommon.SpanID
	depth      int
	service    string
	kind       string
	name       string
	callee     string // for client placeholders: the callee service (the key that merges them)
	start, end pcommon.Timestamp
	realID     bool
	reason     string
}

type pendingTrace struct {
	first time.Time
	td    ptrace.Traces
	byRes map[string]int // service.name -> ResourceSpans index
	route string
}

type recon struct {
	cfg  *Config
	log  *zap.Logger
	next consumer.Traces

	mu       sync.Mutex
	graph    *callGraph
	pending  map[pcommon.TraceID]*pendingTrace
	done     map[pcommon.TraceID]time.Time // flushed traces (late spans pass through)
	resNames map[string]string             // short service -> resource service.name seen
	stats    struct {
		traces, spansIn, placeholders, tracesPatched, lateSpans, synthIDs, realIDs int
		roots, bufferPeak                                                        int
	}
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newRecon(set processor.Settings, cfg *Config, next consumer.Traces) (*recon, error) {
	g, _ := loadCallGraph("") // the file is read at Start (the config is validated without the mounted file)
	return &recon{cfg: cfg, log: set.Logger, next: next, graph: g, pending: map[pcommon.TraceID]*pendingTrace{},
		done: map[pcommon.TraceID]time.Time{}, resNames: map[string]string{}}, nil
}

func (r *recon) Capabilities() consumer.Capabilities { return consumer.Capabilities{MutatesData: true} }

func (r *recon) Start(context.Context, component.Host) error {
	g, err := loadCallGraph(r.cfg.CallGraphFile)
	if err != nil {
		return fmt.Errorf("call_graph_file: %w", err)
	}
	r.mu.Lock()
	r.graph = g
	r.mu.Unlock()
	bg, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.wg.Add(1)
	go r.loop(bg)
	r.log.Info("slobudget reconstruction started", zap.Duration("hold", r.cfg.Hold), zap.String("call_graph", r.cfg.CallGraphFile))
	return nil
}

func (r *recon) Shutdown(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	r.mu.Lock()
	out := r.flushAll()
	r.mu.Unlock()
	for _, td := range out {
		_ = r.next.ConsumeTraces(ctx, td)
	}
	return nil
}

// ConsumeTraces buffers every span by trace; traces whose first span is older than hold
// are reconstructed and forwarded by the loop.  Spans of a trace already forwarded pass
// through unchanged.
func (r *recon) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	now := time.Now()
	late := ptrace.NewTraces()
	r.mu.Lock()
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		resName := ""
		if v, ok := rs.Resource().Attributes().Get("service.name"); ok {
			resName = v.AsString()
		}
		short := slobudget.NormalizeService(resName)
		if resName != "" {
			r.resNames[short] = resName
		}
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				r.stats.spansIn++
				tid := span.TraceID()
				if _, flushed := r.done[tid]; flushed {
					r.stats.lateSpans++
					appendSpan(late, rs, ss, span)
					continue
				}
				pt := r.pending[tid]
				if pt == nil {
					pt = &pendingTrace{first: now, td: ptrace.NewTraces(), byRes: map[string]int{}, route: r.cfg.DefaultRoute}
					r.pending[tid] = pt
					r.stats.traces++
					if len(r.pending) > r.stats.bufferPeak {
						r.stats.bufferPeak = len(r.pending)
					}
				}
				if v, ok := span.Attributes().Get(attrRoot); ok && v.AsString() != "" {
					pt.route = v.AsString()
				}
				idx, ok := pt.byRes[resName]
				if !ok {
					nrs := pt.td.ResourceSpans().AppendEmpty()
					rs.Resource().CopyTo(nrs.Resource())
					nss := nrs.ScopeSpans().AppendEmpty()
					ss.Scope().CopyTo(nss.Scope())
					idx = pt.td.ResourceSpans().Len() - 1
					pt.byRes[resName] = idx
				}
				span.CopyTo(pt.td.ResourceSpans().At(idx).ScopeSpans().At(0).Spans().AppendEmpty())
			}
		}
	}
	r.mu.Unlock()
	if late.SpanCount() > 0 {
		return r.next.ConsumeTraces(ctx, late)
	}
	return nil
}

func appendSpan(td ptrace.Traces, rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, span ptrace.Span) {
	nrs := td.ResourceSpans().AppendEmpty()
	rs.Resource().CopyTo(nrs.Resource())
	nss := nrs.ScopeSpans().AppendEmpty()
	ss.Scope().CopyTo(nss.Scope())
	span.CopyTo(nss.Spans().AppendEmpty())
}

func (r *recon) loop(ctx context.Context) {
	defer r.wg.Done()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	logTick := time.NewTicker(5 * time.Second)
	defer logTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			r.mu.Lock()
			var out []ptrace.Traces
			for tid, pt := range r.pending {
				if now.Sub(pt.first) >= r.cfg.Hold {
					out = append(out, r.reconstruct(tid, pt))
					delete(r.pending, tid)
					r.done[tid] = now
				}
			}
			cut := now.Add(-traceHistTTL)
			for tid, t := range r.done {
				if t.Before(cut) {
					delete(r.done, tid)
				}
			}
			r.mu.Unlock()
			for _, td := range out {
				if err := r.next.ConsumeTraces(ctx, td); err != nil {
					r.log.Warn("forwarding a reconstructed trace failed", zap.Error(err))
				}
			}
		case <-logTick.C:
			if r.cfg.StatsLog {
				r.mu.Lock()
				s := r.stats
				r.mu.Unlock()
				r.log.Info("slobudget recon", zap.Int("traces", s.traces), zap.Int("spans_in", s.spansIn), zap.Int("placeholders", s.placeholders),
					zap.Int("traces_patched", s.tracesPatched), zap.Int("root_placeholders", s.roots), zap.Int("real_ids", s.realIDs),
					zap.Int("synthetic_ids", s.synthIDs), zap.Int("late_spans", s.lateSpans), zap.Int("buffer_peak_traces", s.bufferPeak))
			}
		}
	}
}

// flushAll reconstructs every pending trace (shutdown).  Called with r.mu held.
func (r *recon) flushAll() []ptrace.Traces {
	var out []ptrace.Traces
	for tid, pt := range r.pending {
		out = append(out, r.reconstruct(tid, pt))
		delete(r.pending, tid)
	}
	return out
}

// reconstruct adds the placeholders of one trace to its buffered spans.  Called with r.mu held.
func (r *recon) reconstruct(tid pcommon.TraceID, pt *pendingTrace) ptrace.Traces {
	td := pt.td
	kept := map[pcommon.SpanID]*keptSpan{}
	var order []*keptSpan
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		svc := ""
		if v, ok := rs.Resource().Attributes().Get("service.name"); ok {
			svc = slobudget.NormalizeService(v.AsString())
		}
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				sp := spans.At(k)
				ks := &keptSpan{id: sp.SpanID(), parent: sp.ParentSpanID(), depth: -1, service: svc, name: sp.Name(),
					start: sp.StartTimestamp(), end: sp.EndTimestamp(), kind: kindServer}
				if sp.Kind() == ptrace.SpanKindClient {
					ks.kind = kindClient
				}
				ks.depth, ks.anchor = decodeBridge(sp.Attributes())
				if ks.parent.IsEmpty() {
					ks.depth = 0
				}
				kept[ks.id] = ks
				order = append(order, ks)
			}
		}
	}
	ph := r.placeholders(tid, pt.route, kept, order)
	if len(ph) == 0 {
		return td
	}
	r.stats.tracesPatched++
	r.stats.placeholders += len(ph)
	// emit: one ResourceSpans per placeholder service, scope slobudgetrecon
	byRes := map[string]ptrace.SpanSlice{}
	for _, p := range ph {
		resName := r.resNames[p.service]
		if resName == "" {
			resName = fmt.Sprintf(r.cfg.ServiceNameFormat, p.service)
		}
		slice, ok := byRes[resName]
		if !ok {
			nrs := td.ResourceSpans().AppendEmpty()
			nrs.Resource().Attributes().PutStr("service.name", resName)
			nss := nrs.ScopeSpans().AppendEmpty()
			nss.Scope().SetName(scopeName)
			slice = nss.Spans()
			byRes[resName] = slice
		}
		s := slice.AppendEmpty()
		s.SetTraceID(tid)
		s.SetSpanID(p.id)
		if !p.parent.IsEmpty() {
			s.SetParentSpanID(p.parent)
		}
		s.SetName(p.name)
		if p.kind == kindClient {
			s.SetKind(ptrace.SpanKindClient)
		} else {
			s.SetKind(ptrace.SpanKindServer)
		}
		s.SetStartTimestamp(p.start)
		s.SetEndTimestamp(p.end)
		a := s.Attributes()
		a.PutBool(r.cfg.SyntheticAttribute, true)
		a.PutStr(attrRecon, p.reason)
		a.PutInt(attrDepth, int64(p.depth))
		a.PutBool("slobudget.recon.real_id", p.realID)
		if p.depth == 0 {
			r.stats.roots++
		}
		if p.realID {
			r.stats.realIDs++
		} else {
			r.stats.synthIDs++
		}
	}
	return td
}

// position identifies an ancestor slot in the trace: two kept spans whose chains pass through
// the same slot share one placeholder.  Client slots carry the callee so that a caller's
// client spans to different callees stay apart.
type position struct {
	depth         int
	service, kind string
	callee        string
}

// placeholders builds the missing ancestors of the kept spans.  Real ids first: every kept
// span names its parent (parent id) and, for a server span, the caller's server span two
// levels up (the `_br` checkpoint anchor); slots nobody named get a deterministic
// synthetic id.  Chains from different kept spans merge on the slot.
func (r *recon) placeholders(tid pcommon.TraceID, route string, kept map[pcommon.SpanID]*keptSpan, order []*keptSpan) []*placeholder {
	ph := map[pcommon.SpanID]*placeholder{}
	var out []*placeholder
	pos := map[position]pcommon.SpanID{}
	known := map[pcommon.SpanID]bool{} // ids that came from a parent id or an anchor
	slotOf := func(depth int, svc, kind, callee string) position { return position{depth, svc, kind, callee} }
	parentSlot := func(depth int, svc, kind, name string) (position, string) {
		// the slot of a span's parent and the service of that parent
		if kind == kindServer {
			caller := r.graph.callerOf(route, svc)
			if caller == "" {
				caller = "unknown"
			}
			return slotOf(depth-1, caller, kindClient, svc), caller
		}
		return slotOf(depth-1, svc, kindServer, ""), svc
	}
	// seed the slots with every real id the kept spans know
	for _, ks := range order {
		if ks.depth >= 0 {
			callee := ""
			if ks.kind == kindClient {
				callee = calleeOf(ks.name)
			}
			pos[slotOf(ks.depth, ks.service, ks.kind, callee)] = ks.id
		}
	}
	for _, ks := range order {
		if ks.depth < 1 || ks.parent.IsEmpty() {
			continue
		}
		ps, _ := parentSlot(ks.depth, ks.service, ks.kind, ks.name)
		if _, ok := pos[ps]; !ok {
			pos[ps] = ks.parent
		}
		known[ks.parent] = true
		if ks.kind == kindServer && !ks.anchor.IsEmpty() && ks.depth >= 2 {
			caller := r.graph.callerOf(route, ks.service)
			if caller != "" {
				gs := slotOf(ks.depth-2, caller, kindServer, "")
				if _, ok := pos[gs]; !ok {
					pos[gs] = ks.anchor
				}
			}
			known[ks.anchor] = true
		}
	}
	rootSvc := r.graph.rootOf(route)
	children := map[pcommon.SpanID][]pcommon.SpanID{}
	attach := func(parent, child pcommon.SpanID) { children[parent] = append(children[parent], child) }
	for _, ks := range order {
		if ks.parent.IsEmpty() {
			continue
		}
		curID, curParent, curDepth, curSvc, curKind, curName := ks.id, ks.parent, ks.depth, ks.service, ks.kind, ks.name
		curAnchor := ks.anchor
		curStart, curEnd := ks.start, ks.end
		for !curParent.IsEmpty() {
			if _, ok := kept[curParent]; ok {
				attach(curParent, curID)
				break
			}
			if p, ok := ph[curParent]; ok {
				attach(curParent, curID)
				if curStart < p.start || p.start == 0 {
					p.start = curStart
				}
				if curEnd > p.end {
					p.end = curEnd
				}
				break
			}
			// the missing parent
			p := &placeholder{id: curParent, realID: known[curParent], start: curStart, end: curEnd, reason: "parent-missing"}
			if curDepth > 0 {
				p.depth = curDepth - 1
			} else {
				p.depth = -1
			}
			ps, psvc := parentSlot(curDepth, curSvc, curKind, curName)
			p.service = psvc
			if curKind == kindServer {
				p.kind = kindClient
				p.callee = curSvc
				if strings.Contains(curName, "[synthetic]") {
					p.name = psvc + " client to " + curSvc + " [synthetic]"
				} else {
					p.name = clientNameOf(curName) + " [synthetic]"
				}
			} else {
				p.kind = kindServer
				p.name = curSvc + " server [synthetic]"
			}
			if p.depth == 0 || (p.depth < 0 && p.kind == kindServer && p.service == rootSvc) {
				p.depth = 0
				p.kind = kindServer
				p.callee = ""
				if rootSvc != "" {
					p.service = rootSvc
				}
				p.name = route + " root [synthetic]"
				p.reason = "root-missing"
				ps = slotOf(0, p.service, kindServer, "")
			}
			if _, ok := pos[ps]; !ok && p.depth >= 0 {
				pos[ps] = p.id
			}
			// the placeholder's own parent
			var next pcommon.SpanID
			switch {
			case p.depth == 0:
			case curKind == kindServer && !curAnchor.IsEmpty() && curDepth >= 2:
				next = curAnchor // the checkpoint two levels up names the grandparent
			default:
				ns, _ := parentSlot(p.depth, p.service, p.kind, p.name)
				if id, ok := pos[ns]; ok {
					next = id
				} else {
					next = synthID(tid, ns.depth, ns.service, ns.kind, ns.callee)
					pos[ns] = next
				}
			}
			p.parent = next
			ph[p.id] = p
			out = append(out, p)
			attach(curParent, curID)
			curID, curParent, curDepth, curSvc, curKind, curName = p.id, p.parent, p.depth, p.service, p.kind, p.name
			curAnchor = pcommon.SpanID{}
			if curDepth < 0 {
				break
			}
		}
	}
	// bounds bottom-up so parents cover their children
	sort.Slice(out, func(i, j int) bool { return out[i].depth > out[j].depth })
	for _, p := range out {
		for _, c := range children[p.id] {
			var cs, ce pcommon.Timestamp
			if k, ok := kept[c]; ok {
				cs, ce = k.start, k.end
			} else if q, ok := ph[c]; ok {
				cs, ce = q.start, q.end
			} else {
				continue
			}
			if p.start == 0 || cs < p.start {
				p.start = cs
			}
			if ce > p.end {
				p.end = ce
			}
		}
	}
	return out
}

// decodeBridge reads the path bridge's depth and checkpoint anchor from `_br` / `_d`.
func decodeBridge(attrs pcommon.Map) (depth int, anchor pcommon.SpanID) {
	depth = -1
	if v, ok := attrs.Get(attrBR); ok {
		raw := bytesOf(v)
		d, n := binary.Uvarint(raw)
		if n > 0 {
			depth = int(d)
			if len(raw) >= n+8 {
				copy(anchor[:], raw[n:n+8])
			}
		}
		return
	}
	if v, ok := attrs.Get(attrD); ok {
		raw := bytesOf(v)
		if d, n := binary.Uvarint(raw); n > 0 {
			depth = int(d)
		}
	}
	return
}

func bytesOf(v pcommon.Value) []byte {
	switch v.Type() {
	case pcommon.ValueTypeBytes:
		return v.Bytes().AsRaw()
	case pcommon.ValueTypeStr:
		return []byte(v.Str())
	}
	return nil
}

// clientNameOf turns "<Svc>Server_<Method>" into "<Svc>Client_<Method>".
func clientNameOf(serverName string) string {
	if i := strings.Index(serverName, "Server_"); i >= 0 {
		return serverName[:i] + "Client_" + serverName[i+len("Server_"):]
	}
	return serverName + " client [synthetic]"
}

// calleeOf extracts the callee interface from "<Svc>Client_<Method>" (merging key only).
func calleeOf(clientName string) string {
	if i := strings.Index(clientName, "Client_"); i >= 0 {
		return strings.ToLower(strings.TrimSuffix(clientName[:i], "Service"))
	}
	return clientName
}

// synthID is a deterministic id for an ancestor slot nobody kept: every kept span whose
// chain passes through the slot computes the same id, and the trace id keeps the ids of
// different traces apart (the cluster runs of 2026-11-20 hashed the slot alone, so their
// synthetic ids repeat across traces; Jaeger scopes span ids by trace, so nothing broke).
func synthID(tid pcommon.TraceID, depth int, service, kind, callee string) pcommon.SpanID {
	h := fnv.New64a()
	_, _ = h.Write(tid[:])
	_, _ = h.Write([]byte(fmt.Sprintf("%d|%s|%s|%s", depth, service, kind, callee)))
	var id pcommon.SpanID
	binary.BigEndian.PutUint64(id[:], h.Sum64()|1)
	return id
}
