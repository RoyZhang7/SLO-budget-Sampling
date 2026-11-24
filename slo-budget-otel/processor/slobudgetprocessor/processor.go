package slobudgetprocessor

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/processor/internal/arrivalstamp"
	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
)

// Span attributes the agent reads (baggage copies made by the SDK) and writes.
const (
	AttrBudget     = "slo.budget"    // the request's budget, issued at the root by the SDK (int)
	AttrRoot       = "slo.root"      // the root operation = the route (compose_post)
	AttrClass      = "slo.class"     // request class: route|payload bucket
	AttrRootHost   = "slo.root_host" // the node of the root's agent
	AttrSamplingP  = "sampling.p"
	AttrReason     = "slobudget.reason"
	AttrOwnUS      = "slobudget.own_us"
	AttrChildren   = "slobudget.children_seen"
	AttrDropped    = "slobudget.dropped_children"
	AttrRootPinned = "slobudget.root_pinned"
	AttrBR         = "_br" // path bridge: uvarint(depth) || 8-byte checkpoint anchor || bloom
	// the arrival of a server span's request at the pod, unix ns in the span's clock, and where
	// it comes from: attached here (arrival_table) or already there (the stamp in the tee)
	AttrArrival       = arrivalstamp.Attr
	AttrArrivalSource = arrivalstamp.AttrSource
)

type interval struct{ start, end float64 }

// request is the per-trace state of this agent (the baggage state plus bookkeeping).
type request struct {
	st       slobudget.RequestState
	last     time.Time
	children map[pcommon.SpanID]*childStats // parent span id -> what we saw of its children
	clients  map[pcommon.SpanID][]interval  // parent span id -> its client spans (own time)
}

type childStats struct{ seen, dropped int }

// pendingServer is a non-root server span waiting for its client children (server_hold).
type pendingServer struct {
	td       ptrace.Traces // one resource / scope / span
	deadline time.Time
	heldAt   time.Time
	sp       slobudget.Span
	req      *request
	sid      pcommon.SpanID
	parent   pcommon.SpanID
	bytes    int
}

type pendingRoot struct {
	td       ptrace.Traces // one resource / scope / span
	deadline time.Time
	sp       slobudget.Span
	req      *request
	sid      pcommon.SpanID
}

type agent struct {
	cfg  *Config
	log  *zap.Logger
	next consumer.Traces
	host string

	mu       sync.Mutex
	s        *slobudget.Sampler
	reqs     map[pcommon.TraceID]*request
	pending  map[pcommon.TraceID]*pendingRoot
	version  int
	seedTab  map[string]map[string]float64
	notices  *noticer
	stats    agentCounters
	sdkSrv   *http.Server
	period   int64
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	httpc    *http.Client
	lastStat slobudget.AgentStats

	// keep_parent ablation (sampler.keep_parent): span ids forced here (locally or by a
	// peer's force-notice), span ids this agent broadcast, and recently decided span ids
	// (to tell a late force from one that will be honoured)
	forcedAt  map[uint64]time.Time
	sentForce map[uint64]time.Time
	decided   map[uint64]decidedSpan

	// server_hold (Stage 2d): held server spans, recently released ones (to count client
	// children that arrive too late), and the batch of released spans the flush loop forwards
	pendingSrv map[pcommon.SpanID]*pendingServer
	released   map[pcommon.SpanID]time.Time
	out        ptrace.Traces
	heldBytes  int

	arr *arrivalstamp.Stamper // arrival stamps (arrival_table; nil: off)
}

type decidedSpan struct {
	at   time.Time
	kept bool
}

// agentCounters are the per-period counters reported to the central.
type agentCounters struct {
	notices, rootsHeld, rootsPinned, clientSpans, refusedPool, refusedBudget int
	sdkPolls, budgetProp, requestsNew                                        int
	// keep_parent: kept because forced, forced but refused (budget / pool), forces that
	// arrived after the span was decided (dropped / kept anyway), forces never used
	forcedKept, forcedRefused, forceLate, forceLateKept, forcedExpired int
	// server_hold: spans held, client spans that arrived after their parent was released
	// (own time overestimated), summed hold time, peak spans and bytes held at once
	srvHeld, srvLateClients, srvHeldPeak, srvBytesPeak int
	srvHoldNs                                         int64
}

func newAgent(set processor.Settings, cfg *Config, next consumer.Traces) (*agent, error) {
	host := cfg.Host
	if host == "" {
		host = os.Getenv("NODE_NAME")
	}
	if host == "" {
		host, _ = os.Hostname()
	}
	a := &agent{cfg: cfg, log: set.Logger, next: next, host: host, reqs: map[pcommon.TraceID]*request{},
		pending: map[pcommon.TraceID]*pendingRoot{}, pendingSrv: map[pcommon.SpanID]*pendingServer{},
		released: map[pcommon.SpanID]time.Time{}, out: ptrace.NewTraces(), httpc: &http.Client{Timeout: 2 * time.Second},
		forcedAt: map[uint64]time.Time{}, sentForce: map[uint64]time.Time{}, decided: map[uint64]decidedSpan{}}
	graph := &slobudget.CallGraph{Root: map[string]string{}, Children: map[string]map[string][]string{}, Parallel: map[string]map[string]bool{}}
	vStar := cfg.StandaloneVStar
	if vStar <= 0 {
		vStar = 1000 // replaced by the first central update
	}
	seed := uint64(time.Now().UnixNano())
	s, err := slobudget.NewSampler(cfg.Sampler, graph, map[string]float64{}, []string{host}, 12, vStar, seed, nil)
	if err != nil {
		return nil, err
	}
	a.s = s
	if cfg.ArrivalTable != "" { // opened in Start
		a.arr = arrivalstamp.New(cfg.ArrivalTable, set.Logger)
	}
	if cfg.TTableFile != "" {
		raw, err := os.ReadFile(cfg.TTableFile)
		if err != nil {
			return nil, fmt.Errorf("ttable_file: %w", err)
		}
		if err := json.Unmarshal(raw, &a.seedTab); err != nil {
			return nil, fmt.Errorf("ttable_file: %w", err)
		}
		s.Cal.SeedPrior(a.seedTab)
	}
	return a, nil
}

func (a *agent) Capabilities() consumer.Capabilities { return consumer.Capabilities{MutatesData: true} }

func (a *agent) Start(ctx context.Context, _ component.Host) error {
	n, err := newNoticer(a.log, a.cfg.NoticeListen, a.cfg.NoticePeers, a.cfg.NoticePort, a.onNotice, a.onForce)
	if err != nil {
		return err
	}
	a.notices = n
	n.self = a.host
	bg, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	n.start(bg)
	if a.cfg.SDKListen != "" { // the SDK's budget issuer polls B and this host here
		ln, err := net.Listen("tcp", a.cfg.SDKListen)
		if err != nil {
			return fmt.Errorf("sdk_listen: %w", err)
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/slobudget/sdk", a.handleSDK)
		a.sdkSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
		go func() { _ = a.sdkSrv.Serve(ln) }()
	}
	if a.arr != nil {
		a.arr.Open()
	}
	a.period = a.periodOf(time.Now())
	a.wg.Add(2)
	go a.periodLoop(bg)
	go a.flushLoop(bg)
	if a.cfg.Central != "" {
		go a.pullConfig(bg, true)
	}
	a.log.Info("slobudget agent started", zap.String("host", a.host), zap.Duration("period", a.cfg.Period),
		zap.String("central", a.cfg.Central), zap.Float64("n_need", a.s.NNeed), zap.Float64("B", a.s.B),
		zap.String("own_measure", a.cfg.Sampler.OwnMeasure))
	return nil
}

func (a *agent) Shutdown(context.Context) error {
	if a.cancel != nil {
		a.cancel()
	}
	if a.notices != nil {
		a.notices.stop()
	}
	if a.sdkSrv != nil {
		_ = a.sdkSrv.Close()
	}
	a.wg.Wait()
	if a.arr != nil {
		a.arr.Close()
	}
	return nil
}

func (a *agent) periodOf(t time.Time) int64 { return t.UnixNano() / a.cfg.Period.Nanoseconds() }

// ConsumeTraces decides every span of the batch, removes the dropped ones and
// forwards the rest; roots are taken out and held.
func (a *agent) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	now := time.Now()
	a.mu.Lock()
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		svc := "unknown"
		if v, ok := rs.Resource().Attributes().Get("service.name"); ok {
			svc = slobudget.NormalizeService(v.AsString())
		}
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			spans.RemoveIf(func(span ptrace.Span) bool {
				return !a.decide(rs, sss.At(j), span, svc, now)
			})
		}
		sss.RemoveIf(func(ss ptrace.ScopeSpans) bool { return ss.Spans().Len() == 0 })
	}
	rss.RemoveIf(func(rs ptrace.ResourceSpans) bool { return rs.ScopeSpans().Len() == 0 })
	a.mu.Unlock()
	if td.SpanCount() == 0 {
		return nil
	}
	return a.next.ConsumeTraces(ctx, td)
}

func (a *agent) request(tid pcommon.TraceID, route, cls string, now time.Time) *request {
	r := a.reqs[tid]
	if r == nil {
		r = &request{children: map[pcommon.SpanID]*childStats{}, clients: map[pcommon.SpanID][]interval{}}
		r.st = slobudget.RequestState{Route: route, Class: cls}
		a.s.IssueBudget(&r.st)
		a.stats.requestsNew++
		a.reqs[tid] = r
	}
	r.last = now
	return r
}

// decide returns true when the span stays in the batch (kept now); roots are
// moved to the hold queue and return false.
func (a *agent) decide(rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, span ptrace.Span, svc string, now time.Time) bool {
	attrs := span.Attributes()
	route, cls := a.cfg.DefaultRoute, a.cfg.DefaultClass
	if v, ok := attrs.Get(AttrRoot); ok {
		route = v.AsString()
	}
	if v, ok := attrs.Get(AttrClass); ok {
		cls = v.AsString()
	}
	if cls == "" {
		cls = route + "|any"
	}
	start := float64(span.StartTimestamp()) / 1e9
	end := float64(span.EndTimestamp()) / 1e9
	sp := slobudget.Span{SpanID: spanIDToU64(span.SpanID()), TraceID: traceIDToU64(span.TraceID()), IsRoot: span.ParentSpanID().IsEmpty(),
		Service: svc, Host: a.host, Start: start, Duration: end - start, StatusOK: span.Status().Code() != ptrace.StatusCodeError,
		Route: route, ReqClass: cls, ParentSpanID: spanIDToU64(span.ParentSpanID())}
	if sp.Duration < 0 {
		sp.Duration = 0
	}
	req := a.request(span.TraceID(), route, cls, now)
	if a.cfg.BudgetSource != "self" {
		// issued at the root by the SDK and carried in the trace's baggage: replaces the
		// agent's own issue once per request (the per-host budgets start from Budget0)
		if v, ok := attrs.Get(AttrBudget); ok && req.children[pcommon.SpanID{}] == nil {
			if b, err := strconv.Atoi(v.AsString()); err == nil && b > 0 {
				used := req.st.Budget0 - req.st.Budget
				req.st.Budget0 = b
				req.st.Budget = max(b-used, 0)
				req.children[pcommon.SpanID{}] = &childStats{} // marker: budget taken from baggage
				a.stats.budgetProp++
			}
		}
	}
	parent := span.ParentSpanID()
	if span.Kind() == ptrace.SpanKindServer {
		if a.arr != nil { // before any hold: the table keeps an id only until its slot is taken
			a.arr.Stamp(span)
		}
		if v, ok := attrs.Get(AttrArrival); ok { // own_measure within_service counts from it
			sp.Wait = slobudget.ArrivalWait(int64(span.StartTimestamp()), v.Int())
		}
	}
	isClient := span.Kind() == ptrace.SpanKindClient
	if isClient {
		a.stats.clientSpans++
		req.clients[parent] = append(req.clients[parent], interval{start, end})
		if _, gone := a.released[parent]; gone {
			a.stats.srvLateClients++
		}
		switch a.cfg.ClientSpans {
		case "drop":
			a.noteChild(req, parent, false)
			return false
		case "plan":
			// an ordinary span of the caller: plan coin only, never an outlier (a span
			// forced by keep_parent is kept at p = 1 before the plan coin)
			forced := a.cfg.Sampler.KeepParent && a.s.IsForced(sp.SpanID)
			d := a.planOnly(&sp, req)
			a.afterDecision(span, &sp, d, forced, now)
			a.countRefusal(d)
			a.noteChild(req, parent, d.Keep)
			if d.Keep {
				attrs.PutDouble(AttrSamplingP, d.P)
				attrs.PutStr(AttrReason, d.Reason)
				if d.Reason == "parent" { // a flag reason, as for a server span: pin the root
					a.notify(span.TraceID(), attrs)
				}
			}
			return d.Keep
		}
	}
	// own time of a server span = duration minus the union of its client spans (locally known)
	sp.Own = sp.Duration - unionLength(req.clients[span.SpanID()])
	if sp.Own < 0 {
		sp.Own = 0
	}
	if sp.IsRoot {
		a.s.CountRequest()
		a.holdRoot(rs, ss, span, sp, req, now)
		return false
	}
	if a.cfg.ServerHold > 0 {
		a.holdServer(rs, ss, span, sp, req, now)
		return false
	}
	forced := a.cfg.Sampler.KeepParent && a.s.IsForced(sp.SpanID)
	d := a.s.OnEnd(&sp, &req.st)
	a.afterDecision(span, &sp, d, forced, now)
	a.countRefusal(d)
	a.noteChild(req, parent, d.Keep)
	if !d.Keep {
		return false
	}
	a.stamp(span, &sp, d, req)
	if d.Reason != "plan" || a.cfg.Sampler.RootFlagOn == "any" {
		a.notify(span.TraceID(), attrs)
	}
	return true
}

// afterDecision is the keep_parent bookkeeping around one decision: the outcome of a
// forced span, and for a kept outlier the force of its parent (already forced in the
// sampler) and of its `_br` anchor, broadcast to the peers.  Called with a.mu held.
func (a *agent) afterDecision(span ptrace.Span, sp *slobudget.Span, d slobudget.Decision, forced bool, now time.Time) {
	if !a.cfg.Sampler.KeepParent {
		return
	}
	if forced {
		delete(a.forcedAt, sp.SpanID)
		switch {
		case d.Keep && d.Reason == "parent":
			a.stats.forcedKept++
		case !d.Keep:
			a.stats.forcedRefused++
		}
	}
	a.decided[sp.SpanID] = decidedSpan{now, d.Keep}
	if !d.Keep || d.Reason != "outlier" || sp.IsRoot {
		return
	}
	tid := [16]byte(span.TraceID())
	a.forceHere(sp.ParentSpanID, now)
	a.broadcastForce(tid, span.ParentSpanID(), sp.ParentSpanID, now)
	if a.cfg.KeepParentAnchor {
		if anchor, ok := brAnchor(span.Attributes()); ok {
			if aid := spanIDToU64(anchor); aid != 0 && aid != sp.ParentSpanID {
				a.s.Force(aid)
				a.forceHere(aid, now)
				a.broadcastForce(tid, anchor, aid, now)
			}
		}
	}
}

// forceHere records a local force; a span already decided here makes it late.
func (a *agent) forceHere(id uint64, now time.Time) {
	if d, ok := a.decided[id]; ok {
		a.s.Unforce(id)
		if d.kept {
			a.stats.forceLateKept++
		} else {
			a.stats.forceLate++
		}
		return
	}
	a.forcedAt[id] = now
}

func (a *agent) broadcastForce(tid [16]byte, sid pcommon.SpanID, id uint64, now time.Time) {
	a.sentForce[id] = now
	a.notices.sendForce(tid, [8]byte(sid))
}

// onForce honours a peer's force-notice for a span this agent may see.
func (a *agent) onForce(_ [16]byte, sid [8]byte) {
	id := spanIDToU64(pcommon.SpanID(sid))
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.cfg.Sampler.KeepParent {
		return
	}
	if _, mine := a.sentForce[id]; mine { // our own broadcast coming back
		return
	}
	if _, ok := a.forcedAt[id]; ok {
		return
	}
	if d, ok := a.decided[id]; ok {
		if d.kept {
			a.stats.forceLateKept++
		} else {
			a.stats.forceLate++
		}
		return
	}
	a.s.Force(id)
	a.forcedAt[id] = now
}

// brAnchor decodes the checkpoint anchor (the caller's server span) from `_br`.
func brAnchor(attrs pcommon.Map) (pcommon.SpanID, bool) {
	v, ok := attrs.Get(AttrBR)
	if !ok {
		return pcommon.SpanID{}, false
	}
	var raw []byte
	switch v.Type() {
	case pcommon.ValueTypeBytes:
		raw = v.Bytes().AsRaw()
	case pcommon.ValueTypeStr:
		raw = []byte(v.Str())
	default:
		return pcommon.SpanID{}, false
	}
	_, n := binary.Uvarint(raw)
	if n <= 0 || len(raw) < n+8 {
		return pcommon.SpanID{}, false
	}
	var id pcommon.SpanID
	copy(id[:], raw[n:n+8])
	return id, !id.IsEmpty()
}

// countRefusal records pool / budget refusals: the volume the arm wanted but could not keep.
func (a *agent) countRefusal(d slobudget.Decision) {
	if d.Keep {
		return
	}
	switch d.Reason {
	case "pool":
		a.stats.refusedPool++
	case "budget":
		a.stats.refusedBudget++
	}
}

func (a *agent) planOnly(sp *slobudget.Span, req *request) slobudget.Decision {
	// the plan branch of decideSpan: the caller's plan rate, budget and pool
	cs := *sp
	cs.StatusOK = true
	saved := a.s.Cal.Table
	a.s.Cal.Table = nil // no T -> cold-start branch -> plan coin
	d := a.s.OnEnd(&cs, &req.st)
	a.s.Cal.Table = saved
	return d
}

func (a *agent) noteChild(req *request, parent pcommon.SpanID, kept bool) {
	if parent.IsEmpty() {
		return
	}
	c := req.children[parent]
	if c == nil {
		c = &childStats{}
		req.children[parent] = c
	}
	c.seen++
	if !kept {
		c.dropped++
	}
}

func (a *agent) stamp(span ptrace.Span, sp *slobudget.Span, d slobudget.Decision, req *request) {
	attrs := span.Attributes()
	attrs.PutDouble(AttrSamplingP, d.P)
	attrs.PutStr(AttrReason, d.Reason)
	attrs.PutInt(AttrOwnUS, int64(sp.Own*1e6))
	if c := req.children[span.SpanID()]; c != nil {
		attrs.PutInt(AttrChildren, int64(c.seen))
		attrs.PutInt(AttrDropped, int64(c.dropped))
	}
}

// notify sends the kept-notice for a non-root keep and flags the trace locally.
func (a *agent) notify(tid pcommon.TraceID, attrs pcommon.Map) {
	a.notices.flag(tid)
	rootHost := ""
	if v, ok := attrs.Get(AttrRootHost); ok {
		rootHost = v.AsString()
	}
	a.stats.notices++
	a.notices.send(tid, rootHost)
	if p := a.pending[tid]; p != nil { // the root is already waiting here
		a.releaseRoot(tid, p, true)
	}
}

// holdRoot takes the root out of the batch and keeps it for root_hold unless a
// notice already arrived.
func (a *agent) holdRoot(rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, span ptrace.Span, sp slobudget.Span, req *request, now time.Time) {
	td := ptrace.NewTraces()
	nrs := td.ResourceSpans().AppendEmpty()
	rs.Resource().CopyTo(nrs.Resource())
	nss := nrs.ScopeSpans().AppendEmpty()
	ss.Scope().CopyTo(nss.Scope())
	span.CopyTo(nss.Spans().AppendEmpty())
	p := &pendingRoot{td: td, deadline: now.Add(a.cfg.RootHold), sp: sp, req: req, sid: span.SpanID()}
	if a.notices.isFlagged(span.TraceID()) || a.cfg.Sampler.RootPolicy == "pin" {
		a.releaseRoot(span.TraceID(), p, true)
		return
	}
	a.stats.rootsHeld++
	if a.cfg.RootHold == 0 {
		a.releaseRoot(span.TraceID(), p, false)
		return
	}
	a.pending[span.TraceID()] = p
}

// releaseRoot decides a held root: pinned (p = 1, outside the budget) when a
// notice arrived, otherwise an ordinary span.  Called with a.mu held.
func (a *agent) releaseRoot(tid pcommon.TraceID, p *pendingRoot, flagged bool) {
	delete(a.pending, tid)
	p.req.st.Flag = flagged
	// the root's client span usually arrives during the hold: recompute its own time now
	if own := p.sp.Duration - unionLength(p.req.clients[p.sid]); own >= 0 {
		p.sp.Own = own
	}
	forced := a.cfg.Sampler.KeepParent && a.s.IsForced(p.sp.SpanID)
	d := a.s.OnEnd(&p.sp, &p.req.st)
	a.afterDecision(p.td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0), &p.sp, d, forced, time.Now())
	a.countRefusal(d)
	if !d.Keep {
		return
	}
	if flagged {
		a.stats.rootsPinned++
	}
	span := p.td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	a.stamp(span, &p.sp, d, p.req)
	span.Attributes().PutBool(AttrRootPinned, flagged)
	p.td.ResourceSpans().MoveAndAppendTo(a.out.ResourceSpans())
}

// holdServer takes a non-root server span out of the batch for server_hold.  Called with a.mu held.
func (a *agent) holdServer(rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, span ptrace.Span, sp slobudget.Span, req *request, now time.Time) {
	td := ptrace.NewTraces()
	nrs := td.ResourceSpans().AppendEmpty()
	rs.Resource().CopyTo(nrs.Resource())
	nss := nrs.ScopeSpans().AppendEmpty()
	ss.Scope().CopyTo(nss.Scope())
	span.CopyTo(nss.Spans().AppendEmpty())
	var sizer ptrace.ProtoMarshaler
	p := &pendingServer{td: td, deadline: now.Add(a.cfg.ServerHold), heldAt: now, sp: sp, req: req,
		sid: span.SpanID(), parent: span.ParentSpanID(), bytes: sizer.TracesSize(td)}
	a.pendingSrv[p.sid] = p
	a.heldBytes += p.bytes
	a.stats.srvHeld++
	if n := len(a.pendingSrv); n > a.stats.srvHeldPeak {
		a.stats.srvHeldPeak = n
	}
	if a.heldBytes > a.stats.srvBytesPeak {
		a.stats.srvBytesPeak = a.heldBytes
	}
}

// releaseServer decides a held server span with the client children seen so far and adds a
// kept one to the outgoing batch.  Called with a.mu held.
func (a *agent) releaseServer(p *pendingServer, now time.Time) {
	delete(a.pendingSrv, p.sid)
	a.heldBytes -= p.bytes
	a.released[p.sid] = now
	a.stats.srvHoldNs += now.Sub(p.heldAt).Nanoseconds()
	if own := p.sp.Duration - unionLength(p.req.clients[p.sid]); own >= 0 {
		p.sp.Own = own
	} else {
		p.sp.Own = 0
	}
	span := p.td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	forced := a.cfg.Sampler.KeepParent && a.s.IsForced(p.sp.SpanID)
	d := a.s.OnEnd(&p.sp, &p.req.st)
	a.afterDecision(span, &p.sp, d, forced, now)
	a.countRefusal(d)
	a.noteChild(p.req, p.parent, d.Keep)
	if !d.Keep {
		return
	}
	a.stamp(span, &p.sp, d, p.req)
	if d.Reason != "plan" || a.cfg.Sampler.RootFlagOn == "any" {
		a.notify(span.TraceID(), span.Attributes())
	}
	p.td.ResourceSpans().MoveAndAppendTo(a.out.ResourceSpans())
}

// drainOut hands the batch of released spans to the next consumer (call without a.mu).
func (a *agent) drainOut(out ptrace.Traces) {
	if out.SpanCount() == 0 {
		return
	}
	if err := a.next.ConsumeTraces(context.Background(), out); err != nil {
		a.log.Warn("forwarding released spans failed", zap.Error(err))
	}
}

func (a *agent) onNotice(tid [16]byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if p := a.pending[pcommon.TraceID(tid)]; p != nil {
		a.releaseRoot(pcommon.TraceID(tid), p, true)
	}
}

// flushLoop releases roots whose hold expired and evicts stale request state.
func (a *agent) flushLoop(ctx context.Context) {
	defer a.wg.Done()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	n := 0
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			a.mu.Lock()
			for _, p := range a.pendingSrv {
				if !now.Before(p.deadline) {
					a.releaseServer(p, now)
				}
			}
			n++
			if a.arr != nil && n%40 == 0 { // every second
				a.arr.Tick()
			}
			if n%4 != 0 {
				out := a.out
				a.out = ptrace.NewTraces()
				a.mu.Unlock()
				a.drainOut(out)
				continue
			}
			for tid, p := range a.pending {
				if !now.Before(p.deadline) {
					a.releaseRoot(tid, p, a.notices.isFlagged(tid))
				}
			}
			cut := now.Add(-a.cfg.RequestTTL)
			for tid, r := range a.reqs {
				if r.last.Before(cut) {
					if _, held := a.pending[tid]; !held {
						delete(a.reqs, tid) // held server spans keep their own *request
					}
				}
			}
			for id, t := range a.forcedAt { // keep_parent: forces whose span never came here
				if t.Before(cut) {
					if a.s.IsForced(id) {
						a.s.Unforce(id)
						a.stats.forcedExpired++
					}
					delete(a.forcedAt, id)
				}
			}
			for id, t := range a.sentForce {
				if t.Before(cut) {
					delete(a.sentForce, id)
				}
			}
			for id, d := range a.decided {
				if d.at.Before(cut) {
					delete(a.decided, id)
				}
			}
			for id, t := range a.released {
				if t.Before(cut) {
					delete(a.released, id)
				}
			}
			out := a.out
			a.out = ptrace.NewTraces()
			a.mu.Unlock()
			a.drainOut(out)
			a.notices.expire(a.cfg.RequestTTL + a.cfg.RootHold)
		}
	}
}

// periodLoop closes periods on wall-clock boundaries: report, reset, pull config.
func (a *agent) periodLoop(ctx context.Context) {
	defer a.wg.Done()
	for {
		next := time.Unix(0, (a.period+1)*a.cfg.Period.Nanoseconds())
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(next)):
		}
		a.mu.Lock()
		st := a.s.Snapshot(a.host, a.period)
		st.Notices, st.RootsHeld, st.RootsPinned, st.ClientSpans = a.stats.notices, a.stats.rootsHeld, a.stats.rootsPinned, a.stats.clientSpans
		st.RefusedPool, st.RefusedBudget = a.stats.refusedPool, a.stats.refusedBudget
		st.SDKPolls, st.BudgetPropagated, st.RequestsSeen = a.stats.sdkPolls, a.stats.budgetProp, a.stats.requestsNew
		st.NoticeAddr = a.notices.selfAddr()
		st.Version = a.version
		kp := a.stats // keep_parent counters for the log line
		a.stats = agentCounters{}
		var arrived []zap.Field
		if a.arr != nil {
			arrived = a.arr.Take()
		}
		a.s.ResetPeriod()
		a.lastStat = st
		a.period++
		a.mu.Unlock()
		if a.arr != nil {
			a.arr.Open()
		}
		if a.cfg.StatsLog {
			sent, recv, uni := a.notices.counts()
			a.log.Info("slobudget period", zap.Int64("period", st.Period), zap.Int("requests", st.Requests),
				zap.Int("spans", st.SpansHost), zap.Int("kept", st.KeptTotal), zap.Int("kept_outlier", st.KeptOutlier),
				zap.Int("roots_held", st.RootsHeld), zap.Int("roots_pinned", st.RootsPinned), zap.Int("notices_sent", sent),
				zap.Int("notices_recv", recv), zap.Int("notices_unicast", uni), zap.Int("refused_pool", st.RefusedPool), zap.Int("refused_budget", st.RefusedBudget),
				zap.Int("requests_new", st.RequestsSeen), zap.Int("budget_propagated", st.BudgetPropagated), zap.Int("sdk_polls", st.SDKPolls),
				zap.Int("version", st.Version), zap.Float64("B", a.s.B), zap.Float64("n_need", a.s.NNeed),
				zap.Int("server_held", kp.srvHeld), zap.Int("server_late_clients", kp.srvLateClients),
				zap.Float64("server_hold_ms_mean", holdMs(kp)), zap.Int("server_held_peak", kp.srvHeldPeak),
				zap.Int("server_held_bytes_peak", kp.srvBytesPeak))
			if a.cfg.Sampler.KeepParent {
				a.notices.mu.Lock()
				fs, fr := a.notices.forceSent, a.notices.forceRecv
				a.notices.mu.Unlock()
				a.log.Info("slobudget keep_parent", zap.Int64("period", st.Period), zap.Int("forced_kept", kp.forcedKept),
					zap.Int("forced_refused", kp.forcedRefused), zap.Int("force_late", kp.forceLate),
					zap.Int("force_late_kept", kp.forceLateKept), zap.Int("forced_expired", kp.forcedExpired),
					zap.Int("force_notices_sent", fs), zap.Int("force_notices_recv", fr))
			}
			if arrived != nil {
				a.log.Info("slobudget arrival", append([]zap.Field{zap.Int64("period", st.Period)}, arrived...)...)
			}
		}
		if a.cfg.Central != "" {
			a.postStats(ctx, st)
			a.pullConfig(ctx, false)
		}
	}
}

// handleSDK answers the SDK's budget issuer: the B in force, this agent's host (the root
// host the SDK stamps) and the config version.
func (a *agent) handleSDK(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	out := struct {
		Version int     `json:"version"`
		B       float64 `json:"b"`
		Host    string  `json:"host"`
		Period  float64 `json:"period_s"`
	}{a.version, a.s.B, a.host, a.cfg.Period.Seconds()}
	a.stats.sdkPolls++
	a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (a *agent) postStats(ctx context.Context, st slobudget.AgentStats) {
	body, _ := json.Marshal(st)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.Central+"/slobudget/stats", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.httpc.Do(req)
	if err != nil {
		a.log.Warn("stats post failed", zap.Error(err))
		return
	}
	resp.Body.Close()
}

// pullConfig fetches the central update, polling until a newer version arrives
// (the central closes the period `grace` after the boundary) or the tries run out.
func (a *agent) pullConfig(ctx context.Context, initial bool) {
	tries := 8
	if initial {
		tries = 30
	}
	for i := 0; i < tries; i++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.cfg.Central+"/slobudget/config", nil)
		if err != nil {
			return
		}
		resp, err := a.httpc.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			var u slobudget.CentralUpdate
			err = json.NewDecoder(resp.Body).Decode(&u)
			resp.Body.Close()
			if err == nil {
				a.mu.Lock()
				newer := u.Version > a.version
				if newer {
					a.s.ApplyCentral(&u, a.host)
					a.notices.setPeers(u.Peers)
					a.version = u.Version
				}
				a.mu.Unlock()
				if newer {
					return // keep polling until the period's new version is in
				}
			}
		} else if resp != nil {
			resp.Body.Close()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(300 * time.Millisecond):
		}
	}
}

func holdMs(c agentCounters) float64 {
	if c.srvHeld == 0 {
		return 0
	}
	return float64(c.srvHoldNs) / float64(c.srvHeld) / 1e6
}

func unionLength(iv []interval) float64 {
	if len(iv) == 0 {
		return 0
	}
	sort.Slice(iv, func(i, j int) bool { return iv[i].start < iv[j].start })
	total := 0.0
	cs, ce := iv[0].start, iv[0].end
	for _, x := range iv[1:] {
		if x.start > ce {
			total += ce - cs
			cs, ce = x.start, x.end
		} else if x.end > ce {
			ce = x.end
		}
	}
	return total + (ce - cs)
}

func spanIDToU64(id pcommon.SpanID) uint64 {
	var v uint64
	for _, b := range id {
		v = v<<8 | uint64(b)
	}
	return v
}

func traceIDToU64(id pcommon.TraceID) uint64 {
	var v uint64
	for _, b := range id[8:] {
		v = v<<8 | uint64(b)
	}
	return v
}
