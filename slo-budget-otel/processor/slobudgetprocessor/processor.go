package slobudgetprocessor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
)

// Span attributes the agent reads (baggage copies made by the SDK) and writes.
const (
	AttrBudget     = "slo.b" // issued request budget (int)
	AttrRoot       = "slo.r" // root service / route
	AttrClass      = "slo.c" // request class (route|bucket)
	AttrRootHost   = "slo.h" // host of the root's agent
	AttrSamplingP  = "sampling.p"
	AttrReason     = "slobudget.reason"
	AttrOwnUS      = "slobudget.own_us"
	AttrChildren   = "slobudget.children_seen"
	AttrDropped    = "slobudget.dropped_children"
	AttrRootPinned = "slobudget.root_pinned"
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
	stats    struct{ notices, rootsHeld, rootsPinned, clientSpans, refusedPool, refusedBudget int }
	period   int64
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	httpc    *http.Client
	lastStat slobudget.AgentStats
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
		pending: map[pcommon.TraceID]*pendingRoot{}, httpc: &http.Client{Timeout: 2 * time.Second}}
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
	n, err := newNoticer(a.log, a.cfg.NoticeListen, a.cfg.NoticePeers, a.cfg.NoticePort, a.onNotice)
	if err != nil {
		return err
	}
	a.notices = n
	bg, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	n.start(bg)
	a.period = a.periodOf(time.Now())
	a.wg.Add(2)
	go a.periodLoop(bg)
	go a.flushLoop(bg)
	if a.cfg.Central != "" {
		go a.pullConfig(bg, true)
	}
	a.log.Info("slobudget agent started", zap.String("host", a.host), zap.Duration("period", a.cfg.Period),
		zap.String("central", a.cfg.Central), zap.Float64("n_need", a.s.NNeed), zap.Float64("B", a.s.B))
	return nil
}

func (a *agent) Shutdown(context.Context) error {
	if a.cancel != nil {
		a.cancel()
	}
	if a.notices != nil {
		a.notices.stop()
	}
	a.wg.Wait()
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
		Route: route, ReqClass: cls}
	if sp.Duration < 0 {
		sp.Duration = 0
	}
	req := a.request(span.TraceID(), route, cls, now)
	if v, ok := attrs.Get(AttrBudget); ok { // issued by the SDK: overrides the agent's own issue once
		if b, err := strconv.Atoi(v.AsString()); err == nil && req.st.Budget0 != b && req.children[pcommon.SpanID{}] == nil {
			req.st.Budget, req.st.Budget0 = b, b
			req.children[pcommon.SpanID{}] = &childStats{} // marker: budget taken from baggage
		}
	}
	parent := span.ParentSpanID()
	isClient := span.Kind() == ptrace.SpanKindClient
	if isClient {
		a.stats.clientSpans++
		req.clients[parent] = append(req.clients[parent], interval{start, end})
		switch a.cfg.ClientSpans {
		case "drop":
			a.noteChild(req, parent, false)
			return false
		case "plan":
			// an ordinary span of the caller: plan coin only, never an outlier
			d := a.planOnly(&sp, req)
			a.countRefusal(d)
			a.noteChild(req, parent, d.Keep)
			if d.Keep {
				attrs.PutDouble(AttrSamplingP, d.P)
				attrs.PutStr(AttrReason, d.Reason)
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
	d := a.s.OnEnd(&sp, &req.st)
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
	d := a.s.OnEnd(&p.sp, &p.req.st)
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
	go func(td ptrace.Traces) {
		if err := a.next.ConsumeTraces(context.Background(), td); err != nil {
			a.log.Warn("forwarding a root failed", zap.Error(err))
		}
	}(p.td)
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
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			a.mu.Lock()
			for tid, p := range a.pending {
				if !now.Before(p.deadline) {
					a.releaseRoot(tid, p, a.notices.isFlagged(tid))
				}
			}
			cut := now.Add(-a.cfg.RequestTTL)
			for tid, r := range a.reqs {
				if r.last.Before(cut) {
					if _, held := a.pending[tid]; !held {
						delete(a.reqs, tid)
					}
				}
			}
			a.mu.Unlock()
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
		st.Version = a.version
		a.stats = struct{ notices, rootsHeld, rootsPinned, clientSpans, refusedPool, refusedBudget int }{}
		a.s.ResetPeriod()
		a.lastStat = st
		a.period++
		a.mu.Unlock()
		if a.cfg.StatsLog {
			sent, recv := a.notices.counts()
			a.log.Info("slobudget period", zap.Int64("period", st.Period), zap.Int("requests", st.Requests),
				zap.Int("spans", st.SpansHost), zap.Int("kept", st.KeptTotal), zap.Int("kept_outlier", st.KeptOutlier),
				zap.Int("roots_held", st.RootsHeld), zap.Int("roots_pinned", st.RootsPinned), zap.Int("notices_sent", sent),
				zap.Int("notices_recv", recv), zap.Int("refused_pool", st.RefusedPool), zap.Int("refused_budget", st.RefusedBudget),
				zap.Int("version", st.Version), zap.Float64("B", a.s.B), zap.Float64("n_need", a.s.NNeed))
		}
		if a.cfg.Central != "" {
			a.postStats(ctx, st)
			a.pullConfig(ctx, false)
		}
	}
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
