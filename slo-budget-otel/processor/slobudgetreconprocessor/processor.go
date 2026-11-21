//go:build slobudget_recon

package slobudgetreconprocessor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget/recon"
)

// Attributes read and written.
const (
	attrBR       = "_br"
	attrD        = "_d"
	attrRoot     = "slo.root" // the route (from the SDK's baggage)
	attrRecon    = "slobudget.recon"
	attrDepth    = "slobudget.depth"
	attrRealID   = "slobudget.recon.real_id"
	scopeName    = "slobudgetrecon"
	traceHistTTL = 20 * time.Second
)

type pendingTrace struct {
	first time.Time
	td    ptrace.Traces
	byRes map[string]int // service.name -> ResourceSpans index
	route string
}

type reconProc struct {
	cfg  *Config
	log  *zap.Logger
	next consumer.Traces

	mu       sync.Mutex
	graph    *recon.CallGraph
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

func newRecon(set processor.Settings, cfg *Config, next consumer.Traces) (*reconProc, error) {
	g, _ := recon.ParseCallGraph(nil) // the file is read at Start (the config is validated without the mounted file)
	return &reconProc{cfg: cfg, log: set.Logger, next: next, graph: g, pending: map[pcommon.TraceID]*pendingTrace{},
		done: map[pcommon.TraceID]time.Time{}, resNames: map[string]string{}}, nil
}

func (r *reconProc) Capabilities() consumer.Capabilities { return consumer.Capabilities{MutatesData: true} }

func (r *reconProc) Start(context.Context, component.Host) error {
	g, err := recon.LoadCallGraph(r.cfg.CallGraphFile)
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

func (r *reconProc) Shutdown(ctx context.Context) error {
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
func (r *reconProc) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
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
		if resName != "" {
			r.resNames[slobudget.NormalizeService(resName)] = resName
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

func (r *reconProc) loop(ctx context.Context) {
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
func (r *reconProc) flushAll() []ptrace.Traces {
	var out []ptrace.Traces
	for tid, pt := range r.pending {
		out = append(out, r.reconstruct(tid, pt))
		delete(r.pending, tid)
	}
	return out
}

// reconstruct adds the placeholders of one trace to its buffered spans.  Called with r.mu held.
func (r *reconProc) reconstruct(tid pcommon.TraceID, pt *pendingTrace) ptrace.Traces {
	td := pt.td
	var spans []recon.Span
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		svc := ""
		if v, ok := rs.Resource().Attributes().Get("service.name"); ok {
			svc = slobudget.NormalizeService(v.AsString())
		}
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j).Spans()
			for k := 0; k < ss.Len(); k++ {
				sp := ss.At(k)
				s := recon.Span{ID: recon.SpanID(sp.SpanID()), Parent: recon.SpanID(sp.ParentSpanID()), Service: svc, Operation: sp.Name(),
					Kind: recon.KindServer, Start: uint64(sp.StartTimestamp()), End: uint64(sp.EndTimestamp())}
				if sp.Kind() == ptrace.SpanKindClient {
					s.Kind = recon.KindClient
				}
				if v, ok := sp.Attributes().Get(attrBR); ok {
					s.BR = bytesOf(v)
				}
				if v, ok := sp.Attributes().Get(attrD); ok {
					s.D = bytesOf(v)
				}
				if v, ok := sp.Attributes().Get(r.cfg.SyntheticAttribute); ok && v.Type() == pcommon.ValueTypeBool && v.Bool() {
					s.Synthetic = true
				}
				spans = append(spans, s)
			}
		}
	}
	res := recon.Reconstruct(recon.TraceID(tid), pt.route, spans, r.graph, recon.Options{SynthesizeIDs: true})
	if len(res.Placeholders) == 0 {
		return td
	}
	r.stats.tracesPatched++
	r.stats.placeholders += len(res.Placeholders)
	byRes := map[string]ptrace.SpanSlice{}
	for _, p := range res.Placeholders {
		resName := r.resNames[p.Service]
		if resName == "" {
			resName = fmt.Sprintf(r.cfg.ServiceNameFormat, p.Service)
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
		s.SetSpanID(pcommon.SpanID(p.ID))
		if !p.Parent.IsZero() {
			s.SetParentSpanID(pcommon.SpanID(p.Parent))
		}
		s.SetName(p.Operation)
		if p.Kind == recon.KindClient {
			s.SetKind(ptrace.SpanKindClient)
		} else {
			s.SetKind(ptrace.SpanKindServer)
		}
		s.SetStartTimestamp(pcommon.Timestamp(p.Start))
		s.SetEndTimestamp(pcommon.Timestamp(p.End))
		a := s.Attributes()
		a.PutBool(r.cfg.SyntheticAttribute, true)
		a.PutStr(attrRecon, p.Reason)
		a.PutInt(attrDepth, int64(p.Depth))
		a.PutBool(attrRealID, p.RealID)
		if p.Depth == 0 {
			r.stats.roots++
		}
		if p.RealID {
			r.stats.realIDs++
		} else {
			r.stats.synthIDs++
		}
	}
	return td
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
