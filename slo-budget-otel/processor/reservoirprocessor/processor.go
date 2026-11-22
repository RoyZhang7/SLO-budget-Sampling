package reservoirprocessor

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
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

const (
	attrRoot   = "slo.root"
	attrReason = "slobudget.reason"
	attrWeight = "slobudget.reservoir_w"
)

// origin is a batch's resource and scope, copied once and shared by its retained spans.
type origin struct {
	res   pcommon.Resource
	scope pcommon.InstrumentationScope
	key   string
}

type entry struct {
	key  float64
	seq  uint64
	w    float64
	span ptrace.Span
	from *origin
}

// minHeap orders by (key, seq) like Python's heap of (key, idx) tuples.
type minHeap []*entry

func (h minHeap) Len() int { return len(h) }
func (h minHeap) Less(i, j int) bool {
	if h[i].key != h[j].key {
		return h[i].key < h[j].key
	}
	return h[i].seq < h[j].seq
}
func (h minHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)   { *h = append(*h, x.(*entry)) }
func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[:n-1]
	return e
}

type periodState struct {
	heap minHeap
	seen int
	peak int
}

type reservoir struct {
	cfg  *Config
	log  *zap.Logger
	next consumer.Traces
	now  func() time.Time
	host string
	K    int
	T    map[string]map[string]float64

	mu         sync.Mutex
	rng        *rand.Rand
	open       map[int64]*periodState
	nextToEmit int64
	started    bool
	seq        uint64
	history    []record

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type record struct {
	Period  int64
	Seen    int
	K       int
	Emitted int
	Peak    int
}

func newReservoir(set processor.Settings, cfg *Config, next consumer.Traces) (*reservoir, error) {
	host := cfg.Host
	if host == "" {
		host = os.Getenv("NODE_NAME")
	}
	if host == "" {
		host, _ = os.Hostname()
	}
	r := &reservoir{cfg: cfg, log: set.Logger, next: next, now: time.Now, host: host, open: map[int64]*periodState{},
		rng: rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))}
	share, ok := cfg.Shares[host]
	if !ok {
		share = 1.0
		if len(cfg.Shares) > 0 {
			share = 1.0 / float64(len(cfg.Shares))
			set.Logger.Warn("reservoir: no share for this host, using an equal share", zap.String("host", host), zap.Float64("share", share))
		}
	}
	r.K = int(math.Max(1, math.RoundToEven(cfg.VStar*share))) // Python round(): half to even
	raw, err := os.ReadFile(cfg.TTableFile)
	if err != nil {
		return nil, fmt.Errorf("ttable_file: %w", err)
	}
	if err := json.Unmarshal(raw, &r.T); err != nil {
		return nil, fmt.Errorf("ttable_file: %w", err)
	}
	return r, nil
}

func (r *reservoir) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (r *reservoir) periodOf(t time.Time) int64 { return t.UnixNano() / r.cfg.Period.Nanoseconds() }

func (r *reservoir) periodEnd(p int64) time.Time {
	return time.Unix(0, (p+1)*r.cfg.Period.Nanoseconds())
}

func (r *reservoir) Start(context.Context, component.Host) error {
	r.mu.Lock()
	r.nextToEmit = r.periodOf(r.now())
	r.started = true
	r.mu.Unlock()
	bg, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.wg.Add(1)
	go r.loop(bg)
	r.log.Info("reservoir started", zap.String("host", r.host), zap.Int("K", r.K), zap.Float64("v_star", r.cfg.VStar),
		zap.Int("table_routes", len(r.T)))
	return nil
}

func (r *reservoir) Shutdown(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	// what is still held is released as it would be at its period end
	r.mu.Lock()
	var out []ptrace.Traces
	for len(r.open) > 0 {
		if td := r.closeLocked(r.nextToEmit); td.SpanCount() > 0 {
			out = append(out, td)
		}
		r.nextToEmit++
	}
	r.mu.Unlock()
	for _, td := range out {
		_ = r.next.ConsumeTraces(ctx, td)
	}
	return nil
}

func (r *reservoir) loop(ctx context.Context) {
	defer r.wg.Done()
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.emitDue(ctx, r.now())
		}
	}
}

// emitDue releases every period whose end plus the grace has passed.
func (r *reservoir) emitDue(ctx context.Context, now time.Time) {
	r.mu.Lock()
	if !r.started {
		r.nextToEmit, r.started = r.periodOf(now), true
	}
	var out []ptrace.Traces
	for !now.Before(r.periodEnd(r.nextToEmit).Add(r.cfg.Grace)) {
		if td := r.closeLocked(r.nextToEmit); td.SpanCount() > 0 {
			out = append(out, td)
		}
		r.nextToEmit++
	}
	r.mu.Unlock()
	for _, td := range out {
		if err := r.next.ConsumeTraces(ctx, td); err != nil {
			r.log.Warn("reservoir: forwarding a period's spans failed", zap.Error(err))
		}
	}
}

// closeLocked builds the traces of period p's reservoir and logs its record.
func (r *reservoir) closeLocked(p int64) ptrace.Traces {
	st := r.open[p]
	delete(r.open, p)
	td := ptrace.NewTraces()
	rec := record{Period: p, K: r.K}
	if st != nil {
		rec.Seen, rec.Peak = st.seen, st.peak
		// stable output: by origin, then by key descending
		es := []*entry(st.heap)
		sort.SliceStable(es, func(i, j int) bool {
			if es[i].from.key != es[j].from.key {
				return es[i].from.key < es[j].from.key
			}
			return es[i].seq < es[j].seq
		})
		groups := map[string]ptrace.SpanSlice{} // one ResourceSpans per resource + scope
		for _, e := range es {
			slice, ok := groups[e.from.key]
			if !ok {
				rs := td.ResourceSpans().AppendEmpty()
				e.from.res.CopyTo(rs.Resource())
				ss := rs.ScopeSpans().AppendEmpty()
				e.from.scope.CopyTo(ss.Scope())
				slice = ss.Spans()
				groups[e.from.key] = slice
			}
			out := slice.AppendEmpty()
			e.span.CopyTo(out)
			out.Attributes().PutStr(attrReason, "reservoir")
			out.Attributes().PutDouble(attrWeight, e.w)
		}
		rec.Emitted = len(es)
	}
	if r.cfg.StatsLog {
		r.log.Info("reservoir period", zap.Int64("period", rec.Period), zap.Int("seen", rec.Seen), zap.Int("K", rec.K),
			zap.Int("emitted", rec.Emitted), zap.Int("peak_buffered", rec.Peak))
	}
	r.history = append(r.history, rec)
	if len(r.history) > 64 {
		r.history = r.history[len(r.history)-64:]
	}
	return td
}

// weight is the reservoir weight of a span (ReservoirSampler.on_end).
func (r *reservoir) weight(route, svc string, span ptrace.Span) float64 {
	T, ok := r.T[route][svc]
	if !ok || T <= 0 {
		return r.cfg.Eps
	}
	d := float64(span.EndTimestamp()-span.StartTimestamp()) / 1e9
	if d < 0 {
		d = 0
	}
	miss := (d - T) / T
	switch {
	case miss > 0:
		return miss
	case span.Status().Code() == ptrace.StatusCodeError:
		return r.cfg.StatusWeight
	default:
		return r.cfg.Eps
	}
}

func originKey(rs ptrace.ResourceSpans, ss ptrace.ScopeSpans) string {
	var b strings.Builder
	keys := make([]string, 0, rs.Resource().Attributes().Len())
	rs.Resource().Attributes().Range(func(k string, v pcommon.Value) bool {
		keys = append(keys, k+"="+v.AsString())
		return true
	})
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte(';')
	}
	b.WriteString("|" + ss.Scope().Name() + "|" + ss.Scope().Version())
	return b.String()
}

// ConsumeTraces offers every span to the period's reservoir; nothing passes through now.
func (r *reservoir) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	now := r.now()
	p := r.periodOf(now)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started && p < r.nextToEmit {
		p = r.nextToEmit // already released: the oldest open period takes it
	}
	st := r.open[p]
	if st == nil {
		st = &periodState{}
		r.open[p] = st
	}
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		svc := "unknown"
		if v, ok := rs.Resource().Attributes().Get("service.name"); ok {
			svc = slobudget.NormalizeService(v.AsString())
		}
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			var from *origin // copied lazily: only batches with a retained span pay for it
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				st.seen++
				route := r.cfg.DefaultRoute
				if v, ok := span.Attributes().Get(attrRoot); ok && v.AsString() != "" {
					route = v.AsString()
				}
				w := r.weight(route, svc, span)
				key := math.Pow(r.rng.Float64(), 1.0/w)
				r.seq++
				full := len(st.heap) >= r.K
				if full && !(key > st.heap[0].key) {
					continue
				}
				if from == nil {
					from = &origin{res: pcommon.NewResource(), scope: pcommon.NewInstrumentationScope(), key: originKey(rs, ss)}
					rs.Resource().CopyTo(from.res)
					ss.Scope().CopyTo(from.scope)
				}
				e := &entry{key: key, seq: r.seq, w: w, span: ptrace.NewSpan(), from: from}
				span.CopyTo(e.span)
				if full {
					st.heap[0] = e // heapreplace: drop the smallest, insert the new one
					heap.Fix(&st.heap, 0)
				} else {
					heap.Push(&st.heap, e)
				}
				if len(st.heap) > st.peak {
					st.peak = len(st.heap)
				}
			}
		}
	}
	return nil
}
