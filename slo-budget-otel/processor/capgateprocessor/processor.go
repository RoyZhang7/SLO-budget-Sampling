package capgateprocessor

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// AttrAdmitNs is the int attribute every admitted span carries: unix ns of admission.
const AttrAdmitNs = "slobudget.admit_ns"

var inf = math.Inf(1)

// record is one period's accounting.
type record struct {
	Period        int64   `json:"period"`
	Received      int     `json:"received"`
	Admitted      int     `json:"admitted"`
	CapDropped    int     `json:"cap_dropped"`
	BytesAdmitted int     `json:"bytes_admitted"`
	Server        int     `json:"admitted_server"`
	Client        int     `json:"admitted_client"`
	Other         int     `json:"admitted_other"`
	Late          int     `json:"late"` // spans timestamped in an already closed period (counted here)
	Cap           float64 `json:"cap"`
	Partial       bool    `json:"partial,omitempty"` // closed at shutdown before its end
}

type gate struct {
	cfg  *Config
	log  *zap.Logger
	next consumer.Traces
	now  func() time.Time

	mu         sync.Mutex
	open       map[int64]*record // periods not yet emitted
	nextToEmit int64             // the oldest period not yet emitted
	started    bool
	cap        float64
	sizer      ptrace.ProtoMarshaler
	statsF     *os.File
	emitted    []record // kept for tests (last few)

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newGate(set processor.Settings, cfg *Config, next consumer.Traces) (*gate, error) {
	return &gate{cfg: cfg, log: set.Logger, next: next, now: time.Now, open: map[int64]*record{}, cap: cfg.capUnits()}, nil
}

func (g *gate) Capabilities() consumer.Capabilities { return consumer.Capabilities{MutatesData: true} }

func (g *gate) periodOf(t time.Time) int64 {
	return (t.UnixNano() - g.cfg.Phase.Nanoseconds()) / g.cfg.Period.Nanoseconds()
}

// periodEnd is when period p ends (wall clock).
func (g *gate) periodEnd(p int64) time.Time {
	return time.Unix(0, (p+1)*g.cfg.Period.Nanoseconds()+g.cfg.Phase.Nanoseconds())
}

func (g *gate) Start(context.Context, component.Host) error {
	if g.cfg.StatsFile != "" {
		f, err := os.OpenFile(g.cfg.StatsFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("stats_file: %w", err)
		}
		g.statsF = f
	}
	g.mu.Lock()
	g.nextToEmit = g.periodOf(g.now())
	g.started = true
	g.mu.Unlock()
	bg, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	g.wg.Add(1)
	go g.loop(bg)
	g.log.Info("capgate started", zap.Duration("period", g.cfg.Period), zap.Float64("v_star", g.cfg.VStar), zap.Float64("cap", g.cap))
	return nil
}

func (g *gate) Shutdown(context.Context) error {
	if g.cancel != nil {
		g.cancel()
	}
	g.wg.Wait()
	g.mu.Lock()
	for p := g.nextToEmit; len(g.open) > 0; p++ {
		if r, ok := g.open[p]; ok {
			r.Partial = true
			g.emitLocked(r)
			delete(g.open, p)
		}
	}
	g.mu.Unlock()
	if g.statsF != nil {
		_ = g.statsF.Close()
	}
	return nil
}

func (g *gate) loop(ctx context.Context) {
	defer g.wg.Done()
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.emitDue(g.now())
		}
	}
}

// emitDue emits every period whose end plus the grace has passed (zero records for
// periods without traffic included).
func (g *gate) emitDue(now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.started {
		g.nextToEmit = g.periodOf(now)
		g.started = true
	}
	for !now.Before(g.periodEnd(g.nextToEmit).Add(g.cfg.Grace)) {
		r, ok := g.open[g.nextToEmit]
		if !ok {
			r = &record{Period: g.nextToEmit, Cap: g.cap}
		}
		g.emitLocked(r)
		delete(g.open, g.nextToEmit)
		g.nextToEmit++
	}
}

func (g *gate) emitLocked(r *record) {
	if g.cfg.StatsLog {
		g.log.Info("capgate period", zap.Int64("period", r.Period), zap.Int("received", r.Received), zap.Int("admitted", r.Admitted),
			zap.Int("cap_dropped", r.CapDropped), zap.Int("bytes_admitted", r.BytesAdmitted),
			zap.Any("admitted_by_kind", map[string]int{"server": r.Server, "client": r.Client, "other": r.Other}),
			zap.Int("late", r.Late), zap.Float64("cap", r.Cap), zap.Bool("partial", r.Partial))
	}
	if g.statsF != nil {
		if b, err := json.Marshal(r); err == nil {
			_, _ = g.statsF.Write(append(b, '\n'))
		}
	}
	g.emitted = append(g.emitted, *r)
	if len(g.emitted) > 64 {
		g.emitted = g.emitted[len(g.emitted)-64:]
	}
}

// recordFor returns the open record of period p and whether the batch is late: a span
// timestamped in a period that was already emitted (later than the grace) is counted
// in the oldest open period.
func (g *gate) recordFor(p int64) (*record, bool) {
	late := false
	if g.started && p < g.nextToEmit {
		p, late = g.nextToEmit, true
	}
	r, ok := g.open[p]
	if !ok {
		r = &record{Period: p, Cap: g.cap}
		g.open[p] = r
	}
	return r, late
}

// ConsumeTraces admits spans in arrival order while the period's admitted units stay
// within the cap, stamps them and forwards the batch.
func (g *gate) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	now := g.now()
	p := g.periodOf(now)
	stamp := now.UnixNano()
	g.mu.Lock()
	r, lateBatch := g.recordFor(p)
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			sss.At(j).Spans().RemoveIf(func(span ptrace.Span) bool {
				r.Received++
				if lateBatch {
					r.Late++
				}
				if float64(r.Admitted)+1 > g.cap+1e-9 {
					r.CapDropped++
					return true
				}
				r.Admitted++
				switch span.Kind() {
				case ptrace.SpanKindServer:
					r.Server++
				case ptrace.SpanKindClient:
					r.Client++
				default:
					r.Other++
				}
				span.Attributes().PutInt(AttrAdmitNs, stamp)
				return false
			})
		}
		sss.RemoveIf(func(ss ptrace.ScopeSpans) bool { return ss.Spans().Len() == 0 })
	}
	rss.RemoveIf(func(rs ptrace.ResourceSpans) bool { return rs.ScopeSpans().Len() == 0 })
	if td.SpanCount() > 0 {
		r.BytesAdmitted += g.sizer.TracesSize(td)
	}
	g.mu.Unlock()
	if td.SpanCount() == 0 {
		return nil
	}
	return g.next.ConsumeTraces(ctx, td)
}
