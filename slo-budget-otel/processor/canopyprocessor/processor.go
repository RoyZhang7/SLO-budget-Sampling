package canopyprocessor

import (
	"context"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
)

const (
	attrSamplingP = "sampling.p"
	attrReason    = "slobudget.reason"
	pMin          = 1e-4
)

type canopy struct {
	cfg  *Config
	log  *zap.Logger
	next consumer.Traces
	now  func() time.Time

	mu      sync.Mutex
	rng     *rand.Rand
	p       map[string]float64 // per-service rate (persists across periods)
	seen    map[string]int     // this period
	kept    map[string]int     // this period
	cur     int64              // current period index
	started bool
	history []periodRecord // last few, for tests

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type periodRecord struct {
	Period int64
	Seen   int
	Kept   int
	PMean  float64
	P      map[string]float64
}

func newCanopy(set processor.Settings, cfg *Config, next consumer.Traces) (*canopy, error) {
	return &canopy{cfg: cfg, log: set.Logger, next: next, now: time.Now, rng: rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15)),
		p: map[string]float64{}, seen: map[string]int{}, kept: map[string]int{}}, nil
}

func (c *canopy) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (c *canopy) periodOf(t time.Time) int64 { return t.UnixNano() / c.cfg.Period.Nanoseconds() }

func (c *canopy) Start(context.Context, component.Host) error {
	c.mu.Lock()
	c.cur = c.periodOf(c.now())
	c.started = true
	c.mu.Unlock()
	bg, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go c.loop(bg)
	c.log.Info("canopy started", zap.Duration("period", c.cfg.Period), zap.Float64("v_star", c.cfg.VStar),
		zap.Float64("initial_rate", c.cfg.InitialRate), zap.Float64("alpha", c.cfg.Alpha))
	return nil
}

func (c *canopy) Shutdown(context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	return nil
}

func (c *canopy) loop(ctx context.Context) {
	defer c.wg.Done()
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.mu.Lock()
			c.rollTo(c.periodOf(c.now()))
			c.mu.Unlock()
		}
	}
}

// rollTo closes every period before p (adapting the rates at each period end, as
// on_period_end does).  Called with c.mu held.
func (c *canopy) rollTo(p int64) {
	if !c.started {
		c.cur, c.started = p, true
		return
	}
	for c.cur < p {
		c.endPeriod()
		c.cur++
	}
}

// endPeriod is CanopySampler.on_period_end.
func (c *canopy) endPeriod() {
	n := len(c.seen)
	if n < 1 {
		n = 1
	}
	target := c.cfg.VStar / float64(n)
	seen, kept := 0, 0
	for svc, ns := range c.seen {
		seen += ns
		p, ok := c.p[svc]
		if !ok {
			p = c.cfg.InitialRate
		}
		k := c.kept[svc]
		kept += k
		raw := p * target / math.Max(1.0, float64(k))
		c.p[svc] = math.Min(1.0, math.Max(pMin, (1-c.cfg.Alpha)*p+c.cfg.Alpha*raw))
	}
	mean := 0.0
	for _, v := range c.p {
		mean += v
	}
	if len(c.p) > 0 {
		mean /= float64(len(c.p))
	}
	if c.cfg.StatsLog {
		c.log.Info("canopy period", zap.Int64("period", c.cur), zap.Int("seen", seen), zap.Int("kept", kept),
			zap.Float64("p_mean", mean), zap.Int("services", len(c.seen)))
	}
	snap := make(map[string]float64, len(c.p))
	for k, v := range c.p {
		snap[k] = v
	}
	c.history = append(c.history, periodRecord{Period: c.cur, Seen: seen, Kept: kept, PMean: mean, P: snap})
	if len(c.history) > 64 {
		c.history = c.history[len(c.history)-64:]
	}
	c.seen, c.kept = map[string]int{}, map[string]int{}
}

// ConsumeTraces flips one coin per span with its service's rate.
func (c *canopy) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	c.mu.Lock()
	c.rollTo(c.periodOf(c.now()))
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		svc := "unknown"
		if v, ok := rs.Resource().Attributes().Get("service.name"); ok {
			svc = slobudget.NormalizeService(v.AsString())
		}
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			sss.At(j).Spans().RemoveIf(func(span ptrace.Span) bool {
				c.seen[svc]++
				p, ok := c.p[svc]
				if !ok {
					p = c.cfg.InitialRate
				}
				if p < 1.0 && c.rng.Float64() >= p {
					return true
				}
				c.kept[svc]++
				span.Attributes().PutDouble(attrSamplingP, p)
				span.Attributes().PutStr(attrReason, "rate")
				return false
			})
		}
		sss.RemoveIf(func(ss ptrace.ScopeSpans) bool { return ss.Spans().Len() == 0 })
	}
	rss.RemoveIf(func(rs ptrace.ResourceSpans) bool { return rs.ScopeSpans().Len() == 0 })
	c.mu.Unlock()
	if td.SpanCount() == 0 {
		return nil
	}
	return c.next.ConsumeTraces(ctx, td)
}
