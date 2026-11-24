// Package slobudgetarrivalprocessor attaches the arrival of every server span's request
// (slobudget.arrival_ns, from the eBPF arrival program's table) and passes the spans on.  In the
// campaign's runs with the arrival stamp it stands in the tee, in front of the fork, so that the
// agent and the 100 % export carry the same stamp; it decides nothing and drops nothing.
package slobudgetarrivalprocessor

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/processor/internal/arrivalstamp"
)

var typeStr = component.MustNewType("slobudgetarrival")

// Config of the stamp.
type Config struct {
	// ArrivalTable is the pin directory of the arrival program (arrivald).
	ArrivalTable string `mapstructure:"arrival_table"`
	// Period is how often the table is looked for and the outcomes are logged.
	Period time.Duration `mapstructure:"period"`
	// StatsLog logs the period's outcomes ("slobudget arrival").
	StatsLog bool `mapstructure:"stats_log"`
}

func (c *Config) Validate() error {
	if c.ArrivalTable == "" {
		return errors.New("arrival_table is required")
	}
	if c.Period <= 0 {
		return errors.New("period must be positive")
	}
	return nil
}

// NewFactory creates the factory of the processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(typeStr, func() component.Config { return &Config{Period: 5 * time.Second, StatsLog: true} },
		processor.WithTraces(func(_ context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
			return newStamp(set, cfg.(*Config), next), nil
		}, component.StabilityLevelDevelopment))
}

type stamp struct {
	cfg    *Config
	log    *zap.Logger
	next   consumer.Traces
	st     *arrivalstamp.Stamper
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newStamp(set processor.Settings, cfg *Config, next consumer.Traces) *stamp {
	return &stamp{cfg: cfg, log: set.Logger, next: next, st: arrivalstamp.New(cfg.ArrivalTable, set.Logger)}
}

func (p *stamp) Capabilities() consumer.Capabilities { return consumer.Capabilities{MutatesData: true} }

func (p *stamp) Start(context.Context, component.Host) error {
	p.st.Open()
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.wg.Add(1)
	go p.loop(ctx)
	return nil
}

func (p *stamp) Shutdown(context.Context) error {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
	p.st.Close()
	return nil
}

func (p *stamp) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				if s := spans.At(k); s.Kind() == ptrace.SpanKindServer {
					p.st.Stamp(s)
				}
			}
		}
	}
	return p.next.ConsumeTraces(ctx, td)
}

// loop samples the clocks every second and closes the periods on wall-clock boundaries, as the agents do.
func (p *stamp) loop(ctx context.Context) {
	defer p.wg.Done()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	of := func(t time.Time) int64 { return t.UnixNano() / p.cfg.Period.Nanoseconds() }
	period := of(time.Now())
	end := func(per int64) time.Duration { return time.Until(time.Unix(0, (per+1)*p.cfg.Period.Nanoseconds())) }
	next := time.NewTimer(end(period))
	defer next.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			p.st.Tick()
		case <-next.C:
			f := p.st.Take()
			p.st.Open()
			if p.cfg.StatsLog {
				p.log.Info("slobudget arrival", append([]zap.Field{zap.Int64("period", period)}, f...)...)
			}
			period = of(time.Now())
			next.Reset(end(period))
		}
	}
}
