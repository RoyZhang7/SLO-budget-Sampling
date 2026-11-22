// Package canopyprocessor is the Canopy-style baseline of the Stage 3 campaign, a port
// of `CanopySampler` in `slo-budget-sim/slo_budget_sim/baselines.py`: adaptive per-service
// rate targets at the collector.  Every span gets an independent coin with its service's
// rate p_s (blind to latency and outliers, not trace-consistent); at each wall-clock period
// end every service seen gets an equal share of V* as its target and its rate is scaled
// multiplicatively toward it: p_s <- clip((1-alpha) p_s + alpha p_s target / max(1, kept_s),
// 1e-4, 1).  Kept spans carry `sampling.p` (the rate used) and `slobudget.reason`="rate".
package canopyprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

var typeStr = component.MustNewType("canopy")

// NewFactory creates the factory of the Canopy-style sampler.
func NewFactory() processor.Factory {
	return processor.NewFactory(typeStr, createDefaultConfig,
		processor.WithTraces(createTraces, component.StabilityLevelDevelopment))
}

func createDefaultConfig() component.Config {
	return &Config{
		Period:      5 * time.Second,
		InitialRate: 0.15,
		Alpha:       0.5,
		Seed:        1,
		StatsLog:    true,
	}
}

func createTraces(_ context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	return newCanopy(set, cfg.(*Config), next)
}
