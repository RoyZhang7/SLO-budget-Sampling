// Package reservoirprocessor is the reservoir top-K baseline of the Stage 3 campaign,
// a port of `ReservoirSampler` in `slo-budget-sim/slo_budget_sim/baselines.py`: an
// Efraimidis-Spirakis weighted reservoir per host (agent) and wall-clock period.  Each
// span's weight is the size of its miss, (duration - T)/T with T from a fixed table
// (the oracle table of a healthy run, which favours this baseline), else a small weight
// for an error span and eps for everything else, so ordinary spans only fill spare
// slots; its key is u^(1/w).  The K largest keys of the period are held (the spans are
// retained) and emitted at the period's end, K = max(1, round(V* x share of the host)).
// Emitted spans carry `slobudget.reason`="reservoir" and `slobudget.reservoir_w`.
package reservoirprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

var typeStr = component.MustNewType("reservoir")

// NewFactory creates the factory of the reservoir.
func NewFactory() processor.Factory {
	return processor.NewFactory(typeStr, createDefaultConfig,
		processor.WithTraces(createTraces, component.StabilityLevelDevelopment))
}

func createDefaultConfig() component.Config {
	return &Config{
		Period:       5 * time.Second,
		Grace:        400 * time.Millisecond,
		DefaultRoute: "compose_post",
		StatusWeight: 0.01,
		Eps:          1e-6,
		Seed:         1,
		StatsLog:     true,
	}
}

func createTraces(_ context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	return newReservoir(set, cfg.(*Config), next)
}
