// Package capgateprocessor is the collector's hard cap, common to every arm of the
// Stage 3 campaign: the last processor before `batch` in the central pipeline.  It
// mirrors the simulator's replay (`slo-budget-sim/slo_budget_sim/replay.py`, `admit`):
// spans are admitted first come first served while the period's admitted span units
// stay within cap = cap_factor x v_star, the rest are dropped.  Periods are wall-clock
// aligned and a span belongs to the period in which it reached the gate.  Every
// admitted span is stamped with `slobudget.admit_ns` (when the gate admitted it, unix
// ns), the common decision-time clock of the evidence metric, and one record per
// period (received, admitted, cap_dropped, bytes_admitted, admitted by span kind) is
// logged as `capgate period` and optionally appended to a JSON-lines file.
package capgateprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

var typeStr = component.MustNewType("capgate")

// NewFactory creates the factory of the cap gate.
func NewFactory() processor.Factory {
	return processor.NewFactory(typeStr, createDefaultConfig,
		processor.WithTraces(createTraces, component.StabilityLevelDevelopment))
}

func createDefaultConfig() component.Config {
	return &Config{
		Period:    5 * time.Second,
		Grace:     400 * time.Millisecond,
		CapFactor: 1.25,
		StatsLog:  true,
	}
}

func createTraces(_ context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	return newGate(set, cfg.(*Config), next)
}
