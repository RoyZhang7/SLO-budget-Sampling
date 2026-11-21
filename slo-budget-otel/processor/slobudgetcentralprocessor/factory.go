// Package slobudgetcentralprocessor is the central side of the SLO-budget
// sampler: it receives the agents' kept spans, feeds them 1/p-weighted into the
// per-period T_i calibration with the frozen (outlier-rate) trigger, sums the
// agents' period counters, runs the volume-loopback refill of the request
// budget B and the host pools, applies the hard cap as a backstop, and serves
// the result to the agents over the config channel (HTTP).
package slobudgetcentralprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
)

var typeStr = component.MustNewType("slobudgetcentral")

// NewFactory creates the factory of the central processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(typeStr, createDefaultConfig,
		processor.WithTraces(createTraces, component.StabilityLevelDevelopment))
}

func createDefaultConfig() component.Config {
	return &Config{
		Sampler:      slobudget.ChosenV2(),
		Period:       5 * time.Second,
		Grace:        400 * time.Millisecond,
		Listen:       ":8081",
		VStarFrac:    0.12,
		CapFactor:    1.25,
		SLOMs:        map[string]float64{"compose_post": 23.4},
		DefaultRoute: "compose_post",
		CountKinds:   "all",
		OwnTime:      "inclusive",
		RefillInput:  "demand",
		StatsLog:     true,
	}
}

func createTraces(_ context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	return newCentral(set, cfg.(*Config), next)
}
