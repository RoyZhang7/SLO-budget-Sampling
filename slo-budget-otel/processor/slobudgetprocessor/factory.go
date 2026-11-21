// Package slobudgetprocessor is the per-host agent of the SLO-budget sampler:
// the span-end decider of slo-budget-sim (Decide(...)) as an OpenTelemetry
// Collector processor.  At span end it looks up T_i for (service, root,
// class) with the (service, root) fallback, flags an outlier when the
// duration exceeds T_i or the status is not OK, keeps outliers under the
// N_need rule with the stamped probability `sampling.p`, charges the request
// budget and the host pool, plan-samples ordinary spans uniformly, records
// the dropped-child count on kept parents, holds root spans for a short window
// and pins them when a kept-notice for their trace arrives, and reports its
// period counters to the central, from which it receives the table, B, N_need,
// plan rates and its pool through the config channel.
package slobudgetprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
)

var typeStr = component.MustNewType("slobudget")

// NewFactory creates the factory of the agent processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(typeStr, createDefaultConfig,
		processor.WithTraces(createTraces, component.StabilityLevelDevelopment))
}

func createDefaultConfig() component.Config {
	return &Config{
		Sampler:      slobudget.ChosenV2(),
		Period:       5 * time.Second,
		DefaultRoute: "compose_post",
		DefaultClass: "",
		RootHold:     2 * time.Second,
		NoticeListen: ":7777",
		NoticePort:   7777,
		ClientSpans:  "plan",
		RequestTTL:   10 * time.Second,
		StatsLog:     true,
	}
}

func createTraces(_ context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	return newAgent(set, cfg.(*Config), next)
}
