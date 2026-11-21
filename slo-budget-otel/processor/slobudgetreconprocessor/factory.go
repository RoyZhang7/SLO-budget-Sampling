// Package slobudgetreconprocessor reconstructs the shed structure of a trace at the
// collector: for every kept span whose parent was dropped it inserts placeholder
// ancestors up to the nearest kept ancestor or the root, using the span's parent id, the
// path bridge's `_br` checkpoint anchor and depth, and the call graph.  Placeholders carry
// synthetic=true, the service (and the operation where it is known), start and end
// bounding the kept descendants, and therefore no own time of their own.
package slobudgetreconprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

var typeStr = component.MustNewType("slobudgetrecon")

// NewFactory creates the factory of the reconstruction processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(typeStr, createDefaultConfig,
		processor.WithTraces(createTraces, component.StabilityLevelDevelopment))
}

func createDefaultConfig() component.Config {
	return &Config{
		Hold:               3 * time.Second,
		DefaultRoute:       "compose_post",
		ServiceNameFormat:  "unknown_service:%s_service_pb_es_proc",
		SyntheticAttribute: "synthetic",
		StatsLog:           true,
	}
}

func createTraces(_ context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	return newRecon(set, cfg.(*Config), next)
}
