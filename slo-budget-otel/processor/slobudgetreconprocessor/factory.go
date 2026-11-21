//go:build slobudget_recon

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
