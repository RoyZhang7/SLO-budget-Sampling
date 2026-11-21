package slobudgetreconprocessor

import (
	"errors"
	"time"
)

// Config of the reconstruction processor.
type Config struct {
	// Hold is how long a trace's kept spans are buffered after its first span arrives
	// before the trace is reconstructed and forwarded (longer than the agents' root hold
	// plus their batching, so the root and the late client spans are in).
	Hold time.Duration `mapstructure:"hold"`
	// CallGraphFile gives the call structure (route -> {root, children}); the caller of a
	// service names the service of a missing parent.
	CallGraphFile string `mapstructure:"call_graph_file"`
	// DefaultRoute labels traces whose spans carry no slo.root attribute.
	DefaultRoute string `mapstructure:"default_route"`
	// ServiceNameFormat builds the resource service.name of a placeholder for a service no
	// kept span of the trace (or of an earlier trace) has shown (%s = the short name).
	ServiceNameFormat string `mapstructure:"service_name_format"`
	// SyntheticAttribute is the boolean attribute every placeholder carries.
	SyntheticAttribute string `mapstructure:"synthetic_attribute"`
	// StatsLog logs the per-period counters.
	StatsLog bool `mapstructure:"stats_log"`
}

func (c *Config) Validate() error {
	if c.Hold <= 0 {
		return errors.New("hold must be positive")
	}
	if c.SyntheticAttribute == "" {
		return errors.New("synthetic_attribute is required")
	}
	return nil
}
