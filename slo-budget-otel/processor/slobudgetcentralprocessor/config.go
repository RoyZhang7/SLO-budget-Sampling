package slobudgetcentralprocessor

import (
	"errors"
	"time"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
)

// Config of the central processor: calibration, refill and cap.
type Config struct {
	Sampler slobudget.Config `mapstructure:"sampler"`
	// Period is the sampling period, the same as the agents'.
	Period time.Duration `mapstructure:"period"`
	// Grace is how long after a period boundary the central waits for the agents'
	// stats before it closes the period.
	Grace time.Duration `mapstructure:"grace"`
	// Listen is the HTTP address of the config channel (/slobudget/config,
	// /slobudget/stats, /slobudget/status).
	Listen string `mapstructure:"listen"`
	// VStar fixes the volume target in spans per period; VStarFrac derives it from
	// the observed spans per period (EWMA) when VStar is 0.
	VStar     float64 `mapstructure:"v_star"`
	VStarFrac float64 `mapstructure:"v_star_frac"`
	// CapFactor x V* is the hard cap on span units admitted per period.
	CapFactor float64 `mapstructure:"cap_factor"`
	// SLOMs is the end-to-end SLO per route in milliseconds.
	SLOMs map[string]float64 `mapstructure:"slo_ms"`
	// DefaultRoute / DefaultClass label spans that carry no slo.r / slo.c attribute.
	DefaultRoute string `mapstructure:"default_route"`
	DefaultClass string `mapstructure:"default_class"`
	// CallGraphFile fixes the call structure (route -> {root, children, parallel});
	// without it the graph is learned from the kept spans.
	CallGraphFile string `mapstructure:"call_graph_file"`
	// Services fixes the service count of the relative N_need; 0 = services seen.
	Services int `mapstructure:"services"`
	// CountKinds: "all" counts every span in V* and the cap, "server" only server spans.
	CountKinds string `mapstructure:"count_kinds"`
	// OwnTime: "inclusive" derives a service's own time from the inclusive quantiles
	// (L minus the callees' L, the simulator's fallback), "stamped" trusts the agents'
	// slobudget.own_us.  DSB-SN's SDK exports client spans in a later batch than the
	// server span, so the stamp often equals the duration: inclusive is the default.
	OwnTime string `mapstructure:"own_time"`
	// RefillInput: "demand" feeds the refill the volume the agents wanted to keep
	// (admitted plus what their pools refused, plus cap drops), so that B falls
	// again when the pools bind; "admitted" feeds it the admitted volume as the
	// simulator does (any B above the plan's saturation point is then a fixed point).
	RefillInput string `mapstructure:"refill_input"`
	StatsLog    bool   `mapstructure:"stats_log"`
}

func (c *Config) Validate() error {
	if err := c.Sampler.Validate(); err != nil {
		return err
	}
	if c.Period <= 0 {
		return errors.New("period must be positive")
	}
	if c.VStar <= 0 && c.VStarFrac <= 0 {
		return errors.New("v_star or v_star_frac is required")
	}
	if len(c.SLOMs) == 0 {
		return errors.New("slo_ms needs at least one route")
	}
	if c.CountKinds != "all" && c.CountKinds != "server" {
		return errors.New("count_kinds: all or server")
	}
	if c.OwnTime != "inclusive" && c.OwnTime != "stamped" {
		return errors.New("own_time: inclusive or stamped")
	}
	if c.RefillInput != "demand" && c.RefillInput != "admitted" {
		return errors.New("refill_input: demand or admitted")
	}
	return nil
}
