package reservoirprocessor

import (
	"errors"
	"time"
)

// Config of the weighted top-K reservoir.
type Config struct {
	// Period is the wall-clock aligned reservoir period; decisions are final at its end.
	Period time.Duration `mapstructure:"period"`
	// Grace is how long after a period's end its reservoir stays open for batches that
	// arrived inside the period but reached the processor's lock later.
	Grace time.Duration `mapstructure:"grace"`
	// VStar is the volume target in spans per period (whole deployment).
	VStar float64 `mapstructure:"v_star"`
	// Shares maps a host (node name) to its share of the span volume: K = round(V* x share).
	Shares map[string]float64 `mapstructure:"shares"`
	// Host names this agent's host; default $NODE_NAME, then os.Hostname().
	Host string `mapstructure:"host"`
	// TTableFile holds T_i: JSON route -> service -> seconds (class-agnostic).
	TTableFile string `mapstructure:"ttable_file"`
	// DefaultRoute is the route of spans without an `slo.root` attribute.
	DefaultRoute string `mapstructure:"default_route"`
	// StatusWeight is the weight of an error span that does not miss its T; Eps the weight
	// of every other span (ordinary spans only fill spare slots).
	StatusWeight float64 `mapstructure:"status_weight"`
	Eps          float64 `mapstructure:"eps"`
	// Seed seeds the reservoir keys.
	Seed uint64 `mapstructure:"seed"`
	// StatsLog logs one `reservoir period` line per period.
	StatsLog bool `mapstructure:"stats_log"`
}

func (c *Config) Validate() error {
	if c.Period <= 0 {
		return errors.New("period must be positive")
	}
	if c.Grace < 0 || c.Grace >= c.Period {
		return errors.New("grace must be in [0, period)")
	}
	if c.VStar <= 0 {
		return errors.New("v_star must be positive")
	}
	if c.TTableFile == "" {
		return errors.New("ttable_file is required (the reservoir ranks spans by their miss of T)")
	}
	if c.Eps <= 0 || c.StatusWeight <= 0 {
		return errors.New("eps and status_weight must be positive")
	}
	for h, s := range c.Shares {
		if s <= 0 || s > 1 {
			return errors.New("share of " + h + " must be in (0, 1]")
		}
	}
	return nil
}
