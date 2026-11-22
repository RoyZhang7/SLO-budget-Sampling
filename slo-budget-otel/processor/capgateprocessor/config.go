package capgateprocessor

import (
	"errors"
	"time"
)

// Config of the cap gate.
type Config struct {
	// Period is the length of the wall-clock aligned accounting period.
	Period time.Duration `mapstructure:"period"`
	// Grace is how long after a period's end its record stays open for batches that
	// arrived (were timestamped) inside the period but reached the gate's lock later.
	Grace time.Duration `mapstructure:"grace"`
	// Phase shifts the period boundaries: period index = (unix_ns - phase) / period.
	// 0 aligns them to multiples of Period, as the agents and the central do.
	Phase time.Duration `mapstructure:"phase"`
	// VStar is the volume target in spans per period; the cap is CapFactor x VStar span
	// units per period.  VStar <= 0 disables the cap (the gate still stamps and counts).
	VStar     float64 `mapstructure:"v_star"`
	CapFactor float64 `mapstructure:"cap_factor"`
	// StatsFile, when set, receives one JSON line per period record (appended).
	StatsFile string `mapstructure:"stats_file"`
	// StatsLog logs one `capgate period` line per period.
	StatsLog bool `mapstructure:"stats_log"`
}

func (c *Config) Validate() error {
	if c.Period <= 0 {
		return errors.New("period must be positive")
	}
	if c.Grace < 0 || c.Grace >= c.Period {
		return errors.New("grace must be in [0, period)")
	}
	if c.VStar > 0 && c.CapFactor <= 0 {
		return errors.New("cap_factor must be positive when v_star is set")
	}
	return nil
}

// cap is the span-unit cap per period (+Inf when disabled).
func (c *Config) capUnits() float64 {
	if c.VStar <= 0 {
		return inf
	}
	return c.CapFactor * c.VStar
}
