package canopyprocessor

import (
	"errors"
	"time"
)

// Config of the Canopy-style rate-target sampler.
type Config struct {
	// Period is the wall-clock aligned adaptation period.
	Period time.Duration `mapstructure:"period"`
	// VStar is the volume target in spans per period; every service seen in a period
	// gets an equal share of it.
	VStar float64 `mapstructure:"v_star"`
	// InitialRate is every service's rate before its first adaptation.
	InitialRate float64 `mapstructure:"initial_rate"`
	// Alpha damps the multiplicative update: p <- (1-alpha) p + alpha p target / kept.
	Alpha float64 `mapstructure:"alpha"`
	// Seed seeds the per-span coin.
	Seed uint64 `mapstructure:"seed"`
	// StatsLog logs one `canopy period` line per period.
	StatsLog bool `mapstructure:"stats_log"`
}

func (c *Config) Validate() error {
	if c.Period <= 0 {
		return errors.New("period must be positive")
	}
	if c.VStar <= 0 {
		return errors.New("v_star must be positive")
	}
	if c.InitialRate <= 0 || c.InitialRate > 1 {
		return errors.New("initial_rate must be in (0, 1]")
	}
	if c.Alpha <= 0 || c.Alpha > 1 {
		return errors.New("alpha must be in (0, 1]")
	}
	return nil
}
