//go:build !linux

package slobudgetprocessor

import "time"

// CPU time is only measured on Linux (the benchmark runs on node-0).
func processCPU() time.Duration { return 0 }
func threadCPU() time.Duration  { return 0 }
