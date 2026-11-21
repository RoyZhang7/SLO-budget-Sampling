//go:build linux

package slobudgetprocessor

import (
	"syscall"
	"time"
)

// processCPU is the user + system CPU time of this process (for the rate-paced benchmark).
func processCPU() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

const rusageThread = 1 // RUSAGE_THREAD

// threadCPU is the CPU time of the calling OS thread (the goroutine is locked
// to it): the cost of the ConsumeTraces call itself, without the harness.
func threadCPU() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(rusageThread, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
