//go:build !linux

package arrival

import (
	"errors"
	"time"
)

var errNotLinux = errors.New("the arrival table needs Linux")

// Open, OpenCounters and Identity need Linux.
func Open(string) (*Table, error)            { return nil, errNotLinux }
func OpenCounters(string) (*Counters, error) { return nil, errNotLinux }
func Identity(string) (string, error)        { return "", errNotLinux }

var started = time.Now()

// SampleClocks reads the wall clock and a monotonic one (not the kernel's).
func SampleClocks() (unixNs, monoNs int64) {
	n := time.Now()
	return n.UnixNano(), int64(n.Sub(started))
}
