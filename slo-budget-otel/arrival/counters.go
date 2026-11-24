package arrival

import "fmt"

// The names under the pin directory (arrivald pins them, the agent opens them).
const (
	PinTable    = "arrivals"
	PinCounters = "counters"
)

// Counters reads the program's counters (one array value of NumCounters 64-bit numbers).
type Counters struct {
	mem   []byte
	close func() error
}

// NewCounters wraps the memory of the counter array.
func NewCounters(mem []byte, closer func() error) (*Counters, error) {
	if len(mem) < NumCounters*8 {
		return nil, fmt.Errorf("arrival counters: %d bytes, want %d", len(mem), NumCounters*8)
	}
	return &Counters{mem: mem, close: closer}, nil
}

// Read returns the counters since the program was loaded.
func (c *Counters) Read() (v [NumCounters]uint64) {
	if c == nil {
		return v
	}
	for i := range v {
		v[i] = load(c.mem, uintptr(i)*8)
	}
	return v
}

// Close unmaps the counters.
func (c *Counters) Close() error {
	if c == nil || c.close == nil {
		return nil
	}
	f := c.close
	c.close = nil
	return f()
}
