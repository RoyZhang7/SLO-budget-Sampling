package arrival

import "time"

// Cause says where a server span's arrival came from, or why it is the in-process time.
type Cause int

const (
	Hit         Cause = iota // the program's time
	MissRoot                 // no parent span: the request carries no id
	MissNoTable              // the table is not open
	// An id the program never stored (its digits split between two packets, packets it did not
	// see) is MissEmpty or MissNotStored, by what the slot happens to hold: compare their sum.
	MissEmpty       // the slot was never used
	MissNotStored   // the slot holds an id from before this span's start: ours was never stored, or taken in the wait before the handler
	MissOverwritten // the slot holds an id stored after this span's start
	MissTorn        // the program kept writing the slot
	MissLate        // stored after the span's start: not this request's time
	NumCauses
)

// CauseNames are the names of the causes in logs.
var CauseNames = [NumCauses]string{"hit", "root", "no_table", "empty", "not_stored", "overwritten", "torn", "late"}

// Clock converts the program's monotonic times to the spans' clock (the node's wall clock: the
// SDKs stamp a span's start with it).  The agent samples the offset once a second, so a step of
// the wall clock is followed within that time; slewing moves both clocks together.
type Clock struct {
	sample func() (unix, mono int64)
	offset int64 // wall - monotonic, ns
	step   int64 // largest change of the offset since TakeStep, ns
}

// NewClock samples the offset once; sample returns the two clocks read together.
func NewClock(sample func() (unix, mono int64)) *Clock {
	c := &Clock{sample: sample}
	u, m := sample()
	c.offset = u - m
	return c
}

// Tick samples the offset again.
func (c *Clock) Tick() {
	u, m := c.sample()
	d := (u - m) - c.offset
	c.offset = u - m
	if max(d, -d) > max(c.step, -c.step) {
		c.step = d
	}
}

// Wall converts a monotonic time, Mono a wall time.
func (c *Clock) Wall(mono uint64) int64 { return int64(mono) + c.offset }
func (c *Clock) Mono(wall int64) int64  { return wall - c.offset }

// TakeStep is the largest change of the offset since the last call (large: the wall clock was
// stepped, and the arrivals converted between the step and the next sample are off by it).
func (c *Clock) TakeStep() time.Duration {
	d := time.Duration(c.step)
	c.step = 0
	return d
}

// Resolver looks arrivals up and counts the outcomes.  It is not safe for concurrent use (the
// agent calls it under its lock).
type Resolver struct {
	Table  *Table // nil: not open
	Clock  *Clock
	Counts [NumCauses]int
	WaitNs int64 // summed start - arrival over the hits
}

// Arrival returns the arrival of the request a server span belongs to, in the span's clock:
// the program's time for its parent span id, or the span's own start (the in-process time)
// with the cause of the miss.
func (r *Resolver) Arrival(parent uint64, startUnix int64) (int64, Cause) {
	at, c := r.lookup(parent, startUnix)
	r.Counts[c]++
	if c != Hit {
		return startUnix, c
	}
	r.WaitNs += startUnix - at
	return at, Hit
}

func (r *Resolver) lookup(parent uint64, startUnix int64) (int64, Cause) {
	if parent == 0 {
		return 0, MissRoot
	}
	if r.Table == nil {
		return 0, MissNoTable
	}
	id, ns, ok := r.Table.Read(parent)
	switch {
	case !ok:
		return 0, MissTorn
	case id == 0:
		return 0, MissEmpty
	case id != parent:
		// the request arrived before its span started: an entry stored after the start took the
		// slot from it; an older one means it was never stored (or the slot was taken in the wait)
		if int64(ns) > r.Clock.Mono(startUnix) {
			return 0, MissOverwritten
		}
		return 0, MissNotStored
	}
	at := r.Clock.Wall(ns)
	if at > startUnix {
		return 0, MissLate
	}
	return at, Hit
}

// Take returns the counts since the last call and starts over.
func (r *Resolver) Take() (counts [NumCauses]int, waitNs int64) {
	counts, waitNs = r.Counts, r.WaitNs
	r.Counts, r.WaitNs = [NumCauses]int{}, 0
	return counts, waitNs
}
