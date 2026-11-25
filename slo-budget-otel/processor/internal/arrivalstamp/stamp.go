// Package arrivalstamp attaches to a server span when its request reached the pod: the arrival
// program's time for the span's parent span id (../../../arrival), or the span's own start on a
// miss.  The agent uses it (slobudgetprocessor, arrival_table), and the processor in front of the
// fork to the 100 % export (slobudgetarrivalprocessor), so that the agent and the export see the
// same stamp.
package arrivalstamp

import (
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrival"
)

// The attributes: the arrival (unix ns, in the span's clock) and where it comes from, "ebpf" (the
// arrival program) or "start" (a miss: the span's own start).
const (
	Attr       = "slobudget.arrival_ns"
	AttrSource = "slobudget.arrival_source"
	FromEBPF   = "ebpf"
	FromStart  = "start"
)

// Stamper looks the arrivals up in the table arrivald pins, and counts the outcomes.
type Stamper struct {
	dir string
	log *zap.Logger

	mu   sync.Mutex // lookups, the table's change, the counts
	res  arrival.Resolver
	id   string // the open table's identity: it is opened again when arrivald pins it anew
	held bool   // Use: the table stays
	ctr  *arrival.Counters
	last [arrival.NumCounters]uint64 // the program's counters at the last Take
}

// New is a Stamper for a pin directory, without a table until Open.
func New(dir string, log *zap.Logger) *Stamper {
	return &Stamper{dir: dir, log: log, res: arrival.Resolver{Clock: arrival.NewClock(arrival.SampleClocks)}}
}

// Open maps the table, again when it was pinned anew and not at all while it is not pinned (the
// lookups then miss with no_table).  Called at the start and once a period, by one goroutine.
func (s *Stamper) Open() {
	id, err := arrival.Identity(s.dir)
	s.mu.Lock()
	was, none := s.id, s.res.Table == nil
	s.mu.Unlock()
	if err == nil && id == was {
		return
	}
	var t *arrival.Table
	var c *arrival.Counters
	if err == nil {
		if t, err = arrival.Open(s.dir); err == nil {
			if c, err = arrival.OpenCounters(s.dir); err != nil {
				_ = t.Close()
				t = nil
			}
		}
	}
	if err != nil {
		if id = ""; was != "" || none {
			s.log.Warn("arrival table not open", zap.String("dir", s.dir), zap.Error(err))
		}
	}
	s.swap(t, c, id)
	if t != nil {
		s.log.Info("arrival table open", zap.String("dir", s.dir), zap.Uint32("slots", t.Slots()))
	}
}

// swap replaces the table and the counters; the old ones are unmapped when no lookup can hold
// them any more.
func (s *Stamper) swap(t *arrival.Table, c *arrival.Counters, id string) {
	s.mu.Lock()
	if s.held {
		s.mu.Unlock()
		_ = t.Close()
		_ = c.Close()
		return
	}
	oldT, oldC := s.res.Table, s.ctr
	s.res.Table, s.ctr, s.id, s.last = t, c, id, c.Read()
	s.mu.Unlock()
	_ = oldT.Close()
	_ = oldC.Close()
}

// Close unmaps the table.
func (s *Stamper) Close() { s.swap(nil, nil, "") }

// Stamp attaches the arrival of a server span's request.  The lookup is made when the span is
// handed over: the table keeps an id only until its slot is taken.  A span that brings the
// program's stamp already (the tee in front, or the service itself) keeps it: that lookup was
// earlier, so it saw the slot before anyone could take it.
func (s *Stamper) Stamp(span ptrace.Span) {
	if v, ok := span.Attributes().Get(AttrSource); ok && v.Str() == FromEBPF {
		return
	}
	s.mu.Lock()
	at, cause := s.res.Arrival(id64(span.ParentSpanID()), int64(span.StartTimestamp()))
	s.mu.Unlock()
	src := FromEBPF
	if cause != arrival.Hit {
		src = FromStart
	}
	span.Attributes().PutInt(Attr, at)
	span.Attributes().PutStr(AttrSource, src)
}

// Tick samples the offset between the two clocks (once a second).
func (s *Stamper) Tick() {
	s.mu.Lock()
	s.res.Clock.Tick()
	s.mu.Unlock()
}

// Take is the log fields of the period that ends: the lookups by outcome, the mean wait of the
// hits, the largest step of the wall clock and what the program's counters gained.
func (s *Stamper) Take() []zap.Field {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, wait := s.res.Take()
	f := make([]zap.Field, 0, int(arrival.NumCauses)+arrival.NumCounters+2)
	for i, v := range n {
		f = append(f, zap.Int(arrival.CauseNames[i], v))
	}
	mean := 0.0
	if n[arrival.Hit] > 0 {
		mean = float64(wait) / float64(n[arrival.Hit]) / 1e3
	}
	f = append(f, zap.Float64("wait_us_mean", mean), zap.Duration("clock_step", s.res.Clock.TakeStep()))
	c := s.ctr.Read()
	for i, v := range c {
		f = append(f, zap.Uint64("bpf_"+arrival.CounterNames[i], v-s.last[i]))
	}
	s.last = c
	return f
}

// Counts is the lookups by outcome since the last Take.
func (s *Stamper) Counts() [arrival.NumCauses]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.res.Counts
}

// Opened says whether a table is open.
func (s *Stamper) Opened() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.res.Table != nil
}

// Use replaces the table and the clock, for good (tests: a table in memory).
func (s *Stamper) Use(t *arrival.Table, c *arrival.Clock) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.res.Table, s.res.Clock, s.held = t, c, true
}

// id64 is a span id as the program stores it: the big-endian number its hexadecimal digits write.
func id64(id pcommon.SpanID) uint64 {
	var v uint64
	for _, b := range id {
		v = v<<8 | uint64(b)
	}
	return v
}
