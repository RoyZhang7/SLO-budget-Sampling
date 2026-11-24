package slobudgetprocessor

import (
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/arrival"
)

// stampArrival attaches the arrival of a server span's request: the arrival program's time for
// its parent span id, or the span's own start on a miss.  The lookup is made when the span
// reaches the agent, before any hold: the table keeps an id only until its slot is taken.
// Called with a.mu held.
func (a *agent) stampArrival(span ptrace.Span) {
	at, c := a.arr.Arrival(spanIDToU64(span.ParentSpanID()), int64(span.StartTimestamp()))
	src := "ebpf"
	if c != arrival.Hit {
		src = "start"
	}
	span.Attributes().PutInt(AttrArrival, at)
	span.Attributes().PutStr(AttrArrivalSource, src)
}

// openArrival maps the arrival program's table, again when it was pinned anew and not at all
// while it is not pinned (the lookups then miss with no_table).
func (a *agent) openArrival() {
	id, err := arrival.Identity(a.cfg.ArrivalTable)
	if err == nil && id == a.arrID {
		return
	}
	var t *arrival.Table
	var c *arrival.Counters
	if err == nil {
		if t, err = arrival.Open(a.cfg.ArrivalTable); err == nil {
			if c, err = arrival.OpenCounters(a.cfg.ArrivalTable); err != nil {
				_ = t.Close()
				t = nil
			}
		}
	}
	if err != nil {
		if id = ""; a.arrID != "" || a.arr.Table == nil {
			a.log.Warn("arrival table not open", zap.String("dir", a.cfg.ArrivalTable), zap.Error(err))
		}
	}
	a.swapArrival(t, c, id)
	if t != nil {
		a.log.Info("arrival table open", zap.String("dir", a.cfg.ArrivalTable), zap.Uint32("slots", t.Slots()))
	}
}

// swapArrival replaces the table and the counters; the old ones are unmapped when no lookup
// can hold them any more (lookups run under a.mu).
func (a *agent) swapArrival(t *arrival.Table, c *arrival.Counters, id string) {
	a.mu.Lock()
	oldT, oldC := a.arr.Table, a.arrCtr
	a.arr.Table, a.arrCtr, a.arrID, a.arrLast = t, c, id, c.Read()
	a.mu.Unlock()
	_ = oldT.Close()
	_ = oldC.Close()
}

func (a *agent) closeArrival() { a.swapArrival(nil, nil, "") }

// takeArrivals is the period's log fields: the lookups by outcome, the mean wait of the hits
// and what the program's counters gained.  Called with a.mu held; nil when off.
func (a *agent) takeArrivals() []zap.Field {
	if a.arr == nil {
		return nil
	}
	n, wait := a.arr.Take()
	f := make([]zap.Field, 0, int(arrival.NumCauses)+arrival.NumCounters+2)
	for i, v := range n {
		f = append(f, zap.Int(arrival.CauseNames[i], v))
	}
	mean := 0.0
	if n[arrival.Hit] > 0 {
		mean = float64(wait) / float64(n[arrival.Hit]) / 1e3
	}
	f = append(f, zap.Float64("wait_us_mean", mean), zap.Duration("clock_step", a.arr.Clock.TakeStep()))
	c := a.arrCtr.Read()
	for i, v := range c {
		f = append(f, zap.Uint64("bpf_"+arrival.CounterNames[i], v-a.arrLast[i]))
	}
	a.arrLast = c
	return f
}
