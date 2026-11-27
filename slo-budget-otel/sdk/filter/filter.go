// Package filter is the in-service filter's decision (prepared, not deployed): the service exports only the spans the
// agent could keep, so that the application stops paying to export spans the agent drops.  The agent stays the decision
// maker; the filter is conservative.  At a span's end:
//
//   - a root span is exported;
//   - a client span is exported when the plan coin of its id falls under the rate (the agent re-draws the same coin at the
//     exact rate; the filter uses the highest rate of the last two periods it saw);
//   - a server span is exported when it has an error status, when its within-service latency (its end minus the request's
//     arrival at the pod minus its downstream time) is over its threshold b WITHOUT the margin (the slack covers a threshold
//     that is one period stale), when the plan coin selects it, or when the arrival is unknown (no table entry);
//   - an exported server span carries the latency and the downstream time it computed (slobudget.ws_us,
//     slobudget.downstream_us) and the ordinary spans of its service dropped since the service's last exported span
//     (slobudget.dropped_before), so the agent needs no hold for its client spans and counts the dropped ones;
//   - at each period's end the service sends the dropped spans still uncounted as a count record (a span of its own with
//     slobudget.count_record, the period and slobudget.dropped_before; the agent counts it and drops it), so that a quiet
//     service's drops reach the agent in their period;
//   - the plan coin is compared with the rate times a headroom of 1.10 (the agent applies the exact rate with the same
//     coin, so the kept set does not change; the headroom covers a rate that rose since the filter's table).
//
// With the filter on, every draw of the agent is a function of the span id too (slobudget coin_source: span).
//
// The thresholds and the plan rates come with the agent's budget poll (../../processor/slobudgetprocessor, /slobudget/sdk).
// The fork's span processor (../blueprint/slo_filter_processor.go) is a thin adapter around this package.
package filter

import (
	"math"
	"sort"
	"sync"
	"time"
)

// Attributes an exported server span carries.
const (
	AttrWithinUS     = "slobudget.ws_us"          // the within-service latency the filter computed, us
	AttrDownstreamUS = "slobudget.downstream_us"  // the union of the span's own client spans, us
	AttrDropped      = "slobudget.dropped_before" // ordinary spans of the service dropped since its last exported span
	AttrCountRecord  = "slobudget.count_record"   // true on the period-end record (its dropped_before is the count)
	AttrCountPeriod  = "slobudget.count_period"   // the period the record closes
	CountSpanName    = "slobudget.count"          // the record's span name

	// RateHeadroom: the plan coin passes under rate x headroom (the agent decides at the exact rate with the same coin).
	RateHeadroom = 1.10 // 2026-11-26: raised from 1.05 (the proof on the 100 % exports still dropped kept spans at 1.05)
)

// Poll is what the agent serves at /slobudget/sdk when the filter is on (a superset of the issuer's).
type Poll struct {
	Version int                                      `json:"version"`
	Period  int64                                    `json:"period"`
	PeriodS float64                                  `json:"period_s"` // the period length, s (the count records' clock)
	BTable  map[string]map[string]map[string]float64 `json:"btable"`   // route -> class -> service -> b (s)
	PlanP   map[string]float64                       `json:"plan_p"`   // service -> ordinary-span rate
}

// Interval is a client span's [start, end) in unix ns.
type Interval struct{ Start, End int64 }

// Union is the length of the union of the intervals, ns.
func Union(iv []Interval) int64 {
	if len(iv) == 0 {
		return 0
	}
	s := make([]Interval, len(iv))
	copy(s, iv)
	sort.Slice(s, func(i, j int) bool { return s[i].Start < s[j].Start })
	var tot int64
	cs, ce := s[0].Start, s[0].End
	for _, x := range s[1:] {
		if x.Start > ce {
			tot += ce - cs
			cs, ce = x.Start, x.End
		} else if x.End > ce {
			ce = x.End
		}
	}
	return tot + ce - cs
}

// CoinU is the plan coin's value for a span id, in [0, 1): the same mix as slobudget.CoinU and the Python coin_u.
func CoinU(spanID uint64) float64 {
	z := spanID
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	z ^= z >> 31
	return float64(z>>11) / float64(uint64(1)<<53)
}

// State is the filter's view of the agent's tables, and the counts it keeps between exports.
type State struct {
	Service string // this process's service name, as the agent names it

	mu       sync.Mutex
	polls    [2]Poll // the last two distinct versions, newest first
	n        int
	received [2]time.Time
	dropped  map[string]int // ordinary spans dropped per service since the last exported span (one service here, roots apart)
	clients  map[uint64][]Interval
	stats    Stats
	period   int64 // the period the filter is in (0: not known yet)
}

// CountRecord is what the service sends at a period's end: the dropped spans not yet counted on an exported span.
type CountRecord struct {
	Period  int64
	Dropped int
}

// Stats counts the filter's outcomes.
type Stats struct {
	Roots, Clients, ClientsDropped, Servers, Exported, Errors, Outliers, Plan, NoArrival, NoThreshold, Dropped int64
	CountRecords                                                                                               int64         // period-end records sent
	Staleness                                                                                                  time.Duration // the newest version's age at the last decision
}

// Decision says whether to export a server span and what to stamp on it.
type Decision struct {
	Export     bool
	Reason     string // root | error | outlier | plan | no_arrival | no_threshold | client_plan | dropped
	WithinNs   int64  // the within-service latency, ns (server spans with an arrival)
	Downstream int64  // the union of the span's client spans, ns
	Dropped    int    // the ordinary spans of the service dropped before this one (stamped on exported server spans)
}

func New(service string) *State {
	return &State{Service: service, dropped: map[string]int{}, clients: map[uint64][]Interval{}}
}

// Update takes a poll's answer; versions seen already are ignored.
func (s *State) Update(p Poll, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.n > 0 && p.Version <= s.polls[0].Version {
		return
	}
	s.polls[1], s.received[1] = s.polls[0], s.received[0]
	s.polls[0], s.received[0] = p, now
	if s.n < 2 {
		s.n++
	}
}

// Rate is the plan rate the filter uses for a service: the highest of the last two periods' (so that a rate that fell
// is applied late, never one that rose) times RateHeadroom, or 1 before any poll (everything exported).
func (s *State) rate(service string) float64 {
	if s.n == 0 {
		return 1.0
	}
	best := 0.0
	for i := 0; i < s.n; i++ {
		if p, ok := s.polls[i].PlanP[service]; ok && p > best {
			best = p
		} else if !ok {
			return 1.0 // a service the central has no rate for yet: export
		}
	}
	return math.Min(1.0, best*RateHeadroom)
}

// PeriodEnded says whether a period ended since the last call and, if dropped spans are still uncounted, the record to
// send for it.  The period clock is the poll's period_s on the wall clock; before a poll with it nothing is sent.
func (s *State) PeriodEnded(now time.Time) (CountRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.n == 0 || s.polls[0].PeriodS <= 0 {
		return CountRecord{}, false
	}
	cur := int64(math.Floor(float64(now.UnixNano()) / 1e9 / s.polls[0].PeriodS))
	if s.period == 0 {
		s.period = cur
		return CountRecord{}, false
	}
	if cur <= s.period {
		return CountRecord{}, false
	}
	rec := CountRecord{Period: s.period, Dropped: s.dropped[s.Service]}
	s.period = cur
	if rec.Dropped == 0 {
		return CountRecord{}, false
	}
	s.dropped[s.Service] = 0
	s.stats.CountRecords++
	return rec, true
}

// threshold is b for (route, class, service) from the newest poll, with the route's "*" class as the fallback.
func (s *State) threshold(route, class, service string) (float64, bool) {
	if s.n == 0 {
		return 0, false
	}
	byCls := s.polls[0].BTable[route]
	if byCls == nil {
		return 0, false
	}
	for _, c := range []string{class, "*"} {
		if b, ok := byCls[c][service]; ok && b > 0 {
			return b, true
		}
	}
	return 0, false
}

// ClientEnded records a client span of this process for its server span's downstream time, and decides it.
func (s *State) ClientEnded(spanID, parentID uint64, start, end int64) Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.Clients++
	s.clients[parentID] = append(s.clients[parentID], Interval{start, end})
	p := s.rate(s.Service)
	if p >= 1.0 || CoinU(spanID) < p {
		s.stats.Exported++
		return Decision{Export: true, Reason: "client_plan"}
	}
	s.stats.ClientsDropped++
	s.dropped[s.Service]++
	return Decision{Reason: "dropped"}
}

// ServerEnded decides a server span.  arrivalNs is the request's arrival at the pod from the eBPF table (0: no entry).
func (s *State) ServerEnded(spanID uint64, isRoot bool, route, class string, start, end, arrivalNs int64, isError bool, now time.Time) Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	iv := s.clients[spanID]
	delete(s.clients, spanID)
	d := Decision{Downstream: Union(iv)}
	if s.n > 0 {
		s.stats.Staleness = now.Sub(s.received[0])
	}
	if isRoot {
		s.stats.Roots++
		d.Export, d.Reason = true, "root"
		return s.export(d)
	}
	s.stats.Servers++
	switch {
	case isError:
		d.Export, d.Reason = true, "error"
		s.stats.Errors++
	case arrivalNs <= 0 || arrivalNs > start:
		d.Export, d.Reason = true, "no_arrival"
		s.stats.NoArrival++
	default:
		d.WithinNs = end - arrivalNs - d.Downstream
		b, ok := s.threshold(route, class, s.Service)
		switch {
		case !ok:
			d.Export, d.Reason = true, "no_threshold"
			s.stats.NoThreshold++
		case float64(d.WithinNs)/1e9 > b: // no margin: the slack for a threshold one period stale
			d.Export, d.Reason = true, "outlier"
			s.stats.Outliers++
		default:
			if p := s.rate(s.Service); p >= 1.0 || CoinU(spanID) < p {
				d.Export, d.Reason = true, "plan"
				s.stats.Plan++
			}
		}
	}
	if !d.Export {
		s.stats.Dropped++
		s.dropped[s.Service]++
		d.Reason = "dropped"
		return d
	}
	return s.export(d)
}

func (s *State) export(d Decision) Decision {
	s.stats.Exported++
	d.Dropped = s.dropped[s.Service]
	s.dropped[s.Service] = 0
	return d
}

// Stats is a copy of the counts.
func (s *State) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// Forget drops the client intervals of a server span that will never end here (a leak guard for the adapter's sweep).
func (s *State) Forget(serverSpanID uint64) {
	s.mu.Lock()
	delete(s.clients, serverSpanID)
	s.mu.Unlock()
}

// Pending is how many server spans have client intervals waiting.
func (s *State) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clients)
}
