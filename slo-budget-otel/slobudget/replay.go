package slobudget

import (
	"math"
	"sort"
)

// Event replay (replay.py): span START and END events in time order through
// one sampler, wall-clock periods of corpus.Period seconds, the collector hard
// cap applied first come first served per period in span units.

// PeriodStats mirrors replay.PeriodStats.
type PeriodStats struct {
	Period   int
	Spans    int
	Requests int
	KeptPre  int
	KeptPost int
	CostPre  float64
	CostPost float64
	Dropped  map[string]int
}

// RunResult holds one decision per span plus the period statistics.
type RunResult struct {
	Decisions []Decision // per span index: Keep, P (stamped), Reason (keep or drop reason)
	Periods   []PeriodStats
	Diag      []PeriodDiag
}

type event struct {
	t     float64
	kind  int // 1 start, 0 end
	depth int // +depth for start, -depth for end
	idx   int
}

// Replay runs the corpus through a fresh Sampler built from cfg, exactly as
// replay.replay does: the same event order, the same period boundaries and the
// same cap.  seed seeds the coin RNG (the Python runs use 1000 + job seed).
func Replay(c *Corpus, cfg Config, vStar, cap float64, seed uint64, oracle *Calibrator) (*RunResult, *Sampler, error) {
	s, err := NewSampler(cfg, c.Graph, c.RoutesSLO, c.Hosts, len(c.Services), vStar, seed, oracle)
	if err != nil {
		return nil, nil, err
	}
	n := len(c.Spans)
	events := make([]event, 0, 2*n)
	for i := range c.Spans {
		sp := &c.Spans[i]
		events = append(events, event{sp.Start, 1, sp.Depth, sp.Idx})
		events = append(events, event{sp.Start + sp.Duration, 0, -sp.Depth, sp.Idx})
	}
	sort.Slice(events, func(i, j int) bool {
		a, b := events[i], events[j]
		if a.t != b.t {
			return a.t < b.t
		}
		if a.kind != b.kind {
			return a.kind < b.kind
		}
		if a.depth != b.depth {
			return a.depth < b.depth
		}
		return a.idx < b.idx
	})
	res := &RunResult{Decisions: make([]Decision, n)}
	for i := range res.Decisions {
		res.Decisions[i].Reason = ""
	}
	reqs := map[uint64]*RequestState{}
	left := map[uint64]int{} // per trace: its spans that have not ended (the request is kept until the last one, as an agent does)
	for i := range c.Spans {
		left[c.Spans[i].TraceID]++
	}
	cur := 0
	st := PeriodStats{Period: 0, Dropped: map[string]int{}}
	costInPeriod := 0.0

	admit := func(idx int, p float64, reason string) {
		cost := 1.0
		if reason == "root_record" {
			cost = cfg.RecordCost
		}
		st.KeptPre++
		st.CostPre += cost
		if costInPeriod+cost > cap+1e-9 {
			st.Dropped["cap"]++
			res.Decisions[idx] = Decision{false, p, "cap"}
			return
		}
		costInPeriod += cost
		st.KeptPost++
		st.CostPost += cost
		res.Decisions[idx] = Decision{true, p, reason}
	}
	finalize := func(per int) {
		s.OnPeriodEnd(per, costInPeriod)
		res.Periods = append(res.Periods, st)
		costInPeriod = 0.0
		st = PeriodStats{Period: per + 1, Dropped: map[string]int{}}
	}
	for _, ev := range events {
		per := int(PyFloorDiv(ev.t-c.T0, c.Period))
		for per > cur {
			finalize(cur)
			cur++
		}
		sp := &c.Spans[ev.idx]
		if ev.kind == 1 {
			if sp.IsRoot {
				req := &RequestState{TraceID: sp.TraceID, Route: sp.Route, Class: sp.ReqClass}
				reqs[sp.TraceID] = req
				st.Requests++
				s.OnRootStart(sp, req)
			} else if _, ok := reqs[sp.TraceID]; !ok { // orphan: its own request
				req := &RequestState{TraceID: sp.TraceID, Route: sp.Route, Class: sp.ReqClass}
				reqs[sp.TraceID] = req
				s.OnRootStart(sp, req)
			}
			continue
		}
		req, ok := reqs[sp.TraceID]
		if !ok {
			req = &RequestState{TraceID: sp.TraceID, Route: sp.Route, Class: sp.ReqClass}
			reqs[sp.TraceID] = req
		}
		d := s.OnEnd(sp, req)
		st.Spans++
		if d.Keep {
			admit(ev.idx, d.P, d.Reason)
		} else {
			st.Dropped[d.Reason]++
			res.Decisions[ev.idx] = d
		}
		if left[sp.TraceID]--; left[sp.TraceID] == 0 {
			delete(reqs, sp.TraceID)
		}
	}
	finalize(cur)
	res.Diag = s.Diag
	return res, s, nil
}

// VolumeTarget is the simulator's V*: volume_frac x spans per period.
func VolumeTarget(c *Corpus, volumeFrac float64) float64 {
	span := math.Max(c.Period, c.TEnd()-c.T0)
	return volumeFrac * float64(len(c.Spans)) * c.Period / span
}
