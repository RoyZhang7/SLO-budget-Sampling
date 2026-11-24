package slobudget

import (
	"encoding/json"
	"fmt"
	"os"
)

// Replay dumps written by `python -m slo_budget_sim.cli dump`: the corpus
// with its replay context (events.json) and the Python decisions
// (decisions.json), the input of the differential test.

// EventsDump is the corpus plus everything replay() needs.
type EventsDump struct {
	Name      string             `json:"name"`
	Scenario  string             `json:"scenario"`
	Seed      int                `json:"seed"`
	Period    float64            `json:"period"`
	T0        float64            `json:"t0"`
	VStar     float64            `json:"v_star"`
	Cap       float64            `json:"cap"`
	RNGSeed   uint64             `json:"rng_seed"`
	RoutesSLO map[string]float64 `json:"routes_slo"`
	Graph     struct {
		Root     map[string]string                `json:"root"`
		Children map[string]map[string][]string   `json:"children"`
		Parallel map[string]map[string]bool       `json:"parallel"`
		Stages   map[string]map[string][][]string `json:"stages"`
	} `json:"graph"`
	Hosts    []string           `json:"hosts"`
	HostSkew map[string]float64 `json:"host_skew"`
	Services []string           `json:"services"`
	Arm      string             `json:"arm"`
	Config   Config             `json:"config"`
	Spans    struct {
		SpanID    []uint64  `json:"span_id"`
		TraceID   []uint64  `json:"trace_id"`
		ParentIdx []int     `json:"parent_idx"`
		Service   []string  `json:"service"`
		Host      []string  `json:"host"`
		Start     []float64 `json:"start"`
		Duration  []float64 `json:"duration"`
		StatusOK  []bool    `json:"status_ok"`
		Depth     []int     `json:"depth"`
		Route     []string  `json:"route"`
		ReqClass  []string  `json:"req_class"`
		NBytes    []int     `json:"nbytes"`
		Transit   []float64 `json:"transit"`
		Own       []float64 `json:"own"`
		Wait      []float64 `json:"wait"` // absent: no span has an arrival
	} `json:"spans"`
	// the oracle table (route, class, service -> T) when the arm uses calib_source oracle
	Oracle map[string]map[string]map[string]float64 `json:"oracle,omitempty"`
}

// DecisionsDump is what the Python replay decided.
type DecisionsDump struct {
	Keep    []bool    `json:"keep"`
	P       []float64 `json:"p"`
	Reason  []string  `json:"reason"`
	NNeed   float64   `json:"n_need"`
	Periods []struct {
		Period          int     `json:"period"`
		B               float64 `json:"B"`
		CollectorVolume float64 `json:"collector_volume"`
		Kept            int     `json:"kept"`
		CostPost        float64 `json:"cost_post"`
	} `json:"periods"`
}

// LoadEvents reads an events dump into a Corpus and its replay context.
func LoadEvents(path string) (*EventsDump, *Corpus, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var d EventsDump
	d.Config = DefaultConfig()
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	n := len(d.Spans.SpanID)
	c := &Corpus{Name: d.Name, RoutesSLO: d.RoutesSLO, Hosts: d.Hosts, HostSkew: d.HostSkew, Period: d.Period, T0: d.T0,
		Services: d.Services, Spans: make([]Span, n)}
	c.Graph = &CallGraph{Root: d.Graph.Root, Children: d.Graph.Children, Parallel: d.Graph.Parallel, Stages: d.Graph.Stages}
	for i := 0; i < n; i++ {
		c.Spans[i] = Span{Idx: i, SpanID: d.Spans.SpanID[i], TraceID: d.Spans.TraceID[i], ParentIdx: d.Spans.ParentIdx[i],
			IsRoot: d.Spans.ParentIdx[i] < 0, Service: d.Spans.Service[i], Host: d.Spans.Host[i], Start: d.Spans.Start[i],
			Duration: d.Spans.Duration[i], StatusOK: d.Spans.StatusOK[i], Depth: d.Spans.Depth[i], Route: d.Spans.Route[i],
			ReqClass: d.Spans.ReqClass[i], NBytes: d.Spans.NBytes[i], Transit: d.Spans.Transit[i], Own: d.Spans.Own[i]}
		if len(d.Spans.Wait) == n {
			c.Spans[i].Wait = d.Spans.Wait[i]
		}
		if c.Spans[i].IsRoot {
			c.Roots = append(c.Roots, i)
		}
	}
	for i := range c.Spans { // keep_parent forces the parent by span id
		if p := c.Spans[i].ParentIdx; p >= 0 {
			c.Spans[i].ParentSpanID = c.Spans[p].SpanID
		}
	}
	return &d, c, nil
}

// LoadDecisions reads a decisions dump.
func LoadDecisions(path string) (*DecisionsDump, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var d DecisionsDump
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &d, nil
}

// OracleFromDump rebuilds an oracle Calibrator from the dumped table.
func OracleFromDump(d *EventsDump, graph *CallGraph) *Calibrator {
	if d.Oracle == nil {
		return nil
	}
	c, _ := NewCalibrator(graph, d.RoutesSLO, 0.95, MinSamples, 1, true, "none")
	for route, byCls := range d.Oracle {
		for cls, bySvc := range byCls {
			for svc, T := range bySvc {
				c.Table[TableKey{route, cls, svc}] = T
				c.Calibrated[[2]string{route, svc}] = true
			}
		}
	}
	return c
}

// Mismatch describes the first divergence between two decision streams.
type Mismatch struct {
	Idx            int
	Service, Host  string
	Period         int
	Python, Go     Decision
	PythonB, GoB   float64
	PeriodsChecked int
}

func (m *Mismatch) Error() string {
	return fmt.Sprintf("span %d (%s on %s, period %d): python %+v, go %+v; B python %.17g go %.17g",
		m.Idx, m.Service, m.Host, m.Period, m.Python, m.Go, m.PythonB, m.GoB)
}

// Compare checks the Go replay against the Python decisions span by span (keep,
// reason, exact p) and the per-period B.  It returns nil when identical.
func Compare(c *Corpus, res *RunResult, py *DecisionsDump) error {
	for i, d := range res.Decisions {
		pd := Decision{Keep: py.Keep[i], P: py.P[i], Reason: py.Reason[i]}
		if d.Keep != pd.Keep || d.Reason != pd.Reason || (d.Keep && d.P != pd.P) {
			per := int(PyFloorDiv(c.Spans[i].Start+c.Spans[i].Duration-c.T0, c.Period))
			m := &Mismatch{Idx: i, Service: c.Spans[i].Service, Host: c.Spans[i].Host, Period: per, Python: pd, Go: d}
			if per < len(py.Periods) && per < len(res.Diag) {
				m.PythonB, m.GoB = py.Periods[per].B, res.Diag[per].B
			}
			return m
		}
	}
	for k := range py.Periods {
		if k >= len(res.Diag) {
			break
		}
		if py.Periods[k].B != res.Diag[k].B {
			return fmt.Errorf("period %d: B python %.17g go %.17g (decisions identical)", k, py.Periods[k].B, res.Diag[k].B)
		}
	}
	return nil
}
