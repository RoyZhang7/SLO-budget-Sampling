// Package slobudget is the Go port of the Stage 1 decider
// (slo-budget-sim/slo_budget_sim/sampler.py, ttable.py, replay.py): the
// per-service budgets T_i, the per-period calibration, the span-end
// Decide(...) with its N_need rule, request budget, host pool and plan
// sampling, and the period-end refill.  Pure logic, no OpenTelemetry imports;
// the collector processor and the replay harness both call it.  Arithmetic
// follows the Python statement for statement so that the differential test
// can demand bit-identical p values.
package slobudget

import "math"

// Span is one finished span as the decider sees it.
type Span struct {
	Idx       int     // dense index in a corpus (replay only)
	SpanID    uint64
	TraceID   uint64
	ParentIdx int     // -1 for a root (replay); the processor sets IsRoot directly
	IsRoot    bool
	Service   string
	Host      string
	Start     float64 // seconds
	Duration  float64 // seconds, inclusive of callees
	StatusOK  bool
	Depth     int
	Route     string
	ReqClass  string // route + "|" + payload bucket
	NBytes    int
	Transit   float64
	Own       float64 // own (self) time when known, else 0

	ParentSpanID uint64 // the parent's span id (0 for a root): keep_parent forces it
}

// CallGraph is the service-level call structure per route.
type CallGraph struct {
	Root     map[string]string              // route -> root service
	Children map[string]map[string][]string // route -> service -> callees (sorted)
	Parallel map[string]map[string]bool     // route -> service -> callees run concurrently
	// Stages gives, per route and service, the callees as sequential stages: the callees of one
	// stage run concurrently, a stage starts when the one before it has finished.  A service
	// without stages follows the Parallel flag (one stage with every callee, or one per callee).
	Stages map[string]map[string][][]string
}

// groups returns the callees kids of a service (in the order given) as its sequential stages;
// a callee no stage names is a stage of its own, after the others (CallGraph.groups in Python).
func (g *CallGraph) groups(route, svc string, kids []string) [][]string {
	st, ok := g.Stages[route][svc]
	if !ok {
		if len(kids) > 0 && g.parallel(route, svc) {
			return [][]string{kids}
		}
		out := make([][]string, len(kids))
		for i, c := range kids {
			out[i] = []string{c}
		}
		return out
	}
	named := map[string]bool{}
	var out [][]string
	for _, group := range st {
		in := map[string]bool{}
		for _, c := range group {
			named[c], in[c] = true, true
		}
		var gr []string
		for _, c := range kids {
			if in[c] {
				gr = append(gr, c)
			}
		}
		if len(gr) > 0 {
			out = append(out, gr)
		}
	}
	for _, c := range kids {
		if !named[c] {
			out = append(out, []string{c})
		}
	}
	return out
}

func (g *CallGraph) kids(route, svc string) []string {
	if m, ok := g.Children[route]; ok {
		return m[svc]
	}
	return nil
}

func (g *CallGraph) parallel(route, svc string) bool {
	if m, ok := g.Parallel[route]; ok {
		return m[svc]
	}
	return false
}

// Services lists the services of a route in the Python CallGraph.services
// order (depth-first with a stack).
func (g *CallGraph) Services(route string) []string {
	root, ok := g.Root[route]
	if !ok {
		return nil
	}
	var out []string
	seen := map[string]bool{}
	stack := []string{root}
	for len(stack) > 0 {
		s := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
		stack = append(stack, g.kids(route, s)...)
	}
	return out
}

// Corpus is a complete trace set for the replay harness.
type Corpus struct {
	Name      string
	Spans     []Span
	Roots     []int
	RoutesSLO map[string]float64
	Graph     *CallGraph
	Hosts     []string // sorted
	HostSkew  map[string]float64
	Period    float64
	T0        float64
	Services  []string // sorted distinct services
}

// TEnd is the latest span end.
func (c *Corpus) TEnd() float64 {
	t := c.T0
	for i := range c.Spans {
		if e := c.Spans[i].Start + c.Spans[i].Duration; e > t {
			t = e
		}
	}
	return t
}

func maxf(a, b float64) float64 { return math.Max(a, b) }
