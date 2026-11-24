package slobudgetcentralprocessor

import (
	"sort"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
)

// graphLearner builds the service-level call structure per route from the
// kept spans (CallGraph.learn in the Python model: an edge whenever a kept
// child's parent span is also kept; a parent is parallel when its kept
// children overlap in time in most instances).  With a call_graph_file the
// structure is fixed and nothing is learned.
type graphLearner struct {
	graph    *slobudget.CallGraph
	fixed    bool
	edges    map[string]map[string]map[string]bool // route -> parent -> child
	overlap  map[string]map[string][2]int          // route -> parent -> (overlapping instances, instances)
	spans    map[pcommon.SpanID]seenSpan            // recent kept spans by span id
	children map[pcommon.SpanID][]seenSpan          // kept children waiting for / seen under a parent
	roots    map[string]map[string]int              // route -> service -> root count
	order    []pcommon.SpanID
}

type seenSpan struct {
	route, service string
	start, end     float64
	t              time.Time
}

func newGraphLearner(file string, routesSLO map[string]float64) (*graphLearner, error) {
	gl := &graphLearner{edges: map[string]map[string]map[string]bool{}, overlap: map[string]map[string][2]int{},
		spans: map[pcommon.SpanID]seenSpan{}, children: map[pcommon.SpanID][]seenSpan{}, roots: map[string]map[string]int{}}
	if file != "" {
		g, err := LoadCallGraph(file)
		if err != nil {
			return nil, err
		}
		gl.graph, gl.fixed = g, true
		return gl, nil
	}
	gl.graph = &slobudget.CallGraph{Root: map[string]string{}, Children: map[string]map[string][]string{}, Parallel: map[string]map[string]bool{}}
	return gl, nil
}

func (gl *graphLearner) observe(route, svc string, tid pcommon.TraceID, sid, parent pcommon.SpanID, start, end float64, isRoot bool) {
	if gl.fixed {
		return
	}
	now := time.Now()
	me := seenSpan{route, svc, start, end, now}
	gl.spans[sid] = me
	gl.order = append(gl.order, sid)
	if isRoot {
		m := gl.roots[route]
		if m == nil {
			m = map[string]int{}
			gl.roots[route] = m
		}
		m[svc]++
	} else {
		if p, ok := gl.spans[parent]; ok {
			gl.edge(route, p.service, svc)
		}
		gl.children[parent] = append(gl.children[parent], me)
	}
	// children that arrived before this span (they end first, so this is the common order)
	for _, ch := range gl.children[sid] {
		gl.edge(route, svc, ch.service)
	}
	if kids := gl.children[sid]; len(kids) >= 2 {
		sort.Slice(kids, func(i, j int) bool { return kids[i].start < kids[j].start })
		ov := 0
		for i := 0; i+1 < len(kids); i++ {
			if kids[i+1].start < kids[i].end {
				ov = 1
				break
			}
		}
		m := gl.overlap[route]
		if m == nil {
			m = map[string][2]int{}
			gl.overlap[route] = m
		}
		c := m[svc]
		c[0] += ov
		c[1]++
		m[svc] = c
	}
	// bound the memory: forget spans older than 30 s
	if len(gl.order) > 200000 {
		cut := now.Add(-30 * time.Second)
		keep := gl.order[:0]
		for _, id := range gl.order {
			if s, ok := gl.spans[id]; ok && s.t.Before(cut) {
				delete(gl.spans, id)
				delete(gl.children, id)
				continue
			}
			keep = append(keep, id)
		}
		gl.order = keep
	}
}

func (gl *graphLearner) edge(route, parent, child string) {
	if parent == child {
		return
	}
	m := gl.edges[route]
	if m == nil {
		m = map[string]map[string]bool{}
		gl.edges[route] = m
	}
	if m[parent] == nil {
		m[parent] = map[string]bool{}
	}
	m[parent][child] = true
}

// rebuild writes the learned structure into the CallGraph the calibrator uses.
func (gl *graphLearner) rebuild() {
	if gl.fixed {
		return
	}
	for route, byRoot := range gl.roots {
		best, bestN := "", 0
		for svc, n := range byRoot {
			if n > bestN {
				best, bestN = svc, n
			}
		}
		gl.graph.Root[route] = best
		kids := map[string][]string{}
		par := map[string]bool{}
		for parent, cs := range gl.edges[route] {
			for c := range cs {
				kids[parent] = append(kids[parent], c)
			}
			sort.Strings(kids[parent])
		}
		for svc, c := range gl.overlap[route] {
			par[svc] = c[0]*2 > c[1]
		}
		gl.graph.Children[route] = kids
		gl.graph.Parallel[route] = par
	}
}

func (gl *graphLearner) export() map[string]any {
	return map[string]any{"fixed": gl.fixed, "root": gl.graph.Root, "children": gl.graph.Children, "parallel": gl.graph.Parallel, "stages": gl.graph.Stages}
}
