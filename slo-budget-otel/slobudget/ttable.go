package slobudget

import (
	"fmt"
	"math"
	"sort"
)

// Per-service latency budgets T_i: the Erms square-root split of the root's
// end-to-end SLO over the own-time quantiles along the route's call structure,
// normalised so that T_root = SLO (ttable.py).

const (
	EpsOwn     = 1e-4 // 0.1 ms floor for an own-time estimate
	MinSamples = 8    // fewer kept samples than this gives no L at all
)

type vw struct{ v, w float64 }

// WeightedQuantile is ttable.weighted_quantile: the q-quantile of values with
// positive weights, Horvitz-Thompson style.
func WeightedQuantile(values, weights []float64, q float64) float64 {
	if len(values) == 0 {
		panic("no samples")
	}
	pairs := make([]vw, len(values))
	for i := range values {
		pairs[i] = vw{values[i], weights[i]}
	}
	sort.SliceStable(pairs, func(i, j int) bool {
		if pairs[i].v != pairs[j].v {
			return pairs[i].v < pairs[j].v
		}
		return pairs[i].w < pairs[j].w
	})
	ws := make([]float64, len(pairs))
	for i, p := range pairs {
		ws[i] = p.w
	}
	total := PySum(ws)
	target := q * total
	acc := 0.0
	for _, p := range pairs {
		acc += p.w
		if acc >= target {
			return p.v
		}
	}
	return pairs[len(pairs)-1].v
}

// orderedMap keeps Python dict insertion order where the order of a float sum
// matters (or where the Python code iterates and the order shows).
type orderedMap struct {
	keys []string
	vals map[string]float64
}

func newOrderedMap() *orderedMap { return &orderedMap{vals: map[string]float64{}} }

func (m *orderedMap) set(k string, v float64) {
	if _, ok := m.vals[k]; !ok {
		m.keys = append(m.keys, k)
	}
	m.vals[k] = v
}

func (m *orderedMap) get(k string) (float64, bool) { v, ok := m.vals[k]; return v, ok }
func (m *orderedMap) has(k string) bool            { _, ok := m.vals[k]; return ok }
func (m *orderedMap) len() int                     { return len(m.keys) }

func ownFromInclusive(L *orderedMap, g *CallGraph, route string) *orderedMap {
	own := newOrderedMap()
	for _, s := range L.keys {
		Ls := L.vals[s]
		var kids []string
		for _, c := range g.kids(route, s) {
			if L.has(c) && c != s {
				kids = append(kids, c)
			}
		}
		cover := 0.0
		if len(kids) > 0 {
			var longest []float64 // of every stage
			for _, group := range g.groups(route, s, kids) {
				m := L.vals[group[0]]
				for _, c := range group[1:] {
					if L.vals[c] > m {
						m = L.vals[c]
					}
				}
				longest = append(longest, m)
			}
			cover = PySum(longest)
		}
		own.set(s, math.Max(EpsOwn, Ls-cover))
	}
	return own
}

// SplitBudgets is ttable.split_budgets: (inclusive T per service, stage budget
// b per service), both scaled so that T_root = SLO.  L are inclusive and O own
// quantiles per service of one route; O may be nil.
// SplitBudgets is ttable.split_budgets: (inclusive T per service, stage budget b per
// service) scaled so that T_root = SLO.  Services in heldT (frozen) keep that T; the other
// services' stage budgets are scaled so that T_root = SLO over them.
func SplitBudgets(slo float64, L *orderedMap, g *CallGraph, route string, O *orderedMap, heldT, heldB *orderedMap) (*orderedMap, *orderedMap) {
	root, ok := g.Root[route]
	if !ok || !L.has(root) {
		return newOrderedMap(), newOrderedMap()
	}
	own := ownFromInclusive(L, g, route)
	if O != nil && O.len() > 0 {
		for _, s := range own.keys {
			if v, ok := O.get(s); ok {
				own.vals[s] = math.Max(EpsOwn, v)
			}
		}
	}
	kidsOf := func(s string) []string {
		var out []string
		for _, c := range g.kids(route, s) {
			if own.has(c) && c != s {
				out = append(out, c)
			}
		}
		return out
	}
	// contributing set: the longest callee of every stage (every callee of a sequential node,
	// only the longest callee of a parallel node)
	contrib := map[string]bool{}
	stack := []string{root}
	for len(stack) > 0 {
		s := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if contrib[s] {
			continue
		}
		contrib[s] = true
		ks := kidsOf(s)
		if len(ks) == 0 {
			continue
		}
		for _, group := range g.groups(route, s, ks) {
			best := group[0]
			for _, c := range group[1:] {
				if L.vals[c] > L.vals[best] {
					best = c
				}
			}
			stack = append(stack, best)
		}
	}
	contribSorted := make([]string, 0, len(contrib))
	for s := range contrib {
		contribSorted = append(contribSorted, s)
	}
	sort.Strings(contribSorted)
	roots := make([]float64, len(contribSorted))
	for i, s := range contribSorted {
		roots[i] = math.Sqrt(own.vals[s])
	}
	W := PySum(roots)
	b := newOrderedMap()
	for _, s := range own.keys {
		b.set(s, slo*math.Sqrt(own.vals[s])/W)
	}
	held := map[string]float64{} // frozen services keep their T
	if heldT != nil {
		for _, s := range heldT.keys {
			if own.has(s) && s != root {
				held[s] = heldT.vals[s]
			}
		}
	}
	// tWith: T per service with the unfrozen stage budgets scaled by k; held services keep their T
	tWith := func(k float64) *orderedMap {
		T := newOrderedMap()
		visiting := map[string]bool{}
		var t func(s string) float64
		t = func(s string) float64 {
			if v, ok := T.get(s); ok {
				return v
			}
			if v, ok := held[s]; ok {
				T.set(s, v)
				return v
			}
			if visiting[s] {
				return k * b.vals[s]
			}
			visiting[s] = true
			ks := kidsOf(s)
			var v float64
			if len(ks) == 0 {
				v = k * b.vals[s]
			} else { // the stages run one after the other, the callees of a stage together
				var parts []float64
				for _, group := range g.groups(route, s, ks) {
					m := t(group[0])
					for _, c := range group[1:] {
						if x := t(c); x > m {
							m = x
						}
					}
					parts = append(parts, m)
				}
				v = k*b.vals[s] + PySum(parts)
			}
			delete(visiting, s)
			T.set(s, v)
			return v
		}
		for _, s := range own.keys {
			t(s)
		}
		return T
	}
	if len(held) == 0 || tWith(0.0).vals[root] >= slo {
		// no frozen service (or the frozen ones alone exceed the SLO): the plain split, T_root = SLO
		T := tWith(1.0)
		scale := slo / T.vals[root]
		Tout, bout := newOrderedMap(), newOrderedMap()
		for _, s := range T.keys {
			Tout.set(s, T.vals[s]*scale)
		}
		for _, s := range b.keys {
			bout.set(s, b.vals[s]*scale)
		}
		return Tout, bout
	}
	// frozen services keep their T; the unfrozen stage budgets are scaled by k so that
	// T_root = SLO (T_root is increasing and piecewise linear in k: bisection, 60 steps)
	lo, hi := 0.0, 1.0
	for tWith(hi).vals[root] < slo && hi < 1e6 {
		hi *= 2.0
	}
	for i := 0; i < 60; i++ {
		mid := (lo + hi) / 2.0
		if tWith(mid).vals[root] < slo {
			lo = mid
		} else {
			hi = mid
		}
	}
	k := hi
	T := tWith(k)
	bout := newOrderedMap()
	for _, s := range b.keys {
		if _, isHeld := held[s]; isHeld && heldB != nil {
			if v, ok := heldB.get(s); ok {
				bout.set(s, v)
				continue
			}
		}
		bout.set(s, k*b.vals[s])
	}
	return T, bout
}

// EqualSharePrior is ttable.equal_share_prior: the split with every own time
// taken as equal (the cold-start prior).
func EqualSharePrior(slo float64, g *CallGraph, route string) map[string]float64 {
	one := newOrderedMap()
	for _, s := range g.Services(route) {
		one.set(s, 1.0)
	}
	T, _ := SplitBudgets(slo, one, g, route, one, nil, nil)
	out := map[string]float64{}
	for _, s := range T.keys {
		out[s] = T.vals[s]
	}
	return out
}

// TableKey is (route, class, service); class "*" is the route-level entry.
type TableKey struct{ Route, Class, Service string }

type samples struct{ d, o, w []float64 }

// Calibrator is ttable.Calibrator: builds the T table each period from the
// kept spans of the last `window` periods, reweighted by 1/p.
type Calibrator struct {
	Graph           *CallGraph
	RoutesSLO       map[string]float64
	Q               float64
	MinClassSamples int
	Window          int
	UseClass        bool

	buf   []map[TableKey]*samples // the last `window` closed periods, oldest first
	cur   map[TableKey]*samples
	Table map[TableKey]float64
	B     map[TableKey]float64 // stage budgets of the last split
	L     map[TableKey]float64
	O     map[TableKey]float64
	// Freeze holds the services whose previous L, O are reused at the next rebuild.
	Freeze     map[string]bool
	// UseOwn feeds the observed own-time quantiles O into the split (the simulator's
	// default); false derives own time from the inclusive quantiles (L minus the
	// callees' L), for stacks where the stamped own time is unreliable.
	UseOwn     bool
	Version    int
	PriorTable map[[2]string]float64 // (route, service) -> T before the first calibration
	PriorB     map[[2]string]float64 // (route, service) -> own-time share of the prior split (cold_prior equal)
	Calibrated map[[2]string]bool    // (route, service) calibrated at least once
	PriorHits  int
}

// NewCalibrator mirrors Calibrator.__init__; prior is "none", "equal" or
// "file" (seeded through SeedPrior).
func NewCalibrator(g *CallGraph, routesSLO map[string]float64, q float64, minClassSamples, window int, useClass bool, prior string) (*Calibrator, error) {
	if window < 1 {
		window = 1
	}
	c := &Calibrator{Graph: g, RoutesSLO: routesSLO, Q: q, MinClassSamples: minClassSamples, Window: window, UseClass: useClass, UseOwn: true,
		cur: map[TableKey]*samples{}, Table: map[TableKey]float64{}, B: map[TableKey]float64{}, L: map[TableKey]float64{},
		O: map[TableKey]float64{}, Freeze: map[string]bool{}, PriorTable: map[[2]string]float64{}, PriorB: map[[2]string]float64{},
		Calibrated: map[[2]string]bool{}}
	switch prior {
	case "none", "file":
	case "equal":
		for route, slo := range routesSLO {
			if _, ok := g.Root[route]; !ok {
				continue
			}
			one := newOrderedMap()
			for _, s := range g.Services(route) {
				one.set(s, 1.0)
			}
			Tp, bp := SplitBudgets(slo, one, g, route, one, nil, nil) // equal_share_prior and its shares
			for _, svc := range Tp.keys {
				c.PriorTable[[2]string{route, svc}] = Tp.vals[svc]
			}
			for _, svc := range bp.keys {
				c.PriorB[[2]string{route, svc}] = bp.vals[svc]
			}
		}
	default:
		return nil, fmt.Errorf("unknown prior %q; choose from none, equal, file", prior)
	}
	return c, nil
}

// SeedPrior installs T values (route -> service -> T) used until the
// service's first calibrated period (the "seed the table from a file" option).
func (c *Calibrator) SeedPrior(table map[string]map[string]float64) {
	for route, m := range table {
		for svc, T := range m {
			c.PriorTable[[2]string{route, svc}] = T
		}
	}
}

// Observe is Calibrator.observe.
func (c *Calibrator) Observe(sp *Span, p float64) {
	cls := "*"
	if c.UseClass {
		cls = sp.ReqClass
	}
	k := TableKey{sp.Route, cls, sp.Service}
	s := c.cur[k]
	if s == nil {
		s = &samples{}
		c.cur[k] = s
	}
	s.d = append(s.d, sp.Duration)
	own := sp.Own
	if !(own > 0.0) {
		own = sp.Duration
	}
	s.o = append(s.o, own)
	w := 1.0
	if p > 0 {
		w = 1.0 / p
	}
	s.w = append(s.w, w)
}

// DiscardPeriod is Calibrator.discard_period: drop the open period's samples without a
// rebuild.  For a decider whose table comes from elsewhere (an agent under a central), which
// never closes a period of its own.
func (c *Calibrator) DiscardPeriod() { c.cur = map[TableKey]*samples{} }

// EndPeriod is Calibrator.end_period: close the period, purge the frozen
// services' samples of this period, rebuild from the window.  frozen == nil
// keeps the previous freeze set.
func (c *Calibrator) EndPeriod(frozen map[string]bool) {
	if frozen != nil {
		c.Freeze = map[string]bool{}
		for s := range frozen {
			c.Freeze[s] = true
		}
	}
	if len(c.Freeze) > 0 {
		for k := range c.cur {
			if c.Freeze[k.Service] {
				delete(c.cur, k)
			}
		}
	}
	c.buf = append(c.buf, c.cur)
	if len(c.buf) > c.Window {
		c.buf = c.buf[len(c.buf)-c.Window:]
	}
	c.cur = map[TableKey]*samples{}
	merged := map[TableKey]*samples{}
	for _, per := range c.buf {
		for k, s := range per {
			m := merged[k]
			if m == nil {
				m = &samples{}
				merged[k] = m
			}
			m.d = append(m.d, s.d...)
			m.o = append(m.o, s.o...)
			m.w = append(m.w, s.w...)
		}
	}
	c.rebuild(merged)
}

func (c *Calibrator) rebuild(merged map[TableKey]*samples) {
	pooled := map[[2]string]*samples{}
	for k, s := range merged {
		pk := [2]string{k.Route, k.Service}
		p := pooled[pk]
		if p == nil {
			p = &samples{}
			pooled[pk] = p
		}
		p.d = append(p.d, s.d...)
		p.o = append(p.o, s.o...)
		p.w = append(p.w, s.w...)
	}
	keys := map[[2]string]bool{}
	for k := range merged {
		keys[[2]string{k.Route, k.Class}] = true
		keys[[2]string{k.Route, "*"}] = true
	}
	for k := range c.L { // a frozen service whose samples were purged still needs its keys rebuilt
		keys[[2]string{k.Route, k.Class}] = true
	}
	table, btab, Ltab, Otab := map[TableKey]float64{}, map[TableKey]float64{}, map[TableKey]float64{}, map[TableKey]float64{}
	empty := &samples{}
	for rc := range keys {
		route, cls := rc[0], rc[1]
		slo, ok := c.RoutesSLO[route]
		if !ok {
			continue
		}
		if _, ok := c.Graph.Root[route]; !ok {
			continue
		}
		L, O := newOrderedMap(), newOrderedMap()
		heldT, heldB := newOrderedMap(), newOrderedMap()
		for _, svc := range c.Graph.Services(route) {
			if c.Freeze[svc] {
				// frozen source: the service keeps its L, O (the split's shares) and its T itself
				// (Stage 2c: the renormalisation runs over the unfrozen services only)
				pL, okL := c.L[TableKey{route, cls, svc}]
				if !okL {
					pL, okL = c.L[TableKey{route, "*", svc}]
				}
				pO, okO := c.O[TableKey{route, cls, svc}]
				if !okO {
					pO, okO = c.O[TableKey{route, "*", svc}]
				}
				if okL && okO {
					L.set(svc, pL)
					O.set(svc, pO)
					hT, okT := c.Table[TableKey{route, cls, svc}]
					if !okT {
						hT, okT = c.Table[TableKey{route, "*", svc}]
					}
					if okT {
						heldT.set(svc, hT)
						hb, okb := c.B[TableKey{route, cls, svc}]
						if !okb {
							hb, okb = c.B[TableKey{route, "*", svc}]
						}
						if okb {
							heldB.set(svc, hb)
						}
					}
					continue
				}
			}
			s := empty
			if cls != "*" {
				if m := merged[TableKey{route, cls, svc}]; m != nil {
					s = m
				}
				if len(s.d) < c.MinClassSamples {
					s = empty
					if p := pooled[[2]string{route, svc}]; p != nil {
						s = p
					}
				}
			} else if p := pooled[[2]string{route, svc}]; p != nil {
				s = p
			}
			if len(s.d) >= MinSamples {
				L.set(svc, WeightedQuantile(s.d, s.w, c.Q))
				O.set(svc, WeightedQuantile(s.o, s.w, c.Q))
			}
		}
		Oused := O
		if !c.UseOwn {
			Oused = nil
		}
		Tsplit, bsplit := SplitBudgets(slo, L, c.Graph, route, Oused, heldT, heldB)
		for _, svc := range Tsplit.keys {
			table[TableKey{route, cls, svc}] = Tsplit.vals[svc]
			btab[TableKey{route, cls, svc}] = bsplit.vals[svc]
		}
		for _, svc := range L.keys {
			Ltab[TableKey{route, cls, svc}] = L.vals[svc]
			Otab[TableKey{route, cls, svc}] = O.vals[svc]
		}
	}
	c.Table, c.B, c.L, c.O = table, btab, Ltab, Otab
	for k := range table {
		c.Calibrated[[2]string{k.Route, k.Service}] = true
	}
	c.Version++
}

// Lookup is Calibrator.lookup: the class entry, else the route entry, else
// (before the service's first calibration) the prior.  ok is false when no
// T exists.
func (c *Calibrator) Lookup(route, cls, service string) (float64, bool) {
	if v, ok := c.Table[TableKey{route, cls, service}]; ok {
		return v, true
	}
	if v, ok := c.Table[TableKey{route, "*", service}]; ok {
		return v, true
	}
	if len(c.PriorTable) > 0 && !c.Calibrated[[2]string{route, service}] {
		if v, ok := c.PriorTable[[2]string{route, service}]; ok {
			c.PriorHits++
			return v, true
		}
	}
	return 0, false
}

// LookupB is Calibrator.lookup_b: the stage budget with the same fallback.
func (c *Calibrator) LookupB(route, cls, service string) (float64, bool) {
	if v, ok := c.B[TableKey{route, cls, service}]; ok {
		return v, true
	}
	if v, ok := c.B[TableKey{route, "*", service}]; ok {
		return v, true
	}
	if len(c.PriorB) > 0 && !c.Calibrated[[2]string{route, service}] {
		if v, ok := c.PriorB[[2]string{route, service}]; ok { // cold start: the prior's share
			c.PriorHits++
			return v, true
		}
	}
	return 0, false
}

// HasTable reports whether a calibrated table exists (bool(self.cal.table)).
func (c *Calibrator) HasTable() bool { return len(c.Table) > 0 }

// OracleTable is ttable.oracle_table: the calibration procedure on complete
// (p = 1) data from a healthy reference corpus.
func OracleTable(reference *Corpus, q float64, useClass bool) *Calibrator {
	c, _ := NewCalibrator(reference.Graph, reference.RoutesSLO, q, MinSamples, 1, useClass, "none")
	for i := range reference.Spans {
		c.Observe(&reference.Spans[i], 1.0)
	}
	c.EndPeriod(nil)
	return c
}
