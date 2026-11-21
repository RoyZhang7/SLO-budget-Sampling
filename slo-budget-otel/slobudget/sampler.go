package slobudget

import (
	"fmt"
	"math"
	"sort"
)

// RequestState is the per-request state that rides in baggage (sampler.RequestState).
type RequestState struct {
	TraceID      uint64
	Route        string
	Class        string
	Budget       int
	Budget0      int
	BudgetByHost map[string]int // budget_scope local: per (request, host)
	Flag         bool           // response-path flag: a span of this request was kept
}

// Decision is what on_end returns: keep, the stamped p and the reason
// (a keep reason or a refusal reason).
type Decision struct {
	Keep   bool
	P      float64
	Reason string
}

// PeriodDiag mirrors the per-period diagnostics dict of the Python sampler.
type PeriodDiag struct {
	Period          int
	B               float64
	Requests        int
	CollectorVolume float64
	Kept            int
	KeptOutlier     int
	NNeed           float64
	ByReason        map[string]int
	Frozen          []string
	PriorHits       int
	PlanPMean       float64
}

// Sampler is sampler.BudgetSampler: the chosen mechanism.
type Sampler struct {
	Cfg    Config
	Cal    *Calibrator // the per-period calibration (nil only when calib_source is oracle)
	Oracle *Calibrator // the ground-truth table (tests, calib_source oracle)
	rng    *PyRandom
	g      func(float64) float64

	calibrated  bool
	flagReasons map[string]bool
	NNeed       float64
	Hosts       []string
	VStar       float64
	B           float64
	REXp        float64 // R_exp (nil in Python until the first period: hasR)
	hasR        bool
	outExp      float64
	nExp        *orderedMap // N_exp: insertion order matters for the float sum in replan
	share       map[string]float64
	poolFull    map[string]float64
	pool        map[string]float64
	planP       map[string]float64
	svcHost     map[string]string
	frozen      map[string]bool
	healthyRate map[string]float64

	// per-period counters (reset at the period end)
	nseen        map[string]int // outliers counted by the N_need rule (kept or coin-dropped)
	ndet         map[string]int // outliers detected (including those refused by budget / pool)
	spansSvc     *orderedMap    // _spans_svc, ordered: its iteration order seeds N_exp's order
	spansHost    map[string]int
	requests     int
	keptOutlier  int
	keptTotal    int
	keptByReason map[string]int

	exhaustedReason string
	Diag            []PeriodDiag
}

// NewSampler is BudgetSampler(cfg) + start_run(ctx): corpus facts (hosts,
// services, SLOs, graph), the volume target and the seed of the coin RNG.
// services is the number of distinct services (for the relative N_need).
func NewSampler(cfg Config, graph *CallGraph, routesSLO map[string]float64, hosts []string, nServices int,
	vStar float64, seed uint64, oracle *Calibrator) (*Sampler, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	g, _ := growth(cfg.PRule)
	s := &Sampler{Cfg: cfg, g: g, rng: NewPyRandom(seed), VStar: vStar, Oracle: oracle}
	s.calibrated = cfg.CalibSource != "oracle"
	prior := "none"
	if s.calibrated {
		prior = cfg.ColdPrior
	}
	cal, err := NewCalibrator(graph, routesSLO, cfg.CalibQ, cfg.MinClassSamples, cfg.CalibWindow, cfg.UseClass, prior)
	if err != nil {
		return nil, err
	}
	s.Cal = cal
	if !s.calibrated && oracle == nil {
		return nil, fmt.Errorf("oracle table requested but none provided")
	}
	s.flagReasons = map[string]bool{"outlier": true, "escalate": true, "parent": true, "credit": true, "reserve": true}
	if cfg.RootFlagOn == "any" {
		s.flagReasons["plan"] = true
	}
	nSvc := nServices
	if nSvc < 1 {
		nSvc = 1
	}
	switch {
	case cfg.Unbounded || (cfg.NNeedMode == "fixed" && math.IsInf(cfg.NNeed, 1)):
		s.NNeed = math.Inf(1)
	case cfg.NNeedMode == "relative":
		s.NNeed = math.Max(float64(cfg.NMin), math.Floor(cfg.NNeedFrac*vStar/float64(nSvc)))
	default:
		s.NNeed = cfg.NNeed
	}
	s.Hosts = append([]string(nil), hosts...)
	s.B = cfg.InitialBudget
	s.nExp = newOrderedMap()
	s.share = map[string]float64{}
	s.poolFull = map[string]float64{}
	s.pool = map[string]float64{}
	n := len(s.Hosts)
	if n < 1 {
		n = 1
	}
	for _, h := range s.Hosts {
		s.share[h] = 1.0 / float64(n)
		if cfg.Unbounded {
			s.poolFull[h] = math.Inf(1)
		} else {
			s.poolFull[h] = math.Max(1, PyRound(cfg.PoolFactor*vStar*s.share[h]))
		}
	}
	s.setPools()
	s.planP = map[string]float64{}
	s.svcHost = map[string]string{}
	s.frozen = map[string]bool{}
	s.healthyRate = map[string]float64{}
	s.resetCounters()
	return s, nil
}

func (s *Sampler) resetCounters() {
	s.nseen = map[string]int{}
	s.ndet = map[string]int{}
	s.spansSvc = newOrderedMap()
	s.spansHost = map[string]int{}
	s.requests = 0
	s.keptOutlier = 0
	s.keptTotal = 0
	s.keptByReason = map[string]int{}
}

func (s *Sampler) setPools() {
	for h, full := range s.poolFull {
		if s.Cfg.Unbounded || math.IsInf(full, 1) {
			s.pool[h] = math.Inf(1)
		} else {
			s.pool[h] = full
		}
	}
}

// OnRootStart is on_root_start: issue the integer budget (stochastic rounding of B).
func (s *Sampler) OnRootStart(sp *Span, req *RequestState) {
	s.requests++
	req.Flag = false
	if s.Cfg.Unbounded {
		req.Budget, req.Budget0 = 1<<30, 1<<30
	} else {
		b := int(s.B) // int() truncates like Python's int() for B >= 0
		if s.rng.Random() < s.B-float64(b) {
			b++
		}
		if b < 1 {
			b = 1
		}
		req.Budget, req.Budget0 = b, b
	}
	if s.Cfg.BudgetScope == "local" {
		req.BudgetByHost = map[string]int{}
	}
}

// Lookup is _lookup_key: the T_i in force for (route, class, service).
func (s *Sampler) Lookup(route, cls, service string) (float64, bool) {
	if !s.calibrated {
		return s.Oracle.Lookup(route, cls, service)
	}
	return s.Cal.Lookup(route, cls, service)
}

func (s *Sampler) observe(sp *Span, p float64, reason string) {
	if !s.calibrated {
		return
	}
	if s.Cfg.CalibSource == "plan" && reason != "plan" && reason != "root" {
		return
	}
	s.Cal.Observe(sp, p)
}

func (s *Sampler) exhausted(sp *Span, req *RequestState) bool {
	if s.Cfg.Unbounded {
		return false
	}
	if s.Cfg.BudgetScope == "local" {
		b, ok := req.BudgetByHost[sp.Host]
		if !ok {
			b = req.Budget0
		}
		if b <= 0 {
			s.exhaustedReason = "budget"
			return true
		}
	} else if req.Budget <= 0 {
		s.exhaustedReason = "budget"
		return true
	}
	if p, ok := s.pool[sp.Host]; ok && p <= 0 {
		s.exhaustedReason = "pool"
		return true
	}
	return false
}

func (s *Sampler) account(sp *Span, req *RequestState, p float64, reason string) Decision {
	s.keptTotal++
	s.keptByReason[reason]++
	if reason != "plan" {
		s.keptOutlier++
	}
	s.observe(sp, p, reason)
	if !sp.IsRoot {
		if s.flagReasons[reason] {
			req.Flag = true
		}
		return Decision{true, p, reason}
	}
	return Decision{true, p, reason}
}

func (s *Sampler) spend(sp *Span, req *RequestState, p float64, reason string) Decision {
	if s.exhausted(sp, req) {
		return Decision{false, p, s.exhaustedReason}
	}
	if !s.Cfg.Unbounded {
		if s.Cfg.BudgetScope == "local" {
			b, ok := req.BudgetByHost[sp.Host]
			if !ok {
				b = req.Budget0
			}
			req.BudgetByHost[sp.Host] = b - 1
		} else {
			req.Budget--
		}
		if cur, ok := s.pool[sp.Host]; ok {
			s.pool[sp.Host] = cur - 1
		} else {
			s.pool[sp.Host] = math.Inf(1) // Python: self._pool.get(host, inf) - 1 stays inf
		}
	}
	return s.account(sp, req, p, reason)
}

func (s *Sampler) keepRoot(sp *Span) Decision {
	s.keptTotal++
	s.keptByReason["root"]++
	s.observe(sp, 1.0, "root")
	return Decision{true, 1.0, "root"}
}

func (s *Sampler) decideRoot(sp *Span, req *RequestState) Decision {
	pol := s.Cfg.RootPolicy
	if pol == "pin" {
		return s.keepRoot(sp)
	}
	s.spansSvc.set(sp.Service, s.spansSvcGet(sp.Service)+1) // the root service gets a plan rate
	if pol == "on_demand" && req.Flag {
		return s.keepRoot(sp)
	}
	return s.decideSpan(sp, req)
}

func (s *Sampler) spansSvcGet(svc string) float64 {
	v, _ := s.spansSvc.get(svc)
	return v
}

// decideSpan is _decide_span: the Decide(...) of the plan.  It reads the
// span, the baggage state and the per-period counters only.
func (s *Sampler) decideSpan(sp *Span, req *RequestState) Decision {
	svc := sp.Service
	T, hasT := s.Lookup(sp.Route, sp.ReqClass, svc)
	outlier, miss := false, 0.0
	if hasT { // else cold start: plan sampling only
		miss = (sp.Duration - T) / T
		outlier = miss > 0.0 || !sp.StatusOK
		if miss < 0.0 {
			miss = 0.0
		}
	}
	if outlier {
		s.ndet[svc]++
		if s.exhausted(sp, req) {
			return Decision{false, 0.0, s.exhaustedReason} // _try_credit with outlier_credit none
		}
		n := s.nseen[svc] + 1
		s.nseen[svc] = n
		p := 1.0
		if float64(n) > s.NNeed {
			p = math.Min(1.0, s.NNeed/float64(n)*s.g(miss))
		}
		if p < 1.0 && s.rng.Random() >= p {
			return Decision{false, p, "outlier_coin"}
		}
		return s.spend(sp, req, p, "outlier")
	}
	p, ok := s.planP[svc]
	if !ok {
		p = s.Cfg.InitialPlanP
	}
	if p <= 0.0 || (p < 1.0 && s.rng.Random() >= p) {
		return Decision{false, p, "plan_coin"}
	}
	return s.spend(sp, req, p, "plan")
}

// OnEnd is on_end: the decision for one finished span.
func (s *Sampler) OnEnd(sp *Span, req *RequestState) Decision {
	s.spansHost[sp.Host]++
	s.svcHost[sp.Service] = sp.Host
	if sp.IsRoot {
		return s.decideRoot(sp, req)
	}
	s.spansSvc.set(sp.Service, s.spansSvcGet(sp.Service)+1)
	return s.decideSpan(sp, req)
}

// freezeSet is _freeze_set: services whose calibration is held for the next table.
func (s *Sampler) freezeSet() map[string]bool {
	cfg := s.Cfg
	out := map[string]bool{}
	hasTable := s.Cal.HasTable()
	for _, svc := range s.spansSvc.keys {
		nSp := s.spansSvc.vals[svc]
		nOut := s.ndet[svc]
		if cfg.FreezeTrigger == "count" {
			if float64(nOut) > s.NNeed {
				out[svc] = true
			}
			continue
		}
		if !hasTable || nSp <= 0 {
			continue
		}
		rate := float64(nOut) / nSp
		base, hasBase := s.healthyRate[svc]
		if hasBase && nOut >= cfg.NMin && rate >= cfg.FreezeRateMult*math.Max(base, cfg.FreezeRateFloor) {
			out[svc] = true
		} else if !hasBase {
			s.healthyRate[svc] = rate
		} else {
			s.healthyRate[svc] = (1-cfg.FreezeEwma)*base + cfg.FreezeEwma*rate
		}
	}
	return out
}

// OnPeriodEnd is on_period_end: calibration, refill, pools, plan.
func (s *Sampler) OnPeriodEnd(period int, collectorVolume float64) {
	cfg, vStar := s.Cfg, s.VStar
	if s.calibrated {
		if cfg.CalibSource == "frozen" {
			s.frozen = s.freezeSet()
			s.Cal.EndPeriod(s.frozen)
		} else {
			s.Cal.EndPeriod(nil)
		}
	}
	R := s.requests
	if R > 0 {
		if !s.hasR {
			s.REXp, s.hasR = float64(R), true
		} else {
			s.REXp = (1-cfg.RateEwma)*s.REXp + cfg.RateEwma*float64(R)
		}
	}
	bRaw := s.B
	if !cfg.Unbounded && R > 0 && s.hasR && s.REXp != 0 {
		K := collectorVolume
		if K > 0 {
			u := (K / float64(R)) / s.B
			bRaw = vStar / (s.REXp * u)
		} else {
			bRaw = cfg.BMax
		}
		bRaw = math.Min(cfg.BMax, math.Max(cfg.BMin, bRaw))
		s.B = (1-cfg.Alpha)*s.B + cfg.Alpha*bRaw
	}
	totalHost := 0
	for _, n := range s.spansHost {
		totalHost += n
	}
	if totalHost != 0 && !cfg.Unbounded {
		for _, h := range s.Hosts {
			share := float64(s.spansHost[h]) / float64(totalHost)
			s.share[h] = (1-cfg.RateEwma)*s.share[h] + cfg.RateEwma*share
			s.poolFull[h] = math.Max(1, PyRound(cfg.PoolFactor*vStar*s.share[h]))
		}
	}
	s.setPools()
	s.outExp = (1-cfg.RateEwma)*s.outExp + cfg.RateEwma*float64(s.keptOutlier)
	for _, svc := range s.spansSvc.keys {
		n := s.spansSvc.vals[svc]
		if prev, ok := s.nExp.get(svc); ok {
			s.nExp.set(svc, (1-cfg.RateEwma)*prev+cfg.RateEwma*n)
		} else {
			s.nExp.set(svc, n)
		}
	}
	s.replan(vStar)
	priorHits := 0
	if s.calibrated {
		priorHits, s.Cal.PriorHits = s.Cal.PriorHits, 0
	}
	frozen := make([]string, 0, len(s.frozen))
	for svc := range s.frozen {
		frozen = append(frozen, svc)
	}
	sort.Strings(frozen)
	pm := cfg.InitialPlanP
	if len(s.planP) > 0 {
		sum := 0.0
		for _, p := range s.planP {
			sum += p
		}
		pm = sum / float64(len(s.planP))
	}
	by := map[string]int{}
	for k, v := range s.keptByReason {
		by[k] = v
	}
	s.Diag = append(s.Diag, PeriodDiag{Period: period, B: s.B, Requests: R, CollectorVolume: collectorVolume, Kept: s.keptTotal,
		KeptOutlier: s.keptOutlier, NNeed: s.NNeed, ByReason: by, Frozen: frozen, PriorHits: priorHits, PlanPMean: pm})
	s.resetCounters()
}

// replan is _replan: ordinary-span rates for the next period.
func (s *Sampler) replan(vStar float64) {
	cfg := s.Cfg
	if s.nExp.len() == 0 || !s.hasR || s.REXp == 0 {
		return
	}
	vals := make([]float64, len(s.nExp.keys))
	for i, svc := range s.nExp.keys {
		vals[i] = s.nExp.vals[svc]
	}
	total := PySum(vals)
	if total <= 0 {
		return
	}
	var target float64
	if cfg.Unbounded {
		roots := 0.0
		if cfg.RootPolicy == "pin" {
			roots = s.REXp
		}
		target = math.Max(cfg.OrdinaryFloor*vStar, vStar-roots-s.outExp)
	} else {
		perRequest := math.Max(cfg.OrdinaryFloor*s.B, cfg.PlanFill*(s.B-s.outExp/s.REXp))
		target = perRequest * s.REXp
	}
	switch cfg.Plan {
	case "uniform":
		p := math.Min(1.0, target/total)
		s.planP = map[string]float64{}
		for _, svc := range s.nExp.keys {
			s.planP[svc] = p
		}
	case "sqrt":
		w := map[string]float64{}
		ws := make([]float64, 0, len(s.nExp.keys))
		for _, svc := range s.nExp.keys {
			w[svc] = math.Sqrt(s.nExp.vals[svc])
			ws = append(ws, w[svc])
		}
		wsum := PySum(ws)
		s.planP = map[string]float64{}
		for _, svc := range s.nExp.keys {
			n := s.nExp.vals[svc]
			s.planP[svc] = math.Min(1.0, (target*w[svc]/wsum)/math.Max(1.0, n))
		}
	}
}

// Frozen returns the services held at the last period end (sorted).
func (s *Sampler) Frozen() []string {
	out := make([]string, 0, len(s.frozen))
	for svc := range s.frozen {
		out = append(out, svc)
	}
	sort.Strings(out)
	return out
}

// PlanP returns the ordinary-span rate in force for a service.
func (s *Sampler) PlanP(svc string) float64 {
	if p, ok := s.planP[svc]; ok {
		return p
	}
	return s.Cfg.InitialPlanP
}

// PoolLeft returns the host pool left in this period (+Inf when unbounded).
func (s *Sampler) PoolLeft(host string) float64 {
	if p, ok := s.pool[host]; ok {
		return p
	}
	return math.Inf(1)
}
