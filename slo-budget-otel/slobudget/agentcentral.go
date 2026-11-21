package slobudget

import (
	"math"
	"regexp"
	"sort"
	"strings"
)

// The distributed split of the simulator's single Sampler.  Agents (one per
// host) run the span-end decisions with the table, budget B, N_need, plan
// rates and their host's pool as published by the central; the central
// collects the kept spans (1/p-weighted) into the calibrator, sums the agents'
// per-period counters and runs the period end (freeze set, table, refill,
// pools, plan) exactly as on_period_end does, then publishes a CentralUpdate.

// CentralUpdate is the config-channel payload the central publishes each period.
type CentralUpdate struct {
	Version  int                                      `json:"version"`
	Period   int64                                    `json:"period"`
	B        float64                                  `json:"b"`
	NNeed    float64                                  `json:"n_need"`
	VStar    float64                                  `json:"v_star"`
	Cap      float64                                  `json:"cap"`
	Table    map[string]map[string]map[string]float64 `json:"table"`     // route -> class -> service -> T (s)
	PlanP    map[string]float64                       `json:"plan_p"`    // service -> ordinary-span rate
	PoolFull map[string]float64                       `json:"pool_full"` // host -> span units per period
	Frozen   []string                                 `json:"frozen"`
	Services int                                      `json:"services"`
	Peers    map[string]string                        `json:"peers"` // agent host -> kept-notice address (ip:port)
}

// AgentStats is what an agent reports for one finished period.
type AgentStats struct {
	Host         string             `json:"host"`
	Period       int64              `json:"period"`
	Requests     int                `json:"requests"`      // root spans seen
	SpansSvc     map[string]float64 `json:"spans_svc"`     // spans decided per service
	Ndet         map[string]int     `json:"ndet"`          // outliers detected per service
	SpansHost    int                `json:"spans_host"`    // spans decided on this host
	KeptTotal    int                `json:"kept_total"`
	KeptOutlier  int                `json:"kept_outlier"`
	KeptByReason map[string]int     `json:"kept_by_reason"`
	Notices      int                `json:"notices"`       // kept-notices sent
	RootsHeld    int                `json:"roots_held"`    // roots that waited for a notice
	RootsPinned  int                `json:"roots_pinned"`  // roots kept because a notice arrived
	ClientSpans  int                `json:"client_spans"`
	RefusedPool  int                `json:"refused_pool"`   // keeps the host pool refused (demand the refill should see)
	RefusedBudget int               `json:"refused_budget"` // keeps the request budget refused
	NoticeAddr   string             `json:"notice_addr"`    // this agent's kept-notice address (ip:port) for unicast notices
	RequestsSeen int                `json:"requests_seen"`  // requests first seen this period
	BudgetPropagated int            `json:"budget_propagated"` // of those, requests whose budget came from the SDK's baggage
	SDKPolls     int                `json:"sdk_polls"`      // SDK polls of /slobudget/sdk answered
	Version      int                `json:"version"`       // config version in force
}

// IssueBudget is on_root_start without the request counter: the integer
// budget by stochastic rounding of B (agents count roots separately).
func (s *Sampler) IssueBudget(req *RequestState) {
	req.Flag = false
	if s.Cfg.Unbounded {
		req.Budget, req.Budget0 = 1<<30, 1<<30
	} else {
		b := int(s.B)
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

// CountRequest counts one root span seen (R of the refill loop).
func (s *Sampler) CountRequest() { s.requests++ }

// ApplyCentral installs a published update on an agent: table, B, N_need,
// plan rates and this host's pool size (used from the next ResetPeriod on).
func (s *Sampler) ApplyCentral(u *CentralUpdate, host string) {
	if u == nil {
		return
	}
	if u.Table != nil {
		table := map[TableKey]float64{}
		for route, byCls := range u.Table {
			for cls, bySvc := range byCls {
				for svc, T := range bySvc {
					table[TableKey{route, cls, svc}] = T
					s.Cal.Calibrated[[2]string{route, svc}] = true
				}
			}
		}
		s.Cal.Table = table
		s.Cal.Version++
	}
	if u.B > 0 {
		s.B = u.B
	}
	if u.NNeed > 0 {
		s.NNeed = u.NNeed
	}
	if u.VStar > 0 {
		s.VStar = u.VStar
	}
	if u.PlanP != nil {
		s.planP = map[string]float64{}
		for svc, p := range u.PlanP {
			s.planP[svc] = p
		}
	}
	if pf, ok := u.PoolFull[host]; ok && pf > 0 {
		s.poolFull[host] = pf
		if _, known := s.share[host]; !known {
			s.Hosts = append(s.Hosts, host)
			s.share[host] = 1.0
		}
	}
	s.frozen = map[string]bool{}
	for _, svc := range u.Frozen {
		s.frozen[svc] = true
	}
}

// Snapshot returns this period's counters for the stats report.
func (s *Sampler) Snapshot(host string, period int64) AgentStats {
	st := AgentStats{Host: host, Period: period, Requests: s.requests, SpansSvc: map[string]float64{}, Ndet: map[string]int{},
		SpansHost: s.spansHost[host], KeptTotal: s.keptTotal, KeptOutlier: s.keptOutlier, KeptByReason: map[string]int{}}
	for _, svc := range s.spansSvc.keys {
		st.SpansSvc[svc] = s.spansSvc.vals[svc]
	}
	for svc, n := range s.ndet {
		st.Ndet[svc] = n
	}
	for k, v := range s.keptByReason {
		st.KeptByReason[k] = v
	}
	return st
}

// ResetPeriod starts a new period on an agent: counters to zero, pools refilled.
func (s *Sampler) ResetPeriod() {
	s.resetCounters()
	s.setPools()
}

// LoadCounters sums the agents' reports into the central's period counters
// (what on_end would have accumulated in a single process).  Hosts seen for
// the first time get their observed share directly.
func (s *Sampler) LoadCounters(reports []AgentStats) {
	s.resetCounters()
	total := 0
	newHost := false
	for _, r := range reports {
		total += r.SpansHost
	}
	for _, r := range reports {
		s.requests += r.Requests
		s.keptTotal += r.KeptTotal
		s.keptOutlier += r.KeptOutlier
		for k, v := range r.KeptByReason {
			s.keptByReason[k] += v
		}
		svcs := make([]string, 0, len(r.SpansSvc))
		for svc := range r.SpansSvc {
			svcs = append(svcs, svc)
		}
		sort.Strings(svcs)
		for _, svc := range svcs {
			s.spansSvc.set(svc, s.spansSvcGet(svc)+r.SpansSvc[svc])
		}
		for svc, n := range r.Ndet {
			s.ndet[svc] += n
		}
		if r.Host == "" {
			continue
		}
		s.spansHost[r.Host] += r.SpansHost
		if _, known := s.share[r.Host]; !known {
			s.Hosts = append(s.Hosts, r.Host)
			sort.Strings(s.Hosts)
			s.share[r.Host] = 0
			newHost = true
		}
	}
	if newHost {
		// a host seen for the first time: its observed share when there is traffic, else
		// equal shares (the simulator's 1 / hosts start), so no host starts with a pool of 1
		for _, h := range s.Hosts {
			share := 1.0 / float64(len(s.Hosts))
			if total > 0 {
				share = float64(s.spansHost[h]) / float64(total)
			}
			s.share[h] = share
			s.poolFull[h] = math.Max(1, PyRound(s.Cfg.PoolFactor*s.VStar*share))
		}
	}
}

// SetVolumeTarget updates V* (and the relative N_need) on the central when the
// observed load changes.
func (s *Sampler) SetVolumeTarget(vStar float64, nServices int) {
	s.VStar = vStar
	if nServices < 1 {
		nServices = 1
	}
	switch {
	case s.Cfg.Unbounded || (s.Cfg.NNeedMode == "fixed" && math.IsInf(s.Cfg.NNeed, 1)):
		s.NNeed = math.Inf(1)
	case s.Cfg.NNeedMode == "relative":
		s.NNeed = math.Max(float64(s.Cfg.NMin), math.Floor(s.Cfg.NNeedFrac*vStar/float64(nServices)))
	default:
		s.NNeed = s.Cfg.NNeed
	}
}

// Export builds the update to publish after a central period end.
func (s *Sampler) Export(version int, period int64, cap float64) *CentralUpdate {
	u := &CentralUpdate{Version: version, Period: period, B: s.B, NNeed: s.NNeed, VStar: s.VStar, Cap: cap,
		Table: map[string]map[string]map[string]float64{}, PlanP: map[string]float64{}, PoolFull: map[string]float64{},
		Frozen: s.Frozen(), Services: s.nExp.len()}
	for k, T := range s.Cal.Table {
		byCls := u.Table[k.Route]
		if byCls == nil {
			byCls = map[string]map[string]float64{}
			u.Table[k.Route] = byCls
		}
		bySvc := byCls[k.Class]
		if bySvc == nil {
			bySvc = map[string]float64{}
			byCls[k.Class] = bySvc
		}
		bySvc[k.Service] = T
	}
	for svc, p := range s.planP {
		u.PlanP[svc] = p
	}
	for h, pf := range s.poolFull {
		u.PoolFull[h] = pf
	}
	return u
}

// KeptTotal / KeptOutlier of the current period (diagnostics).
func (s *Sampler) KeptTotal() int   { return s.keptTotal }
func (s *Sampler) KeptOutlier() int { return s.keptOutlier }

var serviceRe = regexp.MustCompile(`^(?:unknown_service:)?([A-Za-z0-9_-]+?)(?:_service)?(?:_[a-z]+_es)?(?:_proc)?$`)

// NormalizeService is jaeger.normalize_service: the service name as the
// simulator spells it (`unknown_service:post_storage_service_pb_es_proc` ->
// `poststorage`).
func NormalizeService(name string) string {
	m := serviceRe.FindStringSubmatch(name)
	if m == nil {
		return name
	}
	return strings.ReplaceAll(m[1], "_", "")
}
