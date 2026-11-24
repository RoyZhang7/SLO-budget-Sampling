package slobudgetcentralprocessor

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"net"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/processor/internal/arrivalstamp"
	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
)

const (
	attrSamplingP = "sampling.p"
	attrOwnUS     = "slobudget.own_us"
	attrArrival   = arrivalstamp.Attr
	attrRoot      = "slo.r"
	attrClass     = "slo.c"
)

type central struct {
	cfg  *Config
	log  *zap.Logger
	next consumer.Traces

	mu        sync.Mutex
	s         *slobudget.Sampler
	graph     *graphLearner
	reports   map[int64][]slobudget.AgentStats // period -> reports
	update    *slobudget.CentralUpdate
	version   int
	period    int64
	admitted  float64 // span units admitted this period (after the cap)
	received  int     // spans received this period
	capDrops  int
	spansEwma float64 // spans per period, smoothed (for V* = frac x spans), over full periods only
	fullPeriods int   // traffic periods that fed the estimate; the cap is armed after two
	vStar     float64
	cap       float64
	hosts     map[string]bool
	peers     map[string]string // agent host -> kept-notice address, from the stats
	lastServices int
	server    *http.Server
	addr      string
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	history   []periodSummary
}

type periodSummary struct {
	Period      int64              `json:"period"`
	Received    int                `json:"received"`
	Admitted    float64            `json:"admitted"`
	CapDropped  int                `json:"cap_dropped"`
	Requests    int                `json:"requests"`
	Spans       int                `json:"spans"`
	KeptTotal   int                `json:"kept_total"`
	KeptOutlier int                `json:"kept_outlier"`
	B           float64            `json:"b"`
	NNeed       float64            `json:"n_need"`
	VStar       float64            `json:"v_star"`
	Frozen      []string           `json:"frozen"`
	Reports     int                `json:"reports"`
	Table       map[string]float64 `json:"table_ms"` // route-level T per service in ms
	Demand      float64            `json:"demand"`   // admitted + pool refusals + cap drops (the refill's input when refill_input is demand)
	RequestsSeen     int           `json:"requests_seen"`     // requests first seen by the agents
	BudgetPropagated int           `json:"budget_propagated"` // of those, with the SDK-issued budget
}

func newCentral(set processor.Settings, cfg *Config, next consumer.Traces) (*central, error) {
	c := &central{cfg: cfg, log: set.Logger, next: next, reports: map[int64][]slobudget.AgentStats{}, hosts: map[string]bool{}, peers: map[string]string{}}
	routesSLO := map[string]float64{}
	for route, ms := range cfg.SLOMs {
		routesSLO[route] = ms / 1000.0
	}
	gl, err := newGraphLearner(cfg.CallGraphFile, routesSLO)
	if err != nil {
		return nil, err
	}
	c.graph = gl
	vStar := cfg.VStar
	if vStar <= 0 {
		vStar = 1000
	}
	nSvc := cfg.Services
	if nSvc <= 0 {
		nSvc = 12
	}
	s, err := slobudget.NewSampler(cfg.Sampler, gl.graph, routesSLO, nil, nSvc, vStar, uint64(time.Now().UnixNano()), nil)
	if err != nil {
		return nil, err
	}
	c.s = s
	s.Cal.UseOwn = cfg.OwnTime == "stamped"
	c.vStar = vStar
	c.cap = cfg.CapFactor * vStar
	return c, nil
}

func (c *central) Capabilities() consumer.Capabilities { return consumer.Capabilities{MutatesData: true} }

func (c *central) Start(ctx context.Context, _ component.Host) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/slobudget/config", c.handleConfig)
	mux.HandleFunc("/slobudget/stats", c.handleStats)
	mux.HandleFunc("/slobudget/status", c.handleStatus)
	ln, err := net.Listen("tcp", c.cfg.Listen)
	if err != nil {
		return err
	}
	c.addr = ln.Addr().String()
	c.server = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = c.server.Serve(ln) }()
	bg, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.period = time.Now().UnixNano() / c.cfg.Period.Nanoseconds()
	c.wg.Add(1)
	go c.periodLoop(bg)
	c.log.Info("slobudget central started", zap.String("listen", c.cfg.Listen), zap.Duration("period", c.cfg.Period),
		zap.Float64("v_star", c.vStar), zap.Float64("cap", c.cap), zap.String("own_measure", c.cfg.Sampler.OwnMeasure))
	return nil
}

func (c *central) Shutdown(ctx context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}
	if c.server != nil {
		_ = c.server.Shutdown(ctx)
	}
	c.wg.Wait()
	return nil
}

// ConsumeTraces: every arriving span is a kept span of an agent.  It feeds the
// calibrator with its stamped p, teaches the graph learner, and is admitted
// unless the period's cap is spent.
func (c *central) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	c.mu.Lock()
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		svc := "unknown"
		if v, ok := rs.Resource().Attributes().Get("service.name"); ok {
			svc = slobudget.NormalizeService(v.AsString())
		}
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			sss.At(j).Spans().RemoveIf(func(span ptrace.Span) bool { return !c.admit(span, svc) })
		}
		sss.RemoveIf(func(ss ptrace.ScopeSpans) bool { return ss.Spans().Len() == 0 })
	}
	rss.RemoveIf(func(rs ptrace.ResourceSpans) bool { return rs.ScopeSpans().Len() == 0 })
	c.mu.Unlock()
	if td.SpanCount() == 0 {
		return nil
	}
	return c.next.ConsumeTraces(ctx, td)
}

func (c *central) admit(span ptrace.Span, svc string) bool {
	counts := c.cfg.CountKinds == "all" || span.Kind() == ptrace.SpanKindServer
	if counts {
		c.received++
	}
	attrs := span.Attributes()
	route, cls := c.cfg.DefaultRoute, c.cfg.DefaultClass
	if v, ok := attrs.Get(attrRoot); ok {
		route = v.AsString()
	}
	if v, ok := attrs.Get(attrClass); ok {
		cls = v.AsString()
	}
	if cls == "" {
		cls = route + "|any"
	}
	start := float64(span.StartTimestamp()) / 1e9
	dur := float64(span.EndTimestamp())/1e9 - start
	if dur < 0 {
		dur = 0
	}
	p := 1.0
	if v, ok := attrs.Get(attrSamplingP); ok {
		p = v.Double()
	}
	own := dur
	if v, ok := attrs.Get(attrOwnUS); ok {
		own = float64(v.Int()) / 1e6
	}
	if span.Kind() != ptrace.SpanKindClient { // client spans are the caller's view: not calibrated
		sp := slobudget.Span{SpanID: 0, TraceID: 0, IsRoot: span.ParentSpanID().IsEmpty(), Service: svc, Start: start, Duration: dur,
			StatusOK: span.Status().Code() != ptrace.StatusCodeError, Route: route, ReqClass: cls, Own: own}
		if v, ok := attrs.Get(attrArrival); ok { // own_measure within_service calibrates from it
			sp.Wait = slobudget.ArrivalWait(int64(span.StartTimestamp()), v.Int())
		}
		c.s.Cal.Observe(&sp, p)
		c.graph.observe(route, svc, span.TraceID(), span.SpanID(), span.ParentSpanID(), start, start+dur, span.ParentSpanID().IsEmpty())
	}
	if !counts {
		return true
	}
	if c.admitted+1.0 > c.cap+1e-9 {
		c.capDrops++
		return false
	}
	c.admitted += 1.0
	return true
}

// periodLoop closes a period `grace` after its wall-clock end.
func (c *central) periodLoop(ctx context.Context) {
	defer c.wg.Done()
	for {
		next := time.Unix(0, (c.period+1)*c.cfg.Period.Nanoseconds()).Add(c.cfg.Grace)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(next)):
		}
		c.closePeriod()
	}
}

func (c *central) closePeriod() {
	c.mu.Lock()
	defer c.mu.Unlock()
	per := c.period
	reports := c.reports[per]
	delete(c.reports, per)
	for k := range c.reports { // stale reports
		if k < per {
			delete(c.reports, k)
		}
	}
	totalSpans := 0
	for _, r := range reports {
		totalSpans += r.SpansHost
		c.hosts[r.Host] = true
	}
	// volume target from the observed load: only full periods feed it (a trickle before the
	// load starts, or an idle period, must not shrink V* and the cap), and the cap is armed
	// after two of them
	if c.cfg.VStar <= 0 && totalSpans > 0 && (c.spansEwma == 0 || float64(totalSpans) >= 0.5*c.spansEwma) {
		if c.spansEwma == 0 {
			c.spansEwma = float64(totalSpans)
		} else {
			c.spansEwma = 0.5*c.spansEwma + 0.5*float64(totalSpans)
		}
		c.fullPeriods++
		c.vStar = c.cfg.VStarFrac * c.spansEwma
		if c.fullPeriods >= 2 {
			c.cap = c.cfg.CapFactor * c.vStar
		} else {
			c.cap = math.Inf(1)
		}
	}
	demand := c.admitted + float64(c.capDrops)
	for _, r := range reports {
		demand += float64(r.RefusedPool)
	}
	refillInput := c.admitted
	if c.cfg.RefillInput == "demand" {
		refillInput = demand
	}
	nSvc := c.cfg.Services
	if nSvc <= 0 {
		seen := map[string]bool{}
		for _, r := range reports {
			for svc := range r.SpansSvc {
				seen[svc] = true
			}
		}
		nSvc = len(seen)
		if nSvc == 0 { // an idle period: keep the last known service count
			nSvc = c.lastServices
		}
		if nSvc > 0 {
			c.lastServices = nSvc
		}
	}
	if nSvc <= 0 {
		nSvc = 12
	}
	c.s.SetVolumeTarget(c.vStar, nSvc)
	c.graph.rebuild() // the learned structure the calibrator splits over
	c.s.LoadCounters(reports)
	c.s.OnPeriodEnd(int(per), refillInput)
	c.version++
	c.update = c.s.Export(c.version, per+1, c.cap)
	c.update.Peers = maps.Clone(c.peers)
	sum := periodSummary{Period: per, Received: c.received, Admitted: c.admitted, CapDropped: c.capDrops, Spans: totalSpans,
		B: c.s.B, NNeed: slobudget.WireFloat(c.s.NNeed), VStar: c.vStar, Frozen: c.update.Frozen, Reports: len(reports), Table: map[string]float64{},
		Demand: demand}
	for _, r := range reports {
		sum.Requests += r.Requests
		sum.KeptTotal += r.KeptTotal
		sum.KeptOutlier += r.KeptOutlier
		sum.RequestsSeen += r.RequestsSeen
		sum.BudgetPropagated += r.BudgetPropagated
	}
	for k, T := range c.s.Cal.Table {
		if k.Class == "*" {
			sum.Table[k.Service] = T * 1000
		}
	}
	c.history = append(c.history, sum)
	if len(c.history) > 720 {
		c.history = c.history[len(c.history)-720:]
	}
	if c.cfg.StatsLog {
		c.log.Info("slobudget central period", zap.Int64("period", per), zap.Int("reports", len(reports)), zap.Int("requests", sum.Requests),
			zap.Int("spans", totalSpans), zap.Int("received", c.received), zap.Float64("admitted", c.admitted), zap.Int("cap_dropped", c.capDrops), zap.Float64("demand", demand),
			zap.Float64("v_star", c.vStar), zap.Float64("B", c.s.B), zap.Float64("n_need", c.s.NNeed), zap.Strings("frozen", c.update.Frozen),
			zap.Int("table", len(c.s.Cal.Table)))
	}
	c.received, c.admitted, c.capDrops = 0, 0, 0
	c.period++
}

func (c *central) handleConfig(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	u := c.update
	c.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if u == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	body, err := json.Marshal(u)
	if err != nil { // the agents would stay on their last update without a word
		c.log.Error("central update is not JSON-encodable", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(body)
}

func (c *central) handleStats(w http.ResponseWriter, r *http.Request) {
	var st slobudget.AgentStats
	if err := json.NewDecoder(r.Body).Decode(&st); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	if st.NoticeAddr != "" {
		c.peers[st.Host] = st.NoticeAddr
	}
	if st.Period >= c.period { // on time (or early): counted for its period
		c.reports[st.Period] = append(c.reports[st.Period], st)
	} else { // late: folded into the period being collected
		c.reports[c.period] = append(c.reports[c.period], st)
	}
	c.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (c *central) handleStatus(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	hosts := make([]string, 0, len(c.hosts))
	for h := range c.hosts {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)
	out := map[string]any{"period": c.period, "version": c.version, "v_star": c.vStar, "cap": c.cap, "hosts": hosts,
		"received_this_period": c.received, "admitted_this_period": c.admitted, "update": c.update,
		"graph": c.graph.export(), "history": c.history, "peers": c.peers}
	c.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	body, err := json.MarshalIndent(out, "", " ")
	if err != nil {
		c.log.Error("central status is not JSON-encodable", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(body)
}

// Addr is the bound address of the config channel (after Start).
func (c *central) Addr() string { return c.addr }

// LoadCallGraph reads a call graph file (the replay dump's graph shape).
func LoadCallGraph(path string) (*slobudget.CallGraph, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var g struct {
		Root     map[string]string                `json:"root"`
		Children map[string]map[string][]string   `json:"children"`
		Parallel map[string]map[string]bool       `json:"parallel"`
		Stages   map[string]map[string][][]string `json:"stages"` // callees by sequential stage; absent: the parallel flag
	}
	if err := json.Unmarshal(raw, &g); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	for route, bySvc := range g.Stages { // a stage may only name callees of the service, each once
		for svc, stages := range bySvc {
			kids := map[string]bool{}
			for _, c := range g.Children[route][svc] {
				kids[c] = true
			}
			seen := map[string]bool{}
			for _, group := range stages {
				for _, c := range group {
					if !kids[c] || seen[c] {
						return nil, fmt.Errorf("%s: stages of (%s, %s) do not match its callees: %s", path, route, svc, c)
					}
					seen[c] = true
				}
			}
		}
	}
	return &slobudget.CallGraph{Root: g.Root, Children: g.Children, Parallel: g.Parallel, Stages: g.Stages}, nil
}

var _ = pcommon.NewTraceIDEmpty
