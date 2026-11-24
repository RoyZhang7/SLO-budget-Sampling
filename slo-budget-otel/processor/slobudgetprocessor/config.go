package slobudgetprocessor

import (
	"errors"
	"fmt"
	"time"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
)

// Config of the per-host agent processor.
type Config struct {
	// Sampler holds the decider knobs (sampler.BudgetConfig names); the defaults are the
	// Stage 2 configuration (chosen-v2).
	Sampler slobudget.Config `mapstructure:"sampler"`
	// Host names this agent's host for the host pool and the stats report; default: $NODE_NAME,
	// then os.Hostname().
	Host string `mapstructure:"host"`
	// Period is the sampling period (wall clock, aligned to multiples of it).
	Period time.Duration `mapstructure:"period"`
	// Central is the base URL of the central collector's config channel
	// (http://host:port); empty runs standalone with StandaloneVStar.
	Central string `mapstructure:"central"`
	// StandaloneVStar is the volume target (spans per period) when there is no central.
	StandaloneVStar float64 `mapstructure:"standalone_v_star"`
	// DefaultRoute / DefaultClass label spans that carry no slo.r / slo.c attribute.
	DefaultRoute string `mapstructure:"default_route"`
	DefaultClass string `mapstructure:"default_class"`
	// RootHold is how long a root span waits for a kept-notice before it is
	// plan-sampled (on-demand root pinning).
	RootHold time.Duration `mapstructure:"root_hold"`
	// NoticeListen is the UDP address kept-notices arrive on; NoticePeers are the
	// other agents (host:port or a DNS name resolving to all agents); a span that
	// carries slo.h is noticed to that host only.
	NoticeListen string   `mapstructure:"notice_listen"`
	NoticePeers  []string `mapstructure:"notice_peers"`
	NoticePort   int      `mapstructure:"notice_port"`
	// ClientSpans: "plan" (ordinary spans of the caller, plan-sampled, never outliers),
	// "drop" (never exported) or "decide" (treated like any span).
	ClientSpans string `mapstructure:"client_spans"`
	// TTableFile seeds T_i (route -> service -> seconds) until the service's first
	// calibrated period (cold_prior: file).
	TTableFile string `mapstructure:"ttable_file"`
	// RequestTTL bounds the per-trace state (budget, children seen).
	RequestTTL time.Duration `mapstructure:"request_ttl"`
	// ServerHold (Stage 2d) holds every non-root server span this long before deciding it, so
	// that its CLIENT children (exported in the SDK's low-priority chunk of the same 100 ms
	// flush, i.e. usually right after the server span) are in: own time = duration minus the
	// union of the client children seen at release.  0 decides on arrival (Stage 2 behaviour).
	ServerHold time.Duration `mapstructure:"server_hold"`
	// StatsLog logs the period stats line.
	StatsLog bool `mapstructure:"stats_log"`
	// BudgetSource: "propagated" takes the request's budget from the SDK's slo.budget
	// attribute (issued at the root, carried in the trace's baggage) and issues it here only
	// for requests that carry none; "self" always issues it here.
	BudgetSource string `mapstructure:"budget_source"`
	// SDKListen is the HTTP address the SDK's budget issuer polls (GET /slobudget/sdk ->
	// {version, b, host}); empty disables it.
	SDKListen string `mapstructure:"sdk_listen"`
	// KeepParentAnchor (keep_parent ablation, sampler.keep_parent): a kept outlier forces
	// its parent span (the caller's client span) and, with this on, also its `_br`
	// checkpoint anchor (the caller's server span, its parent in the simulator's model).
	KeepParentAnchor bool `mapstructure:"keep_parent_anchor"`
	// ArrivalTable is the pin directory of the arrival program (arrivald): a server span gets
	// the arrival of its request at the pod (slobudget.arrival_ns), looked up by its parent span
	// id when the span reaches the agent; on a miss, its own start.  No decision uses it.
	// Empty: off.
	ArrivalTable string `mapstructure:"arrival_table"`
}

func (c *Config) Validate() error {
	cfg := c.Sampler
	if err := cfg.Validate(); err != nil {
		return err
	}
	if c.Period <= 0 {
		return errors.New("period must be positive")
	}
	if c.ServerHold < 0 {
		return errors.New("server_hold must be >= 0")
	}
	if c.RootHold < 0 {
		return errors.New("root_hold must be >= 0")
	}
	switch c.ClientSpans {
	case "plan", "drop", "decide":
	default:
		return fmt.Errorf("client_spans %q: choose plan, drop or decide", c.ClientSpans)
	}
	switch c.BudgetSource {
	case "", "propagated", "self":
	default:
		return fmt.Errorf("budget_source %q: choose propagated or self", c.BudgetSource)
	}
	if c.Central == "" && c.StandaloneVStar <= 0 {
		return errors.New("either central or standalone_v_star is required")
	}
	if c.Sampler.ColdPrior == "file" && c.TTableFile == "" {
		return errors.New("cold_prior file needs ttable_file")
	}
	return nil
}
