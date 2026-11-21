//go:build ignore

// This file is compiled inside the Blueprint fork (runtime/plugins/otelcol), where
// backend.GetBaggageFromContext exists; apply_bp_patch.py strips these lines when it copies
// the file there.  Excluded from the sdk module's own build.

// Package otelcol implements a tracer [backend.Tracer] client interface for the OpenTelemetry collector.
package otelcol

// SLO-budget sampling (SLO-budget-Sampling/slo-budget-otel): the request's sampling budget
// and identity travel in Blueprint's own trace carrier.  On a root span this processor
// stamps
//
//	__bag.slo.budget     the request's integer budget b (stochastic rounding of the central's B)
//	__bag.slo.root       the root operation in snake case, the route (ComposePost -> compose_post)
//	__bag.slo.class      route|payload bucket (s / m / l by request bytes, "any" when unknown)
//	__bag.slo.root_host  the node of the root's collector agent
//
// as "__bag." attributes, which the generated OT wrappers copy into every outgoing carrier's
// baggage (and never export), plus the same four keys as plain attributes, which the
// collector sees on the root.  On every other span it copies the inbound baggage's four keys
// to plain attributes.  B and the host come from the node-local collector agent
// (GET http://<agent host>:SLO_BUDGET_PORT/slobudget/sdk, polled every SLO_BUDGET_POLL_MS,
// default 1000); while no agent answers nothing is stamped and the collector issues the
// budget itself.  SLO_BUDGET_PORT=off disables the processor.  The bridge variant is
// untouched: this is a second span processor beside the bridge processor.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	sloKeyBudget   = "slo.budget"
	sloKeyRoot     = "slo.root"
	sloKeyClass    = "slo.class"
	sloKeyRootHost = "slo.root_host"
	sloBagPrefix   = "__bag."
	// sloRequestBytesKey is set on the context by the generated HTTP server handler
	// (plugins/http/httpcodegen/servergen.go): query bytes plus body bytes of the request.
	sloRequestBytesKey = "requestBytes"
	sloMetricsInterval = 60 * time.Second
)

var sloKeys = [...]string{sloKeyBudget, sloKeyRoot, sloKeyClass, sloKeyRootHost}

// sloAgentState is what the agent serves at /slobudget/sdk.
type sloAgentState struct {
	Version int     `json:"version"`
	B       float64 `json:"b"`
	Host    string  `json:"host"`
}

// SLOBudgetProcessor is the SDK-side budget issuer / baggage copier.
type SLOBudgetProcessor struct {
	url     string
	hc      *http.Client
	buckets [2]int64

	mu sync.RWMutex
	st sloAgentState
	ok bool

	stop chan struct{}
	wg   sync.WaitGroup

	roots, stamped, copied, skipped atomic.Int64
}

// NewSLOBudgetProcessor builds the issuer for the agent at agentEndpoint (host[:port]; the
// port is replaced by SLO_BUDGET_PORT).  Returns nil when disabled.
func NewSLOBudgetProcessor(agentEndpoint string) *SLOBudgetProcessor {
	port := os.Getenv("SLO_BUDGET_PORT")
	if port == "off" || port == "0" {
		slog.Info("slobudget issuer disabled (SLO_BUDGET_PORT)")
		return nil
	}
	if port == "" {
		port = "8082"
	}
	host := agentEndpoint
	if h, _, err := net.SplitHostPort(agentEndpoint); err == nil {
		host = h
	}
	if host == "" {
		host = "localhost"
	}
	poll := 1000
	if v, err := strconv.Atoi(os.Getenv("SLO_BUDGET_POLL_MS")); err == nil && v > 0 {
		poll = v
	}
	p := &SLOBudgetProcessor{
		url:     fmt.Sprintf("http://%s/slobudget/sdk", net.JoinHostPort(host, port)),
		hc:      &http.Client{Timeout: 800 * time.Millisecond},
		buckets: [2]int64{512, 1024},
		stop:    make(chan struct{}),
	}
	if v := os.Getenv("SLO_BUDGET_BUCKETS"); v != "" { // "small,large" byte thresholds
		parts := strings.Split(v, ",")
		if len(parts) == 2 {
			a, e1 := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
			b, e2 := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			if e1 == nil && e2 == nil && a > 0 && b > a {
				p.buckets = [2]int64{a, b}
			}
		}
	}
	p.wg.Add(1)
	go p.loop(time.Duration(poll) * time.Millisecond)
	slog.Info("slobudget issuer: polling the node-local agent", "url", p.url, "poll_ms", poll, "buckets", p.buckets)
	return p
}

func (p *SLOBudgetProcessor) loop(every time.Duration) {
	defer p.wg.Done()
	t := time.NewTicker(every)
	defer t.Stop()
	m := time.NewTicker(sloMetricsInterval)
	defer m.Stop()
	fails := 0
	for {
		p.poll(&fails)
		select {
		case <-p.stop:
			return
		case <-m.C:
			p.mu.RLock()
			st, ok := p.st, p.ok
			p.mu.RUnlock()
			slog.Info("slobudget_issuer_metrics", "roots", p.roots.Load(), "stamped", p.stamped.Load(),
				"copied", p.copied.Load(), "skipped_no_agent", p.skipped.Load(), "agent_ok", ok,
				"B", st.B, "host", st.Host, "version", st.Version)
		case <-t.C:
		}
	}
}

func (p *SLOBudgetProcessor) poll(fails *int) {
	resp, err := p.hc.Get(p.url)
	if err == nil && resp.StatusCode == http.StatusOK {
		var st sloAgentState
		err = json.NewDecoder(resp.Body).Decode(&st)
		resp.Body.Close()
		if err == nil && st.B > 0 && st.Host != "" {
			p.mu.Lock()
			was := p.ok
			p.st, p.ok = st, true
			p.mu.Unlock()
			if !was {
				slog.Info("slobudget issuer: agent reachable", "host", st.Host, "B", st.B, "version", st.Version)
			}
			*fails = 0
			return
		}
	} else if resp != nil {
		resp.Body.Close()
	}
	*fails++
	if *fails == 3 { // three misses in a row: stop stamping until the agent is back
		p.mu.Lock()
		was := p.ok
		p.ok = false
		p.mu.Unlock()
		if was {
			slog.Warn("slobudget issuer: agent unreachable, roots carry no budget", "url", p.url, "error", err)
		}
	}
}

// OnStart implements sdktrace.SpanProcessor.
func (p *SLOBudgetProcessor) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {
	if s.Parent().IsValid() {
		// a child (same process or downstream): the keys ride in the context's baggage
		bag := backend.GetBaggageFromContext(parent)
		if bag == nil {
			return
		}
		var attrs [4]attribute.KeyValue
		n := 0
		for _, k := range sloKeys {
			if v, ok := bag[k]; ok && v != "" {
				attrs[n] = attribute.String(k, v)
				n++
			}
		}
		if n > 0 {
			s.SetAttributes(attrs[:n]...)
			p.copied.Add(1)
		}
		return
	}
	p.roots.Add(1)
	p.mu.RLock()
	st, ok := p.st, p.ok
	p.mu.RUnlock()
	if !ok {
		p.skipped.Add(1)
		return
	}
	b := int(st.B)
	if rand.Float64() < st.B-float64(b) {
		b++
	}
	if b < 1 {
		b = 1
	}
	bs := strconv.Itoa(b)
	route := sloRoute(s.Name())
	class := route + "|" + p.bucket(parent)
	s.SetAttributes(
		attribute.String(sloBagPrefix+sloKeyBudget, bs),
		attribute.String(sloBagPrefix+sloKeyRoot, route),
		attribute.String(sloBagPrefix+sloKeyClass, class),
		attribute.String(sloBagPrefix+sloKeyRootHost, st.Host),
		attribute.String(sloKeyBudget, bs),
		attribute.String(sloKeyRoot, route),
		attribute.String(sloKeyClass, class),
		attribute.String(sloKeyRootHost, st.Host),
	)
	p.stamped.Add(1)
}

// bucket classifies the request by its size on the wire (set by the HTTP handler).
func (p *SLOBudgetProcessor) bucket(ctx context.Context) string {
	v := ctx.Value(sloRequestBytesKey)
	var n int64
	switch x := v.(type) {
	case int64:
		n = x
	case int:
		n = int64(x)
	default:
		return "any"
	}
	switch {
	case n < p.buckets[0]:
		return "s"
	case n < p.buckets[1]:
		return "m"
	default:
		return "l"
	}
}

// sloRoute derives the route from the generated server span name
// "<Service>Server_<Method>": the method in snake case (ComposePost -> compose_post).
func sloRoute(name string) string {
	if i := strings.LastIndex(name, "Server_"); i >= 0 {
		name = name[i+len("Server_"):]
	} else if i := strings.LastIndex(name, "_"); i >= 0 {
		name = name[i+1:]
	}
	var b strings.Builder
	for i, c := range name {
		if c >= 'A' && c <= 'Z' {
			if i > 0 {
				prev := name[i-1]
				nextLower := i+1 < len(name) && name[i+1] >= 'a' && name[i+1] <= 'z'
				if (prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9') || (prev >= 'A' && prev <= 'Z' && nextLower) {
					b.WriteByte('_')
				}
			}
			b.WriteRune(c + ('a' - 'A'))
		} else {
			b.WriteRune(c)
		}
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
}

// OnEnd implements sdktrace.SpanProcessor.
func (p *SLOBudgetProcessor) OnEnd(sdktrace.ReadOnlySpan) {}

// Shutdown implements sdktrace.SpanProcessor.
func (p *SLOBudgetProcessor) Shutdown(context.Context) error {
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
	p.wg.Wait()
	return nil
}

// ForceFlush implements sdktrace.SpanProcessor.
func (p *SLOBudgetProcessor) ForceFlush(context.Context) error { return nil }
