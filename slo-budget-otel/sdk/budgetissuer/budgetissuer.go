// Package budgetissuer is the SDK side of the SLO-budget sampler for stacks
// that propagate W3C baggage: the budget issuer at the frontend and the
// baggage-to-attribute copy every service runs, so the collector-side agent
// (processor/slobudgetprocessor) finds the request budget, the root service,
// the request class and the root's host on each span.
//
//	issuer := budgetissuer.New(budgetissuer.Options{RootService: "wrk2api", Host: hostname, Budget: budgetissuer.Constant(3.0)})
//	handler = issuer.HTTPMiddleware(handler)          // frontend: stamps slo.b, slo.r, slo.c, slo.h into baggage
//	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(budgetissuer.NewAttributeCopier()), ...)
//	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
//
// The baggage keys are short on purpose (they travel on every hop):
//
//	slo.b  integer request budget (stochastic rounding of the period's B)
//	slo.r  root service, i.e. the route key of the T_i table
//	slo.c  request class: route|bucket (bucket from the request size)
//	slo.h  host of the root's agent (kept-notice target)
//
// DSB-SN as built by Blueprint does not use W3C baggage headers: its generated
// RPC wrappers carry a JSON trace carrier with a baggage map inside the request,
// and any span attribute prefixed "__bag." becomes a baggage key downstream.
// There the equivalent of this package is a few lines in the SDK span
// processor: at OnStart, a span with no inbound baggage and no parent is the
// root, so set attributes __bag.slo.b / __bag.slo.r / __bag.slo.c / __bag.slo.h;
// every downstream OnStart reads backend.GetBaggageFromContext(parent) and
// copies the four keys to plain attributes.  See the module README.
package budgetissuer

import (
	"context"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Baggage / attribute keys.
const (
	KeyBudget   = "slo.b"
	KeyRoot     = "slo.r"
	KeyClass    = "slo.c"
	KeyRootHost = "slo.h"
)

// BudgetSource yields the current real-valued budget B (the refill loop's
// output); the issuer rounds it stochastically to an integer per request.
type BudgetSource interface{ Budget() float64 }

type constant float64

func (c constant) Budget() float64 { return float64(c) }

// Constant is a fixed B.
func Constant(b float64) BudgetSource { return constant(b) }

// Atomic is a B that another goroutine (a poller of the central) updates.
type Atomic struct{ bits atomic.Uint64 }

func (a *Atomic) Set(b float64) { a.bits.Store(mathFloat64bits(b)) }
func (a *Atomic) Budget() float64 {
	if v := a.bits.Load(); v != 0 {
		return mathFloat64frombits(v)
	}
	return 3.0
}

// Options of the issuer.
type Options struct {
	RootService string       // route key (defaults to the request's route)
	Host        string       // the frontend host, so kept-notices reach its agent
	Budget      BudgetSource // B; Constant(3) when nil
	// Route derives the route key from the request (default: the URL path with
	// a leading slash stripped, "/" -> "root").
	Route func(*http.Request) string
	// Bucket derives the payload bucket from the request (default: small < 1 KB,
	// medium < 64 KB, large, "any" when the length is unknown).
	Bucket func(*http.Request) string
	Rand   *rand.Rand
}

// Issuer stamps the budget baggage at the frontend.
type Issuer struct {
	o  Options
	mu sync.Mutex
}

// New creates an issuer.
func New(o Options) *Issuer {
	if o.Budget == nil {
		o.Budget = Constant(3.0)
	}
	if o.Rand == nil {
		o.Rand = rand.New(rand.NewSource(1))
	}
	if o.Route == nil {
		o.Route = func(r *http.Request) string {
			p := r.URL.Path
			for len(p) > 0 && p[0] == '/' {
				p = p[1:]
			}
			if p == "" {
				return "root"
			}
			return p
		}
	}
	if o.Bucket == nil {
		o.Bucket = func(r *http.Request) string {
			n := r.ContentLength
			switch {
			case n < 0:
				return "any"
			case n < 1024:
				return "small"
			case n < 65536:
				return "medium"
			}
			return "large"
		}
	}
	return &Issuer{o: o}
}

// IssueBudget rounds B stochastically to the integer budget of one request.
func (i *Issuer) IssueBudget() int {
	B := i.o.Budget.Budget()
	b := int(B)
	i.mu.Lock()
	u := i.o.Rand.Float64()
	i.mu.Unlock()
	if u < B-float64(b) {
		b++
	}
	if b < 1 {
		b = 1
	}
	return b
}

// Stamp adds the four members to the context's baggage unless slo.b is
// already present (then this is not the root hop).  It returns the context and
// whether it issued.
func (i *Issuer) Stamp(ctx context.Context, route, bucket string) (context.Context, bool) {
	bag := baggage.FromContext(ctx)
	if bag.Member(KeyBudget).Value() != "" {
		return ctx, false
	}
	root := i.o.RootService
	if root == "" {
		root = route
	}
	members := []baggage.Member{}
	for k, v := range map[string]string{KeyBudget: strconv.Itoa(i.IssueBudget()), KeyRoot: root, KeyClass: route + "|" + bucket, KeyRootHost: i.o.Host} {
		if v == "" {
			continue
		}
		m, err := baggage.NewMember(k, v)
		if err != nil {
			continue
		}
		members = append(members, m)
	}
	for _, m := range members {
		bag, _ = bag.SetMember(m)
	}
	return baggage.ContextWithBaggage(ctx, bag), true
}

// HTTPMiddleware stamps every inbound request that carries no budget yet; run
// it inside the tracing middleware (otelhttp) so the extracted baggage is
// visible and the server span's OnStart sees the members.
func (i *Issuer) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, _ := i.Stamp(r.Context(), i.o.Route(r), i.o.Bucket(r))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// AttributeCopier is a SpanProcessor that copies the four baggage members of
// the span's parent context onto the span as attributes, so the collector
// agent reads them without seeing baggage.
type AttributeCopier struct{}

// NewAttributeCopier creates the copier.
func NewAttributeCopier() sdktrace.SpanProcessor { return AttributeCopier{} }

func (AttributeCopier) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {
	bag := baggage.FromContext(parent)
	for _, k := range []string{KeyBudget, KeyRoot, KeyClass, KeyRootHost} {
		if v := bag.Member(k).Value(); v != "" {
			s.SetAttributes(attribute.String(k, v))
		}
	}
}
func (AttributeCopier) OnEnd(sdktrace.ReadOnlySpan)              {}
func (AttributeCopier) Shutdown(context.Context) error           { return nil }
func (AttributeCopier) ForceFlush(context.Context) error         { return nil }
