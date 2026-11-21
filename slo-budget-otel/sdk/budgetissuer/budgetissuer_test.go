package budgetissuer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Two processes (frontend and backend), each with its own TracerProvider and
// the W3C TraceContext + Baggage propagators: the frontend issues the budget,
// the backend's span carries it as attributes.
func TestBudgetPropagatesOverHTTPAndLandsOnBackendSpans(t *testing.T) {
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	backExp := tracetest.NewInMemoryExporter()
	backTP := sdktrace.NewTracerProvider(sdktrace.WithSyncer(backExp), sdktrace.WithSpanProcessor(NewAttributeCopier()))
	backend := httptest.NewServer(otelhttp.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// a child span of the backend's server span: the copier sees the same baggage
		_, span := backTP.Tracer("backend").Start(r.Context(), "work")
		span.End()
		w.WriteHeader(204)
	}), "backend", otelhttp.WithTracerProvider(backTP), otelhttp.WithPropagators(prop)))
	defer backend.Close()

	frontExp := tracetest.NewInMemoryExporter()
	frontTP := sdktrace.NewTracerProvider(sdktrace.WithSyncer(frontExp), sdktrace.WithSpanProcessor(NewAttributeCopier()))
	issuer := New(Options{RootService: "wrk2api", Host: "node-1", Budget: Constant(2.5)})
	client := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport, otelhttp.WithTracerProvider(frontTP), otelhttp.WithPropagators(prop))}
	frontend := httptest.NewServer(otelhttp.NewHandler(issuer.HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, backend.URL+"/store", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		resp.Body.Close()
		w.WriteHeader(204)
	})), "frontend", otelhttp.WithTracerProvider(frontTP), otelhttp.WithPropagators(prop)))
	defer frontend.Close()

	for i := 0; i < 20; i++ {
		resp, err := http.Post(frontend.URL+"/compose", "text/plain", strings.NewReader("hello"))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	spans := backExp.GetSpans()
	if len(spans) != 40 { // server span + child per request
		t.Fatalf("backend spans %d", len(spans))
	}
	budgets := map[string]int{}
	for _, s := range spans {
		got := map[string]string{}
		for _, a := range s.Attributes {
			got[string(a.Key)] = a.Value.AsString()
		}
		if got[KeyRoot] != "wrk2api" || got[KeyClass] != "compose|small" || got[KeyRootHost] != "node-1" {
			t.Fatalf("backend span %s attributes %v", s.Name, got)
		}
		budgets[got[KeyBudget]]++
	}
	if budgets["2"] == 0 || budgets["3"] == 0 || budgets["2"]+budgets["3"] != 40 {
		t.Fatalf("budgets should be 2 or 3 (B = 2.5): %v", budgets)
	}
	// the frontend's own spans carry the members too (the middleware runs under the server span, so the
	// server span itself predates the stamp; its client span does not)
	var frontClient int
	for _, s := range frontExp.GetSpans() {
		for _, a := range s.Attributes {
			if a.Key == attribute.Key(KeyBudget) {
				frontClient++
			}
		}
	}
	if frontClient != 20 {
		t.Fatalf("frontend client spans with slo.budget: %d", frontClient)
	}
	// a hop that already carries a budget does not re-issue
	ctx, issued := issuer.Stamp(context.Background(), "x", "any")
	if !issued {
		t.Fatal("first stamp should issue")
	}
	if _, again := issuer.Stamp(ctx, "x", "any"); again {
		t.Fatal("second stamp on the same context must not re-issue")
	}
}
