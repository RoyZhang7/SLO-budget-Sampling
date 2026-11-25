//go:build ignore

// This file is compiled inside the Blueprint fork (runtime/plugins/otelcol); apply_bp_patch.py copies it there with
// sdk/filter (as the package .../otelcol/slofilter) and the arrival package (as .../otelcol/sloarrival), and patches the
// Path Bridge's OnEnd to call sloFilterOnEnd.  Excluded from the sdk module's own build.

package otelcol

// SLO-budget sampling, the in-service filter (SLO-budget-Sampling/slo-budget-otel/sdk/filter): with SLO_FILTER=on the
// service exports only the spans the agent could keep.  At a span's end the Path Bridge asks sloFilterOnEnd; a dropped
// span is counted and never serialised, an exported server span carries the within-service latency and the downstream
// time the filter computed, the count of dropped spans before it, and the request's arrival.  The thresholds and the
// plan rates come with the budget issuer's poll of the agent (sloAgentState: btable, plan_p); the arrival comes from the
// eBPF arrival table (SLO_ARRIVAL_PIN, default /sys/fs/bpf/slobudget: the container needs the bpf filesystem mounted
// read-only and CAP_BPF to open the pinned map; see sloarrival).  Off (the default) nothing changes.

import (
	"context"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blueprint-uservices/blueprint/runtime/plugins/otelcol/sloarrival"
	"github.com/blueprint-uservices/blueprint/runtime/plugins/otelcol/slofilter"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	sloFilterEnv     = "SLO_FILTER"      // on | off
	sloArrivalPinEnv = "SLO_ARRIVAL_PIN" // the arrival program's pin directory
	sloFilterLogEach = 60 * time.Second
)

var sloServiceRE = regexp.MustCompile(`^(?:unknown_service:)?([A-Za-z0-9_-]+?)(?:_service)?(?:_[a-z]+_es)?(?:_proc)?$`)

// sloServiceName is the service as the agent names it (slobudget.NormalizeService):
// unknown_service:post_storage_service_pb_es_proc -> poststorage.
func sloServiceName(resourceName string) string {
	if m := sloServiceRE.FindStringSubmatch(resourceName); m != nil {
		return strings.ReplaceAll(m[1], "_", "")
	}
	return resourceName
}

type sloFilterState struct {
	st     *slofilter.State
	resMu  sync.Mutex
	res    sloarrival.Resolver
	once   sync.Once
	opened atomic.Bool
	pin    string
	last   atomic.Int64 // unix ns of the last metrics line
}

var sloFilter *sloFilterState

func init() {
	if v := strings.ToLower(os.Getenv(sloFilterEnv)); v != "on" && v != "1" && v != "true" {
		return
	}
	pin := os.Getenv(sloArrivalPinEnv)
	if pin == "" {
		pin = "/sys/fs/bpf/slobudget"
	}
	f := &sloFilterState{st: slofilter.New(""), pin: pin, res: sloarrival.Resolver{Clock: sloarrival.NewClock(sloarrival.SampleClocks)}}
	sloFilter = f
	go f.loop()
	slog.Info("slobudget filter: on", "arrival_pin", pin)
}

// loop keeps the arrival table open (arrivald pins it anew when it restarts) and the clocks' offset fresh.
func (f *sloFilterState) loop() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	id := ""
	for range t.C {
		f.resMu.Lock()
		f.res.Clock.Tick()
		f.resMu.Unlock()
		f.periodCheck(time.Now())
		now, err := sloarrival.Identity(f.pin)
		if err != nil || now == id {
			continue
		}
		tab, err := sloarrival.Open(f.pin)
		if err != nil {
			slog.Warn("slobudget filter: arrival table not open", "pin", f.pin, "err", err)
			continue
		}
		f.resMu.Lock()
		old := f.res.Table
		f.res.Table = tab
		f.resMu.Unlock()
		_ = old.Close()
		id = now
		f.opened.Store(true)
		slog.Info("slobudget filter: arrival table open", "pin", f.pin, "slots", tab.Slots())
	}
}

// sloFilterUpdate takes the issuer's poll (thresholds and plan rates); called by SLOBudgetProcessor.poll.
func sloFilterUpdate(st sloAgentState, now time.Time) {
	if sloFilter == nil || st.BTable == nil {
		return
	}
	sloFilter.st.Update(slofilter.Poll{Version: st.Version, Period: st.Period, PeriodS: st.PeriodS, BTable: st.BTable, PlanP: st.PlanP}, now)
}

// periodCheck sends the period-end count record when a period ended with dropped spans still uncounted: a span of this
// service's own (the agent counts it and drops it), which the filter lets through by its name.
func (f *sloFilterState) periodCheck(now time.Time) {
	rec, ok := f.st.PeriodEnded(now)
	if !ok {
		return
	}
	_, sp := otel.Tracer("slobudget-filter").Start(context.Background(), slofilter.CountSpanName, trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.Bool(slofilter.AttrCountRecord, true), attribute.Int64(slofilter.AttrCountPeriod, rec.Period),
			attribute.Int(slofilter.AttrDropped, rec.Dropped)))
	sp.End()
}

func sloSpanID(id trace.SpanID) uint64 {
	var v uint64
	for _, b := range id {
		v = v<<8 | uint64(b)
	}
	return v
}

// sloFilterOnEnd decides a span at its end: export it (with these extra attributes) or not.  Called by the Path
// Bridge's OnEnd before the span is serialised; the wire state of a dropped span is taken by the caller.
func sloFilterOnEnd(s sdktrace.ReadOnlySpan) ([]attribute.KeyValue, bool) {
	f := sloFilter
	if f == nil || s.Name() == slofilter.CountSpanName {
		return nil, true
	}
	f.once.Do(func() {
		for _, kv := range s.Resource().Attributes() {
			if kv.Key == "service.name" {
				f.st.Service = sloServiceName(kv.Value.AsString())
			}
		}
	})
	now := time.Now()
	parent := uint64(0)
	if s.Parent().IsValid() {
		parent = sloSpanID(s.Parent().SpanID())
	}
	start, end := s.StartTime().UnixNano(), s.EndTime().UnixNano()
	switch s.SpanKind() {
	case trace.SpanKindClient:
		d := f.st.ClientEnded(sloSpanID(s.SpanContext().SpanID()), parent, start, end)
		f.metrics(now)
		f.periodCheck(now)
		return nil, d.Export
	case trace.SpanKindServer:
		route, class := "", ""
		for _, kv := range s.Attributes() {
			switch kv.Key {
			case sloKeyRoot:
				route = kv.Value.AsString()
			case sloKeyClass:
				class = kv.Value.AsString()
			}
		}
		arrival := int64(0)
		if parent != 0 {
			f.resMu.Lock()
			at, cause := f.res.Arrival(parent, start)
			f.resMu.Unlock()
			if cause == sloarrival.Hit {
				arrival = at
			}
		}
		d := f.st.ServerEnded(sloSpanID(s.SpanContext().SpanID()), parent == 0, route, class, start, end, arrival, s.Status().Code == codes.Error, now)
		f.metrics(now)
		f.periodCheck(now)
		if !d.Export {
			return nil, false
		}
		extra := []attribute.KeyValue{attribute.Int64(slofilter.AttrDownstreamUS, d.Downstream/1000), attribute.Int(slofilter.AttrDropped, d.Dropped)}
		if arrival > 0 {
			extra = append(extra, attribute.Int64(slofilter.AttrWithinUS, d.WithinNs/1000),
				attribute.Int64("slobudget.arrival_ns", arrival), attribute.String("slobudget.arrival_source", "ebpf"))
		}
		return extra, true
	default:
		return nil, true
	}
}

func (f *sloFilterState) metrics(now time.Time) {
	last := f.last.Load()
	if now.UnixNano()-last < int64(sloFilterLogEach) || !f.last.CompareAndSwap(last, now.UnixNano()) {
		return
	}
	st := f.st.Stats()
	slog.Info("slobudget_filter_metrics", "service", f.st.Service, "roots", st.Roots, "servers", st.Servers, "clients", st.Clients, "exported", st.Exported,
		"dropped", st.Dropped+st.ClientsDropped, "outliers", st.Outliers, "errors", st.Errors, "plan", st.Plan, "no_arrival", st.NoArrival,
		"no_threshold", st.NoThreshold, "count_records", st.CountRecords, "staleness_ms", st.Staleness.Milliseconds(), "table_open", f.opened.Load(), "pending", f.st.Pending())
}
