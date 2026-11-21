package slobudgetcentralprocessor

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/RoyZhang7/SLO-budget-Sampling/slo-budget-otel/slobudget"
)

// kept spans of one request as the agents would forward them: wrk2api -> composepost -> {socialgraph, poststorage}
func request(tid byte, base time.Time, slow bool) ptrace.Traces {
	td := ptrace.NewTraces()
	add := func(service string, id, parent byte, start, dur time.Duration, p float64) {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "unknown_service:"+service+"_service_pb_es_proc")
		span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetName(service)
		var t pcommon.TraceID
		t[15], t[14] = tid, 1
		span.SetTraceID(t)
		var sid pcommon.SpanID
		sid[7], sid[6] = id, tid
		span.SetSpanID(sid)
		if parent != 0 {
			var pid pcommon.SpanID
			pid[7], pid[6] = parent, tid
			span.SetParentSpanID(pid)
		}
		span.SetKind(ptrace.SpanKindServer)
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(base.Add(start)))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(base.Add(start + dur)))
		span.Attributes().PutDouble("sampling.p", p)
		span.Attributes().PutInt("slobudget.own_us", int64(dur/time.Microsecond)/2)
	}
	sg := 500 * time.Microsecond
	if slow {
		sg = 4 * time.Millisecond
	}
	add("socialgraph", 3, 2, 3*time.Millisecond, sg, 0.5)
	add("poststorage", 4, 2, 3*time.Millisecond+200*time.Microsecond, time.Millisecond, 0.5) // overlaps socialgraph: parallel
	add("composepost", 2, 1, time.Millisecond, 10*time.Millisecond, 0.5)
	add("wrk2api", 1, 0, 0, 13*time.Millisecond, 1.0)
	return td
}

func TestCentralCalibratesRefillsAndServesTheUpdate(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Listen = "127.0.0.1:0"
	cfg.VStar = 400
	cfg.StatsLog = false
	cfg.Services = 4
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	sink := new(consumertest.TracesSink)
	c, err := newCentral(processortest.NewNopSettings(typeStr), cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Shutdown(context.Background()) }()
	base := time.Now()
	for i := 0; i < 60; i++ {
		if err := c.ConsumeTraces(context.Background(), request(byte(i), base, false)); err != nil {
			t.Fatal(err)
		}
	}
	// two agents report the period
	c.mu.Lock()
	per := c.period
	c.mu.Unlock()
	for _, host := range []string{"node-1", "node-2"} {
		st := slobudget.AgentStats{Host: host, Period: per, Requests: 30, SpansHost: 1200,
			SpansSvc: map[string]float64{"wrk2api": 60, "composepost": 60, "socialgraph": 60, "poststorage": 60},
			Ndet: map[string]int{}, KeptTotal: 120, KeptOutlier: 0, KeptByReason: map[string]int{"plan": 120}}
		body, _ := json.Marshal(st)
		resp, err := http.Post("http://"+c.Addr()+"/slobudget/stats", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	c.closePeriod()
	resp, err := http.Get("http://" + c.Addr() + "/slobudget/config")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var u slobudget.CentralUpdate
	if err := json.NewDecoder(resp.Body).Decode(&u); err != nil {
		t.Fatal(err)
	}
	if u.Version != 1 || u.B <= 0 || u.NNeed != 20 { // floor(0.2 * 400 / 4)
		t.Fatalf("update %+v", u)
	}
	tab := u.Table["compose_post"]["*"]
	if tab == nil || len(tab) != 4 || tab["wrk2api"] != cfg.SLOMs["compose_post"]/1000.0 {
		t.Fatalf("table %v", u.Table)
	}
	if tab["socialgraph"] <= 0 || tab["socialgraph"] >= tab["composepost"] || tab["composepost"] >= tab["wrk2api"] {
		t.Fatalf("T order %v", tab)
	}
	if u.PoolFull["node-1"] != 200 || u.PoolFull["node-2"] != 200 { // equal shares of V* = 400
		t.Fatalf("pools %v", u.PoolFull)
	}
	if len(u.PlanP) != 4 {
		t.Fatalf("plan %v", u.PlanP)
	}
	g := c.graph.graph
	if g.Root["compose_post"] != "wrk2api" || len(g.Children["compose_post"]["composepost"]) != 2 || !g.Parallel["compose_post"]["composepost"] {
		t.Fatalf("learned graph %+v", g)
	}
	if sink.SpanCount() != 240 {
		t.Fatalf("forwarded %d", sink.SpanCount())
	}
}

func TestCentralCapIsAHardBackstop(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Listen = "127.0.0.1:0"
	cfg.VStar = 8
	cfg.CapFactor = 1.0
	cfg.StatsLog = false
	sink := new(consumertest.TracesSink)
	c, err := newCentral(processortest.NewNopSettings(typeStr), cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	for i := 0; i < 5; i++ { // 20 spans into a cap of 8
		_ = c.ConsumeTraces(context.Background(), request(byte(i), base, false))
	}
	if sink.SpanCount() != 8 || c.capDrops != 12 {
		t.Fatalf("admitted %d dropped %d", sink.SpanCount(), c.capDrops)
	}
}
