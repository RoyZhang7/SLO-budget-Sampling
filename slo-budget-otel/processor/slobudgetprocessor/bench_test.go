package slobudgetprocessor

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/probabilisticsamplerprocessor"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
)

// Microbenchmark: per-span CPU and added latency of the agent processor against
// the stock probabilistic sampler on synthetic DSB-SN-like batches: one service
// per batch as the SDK exports, 12 services, 12 spans per trace (so the
// per-trace state is shared like in production), 5 % outliers, roots in the
// wrk2api batches.
//
//	go test -run XXX -bench . -benchmem ./processor/slobudgetprocessor/
//	SLOBUDGET_BENCH=1 go test -run TestRatePaced -v ./processor/slobudgetprocessor/

var benchServices = []string{"wrk2api", "composepost", "hometimeline", "usertimeline", "poststorage", "text", "urlshorten",
	"usermention", "socialgraph", "uniqueid", "media", "user"}
var benchMedianMs = map[string]float64{"wrk2api": 13.0, "composepost": 12.0, "hometimeline": 3.9, "usertimeline": 5.7, "poststorage": 0.9,
	"text": 3.2, "urlshorten": 0.9, "usermention": 0.6, "socialgraph": 0.5, "uniqueid": 0.05, "media": 0.02, "user": 0.02}

func benchTable() map[string]map[string]float64 {
	t := map[string]float64{}
	for svc, m := range benchMedianMs {
		t[svc] = m / 1000 * 2.2 // roughly P95 x slack
	}
	return map[string]map[string]float64{"compose_post": t}
}

// makeBatch builds n spans of one service (rotating by k).  Span i of batch k
// belongs to trace (k/12, i): the 12 batches of one round share their traces,
// so each trace sees one span per service, as on a real node.
func makeBatch(rng *rand.Rand, k, n int, base time.Time) ptrace.Traces {
	svcIdx := k % len(benchServices)
	svc := benchServices[svcIdx]
	round := k / len(benchServices)
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "unknown_service:"+svc+"_service_pb_es_proc")
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	for i := 0; i < n; i++ {
		span := spans.AppendEmpty()
		span.SetName(svc + ".op")
		var tid pcommon.TraceID
		tid[0], tid[1], tid[2], tid[3] = byte(round>>24), byte(round>>16), byte(round>>8), byte(round)
		tid[4], tid[5] = byte(i>>8), byte(i)
		tid[15] = 0xA5
		span.SetTraceID(tid)
		var sid pcommon.SpanID
		copy(sid[:6], tid[:6])
		sid[7] = byte(svcIdx + 1)
		span.SetSpanID(sid)
		if svc != "wrk2api" { // child of the composepost span (or of the root)
			var pid pcommon.SpanID
			copy(pid[:6], tid[:6])
			pid[7] = 2
			if svc == "composepost" {
				pid[7] = 1
			}
			span.SetParentSpanID(pid)
		}
		span.SetKind(ptrace.SpanKindServer)
		d := benchMedianMs[svc] / 1000 * math.Exp(0.35*rng.NormFloat64())
		if rng.Float64() < 0.05 {
			d *= 6 // an outlier
		}
		st := base.Add(time.Duration(rng.Int63n(int64(time.Second))))
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(st))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(st.Add(time.Duration(d * float64(time.Second)))))
		span.Attributes().PutStr("_br", "BJDGrXHiJyKXIAAA")
		span.Attributes().PutStr("otel.scope.name", svc)
	}
	return td
}

func benchAgent(tb testing.TB, sink consumer.Traces) *agent {
	cfg := createDefaultConfig().(*Config)
	cfg.StandaloneVStar = 1400
	cfg.Host = "bench"
	cfg.NoticeListen = "127.0.0.1:0"
	cfg.RootHold = 0 // decide roots at once: measure the decider, not the hold
	cfg.StatsLog = false
	cfg.Sampler.ColdPrior = "file"
	dir := tb.TempDir()
	raw := []byte("{\"compose_post\":{")
	first := true
	for svc, T := range benchTable()["compose_post"] {
		if !first {
			raw = append(raw, ',')
		}
		first = false
		raw = append(raw, []byte("\""+svc+"\":"+strconv.FormatFloat(T, 'g', -1, 64))...)
	}
	raw = append(raw, []byte("}}")...)
	cfg.TTableFile = dir + "/ttable.json"
	if err := os.WriteFile(cfg.TTableFile, raw, 0o600); err != nil {
		tb.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		tb.Fatal(err)
	}
	a, err := newAgent(processortest.NewNopSettings(typeStr), cfg, sink)
	if err != nil {
		tb.Fatal(err)
	}
	if err := a.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		tb.Fatal(err)
	}
	return a
}

func benchProbabilistic(tb testing.TB, sink consumer.Traces) consumer.Traces {
	f := probabilisticsamplerprocessor.NewFactory()
	cfg := f.CreateDefaultConfig().(*probabilisticsamplerprocessor.Config)
	cfg.SamplingPercentage = 12
	p, err := f.CreateTraces(context.Background(), processortest.NewNopSettings(f.Type()), cfg, sink)
	if err != nil {
		tb.Fatal(err)
	}
	if err := p.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		tb.Fatal(err)
	}
	return p
}

func benchBatches(n, size int) []ptrace.Traces {
	rng := rand.New(rand.NewSource(7))
	base := time.Now()
	out := make([]ptrace.Traces, n)
	for i := range out {
		out[i] = makeBatch(rng, i, size, base)
	}
	return out
}

func runBench(b *testing.B, p consumer.Traces) {
	src := benchBatches(240, 100)
	b.ResetTimer()
	spans := 0
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		td := ptrace.NewTraces()
		src[i%len(src)].CopyTo(td)
		b.StartTimer()
		spans += 100
		_ = p.ConsumeTraces(context.Background(), td)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(spans), "ns/span")
}

func BenchmarkSlobudgetAgent(b *testing.B) {
	a := benchAgent(b, consumertest.NewNop())
	defer func() { _ = a.Shutdown(context.Background()) }()
	runBench(b, a)
}

func BenchmarkProbabilisticSampler(b *testing.B) {
	p := benchProbabilistic(b, consumertest.NewNop())
	runBench(b, p)
}

// TestRatePaced drives both processors at 1k and 10k spans/s for 10 s and
// reports the CPU of the calling thread per span (the call itself), the
// process CPU per span (everything, GC included) and the ConsumeTraces
// latency distribution per batch.
func TestRatePaced(t *testing.T) {
	if os.Getenv("SLOBUDGET_BENCH") == "" {
		t.Skip("set SLOBUDGET_BENCH=1")
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	type target struct {
		name string
		p    consumer.Traces
		stop func()
	}
	fmt.Println("RATE processor spans/s batches size thread_cpu_us/span proc_cpu_us/span wall_us/span p50_us/batch p99_us/batch max_us/batch")
	for _, rate := range []int{1000, 10000} {
		size := rate / 100 // one batch every 10 ms
		src := benchBatches(240, size)
		a := benchAgent(t, consumertest.NewNop())
		targets := []target{
			{"slobudget", a, func() { _ = a.Shutdown(context.Background()) }},
			{"probabilistic", benchProbabilistic(t, consumertest.NewNop()), func() {}},
		}
		for _, tg := range targets {
			lat := make([]float64, 0, 1200)
			tcpu := time.Duration(0)
			cpu0 := processCPU()
			tick := time.NewTicker(10 * time.Millisecond)
			deadline := time.Now().Add(10 * time.Second)
			i, spans := 0, 0
			for now := range tick.C {
				if now.After(deadline) {
					break
				}
				td := ptrace.NewTraces()
				src[i%len(src)].CopyTo(td)
				n := td.SpanCount()
				c0 := threadCPU()
				t0 := time.Now()
				_ = tg.p.ConsumeTraces(context.Background(), td)
				lat = append(lat, float64(time.Since(t0).Microseconds()))
				tcpu += threadCPU() - c0
				spans += n
				i++
			}
			tick.Stop()
			cpu := processCPU() - cpu0
			sort.Float64s(lat)
			q := func(f float64) float64 { return lat[int(f*float64(len(lat)-1))] }
			wall := 0.0
			for _, l := range lat {
				wall += l
			}
			fmt.Printf("RATE %s %d %d %d %.2f %.2f %.2f %.1f %.1f %.1f\n", tg.name, rate, len(lat), size,
				tcpu.Seconds()*1e6/float64(spans), cpu.Seconds()*1e6/float64(spans), wall/float64(spans), q(0.5), q(0.99), lat[len(lat)-1])
			tg.stop()
		}
	}
}
