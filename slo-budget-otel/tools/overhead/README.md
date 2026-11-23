# tools/overhead: the end-to-end overhead experiment

Runs only after the campaign's queue is empty (`overhead_queue.sh` refuses to start before).

## Configurations

Same app and collector images as the campaign; only the services' env and the collector
DaemonSet differ.

| | app env | what the SDK records | Path Bridge | collector |
|---|---|---|---|---|
| A, tracing off | `OTEL_SAMPLE_RATIO=0`, `SLO_BUDGET_PORT=off`, `BRIDGE_KIND=pb` | nothing (NeverSample) | idle: the SDK calls no processor for a non-recording span and the Bridge's checkpoint hook returns for one; its export and metrics loops run empty | the stock DaemonSet, nothing to receive |
| B, SDK head sampling 12 % | `OTEL_SAMPLE_RATIO=0.12`, `SLO_BUDGET_PORT=off`, `BRIDGE_KIND=pb` | 12 % of traces, whole (ParentBased(TraceIDRatioBased) at the root) | full work on those spans: breadcrumbs, `_br` / `_d`, HP/LP buffered export | the stock DaemonSet -> Jaeger |
| C, record everything into our agent | both unset (AlwaysSample; the budget issuer on), `BRIDGE_KIND=pb` | every span | full work on every span, all exported to the node's agent | our agent at a collection volume target of 12 % (the campaign's chosen arm, no tee, no shadow) -> the central on node-0 -> Jaeger |
| D, C with the bridge off | both unset, `BRIDGE_KIND=v` | every span | none: the SDK's vanilla processor (`v`), no new image; the same plain OTLP export (one 100-ms / 512-span buffer instead of the Bridge's HP / LP pair), no `_br` / `_d` | as C |

The Bridge acts only on recorded spans, so its work cannot be held equal between B and C; each run
records the span processor's counters (`pb_processor_metrics`; D: `vanilla_processor_metrics`: spans
received / sent / dropped per service) to report the difference.  D isolates the Bridge: C minus D is
what it costs at 100 % recording and the same collection volume target (D's spans carry no `_br` /
`_d`, and the root span's four `__bag.slo.*` attributes reach the wire, which the Bridge strips:
about 100 bytes per root).

**Budget issuer (A and B).** With `SLO_BUDGET_PORT=off` the issuer's span processor is not registered
with the tracer provider at all: `NewSLOBudgetProcessor` returns nil before starting its poll
goroutine and `trace.go` adds the processor only when it is non-nil. The app image logs
`slobudget issuer disabled (SLO_BUDGET_PORT)` at start-up; `run_overhead.sh` fails a run whose app pods
do not show it, and (C, D) one whose pods are not polling.

## Protocol

1. `knee.sh` (config C, one reseed): 100, 150, 200, 250, 300, 350 req/s, 2 min each; the higher
   rate is the highest one whose p99 stays within 2 x the 100 req/s p99 with the rate achieved.
2. `overhead_queue.sh <knee rate>`: 5 repeats x (100 req/s, the knee rate) x (A B C D), interleaved
   A B C D at 100, A B C D at the knee, then the next repeat.  Each run (`run_overhead.sh`): set the
   env (the services roll when it changes) -> reseed -> C / D: our agent and the central -> the
   issuer check -> 30-s warm-up at the run's rate -> CPU snapshot -> 5-min wrk2 load, fault-free ->
   CPU snapshot -> the Bridge's counters, placement, image digests -> the stock collector back.  At
   the end the env is reset to the campaign's (`BRIDGE_KIND=pb`, the two variables unset).
3. `tables.py <dir>`: per config and rate, mean and min-max over the repeats of wrk2's p50 / p99
   and achieved req/s, app-pod CPU per request (traced services; the data stores apart), agent
   DaemonSet cores, the central's cores (C and D) and Jaeger + Elasticsearch cores.

CPU comes from the kubelet summary API (`cpu_snapshot.py`: cumulative usageCoreNanoSeconds per
container) before and after each load.  Cluster time: the sweep about 20 min, 40 runs about 11 min
each (13 for C and D), about 8 h in all.
