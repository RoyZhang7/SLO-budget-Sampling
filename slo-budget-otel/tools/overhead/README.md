# tools/overhead: the end-to-end overhead experiment

Runs only after the campaign's queue is empty (`overhead_queue.sh` refuses to start before).

## Configurations

Same app and collector images as the campaign; only the services' env and the collector
DaemonSet differ.

| | app env | what the SDK records | Path Bridge (`BRIDGE_KIND=pb`) | collector |
|---|---|---|---|---|
| A, tracing off | `OTEL_SAMPLE_RATIO=0`, `SLO_BUDGET_PORT=off` | nothing (NeverSample) | idle: the SDK calls no processor for a non-recording span and the Bridge's checkpoint hook returns for one; its export and metrics loops run empty | the stock DaemonSet, nothing to receive |
| B, SDK head sampling 12 % | `OTEL_SAMPLE_RATIO=0.12`, `SLO_BUDGET_PORT=off` | 12 % of traces, whole (ParentBased(TraceIDRatioBased) at the root) | full work on those spans: breadcrumbs, `_br` / `_d`, HP/LP buffered export | the stock DaemonSet -> Jaeger |
| C, record everything into our agent | both unset (AlwaysSample; the budget issuer on) | every span | full work on every span, all exported to the node's agent | our agent at V* = 12 % (the campaign's chosen arm, no tee, no shadow) -> the central on node-0 -> Jaeger |

The Bridge acts only on recorded spans, so its work cannot be held equal across the three; each
run records its counters (`pb_processor_metrics`: spans received / sent / dropped per service) to
report the difference.

## Protocol

1. `knee.sh` (config C, one reseed): 100, 150, 200, 250, 300, 350 req/s, 2 min each; the higher
   rate is the highest one whose p99 stays within 2 x the 100 req/s p99 with the rate achieved.
2. `overhead_queue.sh <knee rate>`: 5 repeats x (100 req/s, the knee rate) x (A B C), interleaved
   A B C at 100, A B C at the knee, then the next repeat.  Each run (`run_overhead.sh`): set the
   env (the services roll when it changes) -> reseed -> C: our agent and the central -> 30-s
   warm-up at the run's rate -> CPU snapshot -> 5-min wrk2 load, fault-free -> CPU snapshot -> the
   Bridge's counters, placement, image digests -> the stock collector back.  At the end the env is
   reset to the campaign's.
3. `tables.py <dir>`: per config and rate, mean and min-max over the repeats of wrk2's p50 / p99
   and achieved req/s, app-pod CPU per request (traced services; the data stores apart), agent
   DaemonSet cores, the central's cores (C) and Jaeger + Elasticsearch cores.

CPU comes from the kubelet summary API (`cpu_snapshot.py`: cumulative usageCoreNanoSeconds per
container) before and after each load.  Cluster time: the sweep about 20 min, 30 runs about 11 min
each (13 for C), about 6 h in all.
