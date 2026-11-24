# tools/overhead: the end-to-end overhead experiment

Runs only after the campaign's queue is empty (`overhead_queue.sh` refuses to start before).

## Configurations

Same app images as the campaign; only the services' env and the collector DaemonSet differ.  The
collector image is the stock one for A and B and, unless `IMAGE_TAG` says otherwise, the campaign's for C
and D.  **E (opt-in, `CONFIGS="A B C D E"`) needs a collector image with `arrivald` in it, built from this
tree after the campaign (`build-node0.sh` refuses while a run is in progress), and C and D must run on the
same one** (`IMAGE_TAG=<tag>`, for `knee.sh` too), so that C and E differ only by the program;
`overhead_queue.sh` refuses E without `IMAGE_TAG` and when collected C, D or E runs had another image.  C, D
and E use the staged call graph (`CALLGRAPH`, default `deploy/callgraph.stages.json`; an image from before the
fix ignores the stages).

| | app env | what the SDK records | Path Bridge | collector |
|---|---|---|---|---|
| A, tracing off | `OTEL_SAMPLE_RATIO=0`, `SLO_BUDGET_PORT=off`, `BRIDGE_KIND=pb` | nothing (NeverSample) | idle: the SDK calls no processor for a non-recording span and the Bridge's checkpoint hook returns for one; its export and metrics loops run empty | the stock DaemonSet, nothing to receive |
| B, SDK head sampling 12 % | `OTEL_SAMPLE_RATIO=0.12`, `SLO_BUDGET_PORT=off`, `BRIDGE_KIND=pb` | 12 % of traces, whole (ParentBased(TraceIDRatioBased) at the root) | full work on those spans: breadcrumbs, `_br` / `_d`, HP/LP buffered export | the stock DaemonSet -> Jaeger |
| C, record everything into our agent | both unset (AlwaysSample; the budget issuer on), `BRIDGE_KIND=pb` | every span | full work on every span, all exported to the node's agent | our agent at a collection volume target of 12 % of the spans at the run's rate, the share B keeps (the campaign's chosen arm, no tee, no shadow; 1 380 spans per 5 s at 100 req/s, 4 140 at 300) -> the central on node-0 -> Jaeger |
| D, C with the bridge off | both unset, `BRIDGE_KIND=v` | every span | none: the SDK's vanilla processor (`v`), no new image; the same plain OTLP export (one 100-ms / 512-span buffer instead of the Bridge's HP / LP pair), no `_br` / `_d` | as C |
| E, C with the eBPF arrival program attached | as C | every span | as C | as C, and: `arrivald` (a privileged DaemonSet, same image) keeps the program at every service pod's interface (tc ingress); the agent looks every server span's arrival up in the program's table (mmap, 1 048 576 slots) and attaches `slobudget.arrival_ns` |

The Bridge acts only on recorded spans, so its work cannot be held equal between B and C; each run
records the span processor's counters (`pb_processor_metrics`; D: `vanilla_processor_metrics`: spans
received / sent / dropped per service) to report the difference.  D isolates the Bridge: C minus D is
what it costs at 100 % recording and the same collection volume target (D's spans carry no `_br` /
`_d`, and the root span's four `__bag.slo.*` attributes reach the wire, which the Bridge strips:
about 100 bytes per root).  E minus C is what the arrival stamp costs: the program on every packet to a
service pod, `arrivald`, the agent's lookup and two attributes per server span.  The program runs in the
kernel's receive path, on the caller's CPU for a call within the node (charged to the caller's pod) and
on the network card's otherwise (charged to no container); it is far too small for the nodes' CPU
figures, so its own cost comes from the kernel's run statistics (`arrivald -stats`: columns "arrival
program, cores" and "ns per packet"; the statistics themselves cost some 40 ns a run).
`run_overhead.sh` runs the kernel's verifier first (`arrivald -check`, node-0) and fails an E run unless
the program is at every service pod, every agent has opened the table and, in the warm-up and again after
the load, the ids arrive (`arrival_check.py`: at least 95 % of the agents' lookups answered by the program, no
errors, no restarts).  The run, a failed one too, keeps arrivald's and the agents' lines (`arrival-<node>.log`,
`arrival-agent-<node>.log`); the tables give the share of the server spans that got the program's time.  Every
run and `knee.sh` refuse to start while an arrivald is left.  Every configuration fails a run when a container
restarted or a pod changed during the load, and when the services' env is not the configuration's.

**First live check (before any E run).** `IMAGE_TAG=<tag> tools/overhead/live_check.sh [service]`: the loader of that
image on one pod (`-pid`) during a 30-s warm-up load, `tcpdump` at the same interface at the same time, the table dumped
through the agent's mapping, the program off again.  `live_check.py` compares the stored ids and times with the capture
(by packet, and by TCP reassembly for the ids split between packets), the counters, the interface before and after,
and reports how payload in several fragments was handled; exit 0 only when all holds.  It gives two shares of ids stored
with a right time: of the ids that were whole in a packet (what the program can see; at least 99 % is part of "all holds"),
and of all ids of the capture, where an id split between two packets counts as not found.

**Budget issuer (A and B).** With `SLO_BUDGET_PORT=off` the issuer's span processor is not registered
with the tracer provider at all: `NewSLOBudgetProcessor` returns nil before starting its poll
goroutine and `trace.go` adds the processor only when it is non-nil. The app image logs
`slobudget issuer disabled (SLO_BUDGET_PORT)` at start-up; `run_overhead.sh` fails a run whose app pods
do not show it, and (C, D, E) one whose pods are not polling.

## Protocol

Load: wrk2 with exponential gaps (`-D exp`) on one connection per req/s, the same in every
configuration.  wrk2's default pace sends the requests of all connections as one burst every 100 ms
(the campaign's load, unchanged).  wrk2 does not wait for the response before a connection's next
request and the server takes a connection's requests one at a time: with one connection per req/s
1.5 % of the requests find an earlier one on their connection, with the campaign's 10 connections
15 % would.  The gaps are drawn from an unseeded `rand()`: the same numbers in every run, dealt to
the connections in the order they ask.

1. `knee.sh` (config C, one reseed): 100, 150, 200, 250, 300, 350 req/s, 2 min each, the agents and
   the central fresh at every rate with the target at that rate (`render.py --rate`); the higher
   rate is the highest one whose p99 stays within 2 x the 100 req/s p99 with the rate achieved
   (within 1 % and three standard deviations of the Poisson count).
2. `overhead_queue.sh <knee rate>`: 5 repeats x (100 req/s, the knee rate) x (A B C D), interleaved
   A B C D at 100, A B C D at the knee, then the next repeat; with E
   `CONFIGS="A B C D E" IMAGE_TAG=<tag> overhead_queue.sh <knee rate>`.  Each run (`run_overhead.sh`): set the env (the services roll when it changes) -> reseed -> C / D / E: our
   agent and the central (E: `arrivald`) -> the issuer check -> 30-s warm-up at the run's rate -> CPU
   snapshot -> 5-min wrk2 load, fault-free -> CPU snapshot -> the Bridge's counters, placement, image
   digests -> the stock collector back, `arrivald` removed.  However the queue ends, the env is reset to the
   campaign's (`BRIDGE_KIND=pb`, the two variables unset).  A failed try is repeated once and its directory goes to
   `failed/`; the queue stops when a configuration has lost two runs, when four tries in a row failed, and before the
   next run when `overhead/STOP` is there.  Campaign runs wait while it is going (`run_one.sh`).
3. `tables.py <dir>`: per config and rate, mean and min-max over the repeats of wrk2's p50 / p99
   and achieved req/s, app-pod CPU per request (traced services; the data stores apart), agent
   DaemonSet cores, `arrivald`'s (E), the central's cores (C to E), Jaeger + Elasticsearch cores and the
   app nodes' cores as a whole; for C to E the share of the offered spans the central admitted (the check
   that they keep B's 12 %), for E the share of arrivals that came from the program.

CPU comes from the kubelet summary API (`cpu_snapshot.py`: cumulative usageCoreNanoSeconds per
container and per node, each with the time it was sampled) before and after each load.  Cluster time:
the sweep about 27 min, 40 runs about 11 min each (13 for C and D), about 8 h in all; with E 50 runs,
about 10 h.
