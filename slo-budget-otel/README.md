# slo-budget-otel

Stage 2 of the SLO-budget span sampling project: the Stage 1 decider
(`../slo-budget-sim/slo_budget_sim/sampler.py`, pass 4) ported to Go and run as
OpenTelemetry Collector processors on the bridged DSB-SN cluster.  Settings are
fixed: local request budget, uniform plan weights, escalation off, class key
on, on-demand root pinning, frozen T_i calibration on the outlier-rate trigger,
N_need = max(3, 0.2 V* / services), credit / reserve off.  Cold start: plan
sampling only until a service's first calibrated period, or a T_i table seeded
from a file (`cold_prior: file`, `ttable_file`).

```
slobudget/                         pure Go port of the decider, calibration, refill and replay (no OTel imports)
  pyrandom.go, pyfloat.go          CPython's Mersenne Twister random(), float floor division, round, sum()
  ttable.go                        weighted quantile, square-root split, equal-share prior, Calibrator
  config.go, sampler.go            BudgetConfig and BudgetSampler: Decide(...) statement for statement
  replay.go, dump.go               the event replay and the Python dump loader of the differential test
  agentcentral.go                  the agent / central split: budget issue, period snapshot, central update
processor/slobudgetprocessor/      the per-host agent (decider at span end, root hold + kept-notice, stats)
processor/slobudgetcentralprocessor/  the central (1/p-weighted calibration, frozen trigger, refill, cap, config channel)
sdk/budgetissuer/                  W3C-baggage budget issuer + baggage-to-attribute copier (own module)
builder/                           ocb manifest and Dockerfile; tools/build-node0.sh builds and pushes on node-0
deploy/                            central Deployment/Service/ConfigMaps, agent config, headless peers service,
                                   the otelcol DaemonSet switched to this image for a run
tools/                             difftest.py (differential test), run_cluster.sh (the cluster run),
                                   jaeger_orphans.py (orphans / holes / warnings in a Jaeger export)
```

## Decider (agent)

At span end the agent looks up T_i for (route, class, service) with the
(route, "*") fallback, flags an outlier when the duration exceeds T_i or the
status is not OK, keeps outliers at p = 1 up to N_need per (service, period)
and then with p = N_need / n x (1 + miss), charges the request budget (per
trace, per host) and the host pool, plan-samples ordinary spans at the
central's uniform rate, stamps `sampling.p` and `slobudget.reason`, records
`slobudget.children_seen` / `slobudget.dropped_children` on kept parents (the
children this agent saw: same host) and `slobudget.own_us` (duration minus the
union of the span's client spans, which arrive at the same agent; on the
Blueprint SDK the client spans arrive in a later batch, so the central
derives own time from inclusive quantiles by default, `own_time: inclusive`).  Client
spans are ordinary spans of the caller (`client_spans: plan`), never outliers.
Roots are held `root_hold` (2 s); a kept-notice (17-byte UDP datagram with the
trace id) from any agent that keeps a span of the trace pins the root at p = 1,
otherwise the root is plan-sampled when the hold expires.  Without a root-host
baggage key the notice goes to every peer (`notice_peers`, a headless service).

The agent and the central both drive the same `slobudget.Sampler`; the
differential test (`tools/difftest.py`) replays the simulator's seven scenarios
x three seeds through it and demands identical keep/drop decisions, p values
and per-period B.  Reproducing CPython bit for bit needed its Mersenne Twister,
its float floor division and its compensated `sum()` (Python >= 3.12).

## Central

Every span the agents forward is a kept span with its p: the central feeds it
1/p-weighted into the per-period calibration (window 2 periods, P95 own and
inclusive time, square-root split, the frozen trigger from the agents'
per-service outlier rates), sums the agents' period counters (`POST
/slobudget/stats`), runs the refill B_next = (1 - a) B + a V* / (R_exp u) (its input is the
demanded volume, admitted + pool refusals + cap drops, `refill_input: demand`;
`admitted` reproduces the simulator, where any B above the plan's saturation
point is a fixed point once the pools bind), the host pools and the plan rates
exactly as `on_period_end` does, and publishes
`{version, B, n_need, table, plan_p, pool_full, frozen}` at `GET
/slobudget/config` (the config channel; agents pull it right after every period
boundary).  V* is a fixed `v_star`, or `v_star_frac` x the observed spans per period
(EWMA over full periods only, a period with under half the smoothed volume
is ignored); the hard cap `cap_factor` x V* span units per period is applied
first come first served before the exporter, and with an adaptive V* only
after two full periods (a pre-load trickle otherwise collapses the cap).  The call structure comes from
`call_graph_file` or is learned from kept parent/child pairs.  `GET
/slobudget/status` shows the table, the period history and the graph.

## Build and run

```
go test ./... && (cd sdk && go test ./...)                  # unit tests
python tools/difftest.py                                    # 21 Python dumps vs the Go replay (about 1 GB of dumps)
tools/build-node0.sh                                        # on node-0: tests, benchmark, ocb build, image push
tools/run_cluster.sh                                        # on node-0: deploy, 10 min wrk2 at 100 rps, CPU hog min 4-7, export, restore
python3 tools/jaeger_orphans.py results/cluster-*/export.json --times results/cluster-*/times.json
```

The image is the fork's `otelcontribcol` shape (scratch + static binary,
`--config /etc/otelcol/config.yaml`); the agent pipeline is `otlp -> slobudget
-> priority -> otlp(central)` with the fork's `configdiscovery` receiver kept
for the SDK, the central pipeline `otlp -> slobudgetcentral -> batch ->
otlp(jaeger)`.  For a run the deployment's `otelcol-pb-es-ctr` DaemonSet is
switched to this image and config (same name and labels, so the node-local
Service keeps routing the SDKs to it) and rolled back with `kubectl rollout
undo` afterwards; the app and the SDK bridge are untouched.

## Baggage in DSB-SN: what propagates and what does not

Blueprint's generated OT wrappers (`plugins/opentelemetry/ir_ot_client.go`,
`ir_ot_server.go` on the fork's `retctx-reject-impl`) carry a JSON trace
carrier *inside every RPC* with a baggage map: upstream baggage, the SDK's
side-table entries (`backend.RegisterSpanBaggageSource`) and any span attribute
prefixed `__bag.` (`__bagdel.` deletes).  The server wrapper puts the inbound
map into the context (`backend.GetBaggageFromContext` in the SDK processor's
OnStart).  So an SDK span processor can issue `__bag.slo.b`, `__bag.slo.r`,
`__bag.slo.c`, `__bag.slo.h` on the root span (no inbound baggage and no parent
= root) and copy the four keys to plain attributes on every downstream span;
that is the DSB-SN form of `sdk/budgetissuer`.  What does not work: W3C
`baggage` / `traceparent` headers from the wrk2 client are not read (the
wrappers use their own carrier, no propagator is installed), spans carry no
host, route or class attribute today (`_br` / `_d`, `span.kind`,
`otel.scope.name` only), and adding the copy means rebuilding the app images,
which the cluster run does not do.  The agent therefore falls back to its own
budget issue from the central's B (per trace, per host), the configured
`default_route`, class `route|any`, and kept-notices broadcast to all peers.
