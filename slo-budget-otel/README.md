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
ROOT_POLICY=pin RECON=1 ARM=pin-recon tools/run_cluster.sh  # arms: root policy on_demand|pin, reconstruction on|off
HOG_FOR=0 BURST_AT=480 BURST_FOR=60 BURST_RPS=200 tools/run_cluster.sh   # a 3x traffic burst for 60 s at minute 8
python3 tools/cluster_metrics.py R/export.json R/central-status.json R/times.json   # volume, outlier share, precision, evidence
python3 tools/recon_report.py R/export.json --times R/times.json --status R/central-status.json --sample R/sample-trace.txt
python3 tools/jaeger_orphans.py results/cluster-*/export.json --times results/cluster-*/times.json
```

Stage 2c defaults (both implementations, differential test bit-identical): the
miss margin `outlier_margin` = 0.1 (a span is an outlier when its duration
exceeds 1.1 T_i or its status is an error; spans between T_i and 1.1 T_i are
plan-sampled), and a frozen service keeps its T itself while the split
renormalises to the root SLO over the unfrozen services only (previously the
whole table was rescaled every period, so a frozen T shrank while others
inflated and the freeze sustained itself).  Collector-side reconstruction is out
of the default build (`-tags slobudget_recon`); reconstruction is offline,
`tools/recon`.

Stage 2b additions.  `sdk/blueprint/` holds the DSB-SN form of the budget
issuer: `slobudget_processor.go` is a second SDK span processor in the
Blueprint fork's `runtime/plugins/otelcol` (registered by `trace.go` beside the
bridge processor, so the Path Bridge is untouched) that polls the node-local
agent (`GET otelcol-pb-es-ctr:8082/slobudget/sdk` -> `{version, b, host}`) and
stamps `__bag.slo.budget` (stochastic rounding of B), `__bag.slo.root` (the root
operation in snake case = the route), `__bag.slo.class` (`route|s|m|l` by request
bytes, thresholds `SLO_BUDGET_BUCKETS`, default 512,1024; the HTTP server
handler puts the request size on the context, `plugins/http/httpcodegen/
servergen.go`) and `__bag.slo.root_host` (the agent's node) on every root, which
Blueprint's own carrier propagates; on every other span it copies the four keys
to plain attributes.  `apply_bp_patch.py`, `rebuild_images.sh` (wiring into
`build_slobgt`, 14 images in parallel, push) and `deploy_app_images.sh`
(`kubectl set image` on the live Deployments / wrk2api DaemonSet; DBs, Jaeger
and ES stay) reproduce the rebuilt app images (`*-pb-es-ctr:v20261120-204628-
slobgt`).  Without an agent answering, the SDK stamps nothing and the agent
issues the budget itself (`budget_source: propagated` with that fallback;
`self` ignores the baggage).  The agent serves the SDK on `sdk_listen`
(the deployment's Service gets port 8082 from `run_cluster.sh`), reports its
notice address, and the central publishes `peers` (host -> ip:port) so a
kept-notice for a root held elsewhere is unicast to `slo.root_host`'s agent
(a root held locally needs none; unknown hosts fall back to the broadcast).
Reconstruction is offline since Stage 2c: `tools/recon` (`go run ./tools/recon -in
export.json -out export.recon.json -report recon-offline.json`, see
`tools/recon/README.md`) reads a Jaeger export and writes one with the
placeholders added; the shared logic is `slobudget/recon`.  The collector-side
form, `processor/slobudgetreconprocessor` (central pipeline, arm `RECON=1`), is
out of the default build (build tag `slobudget_recon`, manifest entry commented
out; the `slobudgetrecon:` block in `deploy/central.yaml` then has to go too, the
collector rejects an unknown processor type).  Both buffer / read a trace's kept
spans and insert placeholder ancestors for every kept span whose parent was
dropped, up to the nearest kept ancestor or the root: the parent's id is the span's own parent id, the grandparent's the `_br`
checkpoint anchor, higher ids are reused from the same (depth, service, kind)
position or synthesised; the service comes from the call graph, the operation
from the kept child where it is derivable (`<Svc>Client_<Method>`), start and
end bound the kept descendants (zero own time of their own), and every
placeholder carries `synthetic=true`, `slobudget.recon` and `slobudget.depth`.

The image is the fork's `otelcontribcol` shape (scratch + static binary,
`--config /etc/otelcol/config.yaml`); the agent pipeline is `otlp -> slobudget
-> priority -> otlp(central)` with the fork's `configdiscovery` receiver kept
for the SDK, the central pipeline `otlp -> slobudgetcentral -> batch ->
otlp(jaeger)`.  For a run the deployment's `otelcol-pb-es-ctr` DaemonSet is
switched to this image and config (same name and labels, so the node-local
Service keeps routing the SDKs to it) and rolled back with `kubectl rollout
undo` afterwards; the app and the SDK bridge are untouched.

## Runbook: the cluster

Node-0 of the CloudLab cluster built by `blueprint-setup-slobgt` (the fork
checkouts under `$HOME`, Go, kompose, the registry at 10.10.1.1:30000, the
bridged DSB-SN `pb_es` variant live, `~/bridges-env.sh`).  Everything below runs
on node-0 as the deploy user; nothing needs root.

1. Sync this module to `~/slo-budget-otel` (rsync or a tarball; `results/` and
   `build/` are local).
2. App images with the SDK budget issuer (once per Blueprint checkout; a fresh
   profile instantiation clones the fork without them):
   `python3 sdk/blueprint/apply_bp_patch.py` (idempotent; patches
   `~/blueprint-docc-mod`), `bash sdk/blueprint/rebuild_images.sh` (wiring into
   `build_slobgt`, 14 images, tag in `~/bp-patch/IMAGE_TAG`), then
   `bash sdk/blueprint/deploy_app_images.sh` on a live variant (`kubectl set
   image`; rollback: `kubectl rollout undo` on each service Deployment and the
   wrk2api DaemonSet, previous images in `~/bp-patch/images-before.txt`).  On a
   fresh cluster apply the patch before the profile's `build_deploy_dsb.sh`
   instead, and the first build carries the issuer.
3. Collector image: `tools/build-node0.sh` (tests, benchmark, ocb build with the
   fork's `.tools/builder`, push; tag in `build/IMAGE_TAG`).
3b. Reseed before a measurement series: `make reseed` (`tools/reseed.sh`) restarts
   the variant's Mongo and Redis deployments (no volumes: the data lives in the
   container layer), restarts the services, and replays the profile's seed
   (`init_social_graph.py` from the Blueprint venv: 962 users, the socfb-Reed98
   follow graph, no posts).  Every 10-minute ComposePost run adds 60 000 posts,
   which slows usertimeline and drifts the healthy table.  `make reseed-check`
   runs 5 fault-free minutes at 12 % and prints the healthy outlier share.
4. A run: `tools/run_cluster.sh` with the arm variables (`ROOT_POLICY`,
   `RECON`, `ARM`, `HOG_*`, `BURST_*`, `DUR`).  It deploys the central, the agent
   ConfigMap and headless Service, patches port 8082 onto the `otelcol-pb-es-ctr`
   Service (the SDK's poll), switches the DaemonSet to our image, drives wrk2,
   exports every service from Jaeger and restores the DaemonSet (`kubectl
   rollout undo`) and removes the central.  Run long cycles under `nohup` or
   tmux; an ssh drop kills a foreground script.  `kubectl get ds
   otelcol-pb-es-ctr -o jsonpath='{.spec.template.spec.containers[0].image}'`
   must read `otelcol-pb-es-ctr:v...` afterwards.
5. Analysis: `tools/cluster_metrics.py`, `tools/recon_report.py`,
   `tools/stage2b_tables.py`, `tools/jaeger_orphans.py` on the run directory;
   raw exports stay on node-0.

What a fresh CloudLab instantiation would miss without step 2 and 3: the app
images' issuer (the agent then self-issues budgets, the fallback) and our
collector image.  What needs no profile change: the app images' env knobs have
defaults (`SLO_BUDGET_PORT` 8082, `SLO_BUDGET_POLL_MS` 1000, `SLO_BUDGET_BUCKETS`
512,1024), the SDK stamps nothing while no agent answers, and the Service port
is patched at run time.  The leftover `slobudget-agent-config` ConfigMap and
`slobudget-agents` Service between runs are inert.

## Baggage in DSB-SN: what propagates and what does not

Blueprint's generated OT wrappers (`plugins/opentelemetry/ir_ot_client.go`,
`ir_ot_server.go` on the fork's `retctx-reject-impl`) carry a JSON trace
carrier *inside every RPC* with a baggage map: upstream baggage, the SDK's
side-table entries (`backend.RegisterSpanBaggageSource`) and any span attribute
prefixed `__bag.` (`__bagdel.` deletes).  The server wrapper puts the inbound
map into the context (`backend.GetBaggageFromContext` in the SDK processor's
OnStart).  So an SDK span processor can issue `__bag.slo.budget`, `__bag.slo.root`,
`__bag.slo.class`, `__bag.slo.root_host` on the root span (no parent = root)
and copy the four keys to plain attributes on every downstream span; that is
`sdk/blueprint/slobudget_processor.go`, deployed in Stage 2b (`sdk/budgetissuer`
is the W3C form).  What does not work: W3C
`baggage` / `traceparent` headers from the wrk2 client are not read (the
wrappers use their own carrier, no propagator is installed), spans carry no
host, route or class attribute today (`_br` / `_d`, `span.kind`,
`otel.scope.name` only) until the app images carry the issuer (Stage 2b rebuilt
them).  Without the issuer the agent falls back to its own budget issue from the
central's B (per trace, per host), the configured `default_route`, class
`route|any`, and kept-notices broadcast to all peers.
