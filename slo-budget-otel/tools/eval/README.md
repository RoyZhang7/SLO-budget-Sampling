# tools/eval: Stage 3 run evaluation

One cluster run directory -> `summary.json` with the simulator's metrics
(`slo-budget-sim/slo_budget_sim/metrics.py`, `compute`) computed against the
run's own 100 % shadow; `tables.py` aggregates the summaries into the Stage 3
competitor, ablation and reconstruction-coverage tables.

```
PY=~/slo-budget-sim/.venv/bin/python3                      # has numpy; the tool imports slo_budget_sim from --sim
$PY tools/eval/eval_run.py /storage/royno7/stage3/runs/<run_id> [--sim ~/slo-budget-sim] \
    [--callgraph deploy/callgraph.json] [--recon ~/bin/recon | --no-recon] [--out FILE] [--print]
$PY tools/eval/tables.py /storage/royno7/stage3/runs --volume 0.12 [--scenarios steady saturated] [--seeds 1 2 3] [--json]
$PY -m pytest -q tools/eval/tests                         # fixture arithmetic
$PY tools/eval/make_fake_run.py <100 % Jaeger export> OUT [--frac 0.12] [--fault-service socialgraph]   # dev only
```

`~/bin/recon` is `go build -o ~/bin/recon ./tools/recon`; it runs on the export
when the run directory has no `recon-report.json` and writes
`export.recon.json` and `recon-report.json` next to it.

## Inputs (the campaign runner's run directory)

`meta.json` (arm, scenario, volume_frac, v_star, cap, seed, slo_ms, period_s,
margin, t0, t1, fault {start, end, culprit, targets}, burst, placement,
arm_params), `shadow.jsonl.zst` (or `.gz`, plain: the collector file exporter's
OTLP JSON lines, 100 %), `export.json` (the arm's Jaeger export), `gate.jsonl`
(the central cap gate's periods; else the `capgate period` lines of
`central.log`), `central-status.json` (chosen family), `telemetry/agent-<node>-{start,end}.prom`,
`telemetry/central-<unix>.prom` (tail), `agent-<node>.log`, `central.log`.

## How the cluster numbers map onto the simulator's

- **Population**: traces whose first span starts in [t0, t1].  Kept spans are
  joined to the shadow by span id; `export_unmatched` counts kept spans the
  shadow does not have (should be 0).
- **Oracle**: `ttable.Calibrator(q = 0.95, window = 1, use_class = True)` fed
  once with every shadow server span of the healthy window ([t0, fault start),
  [t0, burst start), or the whole run) with route `slo.root`, class `slo.class`
  (fallback `compose_post|any`), own = duration minus the union of the direct
  children (exact on complete traces), then one `end_period()`; T and the stage
  budget b per span from `lookup` / `lookup_b`.
- **Truth outlier** (server spans): duration > T_oracle or error status, as the
  simulator; **evidence** uses duration > (1 + margin) T_oracle.
- **Server-level tree**: the parent of a server span is its nearest server
  ancestor (through the client span), so children, holes, attributability and
  confirmability mean the same as in the simulator, whose spans are all
  server spans.  Outlier share, precision, recall and P95 are over server
  spans; spend counts every span.
- **Periods**: span-level k = floor((end - t0) / 5 s), warm = k >= 1.  Volume,
  bytes and cap periods come from the gate's wall-clock periods that start at
  least one period after t0 and end by t1.
- **Window**: [fault.start, fault.end] (span end inside) for fault scenarios,
  the burst window for bursty, the whole run otherwise.
- **Weights 1/p**: the `sampling.p` tag (chosen family, canopy); head: head_r;
  tail: 1 for a trace whose root missed its SLO or that has an error, else
  tail_p; reservoir: 1 (no p), as the simulator.
- **Admission time**: `slobudget.admit_ns` from the central's cap gate (the
  decision time of every arm, period end for period-end arms); missing stamps
  fall back to the span's end (`admit_stamped_frac` says how many).
- **Loss split**: unkept spans carry no drop reason on the cluster.  For the
  chosen family a lost truth outlier is `not_flagged` when its duration <= (1 +
  m) x the route-level T the central published for its period (history entry
  of the previous period) or no table existed, else `refused` (budget, pool,
  N_need or cap); other arms: `arm`.
- **Buffer**: tail = peak `*traces_on_memory*` of the central scrapes x spans
  per trace; reservoir = max over periods of the agents' summed
  `peak_buffered`; chosen family = max over periods of the summed
  `roots_held` (roots that waited in the hold during the period: an upper bound
  on the concurrently held roots); head, canopy = 0.
- **Decision latency**: tail = per kept trace, first admission minus the
  trace's first span start; others = per kept span, admission minus end.
- **Agent CPU per span**: sum over nodes of the delta of
  `otelcol_process_cpu_seconds` over the delta of `otelcol_receiver_accepted_spans`
  (receivers named `otlp*`) between the start and end scrapes.

## Summary fields (same names as the simulator where the meaning is the same)

Volume and spend: `kept_per_period`, `vol_ratio`, `min_vol_ratio`,
`max_vol_ratio`, `bytes_per_period`, `bytes_per_span`, `over_cap_periods`,
`cap_dropped_total`, `max_pre_ratio`, `max_per_request`, `p99_per_request`,
`max_per_host_period`, `max_host_ratio`, `root_share`, `roots_kept_frac`,
`kept_by_kind`.  Outliers: `truth_outliers`, `recall`, `recall_window`,
`recall_<bucket>`, `outlier_share`, `outlier_share_window`, `event_recall`,
`outlier_keeps`, `outlier_precision` (+ `_window`, `_server`), context
(`ctx_root_kept`, `outlier_no_root`, `ctx_sufficient`, `ctx_no_holes`),
refusals (`lost_outliers`, `ref_not_flagged`, `ref_refused`, `ref_arm`,
`refusal_split`), period 0 (`outlier_share_p0`, `outlier_precision_p0`,
`recall_p0`), attributability (`attrib_explained`, `attrib_own`,
`attrib_child`, `attrib_clean`, `attrib_loss`, `loss_*`, `loss_split`), fault
shape on complete data (`truth_rate_*`, `truth_systemic`, `truth_culprit_est`),
evidence and culprit (`evidence_s`, `evidence_any_s`, `evidence_flagged_s`,
`recall_culprit`, `culprit_truth`, `culprit_est`, `identified`,
`culprit_confirmable`, `spend_top`), P95 (`p95_err_svc_mean`,
`p95_err_svc_max`, `p95_svc_within5`, `p95_err_root`, `p95_err_svc`,
`p95_svc_n`, per-cell `p95_err_mean` / `median` / `max`, `p95_within5`,
`p95_cells`), holes (`holes_total`, `kept_with_holes_frac`,
`holes_per_kept_parent`), SLO violations (`viol_requests`, `viol_coverage`,
`viol_requests_frac`), request latency from the shadow roots
(`latency_p50_ms`, `latency_p99_ms`, `latency_mean_ms`, `over_slo_frac`,
`over_slo_frac_window`), chosen-family diagnostics from the central's history
(`budget_mean`, `budget_last`, `n_need_eff`, `frozen_svc_periods`,
`frozen_by_svc`, `frozen_entries`, `frozen_entries_by_svc`, `t_drift_*` against
the oracle's route-level T), cost (`buffer_peak_spans`, `buffer_mean_spans`,
`buffer_source`, `decision_latency_mean` / `p95` / `max`,
`agent_cpu_us_per_span`, `agent_cpu_by_node`), reconstruction (`recon_*` from
`recon-report.json`), and bookkeeping (`export_*`, `shadow_*`, `pop_*`,
`oracle_T_ms`, `oracle_healthy_server_spans`, `timing`, `peak_rss_mb`).

## Gap per call edge and own time (Stage 2d)

`gaps.py` measures, for every CLIENT -> SERVER call, the gap = CLIENT duration minus the
callee's SERVER duration (transit plus queueing in the callee before its server span starts
and after it ends), and per service the duration and the Stage 2d own time (duration minus
the union of the CLIENT children only).  The gap is a difference of two durations, each
measured on one host, so clock skew does not enter it; the start offset (server start minus
client start) is skew-affected on cross-host edges and is reported from raw clocks only for
shadows (Jaeger's skew adjuster shifts start times in an export).

    python3 tools/eval/gaps.py --input healthy=<run>/shadow.jsonl.zst \
        --input base=calib-base.json@T0:T1 --placement <run>/meta.json --out gaps.json --md gaps.md

An input is a shadow or an uncompressed 100 % Jaeger export (for example the stock
collector's data during a calibration phase, exported with jaeger_export.py), optionally
restricted to calls whose CLIENT span starts in [T0, T1).  The Markdown ends with a table of
the p95 changes against the first input.

## Stage 2d trigger comparison

`compare2d.py <runs dir> [--gaps] [--md out.md]` tabulates the 2d series (runs
2d-v12-saturated-own-s<seed>, 2d-v12-saturated-dur-s<seed>, 2d-v12-steady-own-s<seed>): culprit
recall and faulty-span coverage, outlier keeps in the fault window split into the culprit and
its callers (the culprit's server ancestors in the shadow), whether the caller keeps are true
under the own-time and the duration truth, precision under three truths (single-culprit,
own-time from the shadow, duration), orphans per kept outlier, spend, P95 error, and the
server_hold cost from the agents' period logs.  `--gaps` adds the gap per edge before vs during
the fault window from each run's shadow.

The 2d keys of summary.json: `outlier_precision_own[_window]`, `outlier_share_own`,
`recall_own`, `truth_outliers_own` (own-time truth: own > oracle share b, or an error);
`outlier_precision_culprit_window`, `faulty_span_coverage`, `outlier_keeps_{window,culprit_window,callers_window,callers}`,
`outlier_keeps_callers_window_frac`, `culprit_outlier_keeps_per_period`,
`caller_outlier_keeps_per_period`, `caller_keeps_truth_{own,dur}`, `culprit_callers`,
`outlier_keeps_by_service_window`; `orphans_per_kept_outlier[_own]`,
`outlier_orphan_share[_own]`; `hold_{server_held,late_clients,late_client_frac,ms_mean,peak_spans,peak_bytes}`.

## Stage 3 fault scoring

Every faulted run's postprocess also writes `gaps.json` / `gaps.md` (`gaps_run.py <run dir>`:
gaps.py on the run's shadow, `before` = the 3 minutes before the fault, `during` = the fault
window).  noisy_neighbour (the unconfined spinner on socialgraph, which slows every service on
its node) is scored against the node-level truth as well: `outlier_precision_node_window` counts
an outlier keep in the fault window as right when its service runs on the culprit's node
(`culprit_node`, `culprit_node_services`, `outlier_keeps_node_window`; placement from
meta.json), next to the own-time truth (`outlier_precision_own_window`, and `recall_own_window`
and `recall_node_own_window`, the kept share of the window's own-time truth outliers, on every
service and on the culprit's node).

## Root-cause ranking (Stage 3)

`rca_run.py <run dir> [--variants shadow recon true true_ref] [--skip-existing]` runs RCAEval's
TraceRCA and MicroRank (`rcaeval/`, vendored, see its README) through `sloeval/rca.py` on the
scored scenarios (saturated, cascade, cold_start: the culprit; noisy_neighbour: a service only on
the culprit's node) and writes `rca.json`: per variant the services each method ranks, the rank
of the first correct one (`rank`, RCAEval's operation-to-service mapping; `rank_callee`, a client
operation counted for its callee), hit@1, hit@3, the reciprocal rank, failures, and the span and
trace counts of the normal and fault sets.  Variants: `shadow` (100 %, the full data), `recon` and
`true` (the arm's kept spans, parents = the nearest kept ancestor in the reconstruction or in the
shadow), `true_ref` (`true` with the shadow's pre-fault spans as the normal set).
`rca_tables.py <runs dir> [--volume V] [--md out.md]` tabulates Acc@1 / Acc@3 / MRR.  It needs
pandas, tqdm and python-dateutil (node-0: `~/rca-venv`).
