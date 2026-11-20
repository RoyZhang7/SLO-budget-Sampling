# slo-budget-sim

Stage 1 of the SLO-budget span sampling project: an offline trace-replay
simulator that evaluates the sampling mechanism and its baselines on complete
traces (Jaeger JSON exports or synthetic DSB-SN-like workloads).  Pure Python,
no cluster access.  Stage 2 (an OTel Collector processor in Go) is meant to
mirror the sampler interface in `sampler.py` one to one.

```sh
python -m venv .venv
.venv/Scripts/pip install -e ".[dev]"        # Windows; use .venv/bin/pip elsewhere
.venv/Scripts/python -m pytest
.venv/Scripts/python -m slo_budget_sim.cli all --jobs 8          # experiments 1-13 -> results/ (resumable: rows already there are kept)
.venv/Scripts/python -m slo_budget_sim.cli exp8 --jobs 8 --fresh # chosen-v2 vs baselines at 25 / 12 / 5 %, rerun from scratch
.venv/Scripts/python -m slo_budget_sim.cli exp9 --jobs 8 --set n_need_frac=0.3   # overrides for every budget arm
.venv/Scripts/python -m slo_budget_sim.cli replay --scenario saturated --arm chosen-v2 reservoir
.venv/Scripts/python -m slo_budget_sim.cli replay --scenario saturated --arm chosen-local --set outlier_credit=reserve credit_frac=0.2
.venv/Scripts/python -m slo_budget_sim.cli replay --jaeger traces.json --reference healthy.json --arm chosen-v2
.venv/Scripts/python -m slo_budget_sim.cli generate --scenario cascade --out corpora/cascade.json
.venv/Scripts/python -m slo_budget_sim.derive results/remote/export-10min.json -o slo_budget_sim/workload_dsbsn.json
python3 tools/jaeger_export.py --base http://<jaeger-query>:16686 --service unknown_service:wrk2api_service_pb_es_proc --start <epoch s> --end <epoch s> --out export.json
bash results/remote/setup_remote.sh      # on node-0 after `tar xzf sim.tgz`: venv, tests, then run_pass4.sh detached in tmux (load, export, derive, cli all)
```

## The mechanism, as implemented

| Design element | Where | Notes |
| --- | --- | --- |
| Unit of sampling = span; request carries budget, root, class, a response-path flag and the credit spans used in baggage | `sampler.RequestState`, `BudgetSampler.on_root_start` | Budget is an integer per request. The refill produces a real number B; the issuer rounds stochastically (floor(B) + Bernoulli(frac)). |
| Per-host per-period span pool | `BudgetSampler._pool`, `_set_pools` | pool_h = V* x host share of spans (last period, smoothed). Under the reserve policy `credit_frac` x pool_h is set aside. |
| T_i from the Erms square-root split of the root SLO | `ttable.sqrt_split` | Stage budget b_i = SLO sqrt(O_i)/W over the route's critical structure, inclusive T_i = b_i + callees' T (sum, or max under a parallel fan-out), scaled so T_root = SLO. O_i is the calibration quantile of the service's own time (duration minus outbound calls, locally computable from server + client spans). `Calibrator.b` keeps the stage budgets. |
| Keyed by (service, root, class), fallback to (service, root) below `min_class_samples` | `ttable.Calibrator` | class = route + payload-size bucket. |
| Calibration source (`calib_source`) | `BudgetSampler._observe`, `_freeze_set`, `Calibrator.end_period` | `kept`: every kept span, 1/p-weighted (P95 per period over a window of 2 periods); `plan`: plan-sampled ordinary spans and roots kept at p = 1 only; `frozen`: as kept, but a service is held at its previous own-time and inclusive quantiles (L, O) while it is frozen and its samples from frozen periods never enter the window; `oracle`: the ground-truth table. Freeze trigger (`freeze_trigger`): `rate` (default) = the period's outlier rate >= `freeze_rate_mult` (2) x the service's healthy rate (an EWMA updated while unfrozen, at least `freeze_rate_floor` = 1%, needs `n_min` outliers); `count` = the pass-2 rule, more than N_need outliers in the period. Cold start (`cold_prior`): `none` (default) = plan sampling only until the service's first calibrated period; `equal` = the equal-share square-root split of the SLO (`ttable.equal_share_prior`: every stage of the root's critical structure gets SLO / stages, T adds up along the tree) stands in for T_i until then (exp12: on this tree it flags half of the biggest leaf's healthy spans and misses the cold-start fault, so it stays off). |
| Outlier = duration > T_i or status not OK | `BudgetSampler._decide_span` | |
| Keep at p = 1 until N_need per (service, period), then p = N_need/N x g(miss) | `BudgetSampler._decide_span`, `_growth` | g: const, linear (1+miss), sqrt, quadratic; miss = (duration - T)/T. Outliers are counted whether or not the coin keeps them. `n_need_mode=fixed`: N_need = `n_need`; `relative`: N_need = max(`n_min`, floor(`n_need_frac` x V* / services)). |
| p stamped on every kept span; request budget and host pool decremented; drop when either is 0 | `BudgetSampler._spend` | `budget_scope=local` counts the budget per (request, host) (the plan's local reading). |
| Outlier credit (`outlier_credit`) | `BudgetSampler._try_credit`, `_repayment` | An outlier refused by the request budget or the host pool: `none` drops it; `credit` keeps it while the host's credit for the period (`credit_frac` x pool) lasts and repays the debt from the next period's plan sampling on that host (plan rate scaled down, p recorded as usual; credit does not roll over; outlier keeps are never reduced); `reserve` keeps it from the `credit_frac` x pool set aside for refused outliers. Eligibility in order: the service is under N_need for the period; below `credit_low_water` of the allowance a coin with p = miss ratio clipped to [0.05, 1] (errors p = 1) decides and p is recorded; at most `credit_k` credit/reserve spans per request. |
| Root policy (`root_policy`) | `BudgetSampler._decide_root` | `pin`: the root is always kept at p = 1, outside budget and pool. `on_demand`: a non-root keep raises the flag in baggage (outlier/escalate/parent/credit keeps; `root_flag_on=any` adds plan keeps); the root is kept at p = 1 only if the flag arrived, otherwise it is an ordinary span (outlier test, then the plan coin, paying from the budget). `record`: a compact record (trace id, root service, class, start, duration, status) is always emitted at `record_cost` = 0.1 of a span; the full root follows `on_demand`; roots are stamped p = 1. `plan`: no pinning. |
| Holes: kept parent records dropped children | `replay.replay` | Derived at the end from the kept set (identical to a runtime counter for synchronous calls). The replay also records why every dropped span was dropped (`RunResult.drop_reason`). |
| Ordinary spans: plan sampling, uniform or sqrt weights | `BudgetSampler._replan` | Ordinary spend per request = `plan_fill` x (B - expected outlier spend per request); uniform: one rate over the request's spans; sqrt: allocation proportional to sqrt(span count per service). `plan_fill` = 0.5 keeps the integer budget from truncating planned keeps, which would bias 1/p (see exp2). The threshold arm, which has no budget, targets V* - pinned roots - outliers directly. |
| Refill: B_next = (1-a) B + a V*/(R_exp u), u = kept per request / B | `BudgetSampler.on_period_end` | Collector volume (post cap, in span units: a root record counts 0.1) feeds u. Clamped to [b_min, b_max]. The only volume control: the plan rates follow B. Collector cap in `replay`. |
| Escalation: none / deadline / duration_sum | `BudgetSampler.on_start` | deadline = root start + SLO on the root host's clock, compared with the local clock (hosts have skew); duration_sum carries elapsed time measured on each caller's own clock (no skew, but transit is not counted). Escalated spans are kept at p = 1 and pay from the request budget and the host pool; with `escalate_bypass_budget` they skip the request budget. |

The decider is `BudgetSampler._decide_span` (plus `_decide_root` for the
root and `_try_credit` for refused outliers); it only reads the span, the
baggage state and per-period counters, as the plan's `Decide(...)` does.
`BEST_ROOT_POLICY`, `BEST_CALIB_SOURCE`, `BEST_N_NEED_FRAC`, `BEST_CREDIT` and
`BEST_COLD_PRIOR` in `sampler.py` record what exp6, exp7, exp10, exp9 and exp12
selected; `chosen-v2` uses them.  Since pass 4 credit / reserve is off
(`outlier_credit=none`; the switch stays in the code) and so is the cold-start
prior.

## Arms

| Arm | Definition |
| --- | --- |
| `chosen` | the Stage 1 mechanism: shared request budget, pinned roots, kept-span calibration, fixed N_need = 15, linear p growth, uniform plan, no escalation, no credit |
| `chosen-local` | `chosen` with the budget counted per (request, host) |
| `chosen-oracleT` | ablation: ground-truth T table instead of per-period calibration |
| `chosen-v2` | the current configuration: local budget, uniform plan, escalation off, class key on, on-demand roots, frozen calibration (rate trigger), relative N_need f = 0.2, credit off, no cold-start prior (pass 4) |
| `threshold` | every outlier kept at p = 1, no budget, no pool, no N_need; ordinary spans plan-sampled; same root policy and calibration source as the chosen arm it is compared with; the collector cap is the only limit |
| `keep-parent` | `chosen` plus the parent of every kept outlier is kept (p = 1, pays from the budget) |
| `reservoir` | weighted reservoir per host (Efraimidis-Spirakis, weight = size of miss), K_h = V* x host share, finalised at period end; uses the ground-truth T table |
| `uniform-head` | one coin per trace at the root; rate adapted each period to V* |
| `canopy` | per-service rate targets (equal shares of V*) adapted multiplicatively at the collector, span level, blind to outliers |

All arms get the same V* (a fraction of the scenario's spans per period) and
the same collector cap (1.25 V*); the cap is first come, first served per
period, counted in span units.

## Workload and scenarios (`synthetic.py`, `workload_dsbsn.json`, `derive.py`)

Structure and timings from real traces.  `derive.py` reads a Jaeger export
(client spans folded) and writes the workload JSON the generator loads: per
route the modal call tree (which service calls which, in what order, how many
times), which services fan out in parallel, and per service a lognormal fit of
own time (median, sigma) and the median one-way transit.  The current JSON is
fitted from the pass-4 long run on the bridged deployment: 10 minutes of wrk2
ComposePost load at 100 requests/s (the mixed read/write workload is not
configured on the cluster), 60000 traces, 60000 spans per service
(`results/remote/export-10min.json`, 1 GB, not in git; produced by
`results/remote/run_pass4.sh` with `tools/jaeger_export.py`).  12 server spans
per request, `wrk2api -> composepost`, `composepost` fans out in parallel to
hometimeline (-> socialgraph), usertimeline, poststorage, text (-> urlshorten
and usermention in parallel), uniqueid, media and user; own-time medians
usertimeline 5.7 ms, composepost 2.9 ms, hometimeline 2.8 ms, text 1.6 ms,
wrk2api 1.1 ms, poststorage 0.9 ms, urlshorten 0.9 ms, usermention 0.6 ms,
socialgraph 0.5 ms, uniqueid / media / user under 0.05 ms; sigma 0.16-0.47;
transit 0.48-0.78 ms; root p50 / p95 13.0 / 15.6 ms.  Against the provisional
200-trace fit (git history `874e829`) the tree is the same, composepost's own
median fell from 4.6 to 2.9 ms, usertimeline's rose from 5.1 to 5.7 ms and the
rest moved by less than 10%.  The route mix follows the export (ComposePost
only); the two read routes are hand-written from the DSB-SN source
(`PROVISIONAL_ROUTES`) and used only when a scenario sets `route_mix`.  Still
not in the data: errors (none observed, 0.001 assumed), a load or time-of-day
dependence of the timings (one steady 100 requests/s level), the read routes.

250 requests/s for 90 s; period 5 s; 1 ms host clock skew; the paper's
placement of services on nodes; SLO per route = 1.5 x healthy P95.  Faults
start at 30 s unless noted.

| Scenario | Fault |
| --- | --- |
| `steady` | none |
| `saturated` | `socialgraph` own time x5 |
| `cascade` | `poststorage` x6, then `hometimeline`/`usertimeline` x1.3 after 4 s, `composepost` x1.2 after 8 s |
| `bursty` | arrival rate x3 for 5 s at 20, 40, 60 s; latencies unchanged |
| `cold_start` | `socialgraph` x5 from t = 0 (no healthy history) |
| `late_everywhere` | every service's own time inflated so that its truth outlier rate (over the healthy reference's T_i) is 25% during the fault (per-service factors solved on a Monte-Carlo replica of the generator, `late_everywhere_rate_factors`): 25% of requests miss the SLO, every stage has the same outlier rate, the oracle reads it as systemic, no culprit |
| `late_everywhere_v1` | the pass-3 tuning, kept as a named variant outside the default set: every stage's own-time P95 at 1.6 x its stage budget; on the fitted timings the outlier rates run from 25% (uniqueid) to 98% (root), so complete data names a culprit |
| `mixed_size` | 30% of ComposePost requests are `large` (text, media x3, poststorage x2.5) plus `urlshorten` x4 at 30 s |

Ground truth: a healthy reference run of the same workload defines the SLOs
and the oracle T table (the calibration procedure run on complete data).

## Metrics (`metrics.py`)

Headline (all over periods >= 1, after the cold-start period; equal volume target and cap):

- `outlier_share`: kept non-root spans that are truth outliers (over the oracle T_i or not OK) / kept non-root spans; `_window` restricts to the fault window
- `outlier_precision`: spans the arm kept *as* outliers that are truth outliers
- bounded spend: `max_per_request` (span units kept for one request, p99 alongside), `max_per_host_period`, `over_cap_periods` (periods in which the arm wanted more than the cap)
- attributability of kept truth outliers: `attrib_explained` = own time over its own-time share (`attrib_own`), or a kept child is itself an outlier (`attrib_child`), or every dropped child was ordinary in ground truth (`attrib_clean`); `attrib_loss` = the excess sat in a dropped child that was an outlier, split by that child's refusal reason (`loss_*`, `loss_split`); the pass-2 `ctx_sufficient` (root kept and zero holes) and `ctx_root_kept` / `outlier_no_root` stay in the CSV
- refusal accounting: for truth outliers not kept, the share per reason (`ref_budget`, `ref_pool`, `ref_n_need` = the p < 1 coin, `ref_credit` = credit/reserve exhausted, `ref_k_cap`, `ref_credit_coin`, `ref_not_flagged` = the arm's T did not flag it, `ref_cap`, `ref_arm` = a baseline's own rule; `refusal_split`, `lost_outliers`)
- `p95_err_svc_mean` / `_max` / `p95_svc_within5` / `p95_err_root`: 1/p-reweighted P95 error per service, pooled over routes and periods >= 1
- secondary: `recall_culprit`, `evidence_s` (first kept truth outlier of the culprit after the fault, at decision time), `culprit_est` / `identified` (reweighted kept outliers per service over the service's span count; deepest service within 50% of the maximum unless 60% of services qualify, "systemic")
- `root_share` / `record_share`, `credit_share`, `debt_total`, `n_need_eff`
- `t_drift_pre` / `t_drift_culprit` / `t_drift_healthy*`: T_used / T_oracle - 1 of the table in force each period; `frozen_svc_periods`, `frozen_by_svc`
- cold-start period (period 0, which the headline metrics leave out): `outlier_share_p0`, `outlier_precision_p0`, `recall_p0`, `lost_outliers_p0`, `ref_not_flagged_p0` (lost truth outliers the arm's T did not flag), `prior_periods` (periods in which the cold-start prior answered a lookup)
- fault shape on complete data (fault window): `truth_rate_median` / `truth_rate_max` / `truth_rate_spread` (max over median) of the truth outlier rate per service, `truth_systemic` / `truth_culprit_est` (the culprit rule applied to complete data)
- reference, CSV only: span recall (`recall`, `recall_window`), `event_recall`, P95 error per (service, route, period) cell and pooled per (service, route), holes, SLO-violating request coverage, drops and keeps by reason

## Jaeger input (`jaeger.py`)

Standard query-API shape (`{"data": [{"spans": [...], "processes": {...}}]}`),
UTF-8 with or without a BOM, 64- or 128-bit ids.  Service names like
`unknown_service:post_storage_service_pb_es_proc` normalise to `poststorage`;
host from process tags (`host.name`, `hostname`, ...) or `host:<service>` when
there is none; status from `otel.status_code` / `error` / `http.status_code`;
route = root operation; payload bucket from a size tag if present; extra tags
such as `_br` and `_d` are ignored.  CLIENT spans (`span.kind=client`) are
folded into the call they represent by default: their children are re-parented
to the caller's SERVER span and half of (client - server duration) becomes the
callee's transit time, so the corpus has one span per call like the generator
(`--keep-client-spans` samples them as spans of the caller instead).  The call
graph (and parallel fan-outs) is learned from the traces; the SLO is 1.5 x P95
per route unless given.

## Results

`results/expN.csv` has one row per run, `results/expN.md` the aggregated
tables, `results/REPORT-*.md` the write-ups; none of them is tracked in git
(gitignored with plots): the repo holds code, README and config only.  `results/REPORT.md` is the Stage 1 write-up (exp1-exp5, Stage 1
metric names), `results/REPORT-followup.md` the pass-2 write-up (root policy
exp6, calibration source exp7, the first exp8, the Jaeger pull; its exp8 numbers
refer to the pass-2 configuration and the old synthetic tree, see git history
`2f33e5c`), `results/REPORT-pass3.md` the pass-3 write-up (credit / reserve exp9,
relative N_need exp10, frozen trigger exp11, exp8 rerun, generator
re-derivation; its numbers are on the provisional timings) and
`results/REPORT-pass4.md` the pass-4 write-up (cold-start prior exp12,
late-everywhere retune exp13, exp8 at three volumes on the fitted timings, the
fitted vs provisional timings).  Every CSV is on the fitted workload; `cli all`
regenerates everything (about 20 minutes with 20 workers) and is resumable:
rows already in a CSV for the same scenario, seed, arm, label, volume,
overrides, resolved arm configuration and workload / scenario tag are kept,
`--fresh` reruns them.  `results/remote/` holds the node-0 scripts of the long
run (the raw export and the logs are not in git).
