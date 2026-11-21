"""Experiments.  Every experiment is a list of jobs (scenario, seed, arm,
overrides); jobs run in a process pool, corpora are cached per worker, and the
results go to results/<exp>.csv plus a short results/<exp>.md.

All arms see the same volume target V* = volume_frac * mean spans per period of
the scenario and the same collector cap = cap_factor * V*.

  exp1        Stage 1: chosen vs every baseline at 25% volume
  exp1_tight  the same at 12%
  exp2-exp5   Stage 1 sweeps (N_need, p rule, alpha, percentile, plan fill; plan
              weights; escalation; class key)
  exp6        pass 2: root policy at 25, 12, 5%
  exp7        pass 2: T_i calibration source at 25, 12%
  exp8        chosen-v2 vs the competitors (uniform head, reservoir top-K, Canopy-style rate
              target, tail-based whole-trace sampler) at 25, 12, 5%, with the ablations
              (threshold, shared budget, oracle T, keep-parent) in their own table
  exp9        pass 3: outlier credit / reserve, c x k sweep at 12, 5%
  exp10       pass 3: N_need fixed 15 vs relative f at 25, 12, 5%
  exp11       pass 3: frozen trigger count vs rate (and kept) on steady/fault scenarios
  exp12       pass 4: cold-start prior (equal-share split) vs none on cold_start/steady/saturated
  exp13       pass 4: late_everywhere re-tuned (equal rates) vs the pass-3 tuning (late_everywhere_v1)

Every experiment is resumable: rows already in results/<exp>.csv for the same
(scenario, seed, arm, label, volume, overrides, workload / scenario tag) are
kept and not rerun; rows that no longer match any job are dropped; the CSV is
rewritten after every chunk of runs.  `--fresh` reruns everything.
"""
from __future__ import annotations

import csv
import hashlib
import math
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path

import numpy as np

from .baselines import CanopySampler, ReservoirSampler, TailSampler, UniformHeadSampler
from .metrics import REASON_GROUPS, CorpusArrays, compute
from .replay import replay
from .sampler import BEST_CALIB_SOURCE, BEST_COLD_PRIOR, BEST_CREDIT, BEST_N_NEED_FRAC, BEST_ROOT_POLICY, make_budget_arm
from .synthetic import ALL_SCENARIOS, SCENARIOS, build_scenario, scenario_tag
from .ttable import oracle_table

BUDGET_ARMS = ("chosen", "chosen-local", "chosen-oracleT", "chosen-v2", "threshold", "keep-parent",
               "v2-shared", "v2-oracleT", "v2-keep-parent")
OTHER_ARMS = ("reservoir", "uniform-head", "canopy", "tail")
ALL_ARMS = BUDGET_ARMS + OTHER_ARMS
# the mechanism, its competitors (other samplers) and its ablations (the same mechanism with one part removed)
MECHANISM_ARMS = ("chosen", "chosen-v2")
COMPETITOR_ARMS = ("uniform-head", "reservoir", "canopy", "tail")
ABLATION_ARMS = ("threshold", "chosen-local", "v2-shared", "chosen-oracleT", "v2-oracleT", "keep-parent", "v2-keep-parent")
STAGE1_ARMS = ("chosen", "chosen-local", "chosen-oracleT", "threshold", "keep-parent", "reservoir", "uniform-head", "canopy")
# pass 2 base: local budget, uniform plan, escalation off, class key on
V2_BASE = {"budget_scope": "local", "plan": "uniform", "escalation": "none", "use_class": True}
# pass 3 base: + on-demand roots, frozen calibration (rate trigger), relative N_need
V3_BASE = {**V2_BASE, "root_policy": BEST_ROOT_POLICY, "calib_source": BEST_CALIB_SOURCE,
           "n_need_mode": "relative", "n_need_frac": BEST_N_NEED_FRAC}
# pass 4 base: + credit off, cold-start prior as exp12 selected
V4_BASE = {**V3_BASE, **BEST_CREDIT, "cold_prior": BEST_COLD_PRIOR}
PRIOR_VARIANTS = [("no prior", {"cold_prior": "none"}), ("equal-share prior", {"cold_prior": "equal"})]
LATE_ARMS = [("chosen-v2", "chosen-v2", {}), ("chosen-v2", "chosen-v2 oracle T", {"calib_source": "oracle"}),
             ("uniform-head", "uniform-head", {})]
ROOT_VARIANTS = [("pin", {"root_policy": "pin"}),
                 ("on_demand", {"root_policy": "on_demand"}),
                 ("on_demand(any)", {"root_policy": "on_demand", "root_flag_on": "any"}),
                 ("record", {"root_policy": "record"}),
                 ("plan", {"root_policy": "plan"})]
CALIB_VARIANTS = [(src, {"calib_source": src}) for src in ("kept", "plan", "frozen", "oracle")]
CREDIT_VARIANTS = [("none", {"outlier_credit": "none"})] + [
    (f"{pol} c={c} k={k}", {"outlier_credit": pol, "credit_frac": c, "credit_k": k})
    for pol in ("credit", "reserve") for c in (0.1, 0.2, 0.3) for k in (1, 2, 3)]
NNEED_VARIANTS = [("fixed 15", {"n_need_mode": "fixed", "n_need": 15})] + [
    (f"relative f={f}", {"n_need_mode": "relative", "n_need_frac": f}) for f in (0.1, 0.2, 0.3)]
FREEZE_VARIANTS = [("count", {"calib_source": "frozen", "freeze_trigger": "count"}),
                   ("rate", {"calib_source": "frozen", "freeze_trigger": "rate"}),
                   ("kept", {"calib_source": "kept"})]
VOLUMES = (0.25, 0.12, 0.05)


@dataclass
class Job:
    exp: str
    scenario: str
    seed: int
    arm: str
    label: str
    overrides: dict = field(default_factory=dict)
    volume_frac: float = 0.25
    cap_factor: float = 1.25
    scenario_overrides: dict = field(default_factory=dict)


_CACHE: dict = {}


def _corpus(scenario: str, seed: int, scenario_overrides: dict):
    key = (scenario, seed, tuple(sorted(scenario_overrides.items())))
    if key not in _CACHE:
        corpus, reference = build_scenario(scenario, seed, **scenario_overrides)
        oracle = oracle_table(reference, q=0.95, use_class=True)
        arrays = CorpusArrays(corpus, oracle)
        _CACHE.clear()                       # one corpus per worker at a time
        _CACHE[key] = (corpus, reference, oracle, arrays)
    return _CACHE[key]


def make_arm(arm: str, overrides: dict):
    if arm in BUDGET_ARMS:
        return make_budget_arm(arm, **overrides)
    if arm == "reservoir":
        return ReservoirSampler()
    if arm == "uniform-head":
        return UniformHeadSampler(alpha=overrides.get("alpha", 0.5))
    if arm == "canopy":
        return CanopySampler(alpha=overrides.get("alpha", 0.5))
    if arm == "tail":
        return TailSampler(initial_rate=overrides.get("initial_rate", 0.15), alpha=overrides.get("alpha", 0.5),
                           timeout_slo_mult=overrides.get("timeout_slo_mult", 2.0), timeout_s=overrides.get("timeout_s"))
    raise KeyError(arm)


def _parse_kv(text: str) -> dict:
    """"k=v k=v" (as written to the CSV) back to typed overrides."""
    out: dict = {}
    for it in (text or "").split():
        k, v = it.split("=", 1)
        if v in ("True", "False"):
            out[k] = v == "True"
        else:
            try:
                out[k] = int(v)
            except ValueError:
                try:
                    out[k] = float(v)
                except ValueError:
                    out[k] = v
    return out


def _config_hash_impl(arm: str, overrides: dict) -> str:
    """Hash of the resolved BudgetConfig of a budget arm (every field), so that
    a changed preset or default invalidates a result row even when the job's
    own overrides did not change; "" for the other arms."""
    if arm not in BUDGET_ARMS:
        return ""
    cfg = make_budget_arm(arm, **overrides).cfg
    text = " ".join(f"{k}={v}" for k, v in sorted(asdict(cfg).items()))
    return hashlib.sha1(text.encode()).hexdigest()[:10]


_config_hash = _config_hash_impl


def run_job(job: Job) -> dict:
    corpus, reference, oracle, arrays = _corpus(job.scenario, job.seed, job.scenario_overrides)
    v_star = job.volume_frac * len(corpus.spans) * corpus.period / max(corpus.period, corpus.t_end - corpus.t0)
    cap = job.cap_factor * v_star
    sampler = make_arm(job.arm, job.overrides)
    res = replay(corpus, sampler, v_star, cap, seed=1000 + job.seed, oracle=oracle)
    m = compute(corpus, arrays, res)
    m.update({"exp": job.exp, "scenario": job.scenario, "seed": job.seed, "label": job.label, "arm": job.arm,
              "volume_frac": job.volume_frac, "tag": scenario_tag(job.scenario),
              "config": _config_hash(job.arm, job.overrides),
              "overrides": " ".join(f"{k}={v}" for k, v in job.overrides.items()),
              "fault_start": corpus.fault.start if corpus.fault else math.nan})
    return m


def run_jobs(jobs: list[Job], jobs_parallel: int = 1, flush=None, chunk: int | None = None) -> list[dict]:
    """Run the jobs grouped by corpus (each worker reuses its cached corpus) in
    chunks; `flush(rows_so_far)` is called after every chunk but the last, so
    an interrupted experiment resumes from the rows already written."""
    jobs = sorted(jobs, key=lambda j: (j.scenario, j.seed, j.volume_frac, j.label))
    chunk = chunk or max(32, 8 * max(1, jobs_parallel))
    rows: list[dict] = []
    if jobs_parallel <= 1:
        for k, j in enumerate(jobs, 1):
            rows.append(run_job(j))
            if flush and k % chunk == 0 and k < len(jobs):
                flush(rows)
        return rows
    with ProcessPoolExecutor(max_workers=jobs_parallel) as ex:
        for start in range(0, len(jobs), chunk):
            part = jobs[start:start + chunk]
            rows.extend(ex.map(run_job, part, chunksize=max(1, len(part) // (jobs_parallel * 4) or 1)))
            if flush and start + chunk < len(jobs):
                flush(rows)
    return rows


# ---------------------------------------------------------------------------
# experiment definitions
# ---------------------------------------------------------------------------
def _ov(arm: str, extra: dict | None) -> dict:
    return dict(extra or {}) if arm in BUDGET_ARMS else {}


def exp1_jobs(seeds: list[int], extra: dict | None = None) -> list[Job]:
    return [Job("exp1", sc, sd, arm, arm, _ov(arm, extra)) for sc in SCENARIOS for sd in seeds for arm in STAGE1_ARMS]


def exp1_tight_jobs(seeds: list[int], extra: dict | None = None) -> list[Job]:
    """exp1 at a tight volume target: 12% of spans."""
    return [Job("exp1_tight", sc, sd, arm, arm, _ov(arm, extra), volume_frac=0.12)
            for sc in SCENARIOS for sd in seeds for arm in STAGE1_ARMS]


def exp2_jobs(seeds: list[int], extra: dict | None = None) -> list[Job]:
    jobs = []
    sweeps = {
        "n_need": [5, 10, 15, 30],
        "p_rule": ["const", "linear", "sqrt", "quadratic"],
        "alpha": [0.2, 0.5, 0.8, 1.0],
        "calib_q": [0.90, 0.95, 0.99],
        "plan_fill": [0.3, 0.5, 0.7, 1.0],
    }
    for sc in ("saturated", "bursty", "cascade"):
        for sd in seeds:
            for param, values in sweeps.items():
                for v in values:
                    jobs.append(Job("exp2", sc, sd, "chosen", f"{param}={v}", {**(extra or {}), param: v}))
    return jobs


def exp3_jobs(seeds: list[int], extra: dict | None = None) -> list[Job]:
    return [Job("exp3", sc, sd, "chosen", f"plan={plan}", {**(extra or {}), "plan": plan})
            for sc in SCENARIOS for sd in seeds for plan in ("uniform", "sqrt")]


def exp4_jobs(seeds: list[int], extra: dict | None = None) -> list[Job]:
    variants = [("none", {"escalation": "none"}),
                ("deadline", {"escalation": "deadline"}),
                ("duration_sum", {"escalation": "duration_sum"}),
                ("deadline+bypass", {"escalation": "deadline", "escalate_bypass_budget": True}),
                ("duration_sum+bypass", {"escalation": "duration_sum", "escalate_bypass_budget": True})]
    return [Job("exp4", sc, sd, "chosen", f"escalation={label}", {**(extra or {}), **ov})
            for sc in ("late_everywhere", "saturated", "steady") for sd in seeds
            for label, ov in variants]


def exp5_jobs(seeds: list[int], extra: dict | None = None) -> list[Job]:
    """Class key on vs off, plus the tail-based baseline as the context reference
    (its attributability and true-loss accounting next to the mechanism's)."""
    jobs = [Job("exp5", sc, sd, "chosen", f"use_class={uc}", {**(extra or {}), "use_class": uc})
            for sc in ("mixed_size", "steady") for sd in seeds for uc in (True, False)]
    jobs += [Job("exp5", sc, sd, "tail", "tail", {}) for sc in ("mixed_size", "steady") for sd in seeds]
    return jobs


def exp6_jobs(seeds: list[int], extra: dict | None = None) -> list[Job]:
    """Root policy, on the pass-2 base configuration, at three volume targets."""
    return [Job("exp6", sc, sd, "chosen", label, {**V2_BASE, **(extra or {}), **ov}, volume_frac=vf)
            for vf in VOLUMES for sc in SCENARIOS for sd in seeds for label, ov in ROOT_VARIANTS]


def exp7_jobs(seeds: list[int], extra: dict | None = None) -> list[Job]:
    """T_i calibration source, on the pass-2 base configuration with the root
    policy exp6 selected (override with --set root_policy=...)."""
    base = {**V2_BASE, "root_policy": BEST_ROOT_POLICY, **(extra or {})}
    return [Job("exp7", sc, sd, "chosen", label, {**base, **ov}, volume_frac=vf)
            for vf in (0.25, 0.12) for sc in SCENARIOS for sd in seeds for label, ov in CALIB_VARIANTS]


def exp8_jobs(seeds: list[int], extra: dict | None = None) -> list[Job]:
    """chosen-v2 vs the competitors (uniform head, reservoir top-K, Canopy-style
    rate target, tail-based whole-trace sampler) and vs its ablations (threshold =
    no budget / pool / N_need, shared budget, oracle T, keep-parent) at 25, 12
    and 5%.  threshold gets the same root policy, calibration source, N_need mode
    and cold-start prior as chosen-v2 so that the budget is the only difference."""
    v2 = {**V4_BASE, **(extra or {})}
    arms = [("chosen-v2", v2), ("uniform-head", {}), ("reservoir", {}), ("canopy", {}), ("tail", {}),
            ("threshold", v2), ("v2-shared", dict(extra or {})), ("v2-oracleT", dict(extra or {})),
            ("v2-keep-parent", dict(extra or {}))]
    return [Job("exp8", sc, sd, arm, arm, dict(ov), volume_frac=vf)
            for vf in VOLUMES for sc in SCENARIOS for sd in seeds for arm, ov in arms]


def exp9_jobs(seeds: list[int], extra: dict | None = None) -> list[Job]:
    """Outlier credit / reserve: c in {0.1, 0.2, 0.3} x k in {1, 2, 3} on the
    pass-3 base, at the two volumes where refusals happen."""
    return [Job("exp9", sc, sd, "chosen", label, {**V3_BASE, **(extra or {}), **ov}, volume_frac=vf)
            for vf in (0.12, 0.05) for sc in SCENARIOS for sd in seeds for label, ov in CREDIT_VARIANTS]


def exp10_jobs(seeds: list[int], extra: dict | None = None) -> list[Job]:
    """N_need fixed 15 vs relative to the volume target, credit off."""
    base = {**V3_BASE, "outlier_credit": "none", **(extra or {})}
    return [Job("exp10", sc, sd, "chosen", label, {**base, **ov}, volume_frac=vf)
            for vf in VOLUMES for sc in SCENARIOS for sd in seeds for label, ov in NNEED_VARIANTS]


def exp11_jobs(seeds: list[int], extra: dict | None = None) -> list[Job]:
    """Frozen calibration trigger: count (pass 2) vs rate (pass 3), kept for reference."""
    base = {**V3_BASE, "outlier_credit": "none", **(extra or {})}
    return [Job("exp11", sc, sd, "chosen", label, {**base, **ov}, volume_frac=vf)
            for vf in (0.25, 0.12) for sc in ("steady", "bursty", "saturated", "cascade", "cold_start")
            for sd in seeds for label, ov in FREEZE_VARIANTS]


def exp12_jobs(seeds: list[int], extra: dict | None = None) -> list[Job]:
    """Cold-start prior: T_i before a service's first calibrated period is the
    equal-share square-root split of the SLO, vs no table at all (plan sampling
    only), on the pass-4 base, at three volumes.  cold_start has the fault from
    t = 0, steady and saturated show what the prior costs when nothing is wrong
    in period 0."""
    base = {**V4_BASE, **(extra or {})}
    return [Job("exp12", sc, sd, "chosen", label, {**base, **ov}, volume_frac=vf)
            for vf in VOLUMES for sc in ("cold_start", "steady", "saturated") for sd in seeds for label, ov in PRIOR_VARIANTS]


def exp13_jobs(seeds: list[int], extra: dict | None = None) -> list[Job]:
    """late_everywhere re-tuned (every service's truth outlier rate at the same
    target) vs the pass-3 tuning (late_everywhere_v1: own-time P95 at 1.6 x the
    stage budget): does complete data read the fault as systemic, and what does
    chosen-v2 keep and conclude?  chosen-v2 with its calibrated table and with
    the oracle table, uniform-head for reference, at three volumes."""
    return [Job("exp13", sc, sd, arm, label, {**(extra or {}), **ov} if arm in BUDGET_ARMS else {}, volume_frac=vf)
            for vf in VOLUMES for sc in ("late_everywhere", "late_everywhere_v1") for sd in seeds
            for arm, label, ov in LATE_ARMS]


EXPERIMENTS = {"exp1": exp1_jobs, "exp1_tight": exp1_tight_jobs, "exp2": exp2_jobs, "exp3": exp3_jobs,
               "exp4": exp4_jobs, "exp5": exp5_jobs, "exp6": exp6_jobs, "exp7": exp7_jobs, "exp8": exp8_jobs,
               "exp9": exp9_jobs, "exp10": exp10_jobs, "exp11": exp11_jobs, "exp12": exp12_jobs, "exp13": exp13_jobs}

COLUMNS = ["exp", "scenario", "seed", "arm", "label", "overrides", "volume_frac", "tag", "config", "spans", "v_star", "cap", "fault_start",
           "kept_per_period", "vol_ratio", "min_vol_ratio", "bytes_per_period", "over_cap_periods", "max_pre_ratio",
           "kept_total", "records_total", "root_share", "record_share", "roots_full_frac", "roots_any_frac",
           "max_per_request", "p99_per_request", "max_per_host_period", "max_host_ratio",
           "outlier_share", "outlier_share_window", "outlier_precision", "outlier_precision_window", "outlier_keeps",
           "kept_outliers", "ctx_root_kept", "ctx_root_full", "outlier_no_root", "ctx_sufficient", "ctx_no_holes",
           "attrib_explained", "attrib_own", "attrib_child", "attrib_clean", "attrib_loss"]
COLUMNS += [f"loss_{g}" for g in REASON_GROUPS] + ["loss_split", "lost_outliers"]
COLUMNS += [f"ref_{g}" for g in REASON_GROUPS] + ["refusal_split"]
COLUMNS += ["buffer_peak_spans", "buffer_mean_spans", "buffer_peak_bytes", "decision_latency_mean", "decision_latency_p95",
            "decision_latency_max", "evicted_traces", "cap_dropped_traces"]
COLUMNS += ["outlier_share_p0", "outlier_precision_p0", "recall_p0", "truth_outliers_p0", "lost_outliers_p0",
            "ref_not_flagged_p0", "refusal_split_p0", "prior_periods",
            "truth_rate_median", "truth_rate_max", "truth_rate_spread", "truth_rate_by_svc", "truth_systemic", "truth_culprit_est"]
COLUMNS += ["truth_outliers", "recall", "recall_window", "event_recall", "recall_culprit", "recall_small", "recall_large",
            "evidence_s", "evidence_flagged_s", "evidence_any_s",
            "culprit_truth", "culprit_est", "identified", "culprit_confirmable", "spend_top",
            "p95_err_svc_mean", "p95_err_svc_max", "p95_svc_within5", "p95_err_root", "p95_svc_n", "p95_err_svc",
            "p95_cells", "p95_err_mean", "p95_err_median", "p95_err_max", "p95_within5", "p95_err_window",
            "p95_err_pooled", "p95_pooled_within5", "p95_err_lowvol", "p95_err_highvol",
            "holes_total", "kept_with_holes_frac", "holes_per_kept_parent",
            "viol_requests", "viol_requests_frac", "viol_coverage",
            "t_drift_pre", "t_drift_culprit", "t_drift_culprit_mean", "t_drift_healthy", "t_drift_healthy_mean",
            "frozen_svc_periods", "frozen_by_svc", "n_need_eff", "credit_share", "debt_total",
            "budget_mean", "budget_last", "kept_by_reason", "dropped_by_reason", "elapsed_s"]


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = COLUMNS + sorted({k for r in rows for k in r} - set(COLUMNS))
    with open(path, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=cols)
        wr.writeheader()
        for r in rows:
            wr.writerow({k: _fmt(r.get(k)) for k in cols})


def _fmt(v):
    if isinstance(v, float):
        if math.isnan(v):
            return ""
        return f"{v:.4g}"
    return v


# ---------------------------------------------------------------------------
# summaries: aggregate over seeds (mean), one table per scenario
# ---------------------------------------------------------------------------
def _agg(rows: list[dict], keys: tuple[str, ...], metrics: list[str]) -> list[dict]:
    groups: dict[tuple, list[dict]] = {}
    for r in rows:
        groups.setdefault(tuple(r.get(k) for k in keys), []).append(r)
    out = []
    for key, rs in groups.items():
        row = dict(zip(keys, key))
        row["n"] = len(rs)
        for m in metrics:
            vals = [r[m] for r in rs if m in r and r[m] is not None]
            if not vals:
                row[m] = ""
                continue
            if isinstance(vals[0], bool):
                row[m] = f"{sum(vals)}/{len(vals)}"
            elif isinstance(vals[0], (int, float)):
                v = np.array([float(x) for x in vals])
                v = v[~np.isnan(v)]
                row[m] = _cell(float(v.mean()), m) if len(v) else "-"
            else:
                row[m] = vals[0] if len(set(map(str, vals))) == 1 else "/".join(sorted(set(map(str, vals))))
        out.append(row)
    return out


def _cell(v: float, m: str) -> str:
    if m.startswith(("recall", "p95_within", "p95_svc_within", "p95_pooled_within", "vol_ratio", "outlier_share",
                     "outlier_precision", "culprit_confirmable", "viol_coverage", "viol_requests_frac",
                     "kept_with_holes_frac", "root_share", "record_share", "roots_", "ctx_", "outlier_no_root",
                     "attrib_", "loss_", "ref_", "credit_share", "truth_rate_")):
        return f"{v:.2f}"
    if m.startswith("p95_err"):
        return f"{100 * v:.1f}%"
    if m.startswith("t_drift"):
        return f"{100 * v:+.0f}%" if m in ("t_drift_culprit", "t_drift_culprit_mean") else f"{100 * v:.0f}%"
    if m in ("evidence_s", "evidence_flagged_s", "evidence_any_s", "budget_mean", "budget_last",
             "holes_per_kept_parent", "max_pre_ratio", "min_vol_ratio", "max_host_ratio"):
        return f"{v:.2f}"
    if m.startswith("decision_latency"):
        return f"{1000 * v:.1f} ms"
    if m == "buffer_peak_bytes":
        return f"{v / 1e6:.2f} MB"
    if m in ("max_per_request", "p99_per_request"):
        return f"{v:.1f}"
    return f"{v:.0f}"


def md_table(rows: list[dict], cols: list[str], header: dict[str, str] | None = None) -> str:
    header = header or {}
    lines = ["| " + " | ".join(header.get(c, c) for c in cols) + " |", "|" + "---|" * len(cols)]
    for r in rows:
        lines.append("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |")
    return "\n".join(lines)


HEAD = {"event_recall": "event recall", "p95_err_pooled": "P95 err pooled", "kept_per_period": "kept/period",
        "evidence_flagged_s": "flagged s", "min_vol_ratio": "min vol/V*", "vol_ratio": "vol/V*", "over_cap_periods": "periods>cap",
        "recall_window": "span recall", "recall_culprit": "culprit recall", "evidence_s": "evidence s",
        "culprit_est": "culprit", "identified": "identified", "p95_err_mean": "P95 err",
        "p95_within5": "P95 ≤5%", "holes_per_kept_parent": "holes/parent", "outlier_share": "outlier share",
        "outlier_share_window": "outlier share (fault)", "viol_coverage": "viol. coverage",
        "outlier_precision": "precision", "outlier_precision_window": "precision (fault)", "budget_mean": "B",
        "label": "variant", "p95_err_lowvol": "P95 err low-vol", "p95_err_highvol": "P95 err high-vol",
        "p95_err_window": "P95 err (fault)", "recall_small": "recall small", "recall_large": "recall large",
        "max_pre_ratio": "max vol/V*", "root_share": "root share", "record_share": "record share",
        "max_per_request": "max/request", "p99_per_request": "p99/request", "max_per_host_period": "max/host/period",
        "max_host_ratio": "max/host/period ÷ V*", "ctx_root_kept": "root kept", "ctx_sufficient": "ctx sufficient (old)",
        "outlier_no_root": "no root", "p95_err_svc_mean": "P95 err/svc mean", "p95_err_svc_max": "P95 err/svc max",
        "p95_svc_within5": "svcs ≤5%", "p95_err_root": "P95 err root svc", "t_drift_pre": "T drift pre-fault",
        "t_drift_culprit": "T drift culprit", "t_drift_culprit_mean": "T drift culprit mean",
        "t_drift_healthy": "T drift healthy max", "t_drift_healthy_mean": "T drift healthy mean",
        "frozen_svc_periods": "frozen svc-periods", "frozen_by_svc": "frozen by service", "roots_full_frac": "roots kept full",
        "kept_by_reason": "kept by reason", "attrib_explained": "explained", "attrib_own": "by own time",
        "attrib_child": "by kept child", "attrib_clean": "holes ordinary", "attrib_loss": "true loss",
        "loss_split": "loss by child's refusal", "refusal_split": "refusals", "lost_outliers": "lost outliers",
        "ref_budget": "ref budget", "ref_pool": "ref pool", "ref_n_need": "ref N_need coin", "ref_credit": "ref credit/reserve",
        "ref_k_cap": "ref k cap", "ref_credit_coin": "ref credit coin", "ref_not_flagged": "ref not flagged", "ref_cap": "ref cap",
        "ref_arm": "ref arm rule", "loss_budget": "loss budget", "loss_pool": "loss pool", "loss_n_need": "loss N_need",
        "loss_credit": "loss credit", "loss_k_cap": "loss k cap", "loss_not_flagged": "loss not flagged",
        "n_need_eff": "N_need", "credit_share": "credit share", "debt_total": "debt",
        "outlier_share_p0": "outlier share p0", "outlier_precision_p0": "precision p0", "recall_p0": "recall p0",
        "lost_outliers_p0": "lost p0", "ref_not_flagged_p0": "not flagged p0", "prior_periods": "prior periods",
        "truth_rate_median": "truth rate median", "truth_rate_max": "truth rate max", "truth_rate_spread": "max/median",
        "truth_systemic": "truth systemic", "truth_culprit_est": "truth culprit", "viol_requests_frac": "SLO-miss requests",
        "spend_top": "top kept rates", "loss_not_flagged": "loss not flagged",
        "buffer_peak_spans": "peak buffered", "buffer_mean_spans": "mean buffered", "buffer_peak_bytes": "peak buffered bytes",
        "decision_latency_mean": "decision latency mean", "decision_latency_p95": "decision latency p95",
        "decision_latency_max": "decision latency max", "evicted_traces": "evicted (timeout)", "cap_dropped_traces": "traces refused by cap"}

HEADLINE = ["kept_per_period", "vol_ratio", "over_cap_periods", "root_share", "outlier_share", "outlier_precision",
            "max_per_request", "max_per_host_period", "buffer_peak_spans", "attrib_explained", "attrib_loss", "outlier_no_root",
            "recall_culprit", "culprit_est", "identified", "p95_err_svc_mean", "p95_err_svc_max", "evidence_s"]
COST_COLS = ["max_per_request", "max_per_host_period", "buffer_peak_spans", "buffer_mean_spans", "buffer_peak_bytes",
             "decision_latency_mean", "decision_latency_p95", "decision_latency_max", "evicted_traces", "cap_dropped_traces",
             "over_cap_periods", "evidence_s"]
HEADLINE_NOTE = ("Headline metrics, periods >= 1: outlier share = kept non-root spans that are truth outliers "
                 "(over T_oracle or not OK); precision = spans kept *as* outliers that are truth outliers; "
                 "max/request and max/host/period in span units (a root record = 0.1); explained = kept truth "
                 "outliers whose excess can be placed (own time over its share, a kept child carries it, or every "
                 "hole was ordinary); true loss = the excess sat in a dropped child that was itself an outlier; "
                 "bounded spend = max/request, max/host/period and peak buffered spans at the collector, real numbers for every arm; "
                 "no root = kept truth outliers without any root; P95 err/svc = |est-true|/true of the 1/p-reweighted "
                 "P95 per service pooled over routes and periods >= 1 (mean and max over services); "
                 "culprit recall and evidence time are secondary.  Span recall stays in the CSV only.\n")
REFUSAL_COLS = ["lost_outliers", "ref_budget", "ref_pool", "ref_n_need", "ref_credit", "ref_k_cap", "ref_credit_coin",
                "ref_not_flagged", "ref_cap"]


def _by_volume(rows: list[dict]) -> list[float]:
    vols = []
    for r in rows:
        v = r.get("volume_frac", 0.25)
        if v not in vols:
            vols.append(v)
    return sorted(vols, reverse=True)


def _scenarios(rows: list[dict]) -> list[str]:
    out = []
    for r in rows:
        if r["scenario"] not in out:
            out.append(r["scenario"])
    return out


def _arm_order(rows: list[dict], key: str, order: tuple) -> None:
    rows.sort(key=lambda r: order.index(r[key]) if r[key] in order else 99)


def _per_volume_tables(parts: list[str], rows: list[dict], key: str, order: tuple, cols: list[str],
                       scenarios: list[str], per_scenario: bool = True, arms: tuple | None = None,
                       title: str | None = None) -> None:
    if arms is not None:
        rows = [r for r in rows if r.get(key) in arms]
    for vf in _by_volume(rows):
        vrows = [r for r in rows if r.get("volume_frac", 0.25) == vf]
        parts.append(f"## {title + ', ' if title else ''}V* = {100 * vf:.0f}% of spans\n")
        agg = _agg(vrows, (key,), cols[1:])
        _arm_order(agg, key, order)
        parts.append("### all scenarios (mean over scenarios and seeds)\n")
        parts.append(md_table(agg, cols, HEAD))
        parts.append("")
        if not per_scenario:
            continue
        for sc in scenarios:
            sub = [r for r in vrows if r["scenario"] == sc]
            if not sub:
                continue
            agg = _agg(sub, (key,), cols[1:])
            _arm_order(agg, key, order)
            parts.append(f"### {sc}  (truth culprit: {sub[0].get('culprit_truth')}, truth outliers: {sub[0].get('truth_outliers')})\n")
            parts.append(md_table(agg, cols, HEAD))
            parts.append("")


def summarize(exp: str, rows: list[dict]) -> str:
    scenarios = _scenarios(rows)
    parts = [f"# {exp}", ""]
    if exp in ("exp1", "exp1_tight", "exp8"):
        cols = ["arm"] + HEADLINE
        competitors = MECHANISM_ARMS + COMPETITOR_ARMS
        parts.append("Chosen mechanism vs the competitors (uniform head, reservoir top-K, Canopy-style rate target, "
                     "tail-based whole-trace sampler) at equal volume target V* and cap (1.25 V*); means over seeds.  "
                     "The ablations (threshold = no budget / pool / N_need, shared instead of local budget, oracle T "
                     "instead of calibration, keep-parent) follow in their own tables and are not competitors.  "
                     + HEADLINE_NOTE)
        _per_volume_tables(parts, rows, "arm", ALL_ARMS, cols, scenarios, arms=competitors, title="Competitors")
        _per_volume_tables(parts, rows, "arm", ALL_ARMS, cols, scenarios, arms=MECHANISM_ARMS + ABLATION_ARMS,
                           title="Ablations", per_scenario=False)
        parts.append("## Collector cost and bounded spend\n")
        parts.append("For every arm: the most span units one request contributed, the most on one host in one period, "
                     "the most spans the arm held at the collector before deciding (tail: whole traces until the root "
                     "ends; reservoir: its candidates until period end; budget and head arms: none), the mean held, the "
                     "peak bytes held, the tail sampler's decision latency (root end minus the trace's first span start), "
                     "the traces its buffer timed out (2 x SLO) and the traces its cap refused, periods over the cap, "
                     "and the evidence time.\n")
        _per_volume_tables(parts, rows, "arm", ALL_ARMS, ["arm"] + COST_COLS, scenarios, per_scenario=False, title="Cost")
        rcols = ["arm"] + REFUSAL_COLS + ["attrib_loss", "loss_split"]
        parts.append("## Refusals and true losses\n")
        parts.append("Refusals: why truth outliers (non-root, periods >= 1) were not kept, as shares of the lost "
                     "outliers; 'not flagged' = the arm's own T did not flag the span and the plan coin dropped it; "
                     "'arm rule' = a baseline's own selection (head coin, rate coin, reservoir).  True losses are "
                     "split by the dropped outlier child's refusal reason.\n")
        _per_volume_tables(parts, rows, "arm", ALL_ARMS, rcols, scenarios, per_scenario=True)
        for vf in _by_volume(rows):
            parts.append(headline_check([r for r in rows if r.get("volume_frac", 0.25) == vf], vf))
    elif exp == "exp2":
        cols = ["label", "kept_per_period", "vol_ratio", "min_vol_ratio", "over_cap_periods", "max_pre_ratio",
                "outlier_share", "outlier_precision", "recall_culprit", "evidence_flagged_s", "identified",
                "p95_err_svc_mean", "p95_err_pooled", "budget_mean", "dropped_by_reason"]
        parts.append("Parameter sweeps of the chosen mechanism (one parameter at a time, others at defaults: "
                     "N_need=15, p rule linear, alpha=0.5, P95).\n")
        for sc in scenarios:
            sub = [r for r in rows if r["scenario"] == sc]
            parts.append(f"## {sc}\n")
            for param in ("n_need", "p_rule", "alpha", "calib_q", "plan_fill"):
                ps = [r for r in sub if r["label"].startswith(param + "=")]
                if not ps:
                    continue
                agg = _agg(ps, ("label",), cols[1:])
                parts.append(f"### {param}\n")
                parts.append(md_table(agg, cols, HEAD))
                parts.append("")
    elif exp == "exp3":
        cols = ["label", "kept_per_period", "outlier_share", "recall_culprit", "p95_err_svc_mean", "p95_err_lowvol",
                "p95_err_highvol", "p95_within5", "identified"]
        parts.append("Ordinary-span plan: uniform rate vs square-root weights (low-volume services sampled "
                     "at higher rates). low-vol / high-vol = services below / above the median span share.\n")
        for sc in scenarios:
            sub = [r for r in rows if r["scenario"] == sc]
            parts.append(f"## {sc}\n")
            parts.append(md_table(_agg(sub, ("label",), cols[1:]), cols, HEAD))
            parts.append("")
    elif exp == "exp4":
        cols = ["label", "kept_per_period", "vol_ratio", "viol_requests_frac", "viol_coverage", "outlier_share",
                "evidence_any_s", "culprit_est", "identified", "p95_err_window", "p95_within5", "budget_mean",
                "kept_by_reason"]
        parts.append("Escalation policies. viol. coverage = mean fraction of non-root spans kept for requests "
                     "that violate their SLO (fault window). steady/saturated are controls for false escalations.\n")
        for sc in scenarios:
            sub = [r for r in rows if r["scenario"] == sc]
            parts.append(f"## {sc}\n")
            parts.append(md_table(_agg(sub, ("label",), cols[1:]), cols, HEAD))
            parts.append("")
    elif exp == "exp5":
        cols = ["label", "kept_per_period", "outlier_share", "recall_small", "recall_large", "recall_culprit",
                "outlier_precision", "evidence_s", "culprit_est", "identified", "p95_err_svc_mean", "p95_within5",
                "attrib_explained", "attrib_loss", "max_per_request", "buffer_peak_spans"]
        parts.append("T_i keyed by (service, root, class) vs (service, root). The oracle is class-aware, so "
                     "recall and precision are judged against per-class truth.  The tail-based whole-trace sampler "
                     "is the context reference: its attributability (explained / true loss) and bounded spend next "
                     "to the mechanism's.\n")
        for sc in scenarios:
            sub = [r for r in rows if r["scenario"] == sc]
            parts.append(f"## {sc}\n")
            parts.append(md_table(_agg(sub, ("label",), cols[1:]), cols, HEAD))
            parts.append("")
    elif exp == "exp6":
        cols = ["label", "kept_per_period", "vol_ratio", "over_cap_periods", "root_share", "record_share",
                "roots_full_frac", "outlier_share", "outlier_precision", "max_per_request", "max_per_host_period",
                "ctx_root_kept", "ctx_sufficient", "outlier_no_root", "recall_culprit", "identified",
                "p95_err_svc_mean", "p95_err_svc_max", "p95_err_root", "evidence_s", "budget_mean"]
        parts.append("Root policy on the pass-2 base configuration (local budget, uniform plan, escalation off, "
                     "class key on, T_i from kept spans).  pin = root always kept; on_demand = root kept at p=1 "
                     "only when a kept outlier/escalation/parent raised the response-path flag, otherwise an "
                     "ordinary span; on_demand(any) = any kept span raises the flag; record = compact root record "
                     "(0.1 span) always, full root on demand; plan = no pinning.  root share = span units spent on "
                     "roots (records included) / all span units; roots kept full = roots kept as full spans; "
                     "root kept = kept truth outliers whose root reached the collector (record counts).\n"
                     + HEADLINE_NOTE)
        _per_volume_tables(parts, rows, "label", tuple(l for l, _ in ROOT_VARIANTS), cols, scenarios)
    elif exp == "exp7":
        cols = ["label", "kept_per_period", "vol_ratio", "t_drift_pre", "t_drift_culprit", "t_drift_culprit_mean",
                "t_drift_healthy_mean", "t_drift_healthy", "frozen_svc_periods", "outlier_share", "outlier_precision",
                "outlier_precision_window", "recall_culprit", "evidence_flagged_s", "culprit_est", "identified",
                "p95_err_svc_mean"]
        parts.append(f"T_i calibration source (root policy {BEST_ROOT_POLICY}, otherwise the pass-2 base "
                     "configuration).  T drift = T_used / T_oracle - 1 of the table in force each period "
                     "(mean over the oracle's (route, class) keys of a service): pre-fault = max |drift| over all "
                     "services in periods 1..fault; culprit = max (and mean) signed drift of the culprit service "
                     "during the fault; healthy = |drift| of the services the fault did not inflate, during the "
                     "fault.  frozen svc-periods = (service, period) pairs the frozen source held fixed.  "
                     "precision (fault) = precision of outlier keeps in the fault window.\n")
        _per_volume_tables(parts, rows, "label", tuple(l for l, _ in CALIB_VARIANTS), cols, scenarios)
    elif exp == "exp9":
        cols = ["label", "kept_per_period", "vol_ratio", "over_cap_periods", "outlier_share", "outlier_precision",
                "max_per_request", "p99_per_request", "max_per_host_period", "credit_share", "debt_total",
                "ref_budget", "ref_pool", "ref_n_need", "ref_credit", "ref_k_cap", "ref_credit_coin",
                "attrib_explained", "attrib_loss", "recall_culprit", "identified", "p95_err_svc_mean"]
        parts.append("Outlier credit (kept beyond budget/pool while the host's credit c x pool lasts, repaid from "
                     "the next period's plan on that host) vs reserve (c x pool set aside for refused outliers), "
                     "c in {0.1, 0.2, 0.3}, k = max credit/reserve spans per request in {1, 2, 3}; pass-3 base "
                     f"(local budget, on-demand roots, frozen rate-triggered calibration, relative N_need f={BEST_N_NEED_FRAC}).  "
                     "credit share = credit/reserve keeps among kept non-root spans; debt = credit spans over the run; "
                     "ref * = shares of lost truth outliers by refusal reason.\n" + HEADLINE_NOTE)
        _per_volume_tables(parts, rows, "label", tuple(l for l, _ in CREDIT_VARIANTS), cols, scenarios)
    elif exp == "exp10":
        cols = ["label", "n_need_eff", "kept_per_period", "vol_ratio", "over_cap_periods", "outlier_share",
                "outlier_precision", "max_per_request", "ref_n_need", "ref_budget", "ref_pool", "attrib_explained",
                "attrib_loss", "recall_culprit", "evidence_s", "culprit_est", "identified", "p95_err_svc_mean", "p95_err_svc_max"]
        parts.append("N_need fixed at 15 vs relative to the volume target: max(N_min = 3, floor(f x V* / services)); "
                     "credit off, otherwise the pass-3 base.  N_need = the value in force; ref N_need coin = share of "
                     "lost truth outliers dropped by the p < 1 coin.\n")
        _per_volume_tables(parts, rows, "label", tuple(l for l, _ in NNEED_VARIANTS), cols, scenarios)
    elif exp == "exp11":
        cols = ["label", "frozen_svc_periods", "frozen_by_svc", "t_drift_pre", "t_drift_culprit", "t_drift_culprit_mean",
                "t_drift_healthy_mean", "outlier_precision", "outlier_precision_window", "outlier_share",
                "recall_culprit", "identified", "p95_err_svc_mean"]
        parts.append("Frozen calibration trigger: count = over N_need in the period (pass 2); rate = the period's "
                     "outlier rate >= 2 x the service's healthy rate (EWMA while unfrozen, floor 1%), with the frozen "
                     "periods purged from the window; kept = no freezing, for reference.  frozen svc-periods should be "
                     "near zero in steady and bursty.\n")
        _per_volume_tables(parts, rows, "label", tuple(l for l, _ in FREEZE_VARIANTS), cols, scenarios)
    elif exp == "exp12":
        cols = ["label", "outlier_share_p0", "outlier_precision_p0", "recall_p0", "lost_outliers_p0", "ref_not_flagged_p0",
                "prior_periods", "outlier_share", "outlier_precision", "ref_not_flagged", "attrib_loss", "loss_not_flagged",
                "recall_culprit", "evidence_s", "culprit_est", "identified", "p95_err_svc_mean", "max_per_request"]
        parts.append("Cold-start prior: before a service's first calibrated period T_i is the equal-share square-root "
                     "split of the SLO (every stage of the root's critical structure gets SLO / stages, T adds up along "
                     "the tree) instead of no table at all (plan sampling only); pass-4 base otherwise.  The p0 columns "
                     "are period 0, the cold-start period the headline metrics leave out: outlier share and precision of "
                     "what was kept there, recall of its truth outliers, how many were lost and the share of those the "
                     "arm's T did not flag; prior periods = periods in which the prior answered a lookup.  The other "
                     "columns are the usual periods >= 1.\n")
        _per_volume_tables(parts, rows, "label", tuple(l for l, _ in PRIOR_VARIANTS), cols, scenarios)
    elif exp == "exp13":
        cols = ["label", "truth_rate_median", "truth_rate_max", "truth_rate_spread", "truth_systemic", "truth_culprit_est",
                "viol_requests_frac", "kept_per_period", "outlier_share", "outlier_precision", "culprit_est", "identified",
                "spend_top", "recall_window", "p95_err_svc_mean"]
        parts.append("late_everywhere re-tuned (pass 4: every service's truth outlier rate at the same target, 25%) vs "
                     "the pass-3 tuning late_everywhere_v1 (own-time P95 at 1.6 x the stage budget).  truth rate = truth "
                     "outlier rate per service on complete data in the fault window (median, max, max / median); truth "
                     "systemic / truth culprit = the culprit rule applied to complete data, i.e. the oracle's reading; "
                     "culprit / identified = the same rule on what the arm kept (identified = read as systemic).  "
                     "chosen-v2 with its calibrated table and with the oracle table, uniform-head for reference.\n")
        _per_volume_tables(parts, rows, "label", tuple(l for _, l, _ in LATE_ARMS), cols, scenarios)
    return "\n".join(parts) + "\n"


def headline_check(rows: list[dict], vf: float | None = None) -> str:
    """Chosen vs the strongest baselines on the headline metrics, per scenario."""
    chosen_arm = "chosen-v2" if any(r["arm"] == "chosen-v2" for r in rows) else "chosen"
    title = "## Headline comparison" + (f" at V* = {100 * vf:.0f}%" if vf is not None else "")
    lines = [title, "",
             f"{chosen_arm} vs the competitors uniform-head, reservoir and tail (ablations excluded): outlier share / "
             "culprit recall / max per request / explained / P95 err per service (mean); P95 within 5% is the only pass-fail item.", ""]

    def _m(rs, k):
        v = [r[k] for r in rs if isinstance(r.get(k), (int, float)) and not math.isnan(r[k])]
        return float(np.mean(v)) if v else math.nan

    def _c(rs, k, fmt="{:.2f}"):
        v = _m(rs, k)
        return fmt.format(v) if not math.isnan(v) else "-"

    for sc in _scenarios(rows):
        ch = [r for r in rows if r["scenario"] == sc and r["arm"] == chosen_arm]
        if not ch:
            continue
        comp = {a: [r for r in rows if r["scenario"] == sc and r["arm"] == a] for a in ("uniform-head", "reservoir", "tail")}
        comp = {a: rs for a, rs in comp.items() if rs}
        p95 = _m(ch, "p95_err_svc_mean")
        others = lambda k, fmt="{:.2f}": ", ".join(f"{a} {_c(rs, k, fmt)}" for a, rs in comp.items())   # noqa: E731
        lines.append(f"- {sc}: {chosen_arm} outlier share {_c(ch, 'outlier_share')} ({others('outlier_share')})"
                     f"; culprit recall {_c(ch, 'recall_culprit')} ({others('recall_culprit')})"
                     f"; max/request {_c(ch, 'max_per_request', '{:.1f}')} ({others('max_per_request', '{:.1f}')})"
                     f"; explained {_c(ch, 'attrib_explained')}, true loss {_c(ch, 'attrib_loss')} ({others('attrib_explained')})"
                     f"; P95 err/svc {100 * p95:.1f}% -> {'PASS' if p95 <= 0.05 else 'FAIL'} ({others('p95_err_svc_mean', '{:.3f}')})")
    return "\n".join(lines) + "\n"


def load_rows(path: Path) -> list[dict]:
    """Read a results CSV back into typed rows (floats, bools, ints) so that
    `summarize` can be rerun without recomputing."""
    rows = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        for raw in csv.DictReader(f):
            row = {}
            for k, v in raw.items():
                if v is None or v == "":
                    row[k] = math.nan if k not in ("overrides", "culprit_est", "culprit_truth", "spend_top", "label", "arm",
                                                   "scenario", "exp", "kept_by_reason", "dropped_by_reason", "refusal_split",
                                                   "loss_split", "frozen_by_svc", "p95_err_svc", "tag", "config", "refusal_split_p0",
                                                   "truth_rate_by_svc", "truth_culprit_est") else ""
                elif v in ("True", "False"):
                    row[k] = v == "True"
                else:
                    try:
                        fv = float(v)
                        row[k] = int(fv) if k in ("seed", "spans", "over_cap_periods", "kept_total", "records_total",
                                                  "lost_outliers", "kept_outliers", "truth_outliers") and fv.is_integer() else fv
                    except ValueError:
                        row[k] = v
            rows.append(row)
    return rows


def resummarize(exp: str, results_dir: Path) -> None:
    rows = load_rows(results_dir / f"{exp}.csv")
    (results_dir / f"{exp}.md").write_text(summarize(exp, rows), encoding="utf-8")


def with_volumes(jobs: list[Job], volumes: list[float] | None) -> list[Job]:
    """Run every (scenario, seed, arm, label) of an experiment at each of the
    given volume targets instead of the experiment's own list."""
    if not volumes:
        return jobs
    out, seen = [], set()
    for j in jobs:
        for v in volumes:
            key = (j.scenario, j.seed, j.arm, j.label, v)
            if key in seen:
                continue
            seen.add(key)
            out.append(Job(j.exp, j.scenario, j.seed, j.arm, j.label, dict(j.overrides), v, j.cap_factor,
                           dict(j.scenario_overrides)))
    return out


def _job_key(j: Job) -> tuple:
    return (j.scenario, int(j.seed), j.arm, j.label, float(j.volume_frac),
            " ".join(f"{k}={v}" for k, v in j.overrides.items()), _config_hash(j.arm, j.overrides),
            scenario_tag(j.scenario))


def _row_key(r: dict) -> tuple:
    # rows written before pass 4 carry no tag and no config hash: taken as the current
    # workload at scenario tuning v1, run with the current presets and defaults
    tag = r.get("tag") or f"{scenario_tag(r['scenario']).split('/')[0]}/{r['scenario']}v1"
    ov = r.get("overrides") or ""
    config = r.get("config")
    if config is None:
        config = _config_hash(r["arm"], _parse_kv(ov))
    return (r["scenario"], int(r["seed"]), r["arm"], r["label"], float(r.get("volume_frac", 0.25)), ov, config, tag)


def run_experiment(exp: str, results_dir: Path, seeds: list[int], jobs_parallel: int = 1,
                   extra: dict | None = None, volumes: list[float] | None = None, fresh: bool = False) -> list[dict]:
    """Run an experiment's jobs and write results/<exp>.csv and .md.  Resumable:
    rows already in the CSV for the same (scenario, seed, arm, label, volume,
    overrides, workload / scenario tag) are kept and not rerun unless `fresh`;
    rows that match no job (changed overrides, workload or scenario tuning) are
    dropped; the CSV is rewritten after every chunk of runs."""
    jobs = with_volumes(EXPERIMENTS[exp](seeds, extra), volumes)
    path = results_dir / f"{exp}.csv"
    done: dict[tuple, dict] = {}
    if not fresh and path.exists():
        for r in load_rows(path):
            done.setdefault(_row_key(r), r)
    keys = [_job_key(j) for j in jobs]
    rows = [done[k] for k in keys if k in done]
    todo = [j for j, k in zip(jobs, keys) if k not in done]
    print(f"{exp}: {len(rows)} of {len(jobs)} runs already in {path.name}, {len(todo)} to run", flush=True)

    def write(all_rows: list[dict]) -> None:
        all_rows = sorted(all_rows, key=lambda r: (-r.get("volume_frac", 0.25),
                                                   ALL_SCENARIOS.index(r["scenario"]) if r["scenario"] in ALL_SCENARIOS else 99,
                                                   r["seed"], ALL_ARMS.index(r["arm"]) if r["arm"] in ALL_ARMS else 99, r["label"]))
        write_csv(all_rows, path)
        (results_dir / f"{exp}.md").write_text(summarize(exp, all_rows), encoding="utf-8")

    if todo:
        rows += run_jobs(todo, jobs_parallel, flush=lambda new: write(rows + new))
    write(rows)
    return rows
