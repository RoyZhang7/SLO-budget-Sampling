"""Metrics of one replay run against ground truth.

Ground truth comes from the complete corpus plus the oracle T table (the
calibration procedure run on complete data from the healthy reference).

  outlier (truth)   duration > T_oracle(route, class, service) or status not OK

Headline metrics (all over periods >= 1, i.e. after the cold-start period):
  outlier_share     kept non-root spans that are truth outliers / kept non-root spans
  outlier_precision spans the arm kept *as* outliers that are truth outliers
  max_per_request   most span units kept for one request; p99_per_request likewise
  max_per_host_period   most span units kept on one host in one period
  over_cap_periods  periods in which the arm wanted more than the collector cap
  ctx_root_kept     kept truth outliers (non-root) whose root reached the collector
                    (full span or root record); outlier_no_root = 1 - ctx_root_kept
  ctx_sufficient    ... and whose own hole count is 0, so the latency can be
                    attributed to the span itself rather than a dropped callee
  p95_err_svc_*     |P95_est - P95_true| / P95_true per service, pooled over
                    routes and periods >= 1, 1/p-weighted kept spans
Cold-start period (period 0, which the headline metrics leave out):
  outlier_share_p0, outlier_precision_p0, recall_p0, lost_outliers_p0,
  ref_not_flagged_p0 (lost truth outliers the arm's T did not flag),
  prior_periods (periods in which the cold-start prior answered a lookup)
Fault shape on complete data (fault window): truth_rate_median / _max /
  _spread (max over median) of the truth outlier rate per service;
  truth_systemic / truth_culprit_est = the culprit rule applied to complete data
Collector cost (bounded spend holds for every arm): buffer_peak_spans /
  buffer_mean_spans / buffer_peak_bytes = spans (bytes) the arm holds at the
  collector before deciding (tail: whole traces until the root ends; reservoir:
  its candidates until period end; budget and head arms: 0), decision_latency_*
  = root end minus the trace's first span start for tail-decided traces,
  evicted_traces = traces the tail buffer timed out
Secondary:
  recall_culprit    kept truth outliers of the culprit service in the fault window
  evidence_s        first kept truth outlier of the culprit service after the
                    fault, at decision time (period end for period-end arms)
  culprit_est       reweighted kept truth outliers per service over the
                    service's span count; the deepest service within 50% of the
                    maximum is named unless 60% of services qualify (systemic)
  t_drift_*         T_used / T_oracle - 1 from the sampler's per-period table
Reference (CSV only): span recall, event recall, P95 per (service, route, period).
"""
from __future__ import annotations

import math

import numpy as np

from .model import Corpus
from .replay import RunResult
from .ttable import Calibrator


class CorpusArrays:
    """Vectorised view of a corpus, built once per corpus."""

    def __init__(self, corpus: Corpus, oracle: Calibrator):
        spans = corpus.spans
        n = len(spans)
        self.corpus = corpus
        self.services = sorted({s.service for s in spans})
        self.routes = sorted({s.route for s in spans})
        self.hosts = sorted({s.host for s in spans})
        svc_code = {s: i for i, s in enumerate(self.services)}
        route_code = {r: i for i, r in enumerate(self.routes)}
        host_code = {h: i for i, h in enumerate(self.hosts)}
        self.svc = np.fromiter((svc_code[s.service] for s in spans), dtype=np.int64, count=n)
        self.route = np.fromiter((route_code[s.route] for s in spans), dtype=np.int64, count=n)
        self.host = np.fromiter((host_code[s.host] for s in spans), dtype=np.int64, count=n)
        self.duration = np.fromiter((s.duration for s in spans), dtype=float, count=n)
        self.start = np.fromiter((s.start for s in spans), dtype=float, count=n)
        self.end = self.start + self.duration
        self.depth = np.fromiter((s.depth for s in spans), dtype=np.int64, count=n)
        self.is_root = np.fromiter((s.parent_idx < 0 for s in spans), dtype=bool, count=n)
        codes: dict[int, int] = {}                 # trace ids may be 128-bit: code them through a dict
        self.trace_code = np.fromiter((codes.setdefault(s.trace_id, len(codes)) for s in spans), dtype=np.int64, count=n)
        self.n_traces = len(codes)
        # root span index of every span (its own index for roots, -1 when the trace has no root)
        root_of_trace = np.full(self.n_traces, -1, dtype=np.int64)
        root_of_trace[self.trace_code[self.is_root]] = np.flatnonzero(self.is_root)
        self.root_idx = root_of_trace[self.trace_code]
        self.period = ((self.end - corpus.t0) // corpus.period).astype(np.int64)
        self.n_periods = int(self.period.max()) + 1 if n else 0
        self.cls = [s.req_class for s in spans]
        self.bucket = np.array([c.split("|", 1)[1] if "|" in c else "any" for c in self.cls])
        T = np.full(n, np.nan)
        cache: dict[tuple[str, str, str], float | None] = {}
        for i, s in enumerate(spans):
            key = (s.route, s.req_class, s.service)
            if key not in cache:
                cache[key] = oracle.lookup(*key)
            v = cache[key]
            if v is not None:
                T[i] = v
        self.T = T
        # oracle stage budget (own-time share) per span, for attributability
        B = np.full(n, np.nan)
        bcache: dict[tuple[str, str, str], float | None] = {}
        for i, s in enumerate(spans):
            key = (s.route, s.req_class, s.service)
            if key not in bcache:
                bcache[key] = oracle.lookup_b(*key) if hasattr(oracle, "lookup_b") else None
            v = bcache[key]
            if v is not None:
                B[i] = v
        self.B = B
        self.own = np.fromiter((s.own for s in spans), dtype=float, count=n)
        status = np.fromiter((s.status_ok for s in spans), dtype=bool, count=n)
        self.truth = (~np.isnan(T) & (self.duration > T)) | ~status
        slo = np.array([corpus.routes_slo.get(s.route, np.inf) for s in spans])
        self.root_violates = self.is_root & (self.duration > slo)
        viol_code = np.zeros(max(1, self.n_traces), dtype=bool)
        viol_code[self.trace_code[self.root_violates]] = True
        self.in_viol_request = viol_code[self.trace_code]
        self.mean_depth = np.array([self.depth[self.svc == i].mean() for i in range(len(self.services))])
        counts = np.bincount(self.svc, minlength=len(self.services))
        self.share = counts / max(1, counts.sum())
        self.root_service = corpus.spans[corpus.roots[0]].service if corpus.roots else None


def _wquantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    order = np.argsort(values)
    v, w = values[order], weights[order]
    cw = np.cumsum(w)
    k = int(np.searchsorted(cw, q * cw[-1], side="left"))
    return float(v[min(k, len(v) - 1)])


def _safe_div(a, b) -> float:
    return float(a) / float(b) if b else math.nan


def _mean(xs) -> float:
    return float(np.mean(xs)) if len(xs) else math.nan


# why a truth outlier was not kept, grouped for the refusal split
REASON_GROUP = {"budget": "budget", "pool": "pool", "outlier_coin": "n_need", "credit_exhausted": "credit",
                "reserve_exhausted": "credit", "credit_k": "k_cap", "credit_coin": "credit_coin",
                "plan_coin": "not_flagged", "cap": "cap", "tail_cap": "cap"}
REASON_GROUPS = ("budget", "pool", "n_need", "credit", "k_cap", "credit_coin", "not_flagged", "cap", "arm")


def _group(reason: str) -> str:
    return REASON_GROUP.get(reason, "arm")      # "arm": a baseline's own rule (head coin, rate coin, reservoir)


def compute(corpus: Corpus, arrays: CorpusArrays, res: RunResult, within: float = 0.05) -> dict:
    A = arrays
    n = res.n_spans
    kept = res.kept_mask                       # duration reached the collector (full spans + root records)
    full = res.full_mask                       # full spans only
    p = res.p_array()
    bad = np.isnan(p) | (p <= 0)
    w = np.where(bad, 1.0, 1.0 / np.where(bad, 1.0, p))
    nonroot = ~A.is_root
    fault = corpus.fault
    t_fault = fault.start if fault else math.nan
    window = (A.end >= t_fault) if fault else np.ones(n, dtype=bool)
    warm = A.period >= 1                        # after the cold-start period
    reasons = np.array(res.kept_reason) if len(res.kept_reason) else np.array([], dtype=str)
    is_rec = reasons == "root_record"
    cost = res.kept_cost
    holes = np.zeros(n, dtype=np.int64)
    holes[res.kept_idx] = res.kept_holes

    out: dict = {"arm": res.arm, "scenario": corpus.name, "spans": n, "v_star": res.v_star, "cap": res.cap,
                 "elapsed_s": round(res.elapsed_s, 2)}

    # volume ------------------------------------------------------------------
    pers = res.periods[1:] if len(res.periods) > 1 else res.periods
    if len(pers) > 1 and pers[-1].spans < 0.5 * np.mean([x.spans for x in pers[:-1]]):
        pers = pers[:-1]                  # trailing partial period
    cp = np.array([x.cost_post for x in pers], dtype=float)
    cpre = np.array([x.cost_pre for x in pers], dtype=float)
    out["kept_per_period"] = float(cp.mean()) if len(cp) else math.nan
    out["vol_ratio"] = _safe_div(cp.mean(), res.v_star) if len(cp) else math.nan
    out["min_vol_ratio"] = _safe_div(cp[1:].min(), res.v_star) if len(cp) > 1 else math.nan
    dropped: dict = {}
    for x in pers:
        for k, v in x.dropped.items():
            dropped[k] = dropped.get(k, 0) + v
    dtot = sum(dropped.values())
    out["dropped_by_reason"] = " ".join(f"{k}:{v / dtot:.0%}" for k, v in sorted(dropped.items())) if dtot else ""
    out["bytes_per_period"] = float(np.mean([x.bytes_post for x in pers])) if pers else math.nan
    out["over_cap_periods"] = int((cpre > res.cap + 1e-9).sum()) if len(cpre) else 0
    out["max_pre_ratio"] = _safe_div(cpre.max(), res.v_star) if len(cpre) else math.nan
    out["kept_total"] = int(full.sum())
    out["records_total"] = int(is_rec.sum())
    kw = warm[res.kept_idx]                              # kept items in warm periods
    tot_cost = float(cost[kw].sum())
    root_items = A.is_root[res.kept_idx]
    out["root_share"] = _safe_div(cost[kw & root_items].sum(), tot_cost)
    out["record_share"] = _safe_div(cost[kw & is_rec].sum(), tot_cost)
    n_roots_warm = int((A.is_root & warm).sum())
    out["roots_full_frac"] = _safe_div((full & A.is_root & warm).sum(), n_roots_warm)
    out["roots_any_frac"] = _safe_div((kept & A.is_root & warm).sum(), n_roots_warm)

    # bounded spend ----------------------------------------------------------------
    per_req = np.bincount(A.trace_code[res.kept_idx][kw], weights=cost[kw], minlength=A.n_traces)
    out["max_per_request"] = float(per_req.max()) if len(per_req) else math.nan
    out["p99_per_request"] = float(np.quantile(per_req, 0.99)) if len(per_req) else math.nan
    hp = A.host[res.kept_idx] * A.n_periods + res.kept_period
    per_hp = np.bincount(hp[kw], weights=cost[kw], minlength=len(A.hosts) * A.n_periods)
    out["max_per_host_period"] = float(per_hp.max()) if len(per_hp) else math.nan
    out["max_host_ratio"] = _safe_div(out["max_per_host_period"], res.v_star)

    # outlier share, precision, recall -----------------------------------------------
    truth_nr = A.truth & nonroot
    out["truth_outliers"] = int(truth_nr.sum())
    out["recall"] = _safe_div((truth_nr & full).sum(), truth_nr.sum())
    tw = truth_nr & window
    out["recall_window"] = _safe_div((tw & full).sum(), tw.sum())
    kept_nr = full & nonroot & warm
    out["outlier_share"] = _safe_div((kept_nr & A.truth).sum(), kept_nr.sum())
    kept_nr_w = kept_nr & window
    out["outlier_share_window"] = _safe_div((kept_nr_w & A.truth).sum(), kept_nr_w.sum())
    # event recall: (service, period) cells in the window with >= 15 truth
    # outliers (an incident) in which at least 3 truth outliers were kept
    sp_cell = A.svc * A.n_periods + A.period
    n_cells = len(A.services) * A.n_periods
    tot_c = np.bincount(sp_cell[tw], minlength=n_cells)
    kept_c = np.bincount(sp_cell[tw & full], minlength=n_cells)
    ev = tot_c >= 15
    out["event_recall"] = float((kept_c[ev] >= 3).mean()) if ev.any() else math.nan
    is_out_reason = np.isin(reasons, ["outlier", "escalate"]) & kw
    out["outlier_keeps"] = int(is_out_reason.sum())
    if is_out_reason.any():
        idx = res.kept_idx[is_out_reason]
        out["outlier_precision"] = float(A.truth[idx].mean())
        idx_w = idx[window[idx]]
        out["outlier_precision_window"] = float(A.truth[idx_w].mean()) if len(idx_w) else math.nan
    else:
        out["outlier_precision"] = math.nan
        out["outlier_precision_window"] = math.nan
    for b in sorted(set(A.bucket.tolist())):
        if b == "any":
            continue
        m = tw & (A.bucket == b)
        out[f"recall_{b}"] = _safe_div((m & full).sum(), m.sum())

    # context sufficiency ---------------------------------------------------------------
    ko = np.flatnonzero(full & truth_nr & warm)          # kept truth outliers, non-root
    out["kept_outliers"] = int(len(ko))
    if len(ko):
        ridx = A.root_idx[ko]
        has_root = ridx >= 0
        root_any = np.zeros(len(ko), dtype=bool)
        root_full = np.zeros(len(ko), dtype=bool)
        root_any[has_root] = kept[ridx[has_root]]
        root_full[has_root] = full[ridx[has_root]]
        out["ctx_root_kept"] = float(root_any.mean())
        out["ctx_root_full"] = float(root_full.mean())
        out["outlier_no_root"] = float(1.0 - root_any.mean())
        out["ctx_sufficient"] = float((root_any & (holes[ko] == 0)).mean())
        out["ctx_no_holes"] = float((holes[ko] == 0).mean())
    else:
        for k in ("ctx_root_kept", "ctx_root_full", "outlier_no_root", "ctx_sufficient", "ctx_no_holes"):
            out[k] = math.nan

    # refusal accounting: why truth outliers (non-root, warm) were not kept ----------------
    dr = res.drop_reason if len(res.drop_reason) == n else [""] * n
    lost = np.flatnonzero(truth_nr & ~full & warm)
    out["lost_outliers"] = int(len(lost))
    groups: dict[str, int] = {}
    for i in lost:
        g = _group(dr[i])
        groups[g] = groups.get(g, 0) + 1
    for g in REASON_GROUPS:
        out[f"ref_{g}"] = _safe_div(groups.get(g, 0), len(lost))
    out["refusal_split"] = " ".join(f"{g}:{v / len(lost):.0%}" for g, v in sorted(groups.items(), key=lambda kv: -kv[1])) if len(lost) else ""

    # cold-start period: what the decider did before the first table -------------------------
    p0 = A.period == 0
    kept_nr0 = full & nonroot & p0
    out["outlier_share_p0"] = _safe_div((kept_nr0 & A.truth).sum(), kept_nr0.sum())
    k0 = np.isin(reasons, ["outlier", "escalate"]) & (res.kept_period == 0)
    out["outlier_precision_p0"] = float(A.truth[res.kept_idx[k0]].mean()) if k0.any() else math.nan
    tr0 = truth_nr & p0
    out["truth_outliers_p0"] = int(tr0.sum())
    out["recall_p0"] = _safe_div((tr0 & full).sum(), tr0.sum())
    lost0 = np.flatnonzero(tr0 & ~full)
    out["lost_outliers_p0"] = int(len(lost0))
    g0: dict[str, int] = {}
    for i in lost0:
        g = _group(dr[i])
        g0[g] = g0.get(g, 0) + 1
    out["ref_not_flagged_p0"] = _safe_div(g0.get("not_flagged", 0), len(lost0))
    out["refusal_split_p0"] = " ".join(f"{g}:{v / len(lost0):.0%}" for g, v in sorted(g0.items(), key=lambda kv: -kv[1])) if len(lost0) else ""
    out["prior_periods"] = int(sum(1 for d in res.diagnostics if d.get("prior_hits", 0) > 0))

    # attributability: can a kept outlier's excess be placed? ------------------------------
    #   (a) its own time exceeds its own-time share (the oracle stage budget)
    #   (b) a kept child is itself a truth outlier (the excess is passed down)
    #   (c) every dropped child was ordinary in ground truth
    # otherwise the excess sat in a dropped child that was an outlier: a true loss,
    # split by that child's refusal reason (the dropped outlier child with the largest excess)
    n_a = n_b = n_c = 0
    loss_groups: dict[str, int] = {}
    for i in ko:
        sp = corpus.spans[i]
        a = bool(not np.isnan(A.B[i]) and A.own[i] > A.B[i])
        kids = sp.children
        b = any(full[c] and A.truth[c] for c in kids)
        dropped_out = [c for c in kids if not full[c] and A.truth[c]]
        c = len(dropped_out) == 0
        if a:
            n_a += 1
        elif b:
            n_b += 1
        elif c:
            n_c += 1
        else:
            worst = max(dropped_out, key=lambda k: (A.duration[k] - (A.T[k] if not np.isnan(A.T[k]) else 0.0)))
            g = _group(dr[worst])
            loss_groups[g] = loss_groups.get(g, 0) + 1
    nk = len(ko)
    out["attrib_explained"] = _safe_div(n_a + n_b + n_c, nk)
    out["attrib_own"] = _safe_div(n_a, nk)
    out["attrib_child"] = _safe_div(n_b, nk)
    out["attrib_clean"] = _safe_div(n_c, nk)
    n_loss = sum(loss_groups.values())
    out["attrib_loss"] = _safe_div(n_loss, nk)
    for g in REASON_GROUPS:
        out[f"loss_{g}"] = _safe_div(loss_groups.get(g, 0), nk)
    out["loss_split"] = " ".join(f"{g}:{v / n_loss:.0%}" for g, v in sorted(loss_groups.items(), key=lambda kv: -kv[1])) if n_loss else ""

    # fault shape on complete data: truth outlier rate per service in the window ------------------
    n_svc = len(A.services)
    tr_cnt = np.bincount(A.svc[A.truth & window], minlength=n_svc).astype(float)
    vol_w = np.bincount(A.svc[window], minlength=n_svc).astype(float)
    t_rate = np.divide(tr_cnt, vol_w, out=np.zeros(n_svc), where=vol_w > 0)
    t_med = float(np.median(t_rate)) if n_svc else math.nan
    out["truth_rate_median"] = t_med
    out["truth_rate_max"] = float(t_rate.max()) if n_svc else math.nan
    out["truth_rate_spread"] = _safe_div(t_rate.max(), t_med) if n_svc and t_med > 0 else math.nan
    out["truth_rate_by_svc"] = " ".join(f"{A.services[i]}:{t_rate[i]:.2f}" for i in np.argsort(-t_rate)[:5] if t_rate[i] > 0)
    if n_svc and t_rate.max() > 0:
        cands = np.flatnonzero(t_rate >= 0.5 * t_rate.max())
        out["truth_systemic"] = bool(len(cands) >= 0.6 * n_svc)
        out["truth_culprit_est"] = "none" if out["truth_systemic"] else A.services[int(cands[np.argmax(A.mean_depth[cands])])]
    else:
        out["truth_systemic"] = False
        out["truth_culprit_est"] = "none"

    # evidence and culprit --------------------------------------------------------
    svc_code = {s: i for i, s in enumerate(A.services)}
    if fault:
        culprit = fault.culprit
        inflated = [svc_code[s] for s in fault.services if s in svc_code]
        ev_mask = tw & np.isin(A.svc, inflated)
        kt = np.full(n, np.inf)
        kt[res.kept_idx] = res.kept_time
        t_any = kt[ev_mask].min() if ev_mask.any() else np.inf
        out["evidence_any_s"] = float(t_any - t_fault) if np.isfinite(t_any) else math.nan
        flagged = np.isin(reasons, ["outlier", "escalate", "parent"])
        kt_flag = np.full(n, np.inf)
        kt_flag[res.kept_idx[flagged]] = res.kept_time[flagged]
        if culprit in svc_code:
            cm = tw & (A.svc == svc_code[culprit])
            out["recall_culprit"] = _safe_div((cm & full).sum(), cm.sum())
            t_c = kt[cm].min() if cm.any() else np.inf
            out["evidence_s"] = float(t_c - t_fault) if np.isfinite(t_c) else math.nan
            t_f = kt_flag[cm].min() if cm.any() else np.inf
            out["evidence_flagged_s"] = float(t_f - t_fault) if np.isfinite(t_f) else math.nan
        else:
            out["recall_culprit"] = math.nan
            out["evidence_s"] = math.nan
            out["evidence_flagged_s"] = math.nan
        # identification from kept spans + holes: reweighted kept truth outliers
        # per service over the service's span count (counts are always known),
        # the deepest service within 50% of the highest rate is named unless
        # 60% of the services are that high (systemic)
        km = full & tw & A.truth
        cnt = np.bincount(A.svc[km], weights=w[km], minlength=len(A.services))
        vol = np.bincount(A.svc[window], minlength=len(A.services)).astype(float)
        rate = np.divide(cnt, vol, out=np.zeros_like(cnt), where=vol > 0)
        est, confirm = None, math.nan
        if rate.max() > 0:
            cands = np.flatnonzero(rate >= 0.5 * rate.max())
            if len(cands) >= 0.6 * len(A.services):
                est = None
            else:
                est = A.services[int(cands[np.argmax(A.mean_depth[cands])])]
                # confirmable: its kept outlier spans whose children were all kept (or leaves)
                em = km & (A.svc == svc_code[est])
                confirm = float((holes[em] == 0).mean()) if em.any() else math.nan
        out["culprit_truth"] = culprit if culprit else "none"
        out["culprit_est"] = est if est else "none"
        out["identified"] = bool(est == culprit)
        out["culprit_confirmable"] = confirm
        top = np.argsort(-rate)[:3]
        out["spend_top"] = " ".join(f"{A.services[i]}:{rate[i]:.2f}" for i in top if rate[i] > 0)
    else:
        for k in ("evidence_any_s", "recall_culprit", "evidence_s", "evidence_flagged_s", "culprit_confirmable"):
            out[k] = math.nan
        out["culprit_truth"] = out["culprit_est"] = "none"
        out["identified"] = True
        out["spend_top"] = ""

    # reweighted P95 error per (service, route, period) cell ------------------------------
    errs, errs_w, errs_low, errs_high = [], [], [], []
    median_share = float(np.median(A.share))
    cell = (A.svc * len(A.routes) + A.route) * A.n_periods + A.period
    kidx = np.flatnonzero(kept)                 # kept span indexes in index order
    kept_cells = cell[kidx]
    order = np.argsort(cell)
    cell_s = cell[order]
    bounds = np.flatnonzero(np.diff(cell_s)) + 1
    starts = np.concatenate([[0], bounds])
    ends = np.concatenate([bounds, [len(cell_s)]])
    kept_order = np.argsort(kept_cells)
    kc_s = kept_cells[kept_order]
    kept_idx_s = kidx[kept_order]
    for a, b in zip(starts, ends):
        if b - a < 30:
            continue
        c = cell_s[a]
        per = c % A.n_periods
        if per == 0:
            continue                      # cold-start period: no table yet
        lo = np.searchsorted(kc_s, c, side="left")
        hi = np.searchsorted(kc_s, c, side="right")
        if hi - lo < 10:
            continue
        idx_all = order[a:b]
        idx_k = kept_idx_s[lo:hi]
        truth_q = float(np.quantile(A.duration[idx_all], 0.95))
        est_q = _wquantile(A.duration[idx_k], w[idx_k], 0.95)
        e = abs(est_q - truth_q) / truth_q
        errs.append(e)
        if window[idx_all].all():
            errs_w.append(e)
        svc = int(A.svc[idx_all[0]])
        (errs_low if A.share[svc] <= median_share else errs_high).append(e)
    # pooled over all periods >= 1 per (service, route): sampling noise averages
    # out, what remains is estimator bias
    pooled = []
    pcell = A.svc * len(A.routes) + A.route
    for c in np.unique(pcell[warm]):
        m_all = warm & (pcell == c)
        m_k = m_all & kept
        if m_all.sum() < 100 or m_k.sum() < 30:
            continue
        truth_q = float(np.quantile(A.duration[m_all], 0.95))
        est_q = _wquantile(A.duration[m_k], w[m_k], 0.95)
        pooled.append(abs(est_q - truth_q) / truth_q)
    out["p95_err_pooled"] = _mean(pooled)
    out["p95_pooled_within5"] = float(np.mean(np.array(pooled) <= within)) if pooled else math.nan
    out["p95_cells"] = len(errs)
    out["p95_err_mean"] = _mean(errs)
    out["p95_err_median"] = float(np.median(errs)) if errs else math.nan
    out["p95_err_max"] = float(np.max(errs)) if errs else math.nan
    out["p95_within5"] = float(np.mean(np.array(errs) <= within)) if errs else math.nan
    out["p95_err_window"] = _mean(errs_w)
    out["p95_err_lowvol"] = _mean(errs_low)
    out["p95_err_highvol"] = _mean(errs_high)

    # reweighted P95 error per service (headline): pooled over routes and periods >= 1 ------
    svc_err: dict[str, float] = {}
    for i, svc in enumerate(A.services):
        m_all = warm & (A.svc == i)
        m_k = m_all & kept
        if m_all.sum() < 100 or m_k.sum() < 30:
            continue
        truth_q = float(np.quantile(A.duration[m_all], 0.95))
        est_q = _wquantile(A.duration[m_k], w[m_k], 0.95)
        svc_err[svc] = abs(est_q - truth_q) / truth_q
    vals = list(svc_err.values())
    out["p95_err_svc_mean"] = _mean(vals)
    out["p95_err_svc_max"] = float(np.max(vals)) if vals else math.nan
    out["p95_svc_within5"] = float(np.mean(np.array(vals) <= within)) if vals else math.nan
    out["p95_err_root"] = svc_err.get(A.root_service, math.nan) if A.root_service else math.nan
    out["p95_err_svc"] = " ".join(f"{s}:{e:.3f}" for s, e in sorted(svc_err.items()))
    out["p95_svc_n"] = len(vals)

    # holes ------------------------------------------------------------------------
    full_items = ~is_rec
    has_children = np.array([bool(corpus.spans[i].children) for i in res.kept_idx], dtype=bool) & full_items
    out["holes_total"] = int(res.kept_holes[full_items].sum()) if len(res.kept_holes) else 0
    out["kept_with_holes_frac"] = float((res.kept_holes[has_children] > 0).mean()) if has_children.any() else math.nan
    out["holes_per_kept_parent"] = float(res.kept_holes[has_children].mean()) if has_children.any() else math.nan

    # SLO-violating requests (late-everywhere) ----------------------------------------
    viol = A.in_viol_request & nonroot & window
    if viol.any():
        tr = A.trace_code[viol]
        kv = full[viol].astype(float)
        uniq, inv = np.unique(tr, return_inverse=True)
        tot = np.bincount(inv)
        kk = np.bincount(inv, weights=kv)
        out["viol_requests"] = int(len(uniq))
        out["viol_coverage"] = float((kk / tot).mean())
        out["viol_requests_frac"] = _safe_div(len(uniq), (A.is_root & window).sum())
    else:
        out["viol_requests"] = 0
        out["viol_coverage"] = math.nan
        out["viol_requests_frac"] = 0.0

    # sampler diagnostics: budget, keeps by reason, T drift ---------------------------------
    Bs = [d["B"] for d in res.diagnostics if "B" in d]
    out["budget_mean"] = _mean(Bs)
    out["budget_last"] = float(Bs[-1]) if Bs else math.nan
    by = {}
    for d in res.diagnostics:
        for k, v in d.get("by_reason", {}).items():
            by[k] = by.get(k, 0) + v
    tot = sum(by.values())
    out["kept_by_reason"] = " ".join(f"{k}:{v / tot:.0%}" for k, v in sorted(by.items())) if tot else ""
    nn = [d["n_need"] for d in res.diagnostics if "n_need" in d]
    out["n_need_eff"] = float(nn[-1]) if nn else math.nan
    credit_items = np.isin(reasons, ["credit", "reserve"]) & kw
    out["credit_share"] = _safe_div(credit_items.sum(), kept_nr.sum())
    out["debt_total"] = int(sum(d.get("debt", 0) for d in res.diagnostics))
    # collector cost: buffering and decision latency (periods >= 1) ----------------------
    warm_d = [d for d in res.diagnostics if d.get("period", 0) >= 1]
    peaks = [d.get("buffer_peak", 0) for d in warm_d]
    out["buffer_peak_spans"] = float(max(peaks)) if peaks else 0.0
    means = [d.get("buffer_mean", 0.0) for d in warm_d]
    out["buffer_mean_spans"] = _mean(means) if means else 0.0
    pb = [d.get("buffer_peak_bytes", 0) for d in warm_d]
    out["buffer_peak_bytes"] = float(max(pb)) if pb else 0.0
    lats = [l for d in warm_d for l in d.get("latencies", [])]
    out["decision_latency_mean"] = _mean(lats) if lats else 0.0
    out["decision_latency_p95"] = float(np.quantile(lats, 0.95)) if lats else 0.0
    out["decision_latency_max"] = float(max(lats)) if lats else 0.0
    out["evicted_traces"] = int(sum(d.get("evicted", 0) for d in res.diagnostics))
    out["cap_dropped_traces"] = int(sum(d.get("cap_dropped_traces", 0) for d in res.diagnostics))
    fz: dict[str, int] = {}
    for d in res.diagnostics:
        for svc in d.get("frozen", []):
            fz[svc] = fz.get(svc, 0) + 1
    out["frozen_by_svc"] = " ".join(f"{s}:{v}" for s, v in sorted(fz.items(), key=lambda kv: -kv[1])[:8])
    # the trailing partial period is excluded, as in the volume metrics: with a
    # handful of spans every service is under N_need and the tables are not meaningful
    partial_last = res.periods[-1].period if (len(res.periods) > 1 and pers and pers[-1] is not res.periods[-1]) else None
    diags = [d for d in res.diagnostics if d.get("T_ratio") and d["period"] != partial_last]
    per_f = corpus.period_of(t_fault) if fault else None
    pre = [abs(r - 1.0) for d in diags if d["period"] >= 1 and (per_f is None or d["period"] < per_f)
           for r in d["T_ratio"].values()]
    out["t_drift_pre"] = float(np.max(pre)) if pre else math.nan
    out["frozen_svc_periods"] = int(sum(len(d.get("frozen", [])) for d in res.diagnostics))
    if fault and per_f is not None:
        in_fault = [d for d in diags if d["period"] >= per_f]
        cul = [d["T_ratio"][fault.culprit] - 1.0 for d in in_fault if fault.culprit in d["T_ratio"]]
        out["t_drift_culprit"] = float(np.max(cul)) if cul else math.nan
        out["t_drift_culprit_mean"] = _mean(cul)
        hv = [abs(r - 1.0) for d in in_fault for svc, r in d["T_ratio"].items() if svc not in fault.services]
        out["t_drift_healthy"] = float(np.max(hv)) if hv else math.nan
        out["t_drift_healthy_mean"] = _mean(hv)
    else:
        for k in ("t_drift_culprit", "t_drift_culprit_mean", "t_drift_healthy", "t_drift_healthy_mean"):
            out[k] = math.nan
    return out
