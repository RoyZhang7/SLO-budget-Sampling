"""Metrics of one cluster run against the shadow's ground truth: the cluster version
of `slo_budget_sim.metrics.compute` (same names where the meaning is the same).

Differences from the simulator, forced by the span model or the data:
- truth, outlier share, precision, recall, P95, attributability and holes are over
  SERVER spans; client spans are the RPC edges of the caller and have no T.  The
  tree for children / holes / attributability is the server-level tree (a server
  span's parent is its nearest server ancestor, i.e. through the client span).
- the oracle T is calibrated on the run's own healthy window of the shadow;
  own time is exact (duration minus the union of the direct children).
- volume and cap periods come from the central's cap gate (wall-clock periods,
  those starting at least one period after t0 and ending by t1); span-level
  periods are k = floor((end - t0) / period) as in the simulator.
- decision / admission time is the cap gate's `slobudget.admit_ns`.
- evidence uses duration > (1 + margin) T_oracle (Stage 3 protocol); truth stays
  duration > T_oracle as in the simulator.
- Stage 2d: the own-time truth is own > b_oracle (the oracle's own-time share) or an error,
  as the simulator's truth_own; own time subtracts the CLIENT children only.  `_own` keys use
  it.  The single-culprit truth counts an outlier keep in the fault window as right only when
  it is a culprit span (`outlier_precision_culprit_window`).  Caller keeps: outlier keeps on
  the culprit's server ancestors (the services the own-time trigger is meant to spare).
- Stage 3: the node-level truth (noisy neighbour: the unconfined spinner slows every service on
  the culprit's node) counts an outlier keep in the fault window as right when its service runs
  only on the culprit's node (`outlier_precision_node_window`).  A service with pods on several
  nodes (wrk2api, a DaemonSet) is not on the node: meta.json's `placement` names one of its pods
  only, so `placement_nodes` (every node) decides, and runs without it treat wrk2api as everywhere.
- drop reasons of unkept spans are not logged on the cluster: chosen-family arms
  split a lost outlier into not_flagged (duration <= (1 + m) x the table in force
  in its period, or no table yet) and refused (budget, pool, N_need or cap);
  every other arm's losses are "arm".
"""
from __future__ import annotations

import math

import numpy as np

from .oracle import lookup_arrays, route_level_table
from .spans import CLIENT, SERVER, KeptSet, SpanTable, own_client

CHOSEN_FAMILY = ("chosen", "threshold", "keep-parent")
FLAG_REASONS = ("outlier", "escalate")
STRUGGLING = 0.02           # of a service's spans in a period
EVIDENCE_FLAG_REASONS = ("outlier", "escalate", "parent")


MULTI_NODE = ("wrk2api",)   # DaemonSet services: one pod per node


def node_services(meta: dict, node: str | None) -> list[str]:
    """Services whose pods all run on `node` (none for node None)."""
    if not node:
        return []
    nodes = meta.get("placement_nodes")
    if nodes:
        return sorted(s for s, ns in nodes.items() if list(ns) == [node])
    return sorted(s for s, h in (meta.get("placement") or {}).items() if h == node and s not in MULTI_NODE)


def _safe_div(a, b) -> float:
    return float(a) / float(b) if b else math.nan


def _mean(xs) -> float:
    return float(np.mean(xs)) if len(xs) else math.nan


def _wquantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    order = np.argsort(values)
    v, w = values[order], weights[order]
    cw = np.cumsum(w)
    k = int(np.searchsorted(cw, q * cw[-1], side="left"))
    return float(v[min(k, len(v) - 1)])


def _fmt_top(pairs, k=5, fmt="{:.2f}") -> str:
    return " ".join(f"{a}:{fmt.format(b)}" for a, b in pairs[:k])


def compute(meta: dict, t: SpanTable, kept: KeptSet, kidx: np.ndarray, cal, status: dict | None,
            gate: list[dict], cpu: dict, buffers: dict, recon: dict | None, ws=None, cal_run=None, run_wait=None) -> dict:
    P = float(meta["period_s"])
    t0, t1 = float(meta["t0"]), float(meta["t1"])
    margin = float(meta.get("margin", 0.1))
    arm = meta["arm"]
    ap = meta.get("arm_params") or {}
    v_star = float(meta["v_star"])
    cap = float(meta.get("cap", 1.25 * v_star))
    slo_s = {r: ms / 1000.0 for r, ms in (meta.get("slo_ms") or {"compose_post": 23.4}).items()}
    n = t.n
    out: dict = {k: meta.get(k) for k in ("run_id", "arm", "scenario", "volume_frac", "seed", "v_star", "cap")}
    out["arm"] = meta.get("arm_label") or arm       # a variant of an arm is an arm of its own in the tables
    out["margin"] = margin

    S = t.kind == SERVER
    start_s = t.start / 1e9
    end_s = t.end / 1e9
    dur = (t.end - t.start) / 1e9
    own = t.own / 1e9

    # population: traces whose first span starts in [t0, t1] -------------------------------
    tmin = np.full(t.n_traces, np.inf)
    np.minimum.at(tmin, t.trace, start_s)
    pop = ((tmin >= t0 - 0.5) & (tmin <= t1))[t.trace]
    SP = S & pop
    nonroot = ~t.is_root
    root_idx_of_trace = np.full(t.n_traces, -1, dtype=np.int64)
    roots = np.flatnonzero(t.is_root)
    root_idx_of_trace[t.trace[roots]] = roots
    rt = root_idx_of_trace[t.trace]

    # kept set ----------------------------------------------------------------------------
    kept_mask = np.zeros(n, dtype=bool)
    ok = kidx >= 0
    kept_mask[kidx[ok]] = True
    out["export_spans"] = int(len(kidx))
    out["export_traces"] = int(kept.traces)
    out["export_unmatched"] = int((~ok).sum())
    out["export_duplicates"] = int(kept.duplicates)
    out["export_synthetic_skipped"] = int(kept.synthetic_skipped)
    out["shadow_spans"] = int(n)
    out["shadow_traces"] = int(t.n_traces)
    out["shadow_duplicates"] = int(t.duplicates)
    out["shadow_missing_parent"] = int(t.missing_parent)
    out["pop_spans"] = int(pop.sum())
    out["pop_traces"] = int(np.unique(t.trace[pop]).size) if pop.any() else 0
    p_tag = np.full(n, np.nan)
    p_tag[kidx[ok]] = kept.p[ok]
    admit_ns = np.full(n, -1, dtype=np.int64)
    admit_ns[kidx[ok]] = kept.admit[ok]
    rcodes: dict[str, int] = {}
    rc = np.full(n, -1, dtype=np.int32)
    kr = np.fromiter((rcodes.setdefault(r, len(rcodes)) for r in kept.reason), dtype=np.int32, count=len(kept.reason))
    rc[kidx[ok]] = kr[ok]
    rname = {v: k for k, v in rcodes.items()}

    def reason_in(names) -> np.ndarray:
        codes = [rcodes[x] for x in names if x in rcodes]
        return np.isin(rc, codes) if codes else np.zeros(n, dtype=bool)

    has_admit = admit_ns >= 0
    out["admit_stamped_frac"] = _safe_div((kept_mask & has_admit).sum(), kept_mask.sum())
    admit_s = np.where(has_admit, admit_ns / 1e9, end_s)          # fallback: the span's end

    # inclusion probabilities -> weights ------------------------------------------------------
    p_eff = p_tag.copy()
    root_dur_of_trace = np.full(t.n_traces, np.nan)
    root_dur_of_trace[t.trace[roots]] = dur[roots]
    root_slo_of_trace = np.full(t.n_traces, np.inf)
    root_slo_of_trace[t.trace[roots]] = [slo_s.get(t.routes[t.route[i]], math.inf) for i in roots]
    err_in_trace = np.bincount(t.trace, weights=t.err.astype(float), minlength=t.n_traces) > 0
    slow_trace = (root_dur_of_trace > root_slo_of_trace) | err_in_trace
    if arm == "head" and ap.get("head_r"):
        p_eff = np.where(np.isnan(p_eff), float(ap["head_r"]), p_eff)
    elif arm == "tail" and ap.get("tail_p"):
        p_eff = np.where(np.isnan(p_eff), np.where(slow_trace[t.trace], 1.0, float(ap["tail_p"])), p_eff)
    bad = np.isnan(p_eff) | (p_eff <= 0)
    w = np.where(bad, 1.0, 1.0 / np.where(bad, 1.0, p_eff))

    # periods, warm, window -----------------------------------------------------------------
    k = np.floor((end_s - t0) / P).astype(np.int64)
    warm = k >= 1
    fault = meta.get("fault")
    burst = meta.get("burst")
    if fault:
        w0, w1 = float(fault["start"]), float(fault["end"])
    elif burst:
        w0, w1 = float(burst["start"]), float(burst["end"])
    else:
        w0, w1 = -math.inf, math.inf
    window = pop & (end_s >= w0) & (end_s <= w1)
    out["window"] = [None if not math.isfinite(w0) else round(w0 - t0, 1), None if not math.isfinite(w1) else round(w1 - t0, 1)]

    # oracle T and truth -------------------------------------------------------------------------
    T, B = lookup_arrays(t, cal)
    has_T = ~np.isnan(T)
    truth = (has_T & (dur > T)) | t.err
    over_m = (has_T & (dur > (1.0 + margin) * T)) | t.err
    # Stage 2d own-time truth: own time (CLIENT children subtracted) over the oracle share b
    other_kids = (t.pidx >= 0) & (t.kind != CLIENT) & (t.kind[np.maximum(t.pidx, 0)] == SERVER)
    own2d = (own_client(t) if other_kids.any() else t.own) / 1e9
    has_B = ~np.isnan(B)
    truth_own = (has_B & (own2d > B)) | t.err
    # the common truth of all arms: the within-service latency on the estimated kernel receive time, over its own oracle share (ws)
    ws_x, ws_b = ws if ws is not None else (np.full(n, np.nan), np.full(n, np.nan))
    truth_ws = (np.isfinite(ws_b) & (ws_x > ws_b)) | t.err
    over_ws = (np.isfinite(ws_b) & (ws_x > (1.0 + margin) * ws_b)) | t.err
    rr = np.bincount(t.route[t.is_root & pop], minlength=max(1, len(t.routes)))
    main_route = t.routes[int(np.argmax(rr))] if rr.sum() else (next(iter(slo_s)) if slo_s else "compose_post")
    out["route"] = main_route
    oracle_rl = route_level_table(cal_run or cal, main_route)       # the drift compares it with the central's table: the central's call graph and measure
    out["oracle_T_ms"] = {s: round(v * 1000, 3) for s, v in sorted(oracle_rl.items())}

    # volume and spend (cap gate) ---------------------------------------------------------------
    gw = [g for g in gate if int(g["period"]) * P >= t0 + P - 1e-6 and (int(g["period"]) + 1) * P <= t1 + 1e-6]
    if gw:
        adm = np.array([float(g.get("admitted", 0)) for g in gw])
        rec = np.array([float(g.get("received", 0)) for g in gw])
        cd = {int(g["period"]): int(g.get("cap_dropped", 0)) for g in gw}
        bts = [float(g["bytes_admitted"]) for g in gw if g.get("bytes_admitted") is not None]
        out["volume_source"] = "gate"
    else:                                   # no gate records: count the kept spans by admission period
        pa = np.floor(admit_s[kept_mask & pop] / P).astype(np.int64)
        lo, hi = int(math.ceil((t0 + P) / P)), int(math.floor(t1 / P)) - 1
        pa = pa[(pa >= lo) & (pa <= hi)]
        adm = np.bincount(pa - lo, minlength=max(0, hi - lo + 1)).astype(float) if hi >= lo else np.zeros(0)
        rec = adm.copy()
        cd, bts = {}, []
        out["volume_source"] = "export"
    pre = rec.copy()
    hist = (status or {}).get("history") or []
    hist_by = {int(h["period"]): h for h in hist if "period" in h}
    if arm in CHOSEN_FAMILY and hist_by:
        for h in hist_by.values():
            per = int(h["period"])
            if per * P >= t0 + P - 1e-6 and (per + 1) * P <= t1 + 1e-6 and int(h.get("cap_dropped", 0)) > 0:
                cd[per] = cd.get(per, 0) + int(h["cap_dropped"])
        if gw:
            pre = np.array([max(float(g.get("received", 0)), float(hist_by.get(int(g["period"]), {}).get("received", 0))) for g in gw])
    out["gate_periods"] = int(len(adm))
    out["kept_per_period"] = float(adm.mean()) if len(adm) else math.nan
    out["vol_ratio"] = _safe_div(adm.mean(), v_star) if len(adm) else math.nan
    out["min_vol_ratio"] = _safe_div(adm.min(), v_star) if len(adm) else math.nan
    out["max_vol_ratio"] = _safe_div(adm.max(), v_star) if len(adm) else math.nan
    out["bytes_per_period"] = float(np.mean(bts)) if bts else math.nan
    out["bytes_per_span"] = _safe_div(np.sum(bts), np.sum(adm)) if bts else math.nan
    out["over_cap_periods"] = int(sum(1 for v in cd.values() if v > 0))
    out["cap_dropped_total"] = int(sum(cd.values()))
    out["max_pre_ratio"] = _safe_div(pre.max(), v_star) if len(pre) else math.nan

    kp = kept_mask & pop
    kw = kp & warm
    per_req = np.bincount(t.trace[kw], minlength=t.n_traces)[np.unique(t.trace[pop])] if pop.any() else np.zeros(0)
    out["max_per_request"] = float(per_req.max()) if len(per_req) else math.nan
    out["p99_per_request"] = float(np.quantile(per_req, 0.99)) if len(per_req) else math.nan
    placement = meta.get("placement") or {}
    hosts = sorted(set(placement.values()) | {"?"})                 # "?": a service the placement does not name
    hcode = {h: i for i, h in enumerate(hosts)}
    svc_host = np.array([hcode[placement.get(s, "?")] for s in t.services], dtype=np.int64) if t.services else np.zeros(0, np.int64)
    out["unplaced_services"] = sorted(s for s in t.services if s not in placement)
    k_adm = np.floor((admit_s - t0) / P).astype(np.int64)
    if kw.any():
        kh = svc_host[t.svc[kw]]
        ka = np.maximum(k_adm[kw], 0)
        cell = kh * (int(ka.max()) + 1) + ka
        out["max_per_host_period"] = float(np.bincount(cell).max())
    else:
        out["max_per_host_period"] = math.nan
    out["max_host_ratio"] = _safe_div(out["max_per_host_period"], v_star)
    out["kept_total"] = int(kp.sum())
    out["root_share"] = _safe_div((kw & t.is_root).sum(), kw.sum())
    out["roots_kept_frac"] = _safe_div((kw & t.is_root).sum(), (pop & t.is_root & warm).sum())
    out["kept_by_kind"] = {"server": int((kp & S).sum()), "client": int((kp & (t.kind == 3)).sum()),
                           "other": int((kp & ~S & (t.kind != 3)).sum())}

    # outlier share, precision, recall ---------------------------------------------------------
    truth_nr = truth & nonroot & SP
    out["truth_outliers"] = int(truth_nr.sum())
    out["recall"] = _safe_div((truth_nr & kept_mask).sum(), truth_nr.sum())
    tw = truth_nr & window
    out["recall_window"] = _safe_div((tw & kept_mask).sum(), tw.sum())
    kept_nr = kept_mask & nonroot & SP & warm
    out["outlier_share"] = _safe_div((kept_nr & truth).sum(), kept_nr.sum())
    out["outlier_share_own"] = _safe_div((kept_nr & truth_own).sum(), kept_nr.sum())
    truth_own_nr = truth_own & nonroot & SP
    out["truth_outliers_own"] = int(truth_own_nr.sum())
    out["recall_own"] = _safe_div((truth_own_nr & kept_mask).sum(), truth_own_nr.sum())
    kept_nr_w = kept_nr & window
    out["outlier_share_window"] = _safe_div((kept_nr_w & truth).sum(), kept_nr_w.sum())
    tow = truth_own_nr & window
    out["recall_own_window"] = _safe_div((tow & kept_mask).sum(), tow.sum())
    if ws is not None:
        truth_ws_nr = truth_ws & nonroot & SP
        tws = truth_ws_nr & window
        out["truth_outliers_ws"] = int(truth_ws_nr.sum())
        out["recall_ws"] = _safe_div((truth_ws_nr & kept_mask).sum(), truth_ws_nr.sum())
        out["recall_ws_window"] = _safe_div((tws & kept_mask).sum(), tws.sum())
        out["outlier_share_ws"] = _safe_div((kept_nr & truth_ws).sum(), kept_nr.sum())
        out["outlier_share_ws_window"] = _safe_div((kept_nr_w & truth_ws).sum(), kept_nr_w.sum())
        out["kept_outliers_ws"] = int((kept_mask & truth_ws_nr & warm).sum())
        hs = SP & warm & (end_s < w0) if math.isfinite(w0) else SP & warm       # before the fault or the burst
        out["truth_rate_healthy_ws"] = _safe_div((truth_ws_nr & hs).sum(), (hs & nonroot).sum())
        out["truth_rate_healthy_own"] = _safe_div((truth_own & nonroot & hs).sum(), (hs & nonroot).sum())

        # per period of the window: the services with a true outlier, and those of them with one kept
        def services_by_period(m: np.ndarray) -> dict[int, int]:
            cells = np.unique(k[m] * max(1, len(t.services)) + t.svc[m])
            return dict(zip(*(x.tolist() for x in np.unique(cells // max(1, len(t.services)), return_counts=True))))
        has_outlier, has_kept = services_by_period(tws & warm), services_by_period(tws & warm & kept_mask)
        if has_outlier:
            out["ws_services_with_an_outlier_per_period"] = _mean(list(has_outlier.values()))
            out["ws_services_covered_per_period"] = _mean([has_kept.get(p, 0) for p in has_outlier])
            out["ws_services_covered"] = _mean([has_kept.get(p, 0) / c for p, c in has_outlier.items()])
        # the same of the struggling ones: at least STRUGGLING of the service's spans of the period are true outliers
        ns = max(1, len(t.services))
        in_w = SP & nonroot & window & warm
        cells_n = ((int(k[in_w].max()) + 1) if in_w.any() else 1) * ns
        per_cell = [np.bincount(k[m] * ns + t.svc[m], minlength=cells_n).reshape(-1, ns) for m in (in_w, in_w & truth_ws, in_w & truth_ws & kept_mask)]
        there = per_cell[0].sum(axis=1) > 0                     # the periods of the window
        struggling = (per_cell[1] > 0) & (per_cell[1] >= STRUGGLING * per_cell[0])
        n_struggling, n_kept = struggling.sum(axis=1)[there], (struggling & (per_cell[2] > 0)).sum(axis=1)[there]
        if there.any():
            some = n_struggling > 0
            out["ws_struggling_per_period"] = float(n_struggling.mean())                # the periods without one count
            out["ws_struggling_covered_per_period"] = float(n_kept.mean())
            out["ws_periods_with_struggling"] = float(some.mean())
            out["ws_struggling_covered"] = float((n_kept[some] / n_struggling[some]).mean()) if some.any() else math.nan
        # where the volume of the window went
        kept_w = kp & warm & window
        is_culprit = t.svc == t.services.index(fault["culprit"]) if fault and fault.get("culprit") in t.services else np.zeros(n, dtype=bool)
        out["ws_kept_window"] = int(kept_w.sum())
        out["ws_kept_culprit_outliers"] = _safe_div((kept_w & truth_ws_nr & is_culprit).sum(), kept_w.sum()) if is_culprit.any() else math.nan
        out["ws_kept_other_outliers"] = _safe_div((kept_w & truth_ws_nr & ~is_culprit).sum(), kept_w.sum())
        out["ws_kept_ordinary"] = _safe_div((kept_w & ~truth_ws_nr).sum(), kept_w.sum())       # client spans and roots too
        if is_culprit.any():
            periods = max(1.0, (w1 - w0) / P)
            out["ws_culprit_kept_per_period"] = float((kept_w & truth_ws_nr & is_culprit).sum()) / periods
            out["ws_culprit_outliers_per_period"] = float((tws & is_culprit).sum()) / periods
            if arm in CHOSEN_FAMILY:        # next to N_need, the kept outliers of a service and period that are kept whole
                out["ws_culprit_kept_as_outlier_per_period"] = float((kept_w & truth_ws_nr & is_culprit & reason_in(FLAG_REASONS)).sum()) / periods
                need = [float(h["n_need"]) for p, h in hist_by.items() if p * P >= w0 and (p + 1) * P <= w1 and h.get("n_need") is not None]
                out["n_need_window"] = math.inf if any(x >= 1e300 for x in need) else _mean(need)
    n_svc_codes = max(1, len(t.services))
    nk = int(k.max()) + 1 if n and k.max() >= 0 else 1
    kc = np.clip(k, 0, nk - 1)
    sp_cell = t.svc.astype(np.int64) * nk + kc
    tot_c = np.bincount(sp_cell[tw], minlength=n_svc_codes * nk)
    kept_c = np.bincount(sp_cell[tw & kept_mask], minlength=n_svc_codes * nk)
    ev = tot_c >= 15
    out["event_recall"] = float((kept_c[ev] >= 3).mean()) if ev.any() else math.nan
    is_flag = kept_mask & reason_in(FLAG_REASONS) & warm & pop
    out["outlier_keeps"] = int(is_flag.sum())
    if is_flag.any():
        out["outlier_precision"] = float(truth[is_flag].mean())
        fw = is_flag & window
        out["outlier_precision_window"] = float(truth[fw].mean()) if fw.any() else math.nan
        out["outlier_precision_server"] = float(truth[is_flag & S].mean()) if (is_flag & S).any() else math.nan
        out["outlier_precision_own"] = float(truth_own[is_flag].mean())
        out["outlier_precision_own_window"] = float(truth_own[fw].mean()) if fw.any() else math.nan
        if ws is not None:
            out["outlier_precision_ws"] = float(truth_ws[is_flag].mean())
            out["outlier_precision_ws_window"] = float(truth_ws[fw].mean()) if fw.any() else math.nan
    else:
        out["outlier_precision"] = out["outlier_precision_window"] = out["outlier_precision_server"] = math.nan
        out["outlier_precision_own"] = out["outlier_precision_own_window"] = math.nan
    bucket = np.array([c.split("|", 1)[1] if "|" in c else "any" for c in t.classes])[t.cls] if t.classes else np.array([])
    for b in sorted(set(bucket[pop].tolist())) if pop.any() else []:
        if b == "any":
            continue
        mb = tw & (bucket == b)
        out[f"recall_{b}"] = _safe_div((mb & kept_mask).sum(), mb.sum())

    # server-level tree: holes ----------------------------------------------------------------------
    sparent = t.sparent
    child_mask = SP & (sparent >= 0)
    nkids = np.bincount(sparent[child_mask], minlength=n)
    holes = np.bincount(sparent[child_mask & ~kept_mask], minlength=n)

    # context sufficiency ---------------------------------------------------------------------------
    ko = np.flatnonzero(kept_mask & truth_nr & warm)
    out["kept_outliers"] = int(len(ko))
    if len(ko):
        r = rt[ko]
        root_kept = np.zeros(len(ko), dtype=bool)
        root_kept[r >= 0] = kept_mask[r[r >= 0]]
        out["ctx_root_kept"] = float(root_kept.mean())
        out["outlier_no_root"] = float(1.0 - root_kept.mean())
        out["ctx_sufficient"] = float((root_kept & (holes[ko] == 0)).mean())
        out["ctx_no_holes"] = float((holes[ko] == 0).mean())
    else:
        out["ctx_root_kept"] = out["outlier_no_root"] = out["ctx_sufficient"] = out["ctx_no_holes"] = math.nan

    # orphans (as the simulator): kept non-root server spans whose server parent was not kept, in
    # the traces that hold a kept truth outlier, per kept truth outlier; and the share of kept
    # truth outliers that are orphans themselves.  Duration truth, and the own-time truth (_own).
    knr = np.flatnonzero(kept_mask & SP & (sparent >= 0))
    orphan = np.zeros(n, dtype=bool)
    orphan[knr] = ~kept_mask[sparent[knr]]

    def _orphans(kidx_):
        if not len(kidx_):
            return math.nan, math.nan
        tr_with = np.zeros(max(1, t.n_traces), dtype=bool)
        tr_with[t.trace[kidx_]] = True
        return _safe_div((orphan & tr_with[t.trace]).sum(), len(kidx_)), float(orphan[kidx_].mean())

    out["orphans_per_kept_outlier"], out["outlier_orphan_share"] = _orphans(ko)
    ko_own = np.flatnonzero(kept_mask & truth_own_nr & warm)
    out["kept_outliers_own"] = int(len(ko_own))
    out["orphans_per_kept_outlier_own"], out["outlier_orphan_share_own"] = _orphans(ko_own)

    # the table in force per span (chosen family): history[P_abs - 1].table_ms ---------------------------
    T_used = np.full(n, np.nan)
    if arm in CHOSEN_FAMILY and hist_by:
        pabs = np.floor(end_s / P).astype(np.int64) - 1
        cand = np.flatnonzero(SP)
        keys = pabs[cand] * n_svc_codes + t.svc[cand]
        uk, inv = np.unique(keys, return_inverse=True)
        vals = np.full(len(uk), np.nan)
        for j, kk in enumerate(uk):
            h = hist_by.get(int(kk // n_svc_codes))
            if h and h.get("table_ms"):
                v = h["table_ms"].get(t.services[int(kk % n_svc_codes)])
                if v is not None:
                    vals[j] = float(v) / 1000.0
        T_used[cand] = vals[inv]

    def drop_group(idx: np.ndarray) -> np.ndarray:
        if arm in CHOSEN_FAMILY:
            tu = T_used[idx]
            d = dur[idx] if run_wait is None else dur[idx] + run_wait[idx] / 1e9        # the run's table is on its own measure
            nf = np.isnan(tu) | (d <= (1.0 + margin) * np.nan_to_num(tu, nan=np.inf))
            return np.where(nf, "not_flagged", "refused")
        return np.full(len(idx), "arm", dtype=object)

    lost = np.flatnonzero(truth_nr & ~kept_mask & warm)
    out["lost_outliers"] = int(len(lost))
    g = drop_group(lost) if len(lost) else np.array([])
    for name in ("not_flagged", "refused", "arm"):
        out[f"ref_{name}"] = _safe_div(int((g == name).sum()), len(lost))
    if len(lost):
        u, c = np.unique(g, return_counts=True)
        out["refusal_split"] = " ".join(f"{a}:{b / len(lost):.0%}" for a, b in sorted(zip(u, c), key=lambda x: -x[1]))
    else:
        out["refusal_split"] = ""

    # cold-start period ---------------------------------------------------------------------------
    p0 = (k == 0) & pop
    kept_nr0 = kept_mask & nonroot & SP & p0
    out["outlier_share_p0"] = _safe_div((kept_nr0 & truth).sum(), kept_nr0.sum())
    f0 = kept_mask & reason_in(FLAG_REASONS) & p0
    out["outlier_precision_p0"] = float(truth[f0].mean()) if f0.any() else math.nan
    tr0 = truth_nr & p0
    out["truth_outliers_p0"] = int(tr0.sum())
    out["recall_p0"] = _safe_div((tr0 & kept_mask).sum(), tr0.sum())

    # attributability on the server-level tree ----------------------------------------------------
    corder = np.flatnonzero(child_mask)
    corder = corder[np.argsort(sparent[corder], kind="stable")]
    cstart = np.searchsorted(sparent[corder], np.arange(n))
    cend = np.searchsorted(sparent[corder], np.arange(n), side="right")
    n_a = n_b = n_c = 0
    loss_groups: dict[str, int] = {}
    for i in ko:
        kids = corder[cstart[i]:cend[i]]
        a = bool(not np.isnan(B[i]) and own[i] > B[i])
        b = bool((kept_mask[kids] & truth[kids]).any()) if len(kids) else False
        dropped_out = kids[~kept_mask[kids] & truth[kids]] if len(kids) else kids
        if a:
            n_a += 1
        elif b:
            n_b += 1
        elif len(dropped_out) == 0:
            n_c += 1
        else:
            excess = dur[dropped_out] - np.nan_to_num(T[dropped_out], nan=0.0)
            worst = dropped_out[int(np.argmax(excess))]
            grp = str(drop_group(np.array([worst]))[0])
            loss_groups[grp] = loss_groups.get(grp, 0) + 1
    nko = len(ko)
    out["attrib_explained"] = _safe_div(n_a + n_b + n_c, nko)
    out["attrib_own"] = _safe_div(n_a, nko)
    out["attrib_child"] = _safe_div(n_b, nko)
    out["attrib_clean"] = _safe_div(n_c, nko)
    n_loss = sum(loss_groups.values())
    out["attrib_loss"] = _safe_div(n_loss, nko)
    for name in ("not_flagged", "refused", "arm"):
        out[f"loss_{name}"] = _safe_div(loss_groups.get(name, 0), nko)
    out["loss_split"] = " ".join(f"{a}:{b / n_loss:.0%}" for a, b in sorted(loss_groups.items(), key=lambda x: -x[1])) if n_loss else ""

    # services, depth, fault shape on complete data ----------------------------------------------------
    svc_present = np.bincount(t.svc[SP], minlength=n_svc_codes) > 0
    svc_ids = np.flatnonzero(svc_present)
    n_services = int(len(svc_ids))
    dsum = np.bincount(t.svc[SP], weights=t.depth[SP].astype(float), minlength=n_svc_codes)
    dcnt = np.bincount(t.svc[SP], minlength=n_svc_codes)
    mean_depth = np.divide(dsum, dcnt, out=np.full(n_svc_codes, -1.0), where=dcnt > 0)
    out["mean_depth"] = {t.services[i]: round(float(mean_depth[i]), 2) for i in svc_ids}
    sw = SP & window
    vol_w = np.bincount(t.svc[sw], minlength=n_svc_codes).astype(float)
    tr_cnt = np.bincount(t.svc[truth & sw], minlength=n_svc_codes).astype(float)
    t_rate = np.divide(tr_cnt, vol_w, out=np.zeros(n_svc_codes), where=vol_w > 0)[svc_ids]
    if n_services:
        t_med = float(np.median(t_rate))
        out["truth_rate_median"] = t_med
        out["truth_rate_max"] = float(t_rate.max())
        out["truth_rate_spread"] = _safe_div(t_rate.max(), t_med) if t_med > 0 else math.nan
        top = sorted(((t.services[svc_ids[j]], float(t_rate[j])) for j in range(n_services)), key=lambda x: -x[1])
        out["truth_rate_by_svc"] = _fmt_top([x for x in top if x[1] > 0])
        if t_rate.max() > 0:
            cands = np.flatnonzero(t_rate >= 0.5 * t_rate.max())
            out["truth_systemic"] = bool(len(cands) >= 0.6 * n_services)
            out["truth_culprit_est"] = "none" if out["truth_systemic"] else t.services[svc_ids[cands[np.argmax(mean_depth[svc_ids[cands]])]]]
        else:
            out["truth_systemic"], out["truth_culprit_est"] = False, "none"
    out["n_services"] = n_services

    # evidence and culprit --------------------------------------------------------------------------------
    scode = {s: i for i, s in enumerate(t.services)}
    if fault:
        culprit = fault.get("culprit")
        targets = [scode[s] for s in (fault.get("targets") or []) if s in scode]
        t_f = float(fault["start"])
        ev_base = kept_mask & over_m & window & nonroot & SP
        m_any = ev_base & np.isin(t.svc, targets) if targets else np.zeros(n, dtype=bool)
        out["evidence_any_s"] = float(admit_s[m_any].min() - t_f) if m_any.any() else math.nan
        if culprit in scode:
            cc = scode[culprit]
            cm = tw & (t.svc == cc)
            out["recall_culprit"] = _safe_div((cm & kept_mask).sum(), cm.sum())
            me = ev_base & (t.svc == cc)
            out["evidence_s"] = float(admit_s[me].min() - t_f) if me.any() else math.nan
            mf = me & reason_in(EVIDENCE_FLAG_REASONS)
            out["evidence_flagged_s"] = float(admit_s[mf].min() - t_f) if mf.any() else math.nan
            if ws is not None:
                cw = truth_ws & window & nonroot & SP & (t.svc == cc)
                out["truth_outliers_ws_culprit_window"] = int(cw.sum())
                out["recall_culprit_ws"] = _safe_div((cw & kept_mask).sum(), cw.sum())
                mw = kept_mask & over_ws & window & nonroot & SP & (t.svc == cc)
                out["evidence_ws_s"] = float(admit_s[mw].min() - t_f) if mw.any() else math.nan
                # requests over the SLO that end in the fault: is a kept outlier of the culprit in them?
                late = window & t.is_root & SP & (dur > np.array([slo_s.get(r, math.inf) for r in t.routes])[t.route])
                has = np.zeros(max(1, t.n_traces), dtype=bool)
                has[t.trace[cw & kept_mask]] = True
                out["over_slo_requests_window"] = int(late.sum())
                out["over_slo_with_culprit_outlier_kept"] = _safe_div(has[t.trace[late]].sum(), late.sum())
                tro = np.zeros(max(1, t.n_traces), dtype=bool)
                tro[t.trace[cw]] = True
                out["over_slo_with_culprit_truth_ws"] = _safe_div(tro[t.trace[late]].sum(), late.sum())
        else:
            out["recall_culprit"] = out["evidence_s"] = out["evidence_flagged_s"] = math.nan
        # Stage 2d: single-culprit truth and the keeps spent on the culprit's callers
        fwk = is_flag & window
        n_per = max(1.0, (w1 - w0) / P)
        out["outlier_keeps_window"] = int(fwk.sum())
        out["outlier_keeps_by_service_window"] = {
            t.services[i]: int(c) for i, c in enumerate(np.bincount(t.svc[fwk], minlength=n_svc_codes)) if c}
        if culprit in scode:
            cc = scode[culprit]
            cul = t.svc == cc
            anc: set[int] = set()
            pp = t.sparent[np.flatnonzero(SP & cul)]
            for _ in range(64):
                pp = pp[pp >= 0]
                if not len(pp):
                    break
                anc.update(np.unique(t.svc[pp]).tolist())
                pp = t.sparent[pp]
            anc.discard(cc)
            callers = np.isin(t.svc, sorted(anc)) if anc else np.zeros(n, dtype=bool)
            ck = fwk & callers
            out["culprit_callers"] = sorted(t.services[i] for i in anc)
            out["outlier_precision_culprit_window"] = _safe_div((fwk & cul).sum(), fwk.sum())
            cwin = SP & window & cul                     # every culprit span in the fault window
            out["faulty_span_coverage"] = _safe_div((cwin & kept_mask).sum(), cwin.sum())
            out["faulty_spans_window"] = int(cwin.sum())
            out["outlier_keeps_culprit_window"] = int((fwk & cul).sum())
            out["outlier_keeps_callers_window"] = int(ck.sum())
            out["outlier_keeps_callers"] = int((is_flag & callers).sum())
            out["outlier_keeps_callers_window_frac"] = _safe_div(ck.sum(), fwk.sum())
            out["culprit_outlier_keeps_per_period"] = float((fwk & cul).sum()) / n_per
            out["caller_outlier_keeps_per_period"] = float(ck.sum()) / n_per
            out["caller_keeps_truth_own"] = _safe_div((ck & truth_own).sum(), ck.sum())
            out["caller_keeps_truth_dur"] = _safe_div((ck & truth).sum(), ck.sum())
            out["caller_truth_own_window"] = int((callers & window & SP & truth_own).sum())
            out["caller_truth_dur_window"] = int((callers & window & SP & truth).sum())
            # Stage 3 node-level truth: an outlier keep in the window is right on any service of the culprit's node
            cnode = placement.get(culprit)
            node_ids = [scode[s] for s in node_services(meta, cnode) if s in scode]
            on_node = np.isin(t.svc, node_ids)
            out["culprit_node"] = cnode or "none"
            out["culprit_node_services"] = sorted(t.services[i] for i in node_ids)
            out["outlier_precision_node_window"] = _safe_div((fwk & on_node).sum(), fwk.sum())
            out["outlier_keeps_node_window"] = int((fwk & on_node).sum())
            nw = tow & on_node
            out["recall_node_own_window"] = _safe_div((nw & kept_mask).sum(), nw.sum())
        else:
            for key in ("outlier_precision_culprit_window", "culprit_outlier_keeps_per_period", "faulty_span_coverage",
                        "caller_outlier_keeps_per_period", "outlier_keeps_callers_window_frac",
                        "caller_keeps_truth_own", "caller_keeps_truth_dur", "outlier_precision_node_window",
                        "recall_node_own_window"):
                out[key] = math.nan
            out["culprit_callers"] = []
            out["culprit_node"], out["culprit_node_services"] = "none", []
        km = kept_mask & tw
        cnt = np.bincount(t.svc[km], weights=w[km], minlength=n_svc_codes)
        rate = np.divide(cnt, vol_w, out=np.zeros(n_svc_codes), where=vol_w > 0)
        est, confirm = None, math.nan
        if n_services and rate[svc_ids].max() > 0:
            rs = rate[svc_ids]
            cands = svc_ids[np.flatnonzero(rs >= 0.5 * rs.max())]
            if len(cands) >= 0.6 * n_services:
                est = None
            else:
                e = int(cands[np.argmax(mean_depth[cands])])
                est = t.services[e]
                em = km & (t.svc == e)
                confirm = float((holes[em] == 0).mean()) if em.any() else math.nan
        out["culprit_truth"] = culprit if culprit else "none"
        out["culprit_est"] = est if est else "none"
        out["identified"] = bool(est == culprit) if culprit else bool(est is None)
        out["culprit_confirmable"] = confirm
        order = np.argsort(-rate[svc_ids])[:3]
        out["spend_top"] = " ".join(f"{t.services[svc_ids[j]]}:{rate[svc_ids[j]]:.3f}" for j in order if rate[svc_ids[j]] > 0)
    else:
        for key in ("evidence_any_s", "recall_culprit", "evidence_s", "evidence_flagged_s", "culprit_confirmable"):
            out[key] = math.nan
        out["culprit_truth"] = out["culprit_est"] = "none"
        out["identified"] = True
        out["spend_top"] = ""

    # reweighted P95 per service (headline) and per (service, period) cell ---------------------------------
    svc_err: dict[str, float] = {}
    for i in svc_ids:
        m_all = warm & SP & (t.svc == i)
        m_k = m_all & kept_mask
        if m_all.sum() < 100 or m_k.sum() < 30:
            continue
        truth_q = float(np.quantile(dur[m_all], 0.95))
        est_q = _wquantile(dur[m_k], w[m_k], 0.95)
        svc_err[t.services[i]] = abs(est_q - truth_q) / truth_q
    vals = list(svc_err.values())
    out["p95_err_svc_mean"] = _mean(vals)
    out["p95_err_svc_max"] = float(np.max(vals)) if vals else math.nan
    out["p95_svc_within5"] = float(np.mean(np.array(vals) <= 0.05)) if vals else math.nan
    root_svc = None
    if t.is_root.any():
        rsv = np.bincount(t.svc[t.is_root & pop], minlength=n_svc_codes)
        root_svc = t.services[int(np.argmax(rsv))] if rsv.sum() else None
    out["p95_err_root"] = svc_err.get(root_svc, math.nan) if root_svc else math.nan
    out["p95_err_svc"] = " ".join(f"{s}:{e:.3f}" for s, e in sorted(svc_err.items()))
    out["p95_svc_n"] = len(vals)
    errs = []
    cell = t.svc.astype(np.int64) * nk + kc
    base = warm & SP
    cidx = np.flatnonzero(base)
    cidx = cidx[np.argsort(cell[cidx], kind="stable")]
    cs = cell[cidx]
    bnd = np.flatnonzero(np.diff(cs)) + 1
    for a_, b_ in zip(np.concatenate([[0], bnd]), np.concatenate([bnd, [len(cs)]])):
        if b_ - a_ < 30:
            continue
        idx_all = cidx[a_:b_]
        idx_k = idx_all[kept_mask[idx_all]]
        if len(idx_k) < 10:
            continue
        tq = float(np.quantile(dur[idx_all], 0.95))
        errs.append(abs(_wquantile(dur[idx_k], w[idx_k], 0.95) - tq) / tq)
    out["p95_cells"] = len(errs)
    out["p95_err_mean"] = _mean(errs)
    out["p95_err_median"] = float(np.median(errs)) if errs else math.nan
    out["p95_err_max"] = float(np.max(errs)) if errs else math.nan
    out["p95_within5"] = float(np.mean(np.array(errs) <= 0.05)) if errs else math.nan

    # holes ----------------------------------------------------------------------------------------------
    kps = kept_mask & SP
    out["holes_total"] = int(holes[kps].sum())
    hp = kps & (nkids > 0)
    out["kept_with_holes_frac"] = float((holes[hp] > 0).mean()) if hp.any() else math.nan
    out["holes_per_kept_parent"] = float(holes[hp].mean()) if hp.any() else math.nan

    # SLO-violating requests -------------------------------------------------------------------------------
    viol_tr = root_dur_of_trace > root_slo_of_trace                # the root missed its SLO (errors not counted, as the simulator)
    viol = viol_tr[t.trace] & nonroot & window & SP
    if viol.any():
        uniq, inv = np.unique(t.trace[viol], return_inverse=True)
        tot = np.bincount(inv)
        kk_ = np.bincount(inv, weights=kept_mask[viol].astype(float))
        out["viol_requests"] = int(len(uniq))
        out["viol_coverage"] = float((kk_ / tot).mean())
        out["viol_requests_frac"] = _safe_div(len(uniq), (t.is_root & window).sum())
    else:
        out["viol_requests"], out["viol_coverage"], out["viol_requests_frac"] = 0, math.nan, 0.0

    # request latency (complete roots) -------------------------------------------------------------------
    rmask = t.is_root & pop
    if rmask.any():
        rd = dur[rmask] * 1000
        out["latency_p50_ms"] = float(np.quantile(rd, 0.5))
        out["latency_p99_ms"] = float(np.quantile(rd, 0.99))
        out["latency_mean_ms"] = float(rd.mean())
        rslo = np.array([slo_s.get(t.routes[t.route[i]], math.inf) for i in np.flatnonzero(rmask)])
        out["over_slo_frac"] = float((dur[rmask] > rslo).mean())
        out["requests"] = int(rmask.sum())
        if fault:
            fw_ = rmask & (end_s >= w0) & (end_s <= w1)
            out["over_slo_frac_window"] = float((dur[fw_] > np.array([slo_s.get(t.routes[t.route[i]], math.inf) for i in np.flatnonzero(fw_)])).mean()) if fw_.any() else math.nan

    # chosen-family diagnostics from the central's history -----------------------------------------------
    kb: dict[str, int] = {}
    for code, cnt in zip(*np.unique(rc[kp & (rc >= 0)], return_counts=True)):
        kb[rname[int(code)] or "-"] = int(cnt)
    tot_k = sum(kb.values())
    out["kept_by_reason"] = " ".join(f"{a}:{b / tot_k:.0%}" for a, b in sorted(kb.items())) if tot_k else ""
    in_run = [hist_by[p] for p in sorted(hist_by) if p * P >= t0 + P - 1e-6 and (p + 1) * P <= t1 + 1e-6]
    if in_run:
        Bs = [float(h["b"]) for h in in_run if "b" in h]
        out["budget_mean"] = _mean(Bs)
        out["budget_last"] = float(Bs[-1]) if Bs else math.nan
        out["n_need_eff"] = float(in_run[-1].get("n_need", math.nan))
        if out["n_need_eff"] >= 1e300:  # +Inf on the config channel (the threshold ablation)
            out["n_need_eff"] = math.inf
        fz: dict[str, int] = {}
        entries: dict[str, int] = {}
        prev: set = set()
        for h in in_run:
            cur = set(h.get("frozen") or [])
            for s in cur:
                fz[s] = fz.get(s, 0) + 1
            for s in cur - prev:
                entries[s] = entries.get(s, 0) + 1
            prev = cur
        out["frozen_svc_periods"] = int(sum(fz.values()))
        out["frozen_by_svc"] = " ".join(f"{a}:{b}" for a, b in sorted(fz.items(), key=lambda x: -x[1])[:8])
        out["frozen_entries"] = int(sum(entries.values()))
        out["frozen_entries_by_svc"] = " ".join(f"{a}:{b}" for a, b in sorted(entries.items(), key=lambda x: -x[1])[:8])
        orl = oracle_rl
        pre_d, cul_d, hv_d = [], [], []
        targets_names = set((fault or {}).get("targets") or [])
        for h in in_run:
            tab = h.get("table_ms") or {}
            ps = int(h["period"]) * P
            for s, v in tab.items():
                To = orl.get(s)
                if not To:
                    continue
                r_ = float(v) / 1000.0 / To
                if fault and ps < float(fault["start"]) or not fault:
                    pre_d.append(abs(r_ - 1.0))
                elif fault and float(fault["start"]) <= ps <= float(fault["end"]):
                    if s == fault.get("culprit"):
                        cul_d.append(r_ - 1.0)
                    elif s not in targets_names:
                        hv_d.append(abs(r_ - 1.0))
        out["t_drift_pre"] = float(np.max(pre_d)) if pre_d else math.nan
        out["t_drift_culprit"] = float(np.max(cul_d)) if cul_d else math.nan
        out["t_drift_culprit_mean"] = _mean(cul_d)
        out["t_drift_healthy"] = float(np.max(hv_d)) if hv_d else math.nan
        out["t_drift_healthy_mean"] = _mean(hv_d)

    # collector cost: buffering, decision latency, agent CPU ------------------------------------------------
    out.update(buffers)
    if arm == "tail":
        ftr = np.full(t.n_traces, np.inf)
        np.minimum.at(ftr, t.trace[pop], start_s[pop])
        ka = kp & has_admit
        if ka.any():
            adm_tr = np.full(t.n_traces, np.inf)
            np.minimum.at(adm_tr, t.trace[ka], admit_s[ka])
            tl = np.unique(t.trace[ka])
            lat = adm_tr[tl] - ftr[tl]
        else:
            lat = np.zeros(0)
    else:
        ka = kp & has_admit
        lat = admit_s[ka] - end_s[ka]
    lat = lat[np.isfinite(lat)]
    out["decision_latency_mean"] = float(lat.mean()) if len(lat) else math.nan
    out["decision_latency_p95"] = float(np.quantile(lat, 0.95)) if len(lat) else math.nan
    out["decision_latency_max"] = float(lat.max()) if len(lat) else math.nan
    cpu_s = sum(v["cpu_s"] for v in cpu.values())
    cpu_n = sum(v["spans"] for v in cpu.values())
    out["agent_cpu_us_per_span"] = _safe_div(cpu_s * 1e6, cpu_n)
    out["agent_cpu_by_node"] = {nd: {"cpu_s": round(v["cpu_s"], 3), "spans": int(v["spans"]),
                                     "us_per_span": round(_safe_div(v["cpu_s"] * 1e6, v["spans"]), 3)} for nd, v in sorted(cpu.items())}

    # reconstruction coverage --------------------------------------------------------------------------------
    if recon:
        for key in ("orphans", "orphans_frac_of_kept", "placeable_by_known_ids", "placeable_frac_of_orphans",
                    "placeable_frac_of_kept", "placed_with_hole_1", "hole_1_frac_of_orphans", "hole_1_frac_of_placeable",
                    "hole_hist_known_ids", "hole_hist_full", "placeholders", "placeholders_per_real_span",
                    "synthetic_id_share", "traces_one_tree_before_frac", "traces_one_tree_after_known_ids_only_frac",
                    "traces_one_tree_after_frac", "traces_with_real_root"):
            if key in recon:
                out[f"recon_{key}"] = recon[key]
    return out
