"""Stage 3 root-cause ranking: RCAEval's TraceRCA and MicroRank (tools/eval/rcaeval) on one run's
spans, scored against the fault's culprit.

Windows: the normal period is the 3 minutes before the fault (never before t0 + 30 s, so 90 s for
cold_start), the anomalous period the fault window; only spans that end inside [normal start,
fault end] are passed, with inject_time = the fault start.  Times are the shadow's (us).

Variants (one row per span, the columns RCAEval reads):
  shadow    every span of the 100 % shadow, true parents: the full data
  recon     the arm's kept spans, each parent the nearest kept ancestor in the reconstructed export
            (tools/recon's placeholders are contracted: they carry no measurement, and an operation
            with no normal-period span makes both methods fail)
  true      the arm's kept spans, each parent the nearest kept ancestor in the shadow's tree
  true_ref  `true` in the fault window, with the shadow's spans before the fault as the normal set
            (the same reference for every arm)

Scoring: the ranked operations map to services in order of first appearance.  saturated, cascade
and cold_start hit at k when the culprit is among the first k services; noisy_neighbour when one
of the first k runs only on the culprit's node (metrics.node_services: wrk2api, on every node,
does not count).  rr = 1 / the rank of the first correct service (0 when none is ranked).  Two
mappings of an operation to a service: RCAEval's (`rank`, its benchmark's service_ranks: the
service that emitted the span) and `rank_callee`, where a client operation counts for the service
it calls (from the shadow's client -> server edges): a client span into the culprit carries the
culprit's latency, so the two operations tie under the 3-sigma rule and the methods' tie order
(first appearance; rows are in start-time order, so the caller's client span comes first) would
otherwise decide.
"""
from __future__ import annotations

import contextlib
import io
import math
import time

import numpy as np

from .metrics import node_services
from .spans import CLIENT, SERVER, SpanTable, _id64, _iter_export_traces

SCORED = ("saturated", "cascade", "cold_start", "noisy_neighbour")
VARIANTS = ("shadow", "recon", "true", "true_ref")
METHODS = ("tracerca", "microrank")


def windows(meta: dict) -> tuple[float, float, float]:
    """(normal start, fault start, fault end), epoch seconds: the normal period skips the first
    30 s of the load when the fault starts later than 60 s in."""
    f = meta["fault"]
    t0, fs, fe = float(meta["t0"]), float(f["start"]), float(f["end"])
    skip = 30.0 if fs - t0 > 60.0 else 0.0
    return max(t0 + skip, fs - 180.0), fs, fe


def truth(meta: dict) -> tuple[str, list[str]]:
    """("culprit" | "node", the services that count as a hit)."""
    culprit = meta["fault"]["culprit"]
    if meta["scenario"] == "noisy_neighbour":
        return "node", node_services(meta, (meta.get("placement") or {}).get(culprit))
    return "culprit", [culprit]


def kept_ancestor(pidx: np.ndarray, kept: np.ndarray, rows: np.ndarray) -> np.ndarray:
    """For span indices `rows`: the nearest ancestor in the tree `pidx` that is kept (-1: none)."""
    anc = pidx[rows].copy()
    for _ in range(64):
        m = (anc >= 0) & ~kept[np.maximum(anc, 0)]
        if not m.any():
            break
        anc[m] = pidx[anc[m]]
    anc[(anc >= 0) & ~kept[np.maximum(anc, 0)]] = -1
    return anc


def recon_parents(path: str) -> dict[int, int]:
    """The reconstructed export's parent span id of every span id (0: none), placeholders included."""
    parent: dict[int, int] = {}
    for tr in _iter_export_traces(path):
        for sp in tr.get("spans") or ():
            par = 0
            for ref in sp.get("references") or ():
                if ref.get("refType") == "CHILD_OF":
                    par = _id64(ref.get("spanID", ""))
                    break
            parent[_id64(sp.get("spanID", ""))] = par
    return parent


def recon_ancestor(t: SpanTable, rows: np.ndarray, kept_ids: set[int], parent: dict[int, int]) -> np.ndarray:
    """For kept span indices `rows`: the nearest kept ancestor in the reconstruction (-1: none),
    walking up through its placeholders."""
    pos = {int(s): i for i, s in enumerate(t.sid)}
    out = np.full(len(rows), -1, dtype=np.int64)
    for k, i in enumerate(rows):
        p = parent.get(int(t.sid[i]), 0)
        for _ in range(64):
            if p == 0:
                break
            if p in kept_ids:
                out[k] = pos.get(p, -1)
                break
            if p not in parent:          # a missing span the reconstruction did not place
                break
            p = parent[p]
    return out


def frame(t: SpanTable, rows: np.ndarray, par: np.ndarray):
    """The RCAEval span frame of span indices `rows` with parent indices `par` (-1: none), in start
    time order (the methods break score ties by order of appearance)."""
    import pandas as pd
    order = np.argsort(t.start[rows], kind="stable")
    rows, par = rows[order], par[order]
    svc = np.array(t.services, dtype=object)[t.svc[rows]]
    if t.names:
        name = np.array(t.names, dtype=object)[t.name[rows]]
    else:
        name = np.full(len(rows), "", dtype=object)
    sid = t.sid.astype(np.uint64)
    parent = np.where(par >= 0, sid[np.maximum(par, 0)].astype(str), None)
    return pd.DataFrame({
        "traceID": t.trace[rows].astype(str), "spanID": sid[rows].astype(str), "parentSpanID": parent,
        "serviceName": svc, "methodName": np.full(len(rows), np.nan, dtype=object), "operationName": name,
        "startTime": t.start[rows] // 1000, "duration": (t.end[rows] - t.start[rows]) // 1000})


def set_stats(t: SpanTable, rows: np.ndarray) -> dict:
    n_tr = int(len(np.unique(t.trace[rows]))) if len(rows) else 0
    return {"spans": int(len(rows)), "traces": n_tr, "spans_per_trace": float(len(rows)) / n_tr if n_tr else math.nan}


def _rank(ranks: list[str], op_service: dict[str, str], hit_services: list[str]) -> tuple[list[str], int | None]:
    services: list[str] = []
    for op in ranks:
        s = op_service.get(op)
        if s is not None and s not in services:
            services.append(s)
    return services, next((k + 1 for k, s in enumerate(services) if s in hit_services), None)


def score(ranks: list[str], op_service: dict[str, str], hit_services: list[str], op_callee: dict[str, str] | None = None) -> dict:
    services, rank = _rank(ranks, op_service, hit_services)
    out = {"services": services[:10], "rank": rank, "hit1": rank == 1, "hit3": rank is not None and rank <= 3,
           "rr": 1.0 / rank if rank else 0.0, "ops": ranks[:12]}
    if op_callee is not None:
        services_c, rank_c = _rank(ranks, {**op_service, **op_callee}, hit_services)
        out.update({"services_callee": services_c[:10], "rank_callee": rank_c, "hit1_callee": rank_c == 1,
                    "hit3_callee": rank_c is not None and rank_c <= 3, "rr_callee": 1.0 / rank_c if rank_c else 0.0})
    return out


def client_callees(t: SpanTable) -> dict[str, str]:
    """RCAEval operation name of every client operation -> the service it calls (most frequent)."""
    if not t.names:
        return {}
    child = np.flatnonzero((t.kind == SERVER) & (t.pidx >= 0))
    par = t.pidx[child]
    m = t.kind[par] == CLIENT
    child, par = child[m], par[m]
    counts: dict[tuple[str, str], int] = {}
    for op_code, svc_code, callee_code in zip(t.name[par], t.svc[par], t.svc[child]):
        key = (f"{t.services[svc_code]}_{t.names[op_code]}", t.services[callee_code])
        counts[key] = counts.get(key, 0) + 1
    best: dict[str, tuple[int, str]] = {}
    for (op, callee), c in counts.items():
        if op not in best or c > best[op][0]:
            best[op] = (c, callee)
    return {op: callee for op, (_, callee) in best.items()}


def run_method(name: str, df, inject_us: int, hit_services: list[str], op_callee: dict[str, str] | None = None) -> dict:
    from rcaeval.microrank import microrank
    from rcaeval.tracerca import tracerca
    fn = {"tracerca": tracerca, "microrank": microrank}[name]
    op_service = dict(zip(df["serviceName"] + "_" + df["operationName"], df["serviceName"]))
    t0 = time.time()
    sink = io.StringIO()
    try:
        with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            res = fn(df.copy(), inject_time=inject_us)
    except Exception as e:   # noqa: BLE001 - a method that cannot rank the input is a result
        return {"error": f"{type(e).__name__}: {e}"[:300], "rank": None, "hit1": False, "hit3": False, "rr": 0.0,
                "seconds": round(time.time() - t0, 2)}
    out = score(list(res.get("ranks") or []), op_service, hit_services, op_callee)
    out["seconds"] = round(time.time() - t0, 2)
    return out


def evaluate_run(meta: dict, t: SpanTable, kidx: np.ndarray, recon: dict | None,
                 variants=VARIANTS, methods=METHODS) -> dict:
    """RCA results of one run; `kidx` = the kept spans' shadow indices (spans.join), `recon` =
    recon_parents of the reconstructed export (None: the recon variant is skipped)."""
    w0, fs, fe = windows(meta)
    kind, hits = truth(meta)
    end_s = t.end / 1e9
    in_win = (end_s >= w0) & (end_s <= fe)
    normal = end_s < fs
    kept = np.zeros(t.n, dtype=bool)
    kept[kidx[kidx >= 0]] = True
    out = {"run_id": meta["run_id"], "arm": meta["arm"], "scenario": meta["scenario"], "volume_frac": meta["volume_frac"],
           "seed": meta["seed"], "truth": {"kind": kind, "services": hits},
           "window_s": {"normal": [w0, fs], "fault": [fs, fe]}, "variants": {}}
    inject_us = int(fs * 1e6)
    op_callee = client_callees(t)
    all_rows = np.flatnonzero(in_win)
    kept_rows = np.flatnonzero(in_win & kept)
    for v in variants:
        if v == "shadow":
            rows, par = all_rows, t.pidx[all_rows]
        elif v == "true":
            rows, par = kept_rows, kept_ancestor(t.pidx, kept, kept_rows)
        elif v == "true_ref":
            ref = np.flatnonzero(in_win & normal)
            fault_kept = np.flatnonzero(in_win & ~normal & kept)
            rows = np.concatenate([ref, fault_kept])
            par = np.concatenate([t.pidx[ref], kept_ancestor(t.pidx, kept, fault_kept)])
        elif v == "recon":
            if recon is None:
                out["variants"][v] = {"error": "no reconstructed export"}
                continue
            kept_ids = {int(s) for s in t.sid[kidx[kidx >= 0]]}
            rows, par = kept_rows, recon_ancestor(t, kept_rows, kept_ids, recon)
        else:
            raise ValueError(v)
        res = {"normal_set": set_stats(t, rows[normal[rows]]), "fault_set": set_stats(t, rows[~normal[rows]])}
        if v in ("true", "recon"):
            res["parent_kept"] = float(np.mean(par >= 0)) if len(par) else math.nan
        df = frame(t, rows, par)
        for m in methods:
            res[m] = run_method(m, df, inject_us, hits, op_callee)
        out["variants"][v] = res
    return out
