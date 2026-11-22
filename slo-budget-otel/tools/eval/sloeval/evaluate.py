"""Read a run directory and compute its summary."""
from __future__ import annotations

import os
import subprocess
import time

try:
    import resource            # not on Windows (the tests run there)
except ImportError:            # pragma: no cover
    resource = None

import numpy as np

from . import runinfo
from .metrics import CHOSEN_FAMILY, compute
from .oracle import build_oracle, import_sim, load_callgraph
from .spans import SERVER, join, read_export, read_shadow


def find(run_dir: str, *names: str) -> str | None:
    for nm in names:
        p = os.path.join(run_dir, nm)
        if os.path.exists(p):
            return p
    return None


def healthy_mask(meta: dict, t) -> np.ndarray:
    """The oracle's calibration window: [t0, fault.start), [t0, burst.start), or the whole run."""
    t0, t1 = float(meta["t0"]), float(meta["t1"])
    end = meta.get("fault", None) and float(meta["fault"]["start"])
    if not end and meta.get("burst"):
        end = float(meta["burst"]["start"])
    if not end:
        end = t1
    s = t.start / 1e9
    e = t.end / 1e9
    return (s >= t0) & (e < end)


def collect_buffers(run_dir: str, meta: dict, spans_per_trace: float) -> dict:
    arm = meta["arm"]
    P = float(meta["period_s"])
    t0, t1 = float(meta["t0"]), float(meta["t1"])
    out = {"buffer_peak_spans": 0.0, "buffer_mean_spans": 0.0, "buffer_source": "none"}

    def per_period_sum(message: str, field: str) -> list[float]:
        acc: dict[int, float] = {}
        for recs in runinfo.agent_period_records(run_dir, message).values():
            for r in recs:
                per = int(r.get("period", -1))
                if per * P >= t0 and (per + 1) * P <= t1 + P:
                    acc[per] = acc.get(per, 0.0) + float(r.get(field, 0) or 0)
        return [acc[k] for k in sorted(acc)]

    if arm == "tail":
        peaks = []
        for ts, samples in runinfo.central_scrapes(run_dir):
            if t0 <= ts <= t1 + 30:
                v = [val for name, lab, val in samples if "traces_on_memory" in name]
                if v:
                    peaks.append(sum(v))
        if peaks:
            out.update(buffer_peak_traces=float(max(peaks)), buffer_mean_traces=float(np.mean(peaks)),
                       buffer_peak_spans=float(max(peaks)) * spans_per_trace,
                       buffer_mean_spans=float(np.mean(peaks)) * spans_per_trace, buffer_source="tail_sampling traces_on_memory x spans/trace")
    elif arm == "reservoir":
        v = per_period_sum("reservoir period", "peak_buffered")
        if v:
            out.update(buffer_peak_spans=float(max(v)), buffer_mean_spans=float(np.mean(v)), buffer_source="reservoir peak_buffered, summed over agents")
    elif arm in CHOSEN_FAMILY:
        v = per_period_sum("slobudget period", "roots_held")
        if v:
            out.update(buffer_peak_spans=float(max(v)), buffer_mean_spans=float(np.mean(v)),
                       buffer_source="roots held per period, summed over agents (upper bound on the concurrently held roots)")
    return out


def run_recon(run_dir: str, recon_bin: str | None, callgraph: str, timeout: int = 1800) -> dict | None:
    rep = find(run_dir, "recon-report.json", "recon-offline.json")
    if rep:
        return runinfo.load_json(rep)
    exp = find(run_dir, "export.json")
    if not (recon_bin and exp and os.path.exists(recon_bin)):
        return None
    rep = os.path.join(run_dir, "recon-report.json")
    cmd = [recon_bin, "-in", exp, "-out", os.path.join(run_dir, "export.recon.json"), "-callgraph", callgraph, "-report", rep]
    r = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=timeout, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"recon failed: {r.stderr[-500:]}")
    return runinfo.load_json(rep)


def hold_cost(run_dir: str, t, pop_tr) -> dict:
    """Stage 2d server_hold cost from the agents' period records: spans held, client spans that
    arrived after their parent's release (own time overestimated), mean hold, peaks."""
    recs = runinfo.agent_period_records(run_dir, "slobudget period")
    rows = [r for rs in recs.values() for r in rs if "server_held" in r]
    if not rows:
        return {"hold_server_held": None}
    held = sum(int(r.get("server_held", 0)) for r in rows)
    late = sum(int(r.get("server_late_clients", 0)) for r in rows)
    wsum = sum(float(r.get("server_hold_ms_mean", 0.0)) * int(r.get("server_held", 0)) for r in rows)
    clients = int(((t.kind == 3) & pop_tr[t.trace]).sum())
    return {"hold_server_held": held, "hold_late_clients": late,
            "hold_late_client_frac": late / clients if clients else None,
            "hold_ms_mean": wsum / held if held else None,
            "hold_peak_spans": max(int(r.get("server_held_peak", 0)) for r in rows),
            "hold_peak_bytes": max(int(r.get("server_held_bytes_peak", 0)) for r in rows)}


def evaluate(run_dir: str, sim_path: str, callgraph: str, recon_bin: str | None = None, do_recon: bool = True) -> dict:
    timing = {}
    tic = time.time()
    meta = runinfo.load_meta(run_dir)
    CallGraph, Calibrator = import_sim(sim_path)
    graph = load_callgraph(callgraph, CallGraph)
    shadow_path = find(run_dir, "shadow.jsonl.zst", "shadow.jsonl.gz", "shadow.jsonl")
    if not shadow_path:
        raise FileNotFoundError(f"{run_dir}: no shadow.jsonl[.zst|.gz]")
    t = read_shadow(shadow_path)
    timing["shadow_s"] = round(time.time() - tic, 1)
    tic = time.time()
    kept = read_export(os.path.join(run_dir, "export.json"))
    kidx = join(t, kept)
    timing["export_s"] = round(time.time() - tic, 1)
    tic = time.time()
    routes_slo = {r: ms / 1000.0 for r, ms in meta["slo_ms"].items()}
    cal = build_oracle(t, healthy_mask(meta, t), graph, Calibrator, routes_slo)
    timing["oracle_s"] = round(time.time() - tic, 1)
    tic = time.time()
    status = runinfo.load_status(run_dir)
    gate = runinfo.load_gate(run_dir)
    cpu = runinfo.agent_cpu(run_dir)
    s0, s1 = float(meta["t0"]), float(meta["t1"])
    st = t.start / 1e9
    tmin = np.full(t.n_traces, np.inf)
    np.minimum.at(tmin, t.trace, st)
    pop_tr = (tmin >= s0 - 0.5) & (tmin <= s1)
    spt = float(pop_tr[t.trace].sum()) / max(1, int(pop_tr.sum()))
    buffers = collect_buffers(run_dir, meta, spt)
    recon = None
    if do_recon:
        tr = time.time()
        recon = run_recon(run_dir, recon_bin, callgraph)
        timing["recon_s"] = round(time.time() - tr, 1)
    out = compute(meta, t, kept, kidx, cal, status, gate, cpu, buffers, recon)
    timing["metrics_s"] = round(time.time() - tic, 1)
    out["spans_per_trace"] = round(spt, 2)
    out.update(hold_cost(run_dir, t, pop_tr))
    out["oracle_healthy_server_spans"] = int((healthy_mask(meta, t) & (t.kind == SERVER)).sum())
    out["timing"] = timing
    out["peak_rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1) if resource else None
    return out
