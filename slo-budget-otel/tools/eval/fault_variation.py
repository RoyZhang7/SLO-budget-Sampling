#!/usr/bin/env python3
"""Per-run facts for the fault-variation analysis (read-only: the shadow, meta.json, wrk.log).

    ~/slo-budget-sim/.venv/bin/python3 tools/eval/fault_variation.py --runs-dir /storage/royno7/stage3/runs \
        --out DIR RUN_ID [RUN_ID ...]      # node-0, beside the campaign: behind a wrk gate (it parses each shadow)

For each run: the share of requests (root spans) over the SLO before the fault ([t0 + 10 s, fault
start)) and during it ([fault start, fault end)), by the root's START time; the pre-fault request
latency p50 / p95 / p99 (the root span's duration, server side); wrk2's own whole-run percentiles
(wrk2 keeps no time series, so its pre-fault percentiles do not exist); the culprit service's
own-time p95 before and during the fault and its own-time threshold b (the oracle's own-time share of
the run's healthy window); where the load's bursts fell in the burner's quota period (burst_phase.json of
burst_phase.py); the run's start time; and the pod placement.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import zoneinfo

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from sloeval import runinfo  # noqa: E402
from sloeval.evaluate import healthy_mask  # noqa: E402
from sloeval.oracle import build_oracle, import_sim, load_callgraph, lookup_arrays  # noqa: E402
from sloeval.spans import SERVER, read_shadow  # noqa: E402

SKIP_S = 10.0        # wrk2 calibrates in its first seconds; requests before t0 + SKIP_S are left out of "pre"


def find(run_dir, *names):
    for nm in names:
        p = os.path.join(run_dir, nm)
        if os.path.exists(p):
            return p
    return None


def wrk_stats(path: str) -> dict:
    out = {}
    if not os.path.exists(path):
        return out
    text = open(path, errors="replace").read()
    m = re.search(r"Requests/sec:\s+([0-9.]+)", text)
    if m:
        out["rps"] = float(m.group(1))
    u = {"us": 1e-3, "ms": 1.0, "s": 1000.0}
    for pct in ("50.000%", "75.000%", "90.000%", "99.000%", "99.900%"):
        m = re.search(r"^\s*" + re.escape(pct) + r"\s+([0-9.]+)(us|ms|s)\s*$", text, re.M)
        if m:
            out[f"p{pct[:2] if pct != '99.900%' else '99.9'}"] = float(m.group(1)) * u[m.group(2)]
    spec = []
    for m in re.finditer(r"^\s*([0-9.]+)\s+([0-9.]+)\s+\d+\s+[0-9.]+(?:\s*)$", text, re.M):
        v, q = float(m.group(1)), float(m.group(2))
        if 0.0 <= q <= 1.0:
            spec.append((q, v))
    out["p95"] = None
    if spec:
        spec.sort()
        qs = np.array([q for q, _ in spec])
        vs = np.array([v for _, v in spec])
        out["p95"] = float(np.interp(0.95, qs, vs))
    return out


def one(run_dir: str, sim: str, callgraph: str) -> dict:
    meta = runinfo.load_meta(run_dir)
    f = meta.get("fault") or {}
    out = {"run_id": meta["run_id"], "arm": meta["arm"], "scenario": meta["scenario"], "seed": meta["seed"],
           "volume_frac": meta["volume_frac"], "slo_ms": float(next(iter(meta["slo_ms"].values()))),
           "fault_mode": f.get("mode"), "fault_level": f.get("level"), "fault_limit": f.get("limit"),
           "culprit": f.get("culprit"), "culprit_node": None, "wrk": wrk_stats(os.path.join(run_dir, "wrk.log"))}
    t0, t1 = float(meta["t0"]), float(meta["t1"])
    cst = zoneinfo.ZoneInfo("America/Chicago")
    est = zoneinfo.ZoneInfo("America/New_York")
    out["start_cst"] = dt.datetime.fromtimestamp(t0, cst).strftime("%Y-%m-%d %H:%M:%S")
    out["start_est"] = dt.datetime.fromtimestamp(t0, est).strftime("%Y-%m-%d %H:%M:%S")
    out["start_hour_est"] = (dt.datetime.fromtimestamp(t0, est).hour + dt.datetime.fromtimestamp(t0, est).minute / 60.0)
    out["t0"] = t0
    out["epoch_hours"] = t0 / 3600.0
    pl = meta.get("placement") or {}
    pln = meta.get("placement_nodes") or {}
    if out["culprit"]:
        cn = pl.get(out["culprit"])
        out["culprit_node"] = cn
        out["culprit_coresident"] = sorted(s for s, n in pl.items() if n == cn and s != out["culprit"])
        out["culprit_nodes"] = pln.get(out["culprit"])
    out["placement"] = pl
    out["burst_after_refresh_ms"] = (runinfo.load_json(os.path.join(run_dir, "burst_phase.json")) or {}).get("after_refresh_ms")
    fs = float(f["start"]) if f else None
    fe = float(f["end"]) if f else None
    out["fault_start_s"] = (fs - t0) if fs else None
    out["fault_dur_s"] = (fe - fs) if fs else None

    t = read_shadow(find(run_dir, "shadow.jsonl.zst", "shadow.jsonl.gz", "shadow.jsonl"))
    start_s = t.start / 1e9
    end_s = t.end / 1e9
    dur_ms = (t.end - t.start) / 1e6
    own_ms = t.own / 1e6
    roots = t.is_root
    slo = out["slo_ms"]
    out["n_spans"], out["n_traces"] = int(t.n), int(t.n_traces)

    pre = roots & (start_s >= t0 + SKIP_S) & ((start_s < fs) if fs else (start_s < t1))
    dur_mask = roots & (start_s >= fs) & (start_s < fe) if fs else np.zeros(t.n, dtype=bool)
    out["n_pre"], out["n_during"] = int(pre.sum()), int(dur_mask.sum())
    if pre.any():
        d = dur_ms[pre]
        out["share_pre"] = float((d > slo).mean())
        out["pre_p50_ms"], out["pre_p95_ms"], out["pre_p99_ms"] = (float(np.quantile(d, q)) for q in (0.5, 0.95, 0.99))
        out["pre_mean_ms"] = float(d.mean())
    if dur_mask.any():
        d = dur_ms[dur_mask]
        out["share_during"] = float((d > slo).mean())
        out["during_p50_ms"], out["during_p95_ms"], out["during_p99_ms"] = (float(np.quantile(d, q)) for q in (0.5, 0.95, 0.99))
        out["diff_share"] = out["share_during"] - out["share_pre"]
    # root-latency by end in the fault window (the eval's over_slo_frac_window definition) for a cross-check
    if fs:
        end_mask = roots & (end_s >= fs) & (end_s <= fe)
        if end_mask.any():
            out["share_during_by_end"] = float((dur_ms[end_mask] > slo).mean())

    # the culprit service's own time and its threshold ----------------------------------------------
    if out["culprit"] and out["culprit"] in t.services:
        code = t.services.index(out["culprit"])
        Calibrator_graph = import_sim(sim)
        CallGraph, Calibrator = Calibrator_graph
        graph = load_callgraph(callgraph, CallGraph)
        routes_slo = {r: ms / 1000.0 for r, ms in meta["slo_ms"].items()}
        cal = build_oracle(t, healthy_mask(meta, t), graph, Calibrator, routes_slo)
        T, B = lookup_arrays(t, cal)
        m = (t.kind == SERVER) & (t.svc == code)
        pm = m & (start_s >= t0 + SKIP_S) & ((start_s < fs) if fs else True)
        dm = m & (start_s >= fs) & (start_s < fe) if fs else np.zeros(t.n, dtype=bool)
        if pm.any():
            o = own_ms[pm]
            out["culprit_own_p50_pre_ms"], out["culprit_own_p95_pre_ms"] = float(np.quantile(o, 0.5)), float(np.quantile(o, 0.95))
        if dm.any():
            o = own_ms[dm]
            out["culprit_own_p50_during_ms"], out["culprit_own_p95_during_ms"] = float(np.quantile(o, 0.5)), float(np.quantile(o, 0.95))
            b = B[dm]
            b = b[np.isfinite(b)]
            thr = float(np.median(b)) * 1e3 if len(b) else float("nan")
            out["culprit_own_thr_ms"] = thr
            out["culprit_own_p95_over_thr"] = out["culprit_own_p95_during_ms"] / thr if thr == thr and thr > 0 else None
            Tm = T[dm]
            Tm = Tm[np.isfinite(Tm)]
            out["culprit_T_ms"] = float(np.median(Tm)) * 1e3 if len(Tm) else None
            durc = dur_ms[dm]
            out["culprit_dur_p95_during_ms"] = float(np.quantile(durc, 0.95))
        if pm.any():
            durc = dur_ms[pm]
            out["culprit_dur_p95_pre_ms"] = float(np.quantile(durc, 0.95))
        out["culprit_n_spans_pre"] = int(pm.sum())
        out["culprit_n_spans_during"] = int(dm.sum())
    # cross-check with the published summary
    sm = runinfo.load_json(os.path.join(run_dir, "summary.json")) or {}
    out["summary_over_slo_frac_window"] = sm.get("over_slo_frac_window")
    out["summary_over_slo_frac"] = sm.get("over_slo_frac")
    out["summary_vol_ratio"] = sm.get("vol_ratio")
    out["summary_oracle_T_ms_culprit"] = (sm.get("oracle_T_ms") or {}).get(out["culprit"]) if out["culprit"] else None
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs", nargs="+")
    ap.add_argument("--runs-dir", default="/storage/royno7/stage3/runs")
    ap.add_argument("--sim", default="~/slo-budget-sim")
    ap.add_argument("--callgraph", default=os.path.join(HERE, "..", "..", "deploy", "callgraph.json"))
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    os.makedirs(os.path.expanduser(a.out), exist_ok=True)
    for r in a.runs:
        rd = os.path.join(a.runs_dir, r)
        of = os.path.join(os.path.expanduser(a.out), r + ".json")
        if os.path.exists(of):
            continue
        try:
            res = one(rd, os.path.expanduser(a.sim), os.path.expanduser(a.callgraph))
        except Exception as e:                                   # noqa: BLE001 - keep the batch going, report the run
            res = {"run_id": r, "error": f"{type(e).__name__}: {e}"}
        with open(of, "w", encoding="utf-8") as f:
            json.dump(res, f, indent=1)
        print(r, "ok" if "error" not in res else res["error"], flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
