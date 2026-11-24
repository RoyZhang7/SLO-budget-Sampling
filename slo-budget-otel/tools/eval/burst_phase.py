#!/usr/bin/env python3
"""Where the load's bursts fall in the burner's CPU-quota period, per burner run.

    ~/slo-budget-sim/.venv/bin/python3 tools/eval/burst_phase.py RUN_DIR [RUN_DIR ...]   # node-0, behind a wrk gate: it parses each shadow

wrk2's fixed pace sends the requests of all its connections within a few ms of every 100 ms, a whole
number of the burner's quota periods (10 ms), so a burst meets the same part of the period during the
whole fault and another one in the next run, and the share of requests over the SLO follows it.  Per
5-s window of the fault: the refresh = the mode of the culprit's server span starts modulo the period,
among the calls that waited over 1 ms; the burst = the circular mean of the calls' arrivals (client
span start plus the edge's healthy median gap, in the culprit's clock: the offset between the hosts'
clocks cancels).  The position is the burst's time after the last refresh.  Writes RUN_DIR/burst_phase.json
(`after_refresh_ms`: the circular mean over the windows, 0 to the period; `sd_ms` over the windows;
`concentration` of the calls within a window, 1 = all at one phase).  A run without a quota writes
nothing.  tables.py, fault_variation.py and rca_tables.py show it.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from sloeval import runinfo  # noqa: E402
from sloeval.evaluate import healthy_mask  # noqa: E402
from sloeval.spans import CLIENT, SERVER, read_shadow  # noqa: E402

WINDOW_S = 5.0
WAIT_MS = 1.0          # a call that waited longer ended its wait at a refresh
BIN_MS = 0.25
MIN_CALLS = 20


def wrap(x, period: float):
    """x as a distance on the circle, in [-period/2, period/2)."""
    return np.mod(x + period / 2, period) - period / 2


def circ_mean(x, period: float) -> tuple[float, float]:
    """Circular mean in [0, period) and the resultant length (1 = all equal)."""
    z = np.exp(2j * np.pi * np.asarray(x) / period).mean()
    return float(np.angle(z) % (2 * np.pi) / (2 * np.pi) * period), float(abs(z))


def refresh_phase(phase, period: float) -> float:
    """The mode of the phases, refined by the mean of those within three bins of it."""
    bins = int(round(period / BIN_MS))
    mode = (np.bincount((phase / BIN_MS).astype(int) % bins, minlength=bins).argmax() + 0.5) * BIN_MS
    d = wrap(phase - mode, period)
    return float(np.mod(mode + d[np.abs(d) < 3 * BIN_MS].mean(), period))


def burst_position(arrive_ms, start_ms, window, period: float) -> dict | None:
    """The burst's position after the refresh: arrive_ms and start_ms are the times each call reached the culprit and
    its handler started (the culprit's clock), window the number of its 5-s window."""
    pos, conc = [], []
    for w in np.unique(window):
        m = window == w
        late = m & (start_ms - arrive_ms > WAIT_MS)
        if late.sum() < MIN_CALLS:
            continue
        burst, r = circ_mean(arrive_ms[m], period)
        pos.append(burst - refresh_phase(np.mod(start_ms[late], period), period))
        conc.append(r)
    if not pos:
        return None
    mean, _ = circ_mean(pos, period)
    return {"after_refresh_ms": mean, "sd_ms": float(wrap(np.array(pos) - mean, period).std()), "concentration": float(np.mean(conc)), "windows": len(pos)}


def run(run_dir: str) -> dict | None:
    meta = runinfo.load_meta(run_dir)
    f = meta.get("fault") or {}
    if not f.get("limit") or not f.get("culprit"):
        return None
    quota, period = (float(x) / 1000.0 for x in str(f["limit"]).split("/"))
    sh = next(p for p in (os.path.join(run_dir, n) for n in ("shadow.jsonl.zst", "shadow.jsonl.gz", "shadow.jsonl")) if os.path.exists(p))
    t = read_shadow(sh)
    v = np.flatnonzero((t.kind == SERVER) & (t.pidx >= 0) & (t.svc == t.services.index(f["culprit"])))
    v = v[t.kind[t.pidx[v]] == CLIENT]
    sent, start = t.start[t.pidx[v]] / 1e6, t.start[v] / 1e6
    arrive = sent + np.median((start - sent)[healthy_mask(meta, t)[v]])
    m = (start / 1e3 >= float(f.get("applied") or f["start"]) + 1) & (start / 1e3 < float(f["end"]) - 1)
    res = burst_position(arrive[m], start[m], np.floor(start[m] / 1e3 / WINDOW_S).astype(np.int64), period)
    return res and {"run_id": meta["run_id"], "culprit": f["culprit"], "quota_ms": quota, "period_ms": period, **res}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs", nargs="+")
    a = ap.parse_args()
    for rd in a.runs:
        out = os.path.join(rd, "burst_phase.json")
        if os.path.exists(out):
            continue
        try:
            res = run(rd)
        except Exception as e:                                   # noqa: BLE001 - keep the batch going, report the run
            print(os.path.basename(rd), f"{type(e).__name__}: {e}", flush=True)
            continue
        if res:
            with open(out, "w", encoding="utf-8") as fh:
                json.dump(res, fh, indent=1)
        print(os.path.basename(rd), f"{res['after_refresh_ms']:.2f} ms after the refresh" if res else "no quota", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
