#!/usr/bin/env python3
"""Sanity checks on one collected and evaluated Stage 3 run.  Prints OK or one line per
problem; a problem halts the queue (postprocess.sh writes STOP), because the brief says to
stop and report when a run exposes a defect.

  python3 tools/campaign/sanity.py <run_dir>
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone

SLOBUDGET_ARMS = ("chosen", "threshold", "keep-parent")


def central_checks(rd: str, meta: dict) -> list:
    """The SLO-budget agents must pick up the central's update every period (it carries the
    calibrated tables), and the central's status must have been collected."""
    probs = []
    st = os.path.join(rd, "central-status.json")
    if not os.path.exists(st) or os.path.getsize(st) == 0:
        probs.append("central-status.json is empty (the central's status endpoint returned nothing)")
    t0, t1 = meta.get("t0"), meta.get("t1")
    for fn in sorted(os.listdir(rd)):
        if not (fn.startswith("agent-") and fn.endswith(".log")):
            continue
        vers = []
        for ln in open(os.path.join(rd, fn), errors="replace"):
            if "slobudget period" not in ln:
                continue
            m = re.search('"version": ([0-9]+)', ln)
            if not m:
                continue
            try:
                ts = datetime.strptime(ln.split(None, 1)[0], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc).timestamp()
            except ValueError:
                continue
            if t0 and t1 and t0 + 10 <= ts <= t1:
                vers.append(int(m.group(1)))
        if not vers:
            probs.append(f"{fn}: no period records in the load window")
            continue
        stale = sum(1 for a, b in zip(vers, vers[1:]) if b <= a)
        if vers[-1] == 0:
            probs.append(f"{fn}: the agent never received a central update (config version 0 all run)")
        elif stale > 0.1 * len(vers):
            probs.append(f"{fn}: the central update did not advance in {stale} of {len(vers)} periods")
    return probs


def main() -> int:
    rd = sys.argv[1]
    probs = []
    meta = json.load(open(os.path.join(rd, "meta.json")))
    wrk = open(os.path.join(rd, "wrk.log")).read() if os.path.exists(os.path.join(rd, "wrk.log")) else ""
    m = re.search(r"(\d+) requests in", wrk)
    reqs = int(m.group(1)) if m else 0
    if reqs < 0.95 * meta["rps"] * meta["dur_s"]:
        probs.append(f"wrk2 completed only {reqs} requests")
    if "Non-2xx" in wrk:
        probs.append("wrk2 reported non-2xx responses: " + re.search(r"Non-2xx[^\n]*", wrk).group(0))
    restarts = [int(x) for x in meta.get("agent_restarts", []) if x.isdigit()]
    if any(restarts):
        probs.append(f"collector container restarts during the run: {meta.get('agent_restarts')}")
    if meta["scenario"] not in ("steady", "bursty") and not meta.get("fault"):
        probs.append("the fault was not recorded")
    if meta["scenario"] == "bursty" and not meta.get("burst"):
        probs.append("the burst was not recorded")
    fl = os.path.join(rd, "fault.log")
    if os.path.exists(fl) and "failed" in open(fl).read():
        probs.append("fault injection failed on some container: " + open(fl).read().strip().replace("\n", " | ")[:300])
    sp = os.path.join(rd, "summary.json")
    if os.path.exists(sp):
        s = json.load(open(sp))
        vr = s.get("vol_ratio")
        if isinstance(vr, (int, float)) and meta["scenario"] in ("steady", "saturated", "noisy_neighbour", "cascade", "cold_start") and not (0.6 <= vr <= 1.3):
            probs.append(f"volume over V* out of range: {vr:.3f}")
        sh = s.get("shadow_spans")
        exp = 23 * meta["rps"] * meta["dur_s"]
        if isinstance(sh, (int, float)) and sh < 0.9 * exp:
            probs.append(f"shadow has {sh} spans, expected about {exp}")
    else:
        probs.append("no summary.json (evaluation did not run)")
    if meta["arm"] in SLOBUDGET_ARMS:
        probs += central_checks(rd, meta)
    print("OK" if not probs else "\n".join(probs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
