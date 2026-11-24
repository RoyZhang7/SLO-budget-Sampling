#!/usr/bin/env python3
"""Sanity checks on one collected Stage 3 run.  Prints OK or one line per problem; a problem
halts the queue (run_one.sh and postprocess.sh write STOP), because the brief says to stop and
report when a run exposes a defect.

  python3 tools/campaign/sanity.py --pre <run_dir>   right after the run, without the Jaeger export
  python3 tools/campaign/sanity.py <run_dir>         after the export and the evaluation

The pre-check covers what the run directory holds at once: wrk2's completed requests and non-2xx
responses, the collectors' restarts, the fault or burst record, the agents' update versions (chosen
family), and the admitted volume against the target, taken from the central's exporter counters
(start and end scrapes; they match the evaluation's vol_ratio within 1 % on 171 runs of every arm).
The full check repeats all of it with the volume from the evaluation, plus the evaluation's shadow
size.  A crash of the pre-check itself is a note, not a problem: the full check still runs.
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone

SLOBUDGET_ARMS = ("chosen", "threshold", "keep-parent")
VOLUME_SCENARIOS = ("steady", "saturated", "noisy_neighbour", "cascade", "cold_start")
VOLUME_RANGE = (0.6, 1.3)
SENT_SPANS = re.compile(r"^otelcol_exporter_sent_spans_total\{([^}]*)\}\s+([0-9.eE+-]+)")


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


def run_checks(rd: str, meta: dict) -> list:
    """wrk2's result, the collectors' restarts and the fault or burst record."""
    probs = []
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
    return probs


def in_range(ratio: float, meta: dict, source: str) -> list:
    if meta["scenario"] in VOLUME_SCENARIOS and not (VOLUME_RANGE[0] <= ratio <= VOLUME_RANGE[1]):
        return [f"admitted volume over the collection volume target out of range: {ratio:.3f} ({source})"]
    return []


def counter_volume(rd: str, meta: dict) -> float | None:
    """Spans the central sent on between the start and the end scrape, over the target for the load."""
    def sent(name: str) -> float | None:
        total, seen = 0.0, False
        path = os.path.join(rd, "telemetry", name)
        if os.path.exists(path):
            for ln in open(path, errors="replace"):
                m = SENT_SPANS.match(ln)
                if m and 'exporter="otlp' in m.group(1):
                    total, seen = total + float(m.group(2)), True
        return total if seen else None

    end = sent("central-end.prom")
    if end is None:
        return None
    target = meta["v_star"] * meta["dur_s"] / meta.get("period_s", 5)
    return (end - (sent("central-start.prom") or 0.0)) / target


def summary_checks(rd: str, meta: dict) -> list:
    probs = []
    sp = os.path.join(rd, "summary.json")
    if not os.path.exists(sp):
        return ["no summary.json (evaluation did not run)"]
    s = json.load(open(sp))
    vr = s.get("vol_ratio")
    if isinstance(vr, (int, float)):
        probs += in_range(vr, meta, "evaluation")
    sh = s.get("shadow_spans")
    exp = 23 * meta["rps"] * meta["dur_s"]
    if isinstance(sh, (int, float)) and sh < 0.9 * exp:
        probs.append(f"shadow has {sh} spans, expected about {exp}")
    return probs


def load_meta(rd: str) -> dict:
    with open(os.path.join(rd, "meta.json")) as f:
        return json.load(f)


def pre_check(rd: str) -> tuple[list, list]:
    """Problems and notes from what the run directory holds before any export."""
    try:
        meta = load_meta(rd)
    except (OSError, ValueError) as e:
        return [f"meta.json is missing or unreadable ({type(e).__name__})"], []
    probs, notes = run_checks(rd, meta), []
    ratio = counter_volume(rd, meta)
    if ratio is None:
        notes.append("no exporter counters from the central: the admitted volume was not checked")
    else:
        probs += in_range(ratio, meta, "central exporter counters")
    if meta["arm"] in SLOBUDGET_ARMS:
        probs += central_checks(rd, meta)
    return probs, notes


def full_check(rd: str) -> list:
    try:
        meta = load_meta(rd)
    except (OSError, ValueError) as e:
        return [f"meta.json is missing or unreadable ({type(e).__name__})"]
    probs = run_checks(rd, meta) + summary_checks(rd, meta)
    if meta["arm"] in SLOBUDGET_ARMS:
        probs += central_checks(rd, meta)
    return probs


def main() -> int:
    if sys.argv[1:2] == ["--pre"]:
        try:
            probs, notes = pre_check(sys.argv[2])
        except Exception as e:                                  # noqa: BLE001 - a defect of the check, not of the run
            probs, notes = [], [f"the pre-check failed and was ignored: {type(e).__name__}: {e}"]
    else:
        probs, notes = full_check(sys.argv[1]), []
    print("OK" if not probs else "\n".join(probs))
    for n in notes:
        print("note: " + n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
