"""burst_phase.py: the bursts' position in the burner's quota period, its burst_phase.json and the table column."""
from __future__ import annotations

import json
import os
import subprocess
import sys

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
EVAL = os.path.dirname(HERE)
for p in (EVAL, HERE):
    if p not in sys.path:
        sys.path.insert(0, p)

import burst_phase as bp  # noqa: E402
import fixture  # noqa: E402

PERIOD, REFRESH = 10.0, 3.2


def calls_at(after, n_bursts=600, seed=0, spread=0.6):
    """(arrive, start) in ms of 10 calls per 100 ms that reach the culprit `after` ms after a refresh and are handled at
    the next refresh (a fifth of them at once)."""
    rng = np.random.default_rng(seed)
    arrive = (np.repeat(np.arange(n_bursts) * 100.0, 10) + REFRESH + after + spread * rng.standard_normal(n_bursts * 10)) + 5e6
    nxt = REFRESH + PERIOD * np.ceil((arrive - REFRESH) / PERIOD)
    start = np.where(rng.random(len(arrive)) < 0.2, arrive + 0.05, nxt + 0.1 * rng.random(len(arrive)))
    return arrive, start


@pytest.mark.parametrize("after", [0.3, 3.8, 6.0, 9.7])
def test_position_after_the_refresh(after):
    arrive, start = calls_at(after)
    d = bp.burst_position(arrive, start, np.floor(arrive / 5000).astype(int), PERIOD)
    assert abs(bp.wrap(d["after_refresh_ms"] - after, PERIOD)) < 0.15
    assert d["windows"] == 12 and d["sd_ms"] < 0.2 and d["concentration"] > 0.8


def test_no_waiting_calls_no_position():
    arrive, _ = calls_at(3.8)
    assert bp.burst_position(arrive, arrive + 0.1, np.zeros(len(arrive), int), PERIOD) is None


def burner_run(root, after=3.8, offset_ms=0.7):
    """A run directory: healthy calls for 30 s, then 30 s of the burner; the caller's clock is behind by offset_ms."""
    rd = os.path.join(root, "v12-saturated-chosen-s1")
    os.makedirs(rd)
    t0 = 1000.0
    arrive, start = calls_at(after, n_bursts=600)
    healthy = arrive < arrive[0] + 30_000
    start = np.where(healthy, arrive + 0.05, start)
    base = t0 * 1e3 - 5e6
    spans = []
    for i, (a, s) in enumerate(zip(arrive + base, start + base)):
        ids = [f"{3 * i + j + 1:016x}" for j in range(3)]
        tr = f"{i + 1:032x}"
        ns = lambda ms: str(int(round(ms * 1e6)))                     # noqa: E731
        spans.append(("caller", tr, ids[0], "", 2, ns(a - offset_ms - 1), ns(s + 3)))
        spans.append(("caller", tr, ids[1], ids[0], 3, ns(a - offset_ms - 0.1), ns(s + 2)))
        spans.append(("socialgraph", tr, ids[2], ids[1], 2, ns(s), ns(s + 1)))
    with open(os.path.join(rd, "shadow.jsonl"), "w") as f:
        for svc in ("caller", "socialgraph"):
            f.write(json.dumps({"resourceSpans": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": svc}}]}, "scopeSpans": [{"spans": [
                {"traceId": tr, "spanId": sid, "parentSpanId": pid, "name": svc, "kind": kind, "startTimeUnixNano": st, "endTimeUnixNano": en}
                for s, tr, sid, pid, kind, st, en in spans if s == svc]}]}]}) + "\n")
    meta = {"run_id": os.path.basename(rd), "arm": "chosen", "scenario": "saturated", "seed": 1, "volume_frac": 0.12, "t0": t0 - 1, "t1": t0 + 61,
            "fault": {"start": t0 + 30, "applied": t0 + 30, "end": t0 + 60, "culprit": "socialgraph", "limit": "3300/10000"}}
    json.dump(meta, open(os.path.join(rd, "meta.json"), "w"))
    return rd


def test_run_cancels_the_clock_offset(tmp_path):
    rd = burner_run(str(tmp_path))
    out = subprocess.run([sys.executable, os.path.join(EVAL, "burst_phase.py"), rd], capture_output=True, text=True, check=True).stdout
    b = json.load(open(os.path.join(rd, "burst_phase.json")))
    assert abs(b["after_refresh_ms"] - 3.8) < 0.15 and b["period_ms"] == 10.0 and "after the refresh" in out
    meta = json.load(open(os.path.join(rd, "meta.json")))
    meta["fault"]["limit"] = None
    json.dump(meta, open(os.path.join(rd, "meta.json"), "w"))
    os.remove(os.path.join(rd, "burst_phase.json"))
    assert bp.run(rd) is None


def test_tables_show_the_position_and_skip_invalid_runs(tmp_path):
    root = str(tmp_path)
    rd = fixture.make_run(root, arm="chosen")
    subprocess.run([sys.executable, os.path.join(EVAL, "eval_run.py"), rd, "--sim", os.path.join(EVAL, "..", "..", "..", "slo-budget-sim"),
                    "--callgraph", os.path.join(rd, "callgraph.json"), "--no-recon"], capture_output=True, text=True, check=True)
    tables = [sys.executable, os.path.join(EVAL, "tables.py"), root, "--volume", "0.12", "--seeds", "1"]
    before = subprocess.run(tables, capture_output=True, text=True, check=True).stdout
    assert "burst after the refresh" not in before and "| chosen |" in before
    json.dump({"after_refresh_ms": 3.81}, open(os.path.join(rd, "burst_phase.json"), "w"))
    after = subprocess.run(tables, capture_output=True, text=True, check=True).stdout
    assert "burst after the refresh, by seed (ms)" in after and "| 3.8 |" in after
    open(os.path.join(rd, "INVALID"), "w").write("x")
    assert "| chosen |" not in subprocess.run(tables, capture_output=True, text=True, check=True).stdout
