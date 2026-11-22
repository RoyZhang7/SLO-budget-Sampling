#!/usr/bin/env python3
"""Development helper: a fake Stage 3 run directory from a complete (100 %) Jaeger export.

    python3 tools/eval/make_fake_run.py ~/slo-budget-sim/results/remote/export-10min.json OUT_DIR \
        [--frac 0.12] [--fault-service socialgraph --fault-factor 5 --fault-from 240 --fault-for 180] [--seed 1]

Writes OUT_DIR/{meta.json, shadow.jsonl.zst, export.json, gate.jsonl}: the shadow is every
span converted to OTLP JSON lines (one line per trace, a resourceSpans entry per service),
the export a whole-trace subsample of `--frac` (an arm "head" with head_r = frac) whose spans
carry `slobudget.admit_ns` = end + 30 ms, and the gate the admitted spans per wall-clock
period.  With --fault-service, that service's server spans of traces starting inside the
window are stretched to `factor` x their duration in both the shadow and the export (the
parents are left alone), so the culprit rule and the evidence time have something to find.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import subprocess

PLACEMENT = {"composepost": "node-1", "hometimeline": "node-1", "urlshorten": "node-1", "usertimeline": "node-1",
             "wrk2api": "node-1", "socialgraph": "node-2", "media": "node-2", "text": "node-2", "userid": "node-2",
             "user": "node-2", "uniqueid": "node-3", "usermention": "node-3", "poststorage": "node-3"}


def norm(name: str) -> str:
    n = name.split(":", 1)[-1]
    for suf in ("_service_pb_es_proc", "_pb_es_proc", "_proc"):
        if n.endswith(suf):
            n = n[: -len(suf)]
            break
    return n.replace("_", "")


def traces_of(path: str):
    with open(path, encoding="utf-8") as f:
        text = f.read()
    dec = json.JSONDecoder()
    i = text.index("[", text.index('"data"')) + 1
    n = len(text)
    while True:
        while i < n and text[i] in " \t\r\n,":
            i += 1
        if i >= n or text[i] == "]":
            return
        obj, i = dec.raw_decode(text, i)
        yield obj


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("export")
    ap.add_argument("out")
    ap.add_argument("--frac", type=float, default=0.12)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--fault-service")
    ap.add_argument("--fault-factor", type=float, default=5.0)
    ap.add_argument("--fault-from", type=float, default=240.0)
    ap.add_argument("--fault-for", type=float, default=180.0)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    rng = random.Random(a.seed)
    first = []
    # pass 1: the run's start (the earliest root start)
    for tr in traces_of(a.export):
        first.append(min(s["startTime"] for s in tr["spans"]) / 1e6)
    t0 = min(first)
    t1 = t0 + 600.0
    f0, f1 = t0 + a.fault_from, t0 + a.fault_from + a.fault_for
    shadow_path = os.path.join(a.out, "shadow.jsonl")
    kept_traces = []
    gate: dict[int, int] = {}
    with open(shadow_path, "w", encoding="utf-8") as sh:
        for tr in traces_of(a.export):
            procs = tr.get("processes") or {}
            tstart = min(s["startTime"] for s in tr["spans"]) / 1e6
            in_fault = a.fault_service and f0 <= tstart < f1
            by_svc: dict[str, list] = {}
            for s in tr["spans"]:
                svc_full = (procs.get(s["processID"]) or {}).get("serviceName", "unknown")
                tags = {t["key"]: t for t in s.get("tags") or ()}
                kind = tags.get("span.kind", {}).get("value")
                if in_fault and kind == "server" and norm(svc_full) == a.fault_service:
                    s["duration"] = int(s["duration"] * a.fault_factor)
                parent = next((r["spanID"] for r in s.get("references") or () if r.get("refType") == "CHILD_OF"), "")
                attrs = []
                for key in ("_br", "_d"):
                    if key in tags:
                        attrs.append({"key": key, "value": {"bytesValue": tags[key]["value"]}})
                by_svc.setdefault(svc_full, []).append({
                    "traceId": s["traceID"].rjust(32, "0"), "spanId": s["spanID"].rjust(16, "0"),
                    "parentSpanId": parent.rjust(16, "0") if parent else "", "name": s["operationName"],
                    "kind": 2 if kind == "server" else 3 if kind == "client" else 1,
                    "startTimeUnixNano": str(s["startTime"] * 1000), "endTimeUnixNano": str((s["startTime"] + s["duration"]) * 1000),
                    "attributes": attrs, "status": {}})
            sh.write(json.dumps({"resourceSpans": [
                {"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": svc}}]}, "scopeSpans": [{"scope": {}, "spans": v}]}
                for svc, v in by_svc.items()]}) + "\n")
            if rng.random() < a.frac:
                for s in tr["spans"]:
                    admit = (s["startTime"] + s["duration"]) * 1000 + 30_000_000
                    s.setdefault("tags", []).append({"key": "slobudget.admit_ns", "type": "int64", "value": admit})
                    per = admit // 5_000_000_000
                    gate[per] = gate.get(per, 0) + 1
                kept_traces.append(tr)
    subprocess.run(["zstd", "-q", "-f", "--rm", "-T4", shadow_path, "-o", shadow_path + ".zst"], check=True)
    with open(os.path.join(a.out, "export.json"), "w", encoding="utf-8") as f:
        json.dump({"data": kept_traces, "total": len(kept_traces), "limit": 0, "offset": 0, "errors": None}, f)
    with open(os.path.join(a.out, "gate.jsonl"), "w") as f:
        for per in sorted(gate):
            f.write(json.dumps({"period": per, "received": gate[per], "admitted": gate[per], "cap_dropped": 0,
                                "bytes_admitted": None}) + "\n")
    meta = {"run_id": os.path.basename(os.path.abspath(a.out)), "arm": "head", "scenario": "saturated" if a.fault_service else "steady",
            "volume_frac": a.frac, "v_star": a.frac * 11500, "cap": 1.25 * a.frac * 11500, "seed": a.seed,
            "slo_ms": {"compose_post": 23.4}, "period_s": 5, "margin": 0.1, "t0": t0, "t1": t1,
            "fault": {"kind": "synthetic_stretch", "start": f0, "end": f1, "culprit": a.fault_service, "targets": [a.fault_service],
                      "level": a.fault_factor} if a.fault_service else None,
            "burst": None, "placement": PLACEMENT, "arm_params": {"head_r": a.frac}, "fake_from": a.export}
    with open(os.path.join(a.out, "meta.json"), "w") as f:
        json.dump(meta, f, indent=1)
    print(json.dumps({"out": a.out, "traces_kept": len(kept_traces), "t0": t0}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
