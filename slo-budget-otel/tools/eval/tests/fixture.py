"""A tiny run directory with known ground truth for the evaluation tests.

Route r, call graph a -> b -> {c, d} (sequential), every call through a client span:

    a server (depth 0) -> a client (1) -> b server (2) -> b client (3) -> c server (4)
                                                       -> b client (3) -> d server (4)

600 traces, one every 0.1 s from t0; the fault (culprit c) covers the traces starting
in [t0 + 30, t0 + 45): c 20 ms instead of 1.8 ms, b and a stretched around it.  The
oracle calibrates on [t0, t0 + 30).  One healthy d span has an error status.
"""
from __future__ import annotations

import base64
import json
import os

T0 = 1_000_000.0          # unix seconds, a multiple of the 5-s period
N_TRACES = 600
FAULT_FROM, FAULT_TO = 300, 450        # trace indexes in the fault
SLO_MS = 12.0
PLACEMENT = {"a": "node-1", "b": "node-1", "c": "node-2", "d": "node-2"}
CALLGRAPH = {"root": {"r": "a"},
             "children": {"r": {"a": ["b"], "b": ["c", "d"], "c": [], "d": []}},
             "parallel": {"r": {"a": False, "b": False}}}


def uvarint(v: int) -> bytes:
    out = bytearray()
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _spans_of_trace(k: int):
    """[(name, service, kind, start_us, end_us, depth, parent_name, error)] relative to the trace start."""
    faulty = FAULT_FROM <= k < FAULT_TO
    if not faulty:
        sp = [("a", "a", 2, 0, 6000, 0, None), ("ac", "a", 3, 500, 5500, 1, "a"),
              ("b", "b", 2, 600, 5400, 2, "ac"), ("bc", "b", 3, 1000, 3000, 3, "b"),
              ("c", "c", 2, 1100, 2900, 4, "bc"), ("bd", "b", 3, 3100, 4100, 3, "b"),
              ("d", "d", 2, 3200, 4000, 4, "bd")]
    else:
        sp = [("a", "a", 2, 0, 24000, 0, None), ("ac", "a", 3, 500, 23500, 1, "a"),
              ("b", "b", 2, 600, 23400, 2, "ac"), ("bc", "b", 3, 1000, 21500, 3, "b"),
              ("c", "c", 2, 1100, 21100, 4, "bc"), ("bd", "b", 3, 21600, 22600, 3, "b"),
              ("d", "d", 2, 21700, 22500, 4, "bd")]
    err_d = k == 100
    return [(n, s, kd, st, en, dp, par, (n == "d" and err_d)) for (n, s, kd, st, en, dp, par) in sp]


def trace_id(k: int) -> str:
    return f"{k + 1:032x}"


def span_id(k: int, name: str) -> int:
    return ((k + 1) << 8) | {"a": 1, "ac": 2, "b": 3, "bc": 4, "c": 5, "bd": 6, "d": 7}[name]


def kept_rule(k: int, name: str, arm: str) -> tuple[bool, float | None, str]:
    """(kept, sampling.p tag or None, reason) of the fixture's arm."""
    faulty = FAULT_FROM <= k < FAULT_TO
    if arm == "head":
        return (k % 2 == 0, None, "")
    if name == "a":
        return True, 1.0, "root"
    if faulty:
        if name == "c":
            return (k != FAULT_FROM + 6), 1.0, "outlier"          # one culprit span dropped (its b parent is kept)
        if name == "b":
            return (k % 3 == 0), 1.0, "outlier"                    # a third of the slow b spans
        return False, None, ""
    if name == "c":
        return (k % 2 == 0), 0.5, "plan"
    if name == "b":
        return (k == 60), 1.0, "outlier"                            # a false positive (warm period)
    if name == "d":
        return (k == 100), 1.0, "outlier"                           # the error span
    return False, None, ""


def make_run(root: str, arm: str = "chosen", seed: int = 1, volume: float = 0.12, gz: bool = False) -> str:
    run = os.path.join(root, f"v{int(volume * 100)}-saturated-{arm}-s{seed}")
    os.makedirs(os.path.join(run, "telemetry"), exist_ok=True)
    meta = {"run_id": os.path.basename(run), "arm": arm, "scenario": "saturated", "volume_frac": volume,
            "v_star": 300.0, "cap": 375.0, "seed": seed, "slo_ms": {"r": SLO_MS}, "period_s": 5, "margin": 0.1,
            "t0": T0, "t1": T0 + 60.0,
            "fault": {"kind": "cpu_hog", "start": T0 + 30.0, "end": T0 + 45.0, "culprit": "c", "targets": ["c"], "level": 1},
            "burst": None, "placement": PLACEMENT, "arm_params": {"head_r": 0.5} if arm == "head" else {}}
    with open(os.path.join(run, "meta.json"), "w") as f:
        json.dump(meta, f)
    shadow_lines = []
    traces = []
    admit_by_period: dict[int, int] = {}
    for k in range(N_TRACES):
        ts = T0 + 0.1 * k
        spans = _spans_of_trace(k)
        ids = {n: span_id(k, n) for (n, *_rest) in spans}
        rs_by_svc: dict[str, list] = {}
        jspans = []
        for (n, svc, kind, st, en, depth, par, err) in spans:
            start_ns = int(round((ts + st / 1e6) * 1e9))
            end_ns = int(round((ts + en / 1e6) * 1e9))
            bkey = "_br" if kind == 2 else "_d"
            raw = uvarint(depth) + ((b"\0" * 8 + b"\0" * 3) if kind == 2 else b"")
            attrs = [{"key": "slo.class", "value": {"stringValue": "r|m"}}, {"key": "slo.root", "value": {"stringValue": "r"}},
                     {"key": bkey, "value": {"bytesValue": base64.b64encode(raw).decode()}}]
            s = {"traceId": trace_id(k), "spanId": f"{ids[n]:016x}", "parentSpanId": f"{ids[par]:016x}" if par else "",
                 "name": n, "kind": kind, "startTimeUnixNano": str(start_ns), "endTimeUnixNano": str(end_ns),
                 "attributes": attrs, "status": {"code": 2} if err else {}}
            rs_by_svc.setdefault(svc, []).append(s)
            keep, p, reason = kept_rule(k, n, arm)
            if keep:
                admit_ns = end_ns + (2_000_000_000 if n == "a" and arm != "head" else 50_000_000)
                per = admit_ns // 5_000_000_000
                admit_by_period[per] = admit_by_period.get(per, 0) + 1
                tags = [{"key": bkey, "type": "binary", "value": base64.b64encode(raw).decode()},
                        {"key": "span.kind", "type": "string", "value": "server" if kind == 2 else "client"},
                        {"key": "slobudget.admit_ns", "type": "int64", "value": admit_ns}]
                if p is not None:
                    tags.append({"key": "sampling.p", "type": "float64", "value": p})
                if reason:
                    tags.append({"key": "slobudget.reason", "type": "string", "value": reason})
                jspans.append({"traceID": trace_id(k), "spanID": f"{ids[n]:016x}", "operationName": n,
                               "references": [{"refType": "CHILD_OF", "traceID": trace_id(k), "spanID": f"{ids[par]:016x}"}] if par else [],
                               "startTime": start_ns // 1000, "duration": (end_ns - start_ns) // 1000, "tags": tags,
                               "logs": [], "processID": "p_" + svc, "warnings": None})
        shadow_lines.append(json.dumps({"resourceSpans": [
            {"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": f"unknown_service:{svc}_service_pb_es_proc"}}]},
             "scopeSpans": [{"scope": {"name": svc}, "spans": v}]} for svc, v in rs_by_svc.items()]}))
        if jspans:
            traces.append({"traceID": trace_id(k), "spans": jspans, "warnings": None,
                           "processes": {"p_" + s: {"serviceName": f"unknown_service:{s}_service_pb_es_proc", "tags": []} for s in PLACEMENT}})
    # one duplicated shadow line (the tee re-sent a batch)
    shadow_lines.append(shadow_lines[5])
    if gz:
        import gzip
        with gzip.open(os.path.join(run, "shadow.jsonl.gz"), "wt", encoding="utf-8") as f:
            f.write("\n".join(shadow_lines) + "\n")
    else:
        with open(os.path.join(run, "shadow.jsonl"), "w", encoding="utf-8") as f:
            f.write("\n".join(shadow_lines) + "\n")
    with open(os.path.join(run, "export.json"), "w", encoding="utf-8") as f:
        json.dump({"data": traces, "total": len(traces), "limit": 0, "offset": 0, "errors": None}, f)
    # cap gate: one record per period, admitted = what the export has; period 200_003 drops 3
    with open(os.path.join(run, "gate.jsonl"), "w") as f:
        for per in sorted(admit_by_period):
            adm = admit_by_period[per]
            f.write(json.dumps({"period": per, "received": adm + (3 if per == 200_003 else 0), "admitted": adm,
                                "cap_dropped": 3 if per == 200_003 else 0, "bytes_admitted": 100 * adm}) + "\n")
    # central history: the table in force has c at 4 ms (so a 20 ms c is flagged -> "refused" when lost)
    hist = [{"period": p, "received": 0, "admitted": 0, "cap_dropped": 0, "b": 4.0, "n_need": 10,
             "frozen": ["c"] if 200_006 <= p <= 200_008 else [], "table_ms": {"a": 12.0, "b": 9.0, "c": 4.0, "d": 3.0}}
            for p in range(200_000, 200_013)]
    with open(os.path.join(run, "central-status.json"), "w") as f:
        json.dump({"history": hist}, f)
    # agent CPU scrapes and the per-period roots held
    prom = {"node-1": ((10.0, 1000), (12.5, 6000)), "node-2": ((5.0, 0), (6.0, 4000))}
    for node, ((c0, s0), (c1, s1)) in prom.items():
        for tag, (c, s) in (("start", (c0, s0)), ("end", (c1, s1))):
            with open(os.path.join(run, "telemetry", f"agent-{node}-{tag}.prom"), "w") as f:
                f.write("# HELP otelcol_process_cpu_seconds_total x\n# TYPE otelcol_process_cpu_seconds_total counter\n")
                f.write(f'otelcol_process_cpu_seconds_total{{service_instance_id="x"}} {c}\n')
                f.write(f'otelcol_receiver_accepted_spans_total{{receiver="otlp",transport="grpc"}} {s}\n')
                f.write(f'otelcol_receiver_accepted_spans_total{{receiver="configdiscovery",transport="http"}} 7\n')
    for node, held in (("node-1", [40, 50, 45]), ("node-2", [10, 5, 20])):
        with open(os.path.join(run, f"agent-{node}.log"), "w") as f:
            for j, h in enumerate(held):
                rec = {"resource": {"service.name": "otelcol-slobudget"}, "period": 200_001 + j, "roots_held": h, "roots_pinned": 1}
                f.write(f"2026-11-21T00:00:0{j}.000Z\tinfo\tslobudgetprocessor/processor.go:469\tslobudget period\t{json.dumps(rec)}\n")
    with open(os.path.join(run, "callgraph.json"), "w") as f:
        json.dump(CALLGRAPH, f)
    return run
