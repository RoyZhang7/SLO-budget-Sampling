#!/usr/bin/env python3
"""Tables of the overhead experiment ($STAGE3/overhead/<cfg>-r<rate>-<rep>/, run_overhead.sh).

    python3 tools/overhead/tables.py /storage/royno7/stage3/overhead [--md out.md]

Per config and rate, the mean over the repeats with the spread (min-max): wrk2's request latency
p50 / p99 and achieved req/s; CPU per request (ms of CPU per completed request) of the traced app
pods (the *-service-pb-es-ctr containers, wrk2api included) and, apart, of the data stores (the
*-db-* Mongo and *-cache-* pods); and CPU (cores, averaged over the 5-min load) of the agent DaemonSet on the
three app nodes, the central collector (config C) and the trace backend (Jaeger, Elasticsearch).
CPU comes from two kubelet snapshots around the load (cpu_snapshot.py).
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import statistics as st

CONFIGS = {"A": "tracing off", "B": "SDK head sampling 12 %", "C": "record everything, our agent at 12 %",
           "D": "C with the Path Bridge off"}


def wrk(path: str) -> dict:
    t = open(path, errors="replace").read()
    out = {}
    for pct, v, u in re.findall(r"^\s*(50\.000|99\.000)%\s+([0-9.]+)(ms|s|us)", t, re.M):
        out["p50" if pct.startswith("50") else "p99"] = float(v) * {"ms": 1, "s": 1000, "us": 0.001}[u]
    m = re.search(r"Requests/sec:\s+([0-9.]+)", t)
    out["rps"] = float(m.group(1)) if m else float("nan")
    m = re.search(r"^\s*([0-9]+) requests in", t, re.M)
    out["requests"] = int(m.group(1)) if m else 0
    return out


def group(key: str) -> str | None:
    ns, pod, cont = key.split("/", 2)
    if ns != "default":
        return None
    if pod.startswith("otelcol-pb-es-ctr"):
        return "agent"
    if pod.startswith("slobudget-central"):
        return "central"
    if pod.startswith(("jaeger-", "elasticsearch-")):
        return "backend"
    if "-service-pb-es-ctr" in cont and "tracepressure" not in cont:
        return "app"
    if "-db-pb-es-ctr" in pod or "-cache-pb-es-ctr" in pod:     # Mongo (*-db-*) and the caches
        return "stores"
    return None


def cpu_by_group(rd: str) -> tuple[dict, float]:
    a = json.load(open(os.path.join(rd, "cpu-start.json")))
    b = json.load(open(os.path.join(rd, "cpu-end.json")))
    secs = b["t"] - a["t"]
    out: dict[str, float] = {}
    for k, v in b["containers"].items():
        g = group(k)
        if g is None or k not in a["containers"]:
            continue
        out[g] = out.get(g, 0.0) + (v["cpu_ns"] - a["containers"][k]["cpu_ns"]) / 1e9
    return out, secs


def load(root: str) -> list[dict]:
    rows = []
    for mp in sorted(glob.glob(os.path.join(root, "*-r*-*", "meta.json"))):
        rd = os.path.dirname(mp)
        if not os.path.exists(os.path.join(rd, "collected")):
            continue
        m = json.load(open(mp))
        w = wrk(os.path.join(rd, "wrk.log"))
        cpu, secs = cpu_by_group(rd)
        n = max(1, w["requests"])
        rows.append({"cfg": m["config"], "rate": m["rate"], "rep": m["rep"], **w,
                     "app_ms_per_req": 1000 * cpu.get("app", 0.0) / n, "stores_ms_per_req": 1000 * cpu.get("stores", 0.0) / n,
                     "agent_cores": cpu.get("agent", 0.0) / secs, "central_cores": cpu.get("central", 0.0) / secs,
                     "backend_cores": cpu.get("backend", 0.0) / secs})
    return rows


def cell(xs: list[float], fmt: str = "{:.2f}") -> str:
    xs = [x for x in xs if x == x]
    if not xs:
        return "-"
    if len(xs) == 1:
        return fmt.format(xs[0])
    return f"{fmt.format(st.mean(xs))} ({fmt.format(min(xs))}-{fmt.format(max(xs))})"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("root")
    ap.add_argument("--md")
    a = ap.parse_args()
    rows = load(a.root)
    cols = [("p50 ms", "p50"), ("p99 ms", "p99"), ("req/s", "rps"), ("app CPU ms/request", "app_ms_per_req"),
            ("stores CPU ms/request", "stores_ms_per_req"), ("agent cores", "agent_cores"),
            ("central cores", "central_cores"), ("Jaeger + ES cores", "backend_cores")]
    lines = ["| config | rate | repeats | " + " | ".join(c for c, _ in cols) + " |", "|---" * (3 + len(cols)) + "|"]
    for rate in sorted({r["rate"] for r in rows}):
        for cfg in CONFIGS:
            rs = [r for r in rows if r["cfg"] == cfg and r["rate"] == rate]
            if not rs:
                continue
            lines.append(f"| {cfg}: {CONFIGS[cfg]} | {rate} | {len(rs)} | " + " | ".join(cell([r[k] for r in rs]) for _, k in cols) + " |")
    text = "\n".join(lines) + "\n"
    if a.md:
        open(a.md, "w", encoding="utf-8").write(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
