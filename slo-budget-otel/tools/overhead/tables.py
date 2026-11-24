#!/usr/bin/env python3
"""Tables of the overhead experiment ($STAGE3/overhead/<cfg>-r<rate>-<rep>/, run_overhead.sh).

    python3 tools/overhead/tables.py /storage/royno7/stage3/overhead [--md out.md]

Per config and rate, the mean over the repeats with the spread (min-max): wrk2's request latency
p50 / p99 and achieved req/s; CPU per request (ms of CPU per completed request) of the traced app
pods (the *-service-pb-es-ctr containers, wrk2api included) and, apart, of the data stores (the
*-db-* Mongo and *-cache-* pods); and CPU (cores, averaged over the 5-min load) of the agent DaemonSet on the
three app nodes, arrivald (E), the central collector (configs C to E), the trace backend (Jaeger,
Elasticsearch) and the three app nodes as a whole.  CPU comes from two kubelet snapshots around the load
(cpu_snapshot.py); the node's figure over its own sampling times.  For C to E also the spans the central
admitted per 5-s period over the spans offered at the rate (23 per request), the check that they keep the
12 % of B (central.log, the cap gate's records); for E the program's own CPU from the kernel's run
statistics (arrivald's counters; too small for the node's figure to show) and the share of the server
spans with a parent whose arrival came from the program.
"""
from __future__ import annotations

import argparse
import datetime
import glob
import json
import os
import re
import statistics as st

CONFIGS = {"A": "tracing off", "B": "SDK head sampling 12 %", "C": "record everything, our agent at 12 %",
           "D": "C with the Path Bridge off", "E": "C with the eBPF arrival program attached"}
APP_NODES = ("node-1", "node-2", "node-3")


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
    if pod.startswith("slobudget-arrival"):
        return "arrivald"
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
    rates = []
    for n in APP_NODES:
        x, y = (a.get("nodes") or {}).get(n), (b.get("nodes") or {}).get(n)
        if x and y and x.get("t") and y.get("t") and y["t"] > x["t"]:
            rates.append((y["cpu_ns"] - x["cpu_ns"]) / 1e9 / (y["t"] - x["t"]))
    if len(rates) == len(APP_NODES):
        out["node_cores"] = sum(rates)
    return out, secs


SPANS_PER_REQUEST = 23


def kept_share(rd: str, m: dict) -> float:
    """Mean admitted spans per full period inside the load (the cap gate's records) over the offered spans."""
    p = os.path.join(rd, "central.log")
    if not os.path.exists(p):
        return float("nan")
    adm = []
    for ln in open(p, errors="replace"):
        if "capgate period" not in ln or "{" not in ln:
            continue
        try:
            g = json.loads(ln[ln.index("{"):])
        except ValueError:
            continue
        per = g.get("period", -1)
        if per * 5 >= m["t0"] + 5 and (per + 1) * 5 <= m["t1"]:
            adm.append(float(g.get("admitted", 0)))
    return st.mean(adm) / (SPANS_PER_REQUEST * 5 * m["rate"]) if adm else float("nan")


def arrival_share(rd: str) -> float:
    """Share of the server spans with a parent that got the program's time (E): the agents' period lines."""
    hit = total = 0
    for p in glob.glob(os.path.join(rd, "arrival-agent-*.log")):
        for ln in open(p, errors="replace"):
            if "slobudget arrival" not in ln or "{" not in ln:
                continue
            try:
                g = json.loads(ln[ln.index("{"):])
            except ValueError:
                continue
            hit += g.get("hit", 0)
            total += sum(g.get(k, 0) for k in ("hit", "no_table", "empty", "not_stored", "overwritten", "torn", "late"))
    return hit / total if total else float("nan")


def program(rd: str, t0: float, t1: float) -> tuple[float, float]:
    """The arrival program on the three app nodes during the load (E): cores, and ns per packet it ran on (arrivald's counter lines).
    nan when a node's lines are missing."""
    ns = runs = nodes = 0
    for p in glob.glob(os.path.join(rd, "arrival-node-*.log")):
        if p.endswith(".previous.log"):
            continue
        before = runs
        for ln in open(p, errors="replace"):
            try:
                d = json.loads(ln)
                t = datetime.datetime.fromisoformat(re.sub(r"(\.\d{6})\d*", r"\1", d["time"]).replace("Z", "+00:00")).timestamp()
            except (ValueError, KeyError, TypeError):
                continue
            if d.get("msg") == "counters" and "run_ns" in d and t0 + 5 <= t <= t1:
                ns += d["run_ns"]
                runs += d["runs"]
        nodes += runs > before
    return (ns / 1e9 / (t1 - t0 - 5), ns / runs) if nodes == len(APP_NODES) else (float("nan"), float("nan"))


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
                     "backend_cores": cpu.get("backend", 0.0) / secs, "arrivald_cores": cpu.get("arrivald", 0.0) / secs,
                     "node_cores": cpu.get("node_cores", float("nan")),
                     "kept_share": kept_share(rd, m), "arrival_share": arrival_share(rd),
                     **dict(zip(("program_cores", "program_ns"), program(rd, m["t0"], m["t1"])))})
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
            ("arrivald cores", "arrivald_cores"), ("central cores", "central_cores"), ("Jaeger + ES cores", "backend_cores"), ("app nodes, all cores", "node_cores"),
            ("arrival program, cores (E)", "program_cores"), ("ns per packet (E)", "program_ns"),
            ("kept / offered spans (C to E)", "kept_share"), ("arrivals from the program (E)", "arrival_share")]
    lines = ["| config | rate | repeats | " + " | ".join(c for c, _ in cols) + " |", "|---" * (3 + len(cols)) + "|"]
    for rate in sorted({r["rate"] for r in rows}):
        for cfg in CONFIGS:
            rs = [r for r in rows if r["cfg"] == cfg and r["rate"] == rate]
            if not rs:
                continue
            lines.append(f"| {cfg}: {CONFIGS[cfg]} | {rate} | {len(rs)} | " + " | ".join(cell([r[k] for r in rs], "{:.3f}" if k == "kept_share" else "{:.4f}" if k in ("arrival_share", "program_cores") else "{:.2f}") for _, k in cols) + " |")
    text = "\n".join(lines) + "\n"
    if a.md:
        open(a.md, "w", encoding="utf-8").write(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
