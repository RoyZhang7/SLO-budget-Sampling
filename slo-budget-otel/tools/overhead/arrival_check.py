#!/usr/bin/env python3
"""Configuration E: is the arrival program at every service pod's interface, does every agent read its table, and (--traffic, with
load running or just over) do the ids arrive?

    python3 tools/overhead/arrival_check.py [--traffic] [--wait 60]

Per node: arrivald's container is ready and has not restarted, its newest "counters" line is under 15 s old and counts the node's gRPC
service pods (the *-service-pb-es-ctr pods but the HTTP root and tracepressure, which have no listener on the services' port);
the last thing the node's stamping container (the tee where the collector pod has one, else the agent) said about the table is that it
is open.  With --traffic also: the program found ids and had no errors in its counters since it started, and of that container's
newest ENOUGH lookups with a parent (whole period lines) at least HIT were answered by the program.
Exits 1, the last line saying what is missing, when that does not hold within --wait seconds (at least two attempts).
"""
from __future__ import annotations

import argparse
import datetime
import json
import re
import subprocess
import sys
import time

NOT_GRPC = ("wrk2api-", "tracepressure-")
FRESH_S = 15
HIT = 0.95
ENOUGH = 200                    # lookups to judge by: the newest periods that have as many (a load's last one can hold a few only)
MISSES = ("no_table", "empty", "not_stored", "overwritten", "torn", "late")
AGENT, TEE = "otelcol-pb-es-ctr", "tee"     # a campaign run's collector pod has the tee in front of the agent


def kubectl(*args: str) -> str:
    r = subprocess.run(["kubectl", *args], capture_output=True, text=True)
    if r.returncode:
        raise ValueError(f"kubectl {' '.join(args)}: {r.stderr.strip().splitlines()[-1:] or r.returncode}")
    return r.stdout


def pods_of(doc: dict) -> list[dict]:
    """the pods that run and are not being deleted: name, node, ready, restarts"""
    out = []
    for p in doc["items"]:
        if p["status"].get("phase") != "Running" or p["metadata"].get("deletionTimestamp"):
            continue
        cs = p["status"].get("containerStatuses") or []
        out.append({"name": p["metadata"]["name"], "node": p["spec"].get("nodeName", ""), "containers": [c["name"] for c in p["spec"].get("containers") or []], "ready": bool(cs) and all(c.get("ready") for c in cs),
                    "restarts": sum(c.get("restartCount", 0) for c in cs)})
    return out


def expected(pods: list[dict]) -> dict[str, int]:
    out: dict[str, int] = {}
    for p in pods:
        if "-service-pb-es-ctr" in p["name"] and not p["name"].startswith(NOT_GRPC):
            out[p["node"]] = out.get(p["node"], 0) + 1
    return out


def json_lines(log: str):
    for ln in log.splitlines():
        try:
            d = json.loads(ln)
        except ValueError:
            continue
        if isinstance(d, dict):
            yield d


def age(stamp: str, now: float) -> float:
    """seconds since an RFC 3339 time (the fraction cut to microseconds)"""
    m = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)", stamp)
    if not m:
        raise ValueError(f"not a time: {stamp}")
    zone = "+00:00" if m[3] == "Z" else m[3]
    return now - datetime.datetime.fromisoformat(f"{m[1]}.{(m[2] or '0')[:6].ljust(6, '0')}{zone}").timestamp()


def loader_problems(node: str, pod: dict | None, log: str, want: int, now: float, traffic: bool) -> list[str]:
    if pod is None:
        return [f"{node}: no arrivald"]
    bad = []
    if not pod["ready"] or pod["restarts"]:
        bad.append(f"{node}: arrivald ready {pod['ready']}, {pod['restarts']} restarts")
    lines = [d for d in json_lines(log) if d.get("msg") == "counters"]
    if not lines:
        return bad + [f"{node}: no counters from arrivald"]
    if age(lines[-1].get("time", "1970-01-01T00:00:00Z"), now) > FRESH_S:
        bad.append(f"{node}: arrivald's last counters are {age(lines[-1]['time'], now):.0f} s old")
    if lines[-1].get("interfaces") != want:
        bad.append(f"{node}: the program is at {lines[-1].get('interfaces')} interfaces, {want} service pods")
    if traffic:
        ids, errors = (sum(d.get(k, 0) for d in lines) for k in ("ids", "errors"))
        if ids == 0 or errors:
            bad.append(f"{node}: the program found {ids} ids, {errors} errors")
    return bad


def agent_problems(node: str, pod: dict | None, log: str, traffic: bool) -> list[str]:
    said = [ln for ln in log.splitlines() if "arrival table open" in ln or "arrival table not open" in ln]
    if pod is None or not said or "arrival table not open" in said[-1]:
        return [f"{node}: the agent has not opened the table" + (f" ({said[-1][-160:]})" if said else "")]
    if not traffic:
        return []
    hit = miss = 0
    for ln in reversed(log.splitlines()):
        if hit + miss >= ENOUGH:
            break
        if "slobudget arrival" not in ln or "{" not in ln:
            continue
        try:
            d = json.loads(ln[ln.index("{"):])
        except ValueError:
            continue
        hit, miss = hit + d.get("hit", 0), miss + sum(d.get(k, 0) for k in MISSES)
    if hit + miss == 0 or hit < HIT * (hit + miss):
        return [f"{node}: {hit} of the {hit + miss} lookups of the agent's last periods came from the program"]
    return []


def problems(pods: list[dict], logs, now: float, traffic: bool) -> list[str]:
    want = expected(pods)
    if not want:
        return ["no service pods"]
    bad = []
    for node, n in sorted(want.items()):
        loader = next((p for p in pods if p["name"].startswith("slobudget-arrival") and p["node"] == node), None)
        agent = next((p for p in pods if p["name"].startswith("otelcol-pb-es-ctr") and p["node"] == node), None)
        bad += loader_problems(node, loader, logs(loader["name"]) if loader else "", n, now, traffic)
        bad += agent_problems(node, agent, logs(agent["name"], TEE if TEE in agent["containers"] else AGENT) if agent else "", traffic)
    return bad


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--traffic", action="store_true")
    ap.add_argument("--wait", type=float, default=60)
    a = ap.parse_args()
    end, tries = time.time() + a.wait, 0
    while True:
        tries += 1
        try:
            bad = problems(pods_of(json.loads(kubectl("get", "pods", "-o", "json"))), lambda p, c=None: kubectl("logs", p, *(["-c", c] if c else [])), time.time(), a.traffic)
        except (ValueError, KeyError) as e:     # kubectl failed, or half an answer: once more
            bad = [str(e)]
        if not bad:
            print("arrival: the program is at every service pod, every agent reads the table" + (", the ids arrive" if a.traffic else ""))
            return 0
        if time.time() > end and tries >= 2:        # one kubectl error is not a missing arrival
            print("; ".join(bad))
            return 1
        time.sleep(3)


if __name__ == "__main__":
    sys.exit(main())
