#!/usr/bin/env python3
"""Cumulative CPU (ns) of every container on every node, and of every node, from the kubelet summary
API (`kubectl get --raw /api/v1/nodes/<node>/proxy/stats/summary`, cAdvisor's usageCoreNanoSeconds).
Two snapshots around a load give the CPU-seconds each container used in between; the node's figure
also has what no container is charged for (most of the kernel's network receive path).  Every figure
carries the time cAdvisor sampled it (up to 15 s before the snapshot), so that the rate is taken over
its own interval.

    python3 tools/overhead/cpu_snapshot.py <out.json>
"""
from __future__ import annotations

import datetime
import json
import subprocess
import sys
import time


def main() -> int:
    nodes = subprocess.check_output(["kubectl", "get", "nodes", "-o", "jsonpath={.items[*].metadata.name}"], text=True).split()
    snap = {"t": time.time(), "containers": {}, "nodes": {}}

    def sampled(cpu: dict) -> float | None:
        try:
            return datetime.datetime.fromisoformat(cpu["time"].replace("Z", "+00:00")).timestamp()
        except (KeyError, ValueError):
            return None
    for n in nodes:
        s = json.loads(subprocess.check_output(["kubectl", "get", "--raw", f"/api/v1/nodes/{n}/proxy/stats/summary"], text=True))
        node = (s.get("node") or {}).get("cpu") or {}
        if node.get("usageCoreNanoSeconds") is not None:
            snap["nodes"][n] = {"cpu_ns": int(node["usageCoreNanoSeconds"]), "t": sampled(node)}
        for p in s.get("pods") or []:
            ref = p["podRef"]
            for c in p.get("containers") or []:
                cpu = (c.get("cpu") or {}).get("usageCoreNanoSeconds")
                if cpu is not None:
                    snap["containers"][f"{ref['namespace']}/{ref['name']}/{c['name']}"] = {"node": n, "cpu_ns": int(cpu), "t": sampled(c["cpu"])}
    snap["t_end"] = time.time()
    with open(sys.argv[1], "w", encoding="utf-8") as f:
        json.dump(snap, f, indent=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
