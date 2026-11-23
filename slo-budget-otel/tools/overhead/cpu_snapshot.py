#!/usr/bin/env python3
"""Cumulative CPU (ns) of every container on every node, from the kubelet summary API
(`kubectl get --raw /api/v1/nodes/<node>/proxy/stats/summary`, cAdvisor's usageCoreNanoSeconds).
Two snapshots around a load give the CPU-seconds each container used in between.

    python3 tools/overhead/cpu_snapshot.py <out.json>
"""
from __future__ import annotations

import json
import subprocess
import sys
import time


def main() -> int:
    nodes = subprocess.check_output(["kubectl", "get", "nodes", "-o", "jsonpath={.items[*].metadata.name}"], text=True).split()
    snap = {"t": time.time(), "containers": {}}
    for n in nodes:
        s = json.loads(subprocess.check_output(["kubectl", "get", "--raw", f"/api/v1/nodes/{n}/proxy/stats/summary"], text=True))
        for p in s.get("pods") or []:
            ref = p["podRef"]
            for c in p.get("containers") or []:
                cpu = (c.get("cpu") or {}).get("usageCoreNanoSeconds")
                if cpu is not None:
                    snap["containers"][f"{ref['namespace']}/{ref['name']}/{c['name']}"] = {"node": n, "cpu_ns": int(cpu)}
    snap["t_end"] = time.time()
    with open(sys.argv[1], "w", encoding="utf-8") as f:
        json.dump(snap, f, indent=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
