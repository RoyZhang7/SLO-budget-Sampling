#!/usr/bin/env python3
"""Per calibration phase: a sample of the phase's traces from the Jaeger query API (stock
collector, 100 % of spans), the server-span P50 / P95 of the services of interest, and the
share of requests whose root span exceeds the SLO.

  python3 tools/hog/calib_analyze.py results/calib-<stamp> [--jaeger URL] [--limit 1500]
"""
from __future__ import annotations

import argparse
import json
import urllib.parse
import urllib.request

SERVICES = ("socialgraph", "post_storage", "hometimeline", "composepost", "usertimeline", "text", "wrk2api")
SLO_US = 23400.0


def norm(name: str) -> str:
    n = name.split(":", 1)[-1]
    for suf in ("_service_pb_es_proc", "_service"):
        if n.endswith(suf):
            n = n[: -len(suf)]
    return n


def q(vals, p):
    if not vals:
        return float("nan")
    v = sorted(vals)
    return v[min(len(v) - 1, int(p * (len(v) - 1) + 0.5))]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("run")
    ap.add_argument("--jaeger", default="http://192.168.134.21:16686")
    ap.add_argument("--limit", type=int, default=1500)
    a = ap.parse_args()
    phases = json.load(open(f"{a.run}/phases.json"))
    base = None
    rows = []
    for ph in phases:
        t0, t1 = int((ph["t0"] + 3) * 1e6), int(ph["t1"] * 1e6)
        params = {"service": "unknown_service:wrk2api_service_pb_es_proc", "start": t0, "end": t1, "limit": a.limit}
        url = f"{a.jaeger}/api/traces?" + urllib.parse.urlencode(params)
        data = json.load(urllib.request.urlopen(url, timeout=300)).get("data", [])
        dur = {s: [] for s in SERVICES}
        roots = []
        for tr in data:
            procs = tr.get("processes", {})
            for s in tr.get("spans", []):
                tags = {t["key"]: t.get("value") for t in s.get("tags", [])}
                if tags.get("span.kind") != "server":
                    continue
                svc = norm(procs.get(s.get("processID"), {}).get("serviceName", "?"))
                if not s.get("references"):
                    roots.append(s["duration"])
                if svc in dur:
                    dur[svc].append(s["duration"])
        row = {"phase": ph["phase"], "traces": len(data), "roots": len(roots),
               "over_slo": sum(1 for d in roots if d > SLO_US) / len(roots) if roots else float("nan"),
               "root_p50_ms": q(roots, 0.5) / 1000, "root_p95_ms": q(roots, 0.95) / 1000}
        for svc in SERVICES:
            row[f"{svc}_p50_ms"] = q(dur[svc], 0.5) / 1000
            row[f"{svc}_p95_ms"] = q(dur[svc], 0.95) / 1000
        if base is None:
            base = row
        rows.append(row)
    print(f"{'phase':28s} traces  over-SLO  root p50/p95 ms   socialgraph p95 (x)   post_storage p95 (x)   hometimeline p95   composepost p95")
    for r in rows:
        sg = r["socialgraph_p95_ms"]; ps = r["post_storage_p95_ms"]
        print(f"{r['phase']:28s} {r['traces']:6d}  {100 * r['over_slo']:6.1f} %  {r['root_p50_ms']:6.2f}/{r['root_p95_ms']:6.2f}   "
              f"{sg:7.2f} ({sg / base['socialgraph_p95_ms']:4.1f}x)      {ps:7.2f} ({ps / base['post_storage_p95_ms']:4.1f}x)        "
              f"{r['hometimeline_p95_ms']:7.2f}          {r['composepost_p95_ms']:7.2f}")
    json.dump(rows, open(f"{a.run}/calib.json", "w"), indent=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
