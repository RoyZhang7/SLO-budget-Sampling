#!/usr/bin/env python3
"""Export traces from a Jaeger query API in time chunks (standard library only,
runs on the cluster head node with the system python3).

    python3 tools/jaeger_export.py --base http://192.168.134.21:16686 \
        --service unknown_service:wrk2api_service_pb_es_proc \
        --start <epoch s> --end <epoch s> --chunk 60 --limit 50000 --out export.json

The window [start, end) is split into `chunk`-second pieces so that no single
query has to return the whole run; traces are de-duplicated by traceID.  The
output has the query-API shape ({"data": [...]}) that `slo_budget_sim.jaeger`
loads.  A chunk that returns exactly `limit` traces is reported as truncated.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request


def fetch(base: str, service: str, start_s: float, end_s: float, limit: int, retries: int = 3) -> list:
    q = urllib.parse.urlencode({"service": service, "start": int(start_s * 1e6), "end": int(end_s * 1e6),
                                "limit": limit})
    url = f"{base.rstrip('/')}/api/traces?{q}"
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=900) as r:
                obj = json.load(r)
            return obj.get("data") or []
        except Exception as e:                      # noqa: BLE001 - report and retry
            print(f"  attempt {attempt + 1} failed for {start_s:.0f}-{end_s:.0f}: {e}", file=sys.stderr)
            time.sleep(5)
    return []


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base", required=True)
    ap.add_argument("--service", required=True)
    ap.add_argument("--start", type=float, required=True, help="epoch seconds")
    ap.add_argument("--end", type=float, required=True, help="epoch seconds")
    ap.add_argument("--chunk", type=float, default=60.0)
    ap.add_argument("--limit", type=int, default=50000)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    seen: set[str] = set()
    traces: list = []
    spans = 0
    t = a.start
    while t < a.end:
        e = min(a.end, t + a.chunk)
        data = fetch(a.base, a.service, t, e, a.limit)
        new = 0
        for tr in data:
            tid = tr.get("traceID")
            if tid in seen:
                continue
            seen.add(tid)
            traces.append(tr)
            spans += len(tr.get("spans", []))
            new += 1
        flag = "  TRUNCATED (raise --limit or lower --chunk)" if len(data) >= a.limit else ""
        print(f"{time.strftime('%H:%M:%S', time.gmtime(t))}-{time.strftime('%H:%M:%S', time.gmtime(e))}: "
              f"{len(data)} traces, {new} new{flag}")
        t = e
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump({"data": traces, "total": len(traces), "limit": a.limit, "offset": 0, "errors": None}, f)
    print(f"wrote {a.out}: {len(traces)} traces, {spans} spans")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
