#!/usr/bin/env python3
"""One CSV of the gap per call edge over many runs, from each run's gaps.json (gaps_run.py).

    python3 tools/eval/gaps_csv.py /storage/royno7/stage3/runs [--out gaps.csv]

Long format: one row per run, window (before / during the fault) and edge.  The gap per edge is kept
here only; the summaries leave it out.
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import sys

FIELDS = ["n", "gap_p50_us", "gap_p95_us", "gap_p99_us", "gap_max_us", "gap_neg_frac", "client_p50_us", "client_p95_us",
          "server_p50_us", "server_p95_us", "gap_share_of_client_p50", "start_offset_p50_us"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs_dir")
    ap.add_argument("--out", default="gaps.csv")
    a = ap.parse_args()
    rows = []
    for path in sorted(glob.glob(os.path.join(a.runs_dir, "*", "gaps.json"))):
        rd = os.path.dirname(path)
        try:
            meta = json.load(open(os.path.join(rd, "meta.json"), encoding="utf-8"))
            gaps = json.load(open(path, encoding="utf-8"))
        except (OSError, ValueError) as e:
            print(f"skip {rd}: {e}", file=sys.stderr)
            continue
        for window, res in gaps.items():
            for edge, v in (res.get("edges") or {}).items():
                rows.append([meta["run_id"], meta["scenario"], meta["arm"] + (f"-{meta['variant']}" if meta.get("variant") else ""), meta["seed"], window, edge, v.get("hosts")]
                            + [v.get(f) for f in FIELDS])
    with open(a.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["run_id", "scenario", "arm", "seed", "window", "edge", "hosts"] + FIELDS)
        w.writerows(rows)
    print(f"wrote {a.out}: {len(rows)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
