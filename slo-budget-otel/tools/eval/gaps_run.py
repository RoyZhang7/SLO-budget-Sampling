#!/usr/bin/env python3
"""Stage 3: the gap per call edge of one faulted run, before vs during its fault (gaps.py on the
run's shadow; `before` = the 3 minutes before the fault, after the first 30 s): writes
<run_dir>/gaps.json and gaps.md.  Runs without a fault (steady, bursty) are skipped.

    ~/slo-budget-sim/.venv/bin/python3 tools/eval/gaps_run.py /storage/royno7/stage3/runs/<run_id>
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gaps  # noqa: E402


def main(argv=None) -> int:
    rd = (argv if argv is not None else sys.argv[1:])[0]
    meta = json.load(open(os.path.join(rd, "meta.json")))
    fault = meta.get("fault")
    if not fault:
        print("no fault: skipped")
        return 0
    shadow = next((os.path.join(rd, n) for n in ("shadow.jsonl.zst", "shadow.jsonl.gz", "shadow.jsonl")
                   if os.path.exists(os.path.join(rd, n))), None)
    if shadow is None:
        print("no shadow: skipped")
        return 0
    t0, fs, fe = float(meta["t0"]), float(fault["start"]), float(fault["end"])
    return gaps.main(["--input", f"before={shadow}@{max(t0 + 30.0, fs - 180.0)}:{fs}", "--input", f"during={shadow}@{fs}:{fe}",
                      "--placement", os.path.join(rd, "meta.json"),
                      "--out", os.path.join(rd, "gaps.json"), "--md", os.path.join(rd, "gaps.md")])


if __name__ == "__main__":
    raise SystemExit(main())
