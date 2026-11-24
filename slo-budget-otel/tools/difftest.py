#!/usr/bin/env python3
"""Differential test driver: dump the Python replays of every scenario and seed,
then replay them with the Go port and demand identical decisions.

    python tools/difftest.py [--dumps DIR] [--volume-frac 0.12] [--seeds 1 2 3] [--go /path/to/go]
                             [--arm v2-keep-parent] [--set key=value ...] [--stages FILE]

Runs `python -m slo_budget_sim.cli dump` in ../slo-budget-sim (its venv) and
`go test -run TestDifferentialAgainstPythonDumps ./slobudget/` here with
SLOBUDGET_DUMPS pointing at the dumps.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
SIM = HERE.parent / "slo-budget-sim"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dumps", help="directory for the dumps (default: a temp dir, about 1 GB for 21 cases)")
    ap.add_argument("--volume-frac", type=float, default=0.12)
    ap.add_argument("--seeds", type=int, nargs="+", default=[1, 2, 3])
    ap.add_argument("--scenario", nargs="+")
    ap.add_argument("--arm", default="chosen-v2", help="simulator arm (preset) to dump, e.g. v2-keep-parent")
    ap.add_argument("--set", nargs="*", metavar="KEY=VALUE", help="BudgetConfig overrides passed to `cli dump`")
    ap.add_argument("--stages", metavar="FILE", help="call graph file whose sequential stages the corpora's graphs get (deploy/callgraph.stages.json)")
    ap.add_argument("--go", default=shutil.which("go") or "go")
    ap.add_argument("--keep", action="store_true", help="keep the dumps")
    a = ap.parse_args()
    dumps = Path(a.dumps) if a.dumps else Path(tempfile.mkdtemp(prefix="slobudget-dumps-"))
    py = SIM / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    if not py.exists():
        py = Path(sys.executable)
    cmd = [str(py), "-m", "slo_budget_sim.cli", "dump", "--volume-frac", str(a.volume_frac), "--seeds", *map(str, a.seeds), "--out", str(dumps)]
    if a.scenario:
        cmd += ["--scenario", *a.scenario]
    if a.arm != "chosen-v2":
        cmd += ["--arm", a.arm]
    if a.set:
        cmd += ["--set", *a.set]
    if a.stages:
        cmd += ["--stages", str(Path(a.stages).resolve())]
    print("+", " ".join(cmd))
    subprocess.run(cmd, cwd=SIM, check=True)
    env = dict(os.environ, SLOBUDGET_DUMPS=str(dumps), GOFLAGS="-mod=mod")
    cmd = [a.go, "test", "-count=1", "-v", "-run", "TestDifferentialAgainstPythonDumps", "./slobudget/"]
    print("+", " ".join(cmd))
    r = subprocess.run(cmd, cwd=HERE, env=env, capture_output=True, text=True)
    for line in r.stdout.splitlines():
        if "identical" in line or "FAIL" in line or line.startswith("ok") or "python" in line:
            print(line.strip())
    if not a.keep and not a.dumps:
        shutil.rmtree(dumps, ignore_errors=True)
    return r.returncode


if __name__ == "__main__":
    raise SystemExit(main())
