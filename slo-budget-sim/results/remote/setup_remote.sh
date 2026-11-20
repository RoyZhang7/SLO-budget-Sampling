#!/usr/bin/env bash
# One-shot setup on the remote machine after `tar xzf sim.tgz` in $HOME:
# venv, install, tests, then launch run_pass4.sh detached in tmux.
set -eu
cd ~/slo-budget-sim
if [ ! -x .venv/bin/python ]; then
    python3 -m venv .venv
fi
.venv/bin/pip install -q -e ".[dev]"
.venv/bin/python -m pytest -q 2>&1 | tail -1
mkdir -p results/remote
chmod +x results/remote/run_pass4.sh tools/jaeger_export.py
tmux kill-session -t pass4 2>/dev/null || true
tmux new-session -d -s pass4 "bash results/remote/run_pass4.sh > results/remote/nohup.out 2>&1"
sleep 5
echo "--- tmux sessions"; tmux ls
echo "--- run.log"; tail -3 results/remote/run.log
echo "--- processes"; pgrep -fa "wrk -t2|run_pass4" | head -5
