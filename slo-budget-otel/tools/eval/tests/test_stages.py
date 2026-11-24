"""derive_stages.py: sequential stages from complete traces, and the call graph file with them."""
from __future__ import annotations

import json
import os
import subprocess
import sys

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
EVAL = os.path.dirname(HERE)
SIM = os.path.join(EVAL, "..", "..", "..", "slo-budget-sim")
for p in (EVAL, HERE, SIM):
    if p not in sys.path:
        sys.path.insert(0, p)

import derive_stages as ds  # noqa: E402
import fixture  # noqa: E402
from sloeval import runinfo  # noqa: E402
from sloeval.evaluate import healthy_mask  # noqa: E402
from sloeval.oracle import build_oracle, import_sim, load_callgraph  # noqa: E402
from sloeval.spans import read_shadow  # noqa: E402


def part(calls, n=500, jitter=0.02, seed=0):
    """(callees, starts, ends in ns) of n spans; calls: name -> (start, end) in ms, each end jittered."""
    rng = np.random.default_rng(seed)
    names = sorted(calls)
    S = np.empty((n, len(names)))
    E = np.empty((n, len(names)))
    for k, nm in enumerate(names):
        a, b = calls[nm]
        S[:, k] = (a + jitter * rng.random(n)) * 1e6
        E[:, k] = (b + jitter * rng.random(n)) * 1e6
    return names, S, E


def test_two_groups_one_after_the_other():
    calls = {"text": (0.1, 4.6), "media": (0.1, 2.0), "user": (0.1, 1.0), "poststorage": (4.8, 6.5), "usertimeline": (4.8, 11.0), "hometimeline": (4.8, 9.0)}
    d = ds.derive([part(calls)], 0.99)
    assert d["stages"] == [["media", "text", "user"], ["hometimeline", "poststorage", "usertimeline"]]
    assert not d["problems"] and d["min_before_between_stages"] == 1.0 and d["max_before_inside_a_stage"] < 0.99
    assert d["stage_gap_ms_p1_p50_p99"][0][1] > 0.1


def test_one_parallel_stage_and_a_chain():
    d = ds.derive([part({"a": (0.1, 2.0), "b": (0.2, 3.0)})], 0.99)
    assert d["stages"] == [["a", "b"]] and not d["problems"]
    d = ds.derive([part({"a": (0.1, 1.0), "b": (1.2, 2.0), "c": (2.2, 3.0)})], 0.99)
    assert d["stages"] == [["a"], ["b"], ["c"]] and not d["problems"]


def test_a_mostly_sequential_pair_below_theta_is_one_stage():
    names, S, E = part({"a": (0.1, 2.0), "b": (2.1, 3.0)})
    S[:30, 1] = 1.0e6                         # b starts before a ends in 6 % of the spans
    d = ds.derive([(names, S, E)], 0.99)
    assert d["stages"] == [["a", "b"]]
    assert ds.derive([(names, S, E)], 0.9)["stages"] == [["a"], ["b"]]


def test_a_pair_inside_a_stage_that_is_sequential_is_a_problem():
    # a and b run one after the other (b starts after a), c covers both: no two-stage order of {a, b, c} exists
    d = ds.derive([part({"a": (0.1, 1.0), "b": (1.2, 2.0), "c": (0.0, 2.5)})], 0.99)
    assert d["problems"] and any("sequential but in one stage" in p for p in d["problems"])


def test_optional_calls_count_only_where_both_are_made():
    names, S, E = part({"a": (0.1, 1.0), "b": (1.2, 2.0)})
    S[:100, 1] = E[:100, 1] = np.nan          # b is not called in 100 spans
    assert ds.derive([(names, S, E)], 0.99)["stages"] == [["a"], ["b"]]


def test_on_a_run_directory_and_the_written_file(tmp_path):
    run = fixture.make_run(str(tmp_path / "run"))
    base = tmp_path / "graph.json"
    base.write_text(json.dumps(fixture.CALLGRAPH))
    out, rep = tmp_path / "staged.json", tmp_path / "report.json"
    cmd = [sys.executable, os.path.join(EVAL, "derive_stages.py"), run, "--min-spans", "50", "--merge", str(base), "--out", str(out), "--json", str(rep)]
    subprocess.run(cmd, check=True, capture_output=True, text=True, env=dict(os.environ, PYTHONPATH=EVAL))
    report = json.loads(rep.read_text())
    assert report["r/b"]["stages"] == [["c"], ["d"]] and report["r/a"]["stages"] == [["b"]]
    g = json.loads(out.read_text())
    assert g["stages"] == {"r": {"a": [["b"]], "b": [["c"], ["d"]]}} and g["children"] == fixture.CALLGRAPH["children"]
    CallGraph, Calibrator = import_sim(os.path.abspath(SIM))
    cg = load_callgraph(str(out), CallGraph)
    assert cg.groups("r", "b", ["c", "d"]) == [["c"], ["d"]]
    # the oracle on the staged file: the stages say what the flags said here, so the same T; c and d together would not
    t, meta = read_shadow(os.path.join(run, "shadow.jsonl")), runinfo.load_meta(run)
    oracle = lambda graph: build_oracle(t, healthy_mask(meta, t), graph, Calibrator, {"r": 0.012}).table   # noqa: E731
    flags = oracle(load_callgraph(str(base), CallGraph))
    assert oracle(cg) == flags
    cg.stages[("r", "b")] = [["c", "d"]]
    together = oracle(cg)
    assert together[("r", "*", "c")] > flags[("r", "*", "c")]


def test_a_callee_the_file_does_not_know_is_not_written(tmp_path):
    run = fixture.make_run(str(tmp_path / "run"))
    base = tmp_path / "graph.json"
    g = json.loads(json.dumps(fixture.CALLGRAPH))
    g["children"]["r"]["b"] = ["c"]
    base.write_text(json.dumps(g))
    out = tmp_path / "staged.json"
    cmd = [sys.executable, os.path.join(EVAL, "derive_stages.py"), run, "--min-spans", "50", "--merge", str(base), "--out", str(out)]
    r = subprocess.run(cmd, check=True, capture_output=True, text=True, env=dict(os.environ, PYTHONPATH=EVAL))
    assert "not written" in r.stderr
    assert "b" not in json.loads(out.read_text()).get("stages", {}).get("r", {})


def test_a_simulator_without_stages_refuses_the_staged_file(tmp_path):
    class OldGraph:
        def __init__(self, root, children, parallel):
            self.root, self.children, self.parallel = root, children, parallel

    f = tmp_path / "g.json"
    f.write_text(json.dumps(dict(fixture.CALLGRAPH, stages={"r": {"b": [["c"], ["d"]]}})))
    with pytest.raises(ValueError, match="stages"):
        load_callgraph(str(f), OldGraph)
    f.write_text(json.dumps(fixture.CALLGRAPH))
    assert load_callgraph(str(f), OldGraph).root == {"r": "a"}


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
