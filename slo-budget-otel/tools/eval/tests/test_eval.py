"""Arithmetic of the Stage 3 evaluation on the hand-built fixture (tests/fixture.py)."""
from __future__ import annotations

import json
import math
import os
import subprocess
import sys

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
EVAL = os.path.dirname(HERE)
SIM = os.path.abspath(os.path.join(EVAL, "..", "..", "..", "slo-budget-sim"))
sys.path.insert(0, EVAL)
sys.path.insert(0, HERE)

import fixture  # noqa: E402
from sloeval.evaluate import evaluate  # noqa: E402
from sloeval.metrics import _wquantile  # noqa: E402
from sloeval.spans import read_export, read_shadow  # noqa: E402


def _eval(run):
    return evaluate(run, SIM, os.path.join(run, "callgraph.json"), recon_bin=None, do_recon=False)


@pytest.fixture(scope="module")
def chosen(tmp_path_factory):
    run = fixture.make_run(str(tmp_path_factory.mktemp("runs")), arm="chosen")
    return run, _eval(run)


def _oracle_T(run):
    """T of c and the stage budgets from the same calibrator the tool uses."""
    from sloeval.oracle import build_oracle, import_sim, load_callgraph
    from sloeval.evaluate import healthy_mask
    from sloeval import runinfo
    CallGraph, Calibrator = import_sim(SIM)
    g = load_callgraph(os.path.join(run, "callgraph.json"), CallGraph)
    t = read_shadow(os.path.join(run, "shadow.jsonl"))
    meta = runinfo.load_meta(run)
    return build_oracle(t, healthy_mask(meta, t), g, Calibrator, {"r": 0.012})


def test_parsing_and_join(chosen):
    run, s = chosen
    assert s["shadow_spans"] == fixture.N_TRACES * 7
    assert s["shadow_duplicates"] == 7                 # the re-sent batch
    assert s["export_unmatched"] == 0
    assert s["shadow_missing_parent"] == 0
    t = read_shadow(os.path.join(run, "shadow.jsonl"))
    assert sorted(set(t.depth.tolist())) == [0, 1, 2, 3, 4]
    # own time: b = 4.8 - (2.0 + 1.0) ms in healthy traces, c a leaf
    b = np.flatnonzero((t.kind == 2) & (np.array(t.services)[t.svc] == "b"))
    assert np.isclose(t.own[b[0]] / 1e6, 1.8)
    # nearest server ancestor of c is b
    c = np.flatnonzero((t.kind == 2) & (np.array(t.services)[t.svc] == "c"))[0]
    assert t.services[t.svc[t.sparent[c]]] == "b"


def test_oracle_split():
    import tempfile
    run = fixture.make_run(tempfile.mkdtemp(), arm="chosen")
    cal = _oracle_T(run)
    W = 1 + 2 * math.sqrt(1.8) + math.sqrt(0.8)                     # own a 1.0, b 1.8, c 1.8, d 0.8 ms
    assert cal.lookup("r", "r|m", "c") == pytest.approx(0.012 * math.sqrt(1.8) / W, rel=1e-6)
    assert cal.lookup("r", "r|m", "a") == pytest.approx(0.012, rel=1e-9)


def test_outlier_metrics(chosen):
    run, s = chosen
    # kept non-root server spans in warm periods: healthy c 200, faulty c 149, faulty b 50, the false positive b, the error d
    assert s["outlier_share"] == pytest.approx(200 / 401)
    assert s["outlier_precision"] == pytest.approx(200 / 201)
    assert s["recall_culprit"] == pytest.approx(149 / 150)
    assert s["evidence_s"] == pytest.approx(0.0211 + 0.05, abs=1e-6)
    assert s["evidence_any_s"] == pytest.approx(0.0711, abs=1e-6)
    assert s["identified"] is True and s["culprit_est"] == "c"
    assert s["culprit_confirmable"] == pytest.approx(1.0)
    # on complete data the slow root, b and c all qualify (3 of 4 services): systemic, as the simulator rules
    assert s["truth_systemic"] is True and s["truth_culprit_est"] == "none"


def test_attributability(chosen):
    run, s = chosen
    n = 149 + 50 + 1
    assert s["kept_outliers"] == n
    assert s["attrib_own"] == pytest.approx(149 / n)
    assert s["attrib_child"] == pytest.approx(49 / n)
    assert s["attrib_clean"] == pytest.approx(1 / n)
    assert s["attrib_loss"] == pytest.approx(1 / n)
    assert s["loss_refused"] == pytest.approx(1 / n)
    assert s["attrib_explained"] == pytest.approx(199 / n)
    # the lost culprit span is a refusal (20 ms > 1.1 x the 4 ms table in force)
    assert s["ref_refused"] > 0


def test_volume_spend_and_cost(chosen):
    run, s = chosen
    gate = [json.loads(l) for l in open(os.path.join(run, "gate.jsonl"))]
    warm = [g for g in gate if g["period"] * 5 >= fixture.T0 + 5 and (g["period"] + 1) * 5 <= fixture.T0 + 60]
    assert s["kept_per_period"] == pytest.approx(np.mean([g["admitted"] for g in warm]))
    assert s["vol_ratio"] == pytest.approx(np.mean([g["admitted"] for g in warm]) / 300.0)
    assert s["bytes_per_period"] == pytest.approx(100 * np.mean([g["admitted"] for g in warm]))
    assert s["over_cap_periods"] == 1
    assert s["max_per_request"] == 3
    assert s["agent_cpu_us_per_span"] == pytest.approx(3.5e6 / 9000)
    assert s["buffer_peak_spans"] == 65 and s["buffer_mean_spans"] == pytest.approx((50 + 55 + 65) / 3)
    assert s["frozen_svc_periods"] == 3 and s["frozen_entries"] == 1
    assert s["p95_err_svc_mean"] == pytest.approx(0.0, abs=1e-12)
    assert s["latency_p50_ms"] == pytest.approx(6.0)
    assert s["over_slo_frac"] == pytest.approx(150 / 600)
    # max spans per host per period, recomputed from the export
    kept = read_export(os.path.join(run, "export.json"))
    cells = {}
    for sv, a in zip(kept.service, kept.admit):
        k = int((a / 1e9 - fixture.T0) // 5)
        if k >= 1:
            key = (fixture.PLACEMENT[sv], k)
            cells[key] = cells.get(key, 0) + 1
    # warm = span end period >= 1, which excludes the traces of the first 5 s; recompute that way
    assert s["max_per_host_period"] <= max(cells.values())


def test_t_drift(chosen):
    run, s = chosen
    Tc = 0.012 * math.sqrt(1.8) / (1 + 2 * math.sqrt(1.8) + math.sqrt(0.8))
    assert s["t_drift_culprit"] == pytest.approx(0.004 / Tc - 1.0, rel=1e-6)


def test_head_weights(tmp_path):
    run = fixture.make_run(str(tmp_path), arm="head")
    s = _eval(run)
    assert np.isnan(s["outlier_precision"]) if isinstance(s["outlier_precision"], float) else s["outlier_precision"] is None
    # whole even traces kept with p = 0.5: the reweighted culprit and b rates are both 1
    assert s["spend_top"].startswith("c:1.000") or s["spend_top"].startswith("b:1.000")
    assert "c:1.000" in s["spend_top"] and "b:1.000" in s["spend_top"]
    assert s["identified"] is True
    assert s["recall_culprit"] == pytest.approx(75 / 150)


def test_wquantile_matches_simulator():
    sys.path.insert(0, SIM)
    from slo_budget_sim.metrics import _wquantile as sim_wq
    rng = np.random.default_rng(3)
    v = rng.lognormal(size=500)
    w = rng.uniform(0.5, 3, size=500)
    assert _wquantile(v, w, 0.95) == sim_wq(v, w, 0.95)


def test_gzip_shadow(tmp_path):
    run = fixture.make_run(str(tmp_path), arm="chosen", gz=True)
    s = _eval(run)
    assert s["shadow_spans"] == fixture.N_TRACES * 7 and s["recall_culprit"] == pytest.approx(149 / 150)


def test_cli_and_tables(tmp_path):
    root = str(tmp_path)
    for seed in (1, 2):
        run = fixture.make_run(root, arm="chosen", seed=seed)
        subprocess.run([sys.executable, os.path.join(EVAL, "eval_run.py"), run, "--sim", SIM, "--callgraph",
                        os.path.join(run, "callgraph.json"), "--no-recon"], check=True, capture_output=True)
        s = json.load(open(os.path.join(run, "summary.json")))
        assert s["evidence_s"] == pytest.approx(0.0711, abs=1e-6)
    out = subprocess.run([sys.executable, os.path.join(EVAL, "tables.py"), root, "--volume", "0.12"],
                         check=True, capture_output=True, text=True).stdout
    assert "| chosen | saturated | 2 (missing 3) |" in out
    assert "| 2/2 |" in out
    js = json.loads(subprocess.run([sys.executable, os.path.join(EVAL, "tables.py"), root, "--volume", "0.12", "--json"],
                                   check=True, capture_output=True, text=True).stdout)
    assert js["competitors"][0]["identified"] == {"k": 2, "n": 2}


def test_stage2d_truths_and_caller_keeps(chosen):
    """The fixture's culprit c is called through b by a: keeps on b are true under the
    duration truth (b's duration grows) and false under the own-time truth (its own time
    does not), so the single-culprit and own-time precisions agree here."""
    _, out = chosen
    assert out["culprit_callers"] == ["a", "b"]
    assert out["outlier_keeps_callers_window"] > 0
    assert out["caller_keeps_truth_dur"] == 1.0
    assert out["caller_keeps_truth_own"] == 0.0
    assert abs(out["outlier_precision_culprit_window"] - out["outlier_precision_own_window"]) < 1e-9
    assert out["outlier_precision_window"] > out["outlier_precision_own_window"]
    assert out["hold_server_held"] is None                  # the fixture's agents log no hold counters


def test_stage3_node_truth(chosen):
    """c (the culprit) and d share node-2; the window's outlier keeps fall on c and on its
    node-1 callers, so the node-level precision equals the single-culprit one here."""
    _, out = chosen
    assert out["culprit_node"] == "node-2" and "c" in out["culprit_node_services"]
    assert set(out["culprit_node_services"]) <= {"c", "d"}
    assert out["outlier_keeps_node_window"] == out["outlier_keeps_culprit_window"] > 0
    assert out["outlier_precision_node_window"] == pytest.approx(out["outlier_precision_culprit_window"])
    assert 0.0 < out["recall_own_window"] <= 1.0
