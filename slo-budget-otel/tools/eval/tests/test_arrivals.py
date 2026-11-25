"""The estimated kernel receive time, the stamp, and the truth on them (sloeval/arrivals.py)."""
import json
import os
import sys

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, HERE)
SIM = os.path.abspath(os.path.join(HERE, "..", "..", "..", "..", "slo-budget-sim"))
import arrival_estimate  # noqa: E402
from sloeval import arrivals  # noqa: E402
from sloeval.oracle import import_sim, load_callgraph  # noqa: E402
from sloeval.spans import read_shadow  # noqa: E402

T0 = 1_795_500_000
MS = 10 ** 6
NET = {"node-1": 0.4, "node-2": 0.9}        # send to arrival, by the root's host, ms
HANDLER = 0.3                               # arrival to handler start, healthy, ms
CLOCK = 0.2                                 # back's node clock ahead of both, ms


def attrs(**kw):
    out = []
    for k, v in kw.items():
        out.append({"key": k.replace("__", "."), "value": {"intValue": str(v)} if isinstance(v, int) else {"stringValue": v}})
    return out


def shadow(tmp_path, n=2000, stamped=True, misses=0.01, seed=1):
    """front (on two nodes, the root) calls back (node-3): 100 req/s, a fault in seconds 12-16: back's handler waits 8 ms more."""
    rng = np.random.default_rng(seed)
    lines = []
    real = []
    for i in range(n):
        host = "node-1" if i % 3 else "node-2"
        s = T0 * 10 ** 9 + int(i * 10 * MS)
        late = 8.0 if 12 <= i / 100 < 16 else 0.0
        send = s + int(0.5 * MS)
        wait = HANDLER + rng.exponential(0.1) + late
        arrive = send + int((NET[host] + rng.exponential(0.02) + CLOCK) * MS)
        start = arrive + int(wait * MS)
        end = start + int(1.0 * MS)
        cend = end + int(0.4 * MS) - int(CLOCK * MS)
        hit = stamped and rng.random() >= misses
        a = attrs(slobudget__arrival_ns=arrive if hit else start, slobudget__arrival_source="ebpf" if hit else "start") if stamped else []
        real.append((hit, arrive, start))
        tid = f"{i + 1:032x}"
        sp = lambda sid, pid, kind, b, e, extra=(): {"traceId": tid, "spanId": f"{sid:016x}", "parentSpanId": f"{pid:016x}" if pid else "", "kind": kind, "name": "x",
                                                      "startTimeUnixNano": str(b), "endTimeUnixNano": str(e), "attributes": attrs(slo__root_host=host, slo__root="r") + list(extra)}
        root, cl, sv = 3 * i + 1, 3 * i + 2, 3 * i + 3
        ra = attrs(slobudget__arrival_ns=s, slobudget__arrival_source="start") if stamped else []
        lines.append(json.dumps({"resourceSpans": [
            {"resource": {"attributes": attrs(service__name="unknown_service:front_service_pb_es_proc")}, "scopeSpans": [{"spans": [sp(root, 0, 2, s, cend + int(0.2 * MS), ra), sp(cl, root, 3, send, cend)]}]},
            {"resource": {"attributes": attrs(service__name="unknown_service:back_service_pb_es_proc")}, "scopeSpans": [{"spans": [sp(sv, cl, 2, start, end, a)]}]}]}))
    p = tmp_path / "shadow.jsonl"
    p.write_text("\n".join(lines))
    t = read_shadow(str(p))
    meta = {"run_id": "x", "scenario": "saturated", "arm": "chosen", "t0": T0, "t1": T0 + n / 100, "fault": {"start": T0 + 12, "end": T0 + 16, "culprit": "back"}, "margin": 0.1,
            "placement": {"back": "node-3", "front": "node-1"}, "placement_nodes": {"front": ["node-1", "node-2"], "back": ["node-3"]}, "slo_ms": {"r": 12.0}}
    s = t.start / 1e9
    return t, meta, (s >= T0) & (t.end / 1e9 < T0 + 12), real


def graph(tmp_path):
    p = tmp_path / "g.json"
    p.write_text(json.dumps({"root": {"r": "front"}, "children": {"r": {"front": ["back"], "back": []}}, "parallel": {}}))
    CallGraph, Calibrator = import_sim(SIM)
    return load_callgraph(str(p), CallGraph), Calibrator


def test_reader(tmp_path):
    t, meta, healthy, real = shadow(tmp_path, n=50)
    sv = np.flatnonzero(t.svc == t.services.index("back"))
    assert [bool(t.arr_ebpf[i]) for i in sv] == [h for h, _, _ in real]
    assert all(t.arrival[i] == (a if h else s) for i, (h, a, s) in zip(sv, real))
    assert sorted(t.hosts) == ["node-1", "node-2"] and (t.arrival[t.kind == 3] == 0).all()
    plain, _, _, _ = shadow(tmp_path, n=5, stamped=False)
    assert (plain.arrival == 0).all() and not plain.arr_ebpf.any()


def test_edges_and_estimate(tmp_path):
    t, meta, healthy, real = shadow(tmp_path)
    k = arrivals.calls(t, meta)
    assert sorted(k.names) == ["front>back [node-1->node-3]", "front>back [node-2->node-3]"] and len(k.v) == 2000
    med = arrivals.edge_medians(k, healthy)
    for e, name in enumerate(k.names):          # network + clock offset + the handler's median wait
        host = name[name.index("[") + 1:name.index("->")]
        assert med[e] / MS == pytest.approx(NET[host] + CLOCK + HANDLER + 0.07 + 0.014, abs=0.03)
    w = arrivals.estimated_wait(t, meta, healthy) / MS
    assert (w[t.kind == 3] == 0).all() and (w[t.is_root] == 0).all()
    fault = (t.start / 1e9 >= T0 + 12.1) & (t.start / 1e9 < T0 + 15.9) & (t.svc == t.services.index("back"))
    assert np.median(w[fault]) == pytest.approx(8.0, abs=0.1) and np.median(w[healthy & (t.svc == t.services.index("back"))]) < 0.05
    few, meta2, h2, _ = shadow(tmp_path, n=30)          # under MIN_CALLS on one edge: no estimate there
    w2 = arrivals.estimated_wait(few, meta2, h2)
    assert (w2[[v for v, c in zip(arrivals.calls(few, meta2).v, arrivals.calls(few, meta2).edge) if arrivals.calls(few, meta2).names[c].startswith("front>back [node-2")]] == 0).all()


def test_stamp_and_comparison(tmp_path):
    t, meta, healthy, real = shadow(tmp_path)
    w = arrivals.stamped_wait(t)
    sv = np.flatnonzero(t.svc == t.services.index("back"))
    for i, (h, a, s) in list(zip(sv, real))[:200]:
        assert w[i] == (s - a if h else 0)
    g, Calibrator = graph(tmp_path)
    r = arrivals.compare(t, meta, healthy, t.own, g, Calibrator, {"r": 0.012})
    assert r["share_stamped"] == pytest.approx(0.99, abs=0.01) and r["calls"] == 2000
    for e in r["edges"].values():
        d = e["healthy"]["estimate_minus_stamp_ms"]         # the estimate is the stamp plus the handler's median wait, the clocks' offset cancels
        assert d["p50"] == pytest.approx(HANDLER + 0.07, abs=0.03) and abs(d["p95"] - d["p5"]) < 0.2
        c = e["window"]["around_it_ms"]                      # in the fault the network is the same: nothing around it
        assert abs(e["healthy"]["around_it_ms"]["p50"]) < 1e-9 and abs(c["p50"]) < 0.05 and e["window"]["share_off_by_over_1ms"] == 0.0
        assert e["healthy"]["calls"] - e["healthy"]["stamped"] in range(0, 30) and e["window"]["calls"] >= e["window"]["stamped"] > 100
        assert e["window"]["wait_stamp_ms"]["p50"] == pytest.approx(8.37, abs=0.1) and e["window"]["wait_estimate_ms"]["p50"] == pytest.approx(8.0, abs=0.1)
    f = r["all"]["window"]
    assert r["window"] == "fault" and json.loads(json.dumps(r, allow_nan=False)) == r
    assert f["over_by_both"] > 350 and f["over_by_the_estimate_only"] + f["over_by_the_stamp_only"] <= 2
    assert r["all"]["healthy"]["over_by_both"] == 0
    other = {**r, "scenario": "steady", "culprit": None, "window": None, "run_id": "y"}
    text = arrival_estimate.report([r, r, other])
    assert "front>back [node-1->node-3]" in text and "3 runs" in text and "| back |" in text and "## saturated, window: the fault, culprit back: x, x" in text and "## steady: y" in text
    lost = t.arr_ebpf.copy()                            # an edge whose calls have no stamp stays in the report
    t.arr_ebpf[(t.svc == t.services.index("back")) & (t.rhost == t.hosts.index("node-2"))] = False
    r2 = arrivals.compare(t, meta, healthy, t.own, g, Calibrator, {"r": 0.012})
    t.arr_ebpf = lost
    e2 = r2["edges"]["front>back [node-2->node-3]"]
    assert e2["healthy"]["stamped"] == 0 and e2["healthy"]["calls"] > 300 and e2["healthy"]["estimate_minus_stamp_ms"] == {"n": 0}
    assert "(0.00 %)" in arrival_estimate.report([r2])


def test_truth_counts_from_the_arrival(tmp_path):
    t, meta, healthy, _ = shadow(tmp_path)
    g, Calibrator = graph(tmp_path)
    x, b = arrivals.within_service(t, arrivals.estimated_wait(t, meta, healthy), t.own, healthy, g, Calibrator, {"r": 0.012})
    back = t.svc == t.services.index("back")
    fault = (t.start / 1e9 >= T0 + 12.1) & (t.start / 1e9 < T0 + 15.9)
    assert (x[back & fault] > b[back & fault]).all() and not (x[back & healthy] > b[back & healthy]).any()
    own = t.own / 1e9                                       # the handler's own time sees nothing
    from sloeval.oracle import build_oracle, lookup_arrays
    _, b0 = lookup_arrays(t, build_oracle(t, healthy, g, Calibrator, {"r": 0.012}))
    assert not (own[back & fault] > b0[back & fault]).any()


def test_a_stamped_run_keeps_every_other_number(tmp_path):
    import fixture
    from sloeval.evaluate import evaluate
    run = fixture.make_run(str(tmp_path), arm="chosen")
    cg = os.path.join(run, "callgraph.json")
    same = lambda a, b: (a != a and b != b) if isinstance(a, float) and isinstance(b, float) and a != a else a == b
    plain = evaluate(run, SIM, cg, recon_bin=None, do_recon=False)
    assert "arrival_estimate_vs_stamp" not in plain and plain["ws_callgraph"] == plain["truth_callgraph"] == "callgraph.json" and not os.path.exists(os.path.join(run, "arrival-vs-estimate.json"))
    shadow = os.path.join(run, "shadow.jsonl")
    lines = []
    for ln in open(shadow, encoding="utf-8"):
        d = json.loads(ln)
        for rs in d["resourceSpans"]:
            for sp in rs["scopeSpans"][0]["spans"]:
                if sp["kind"] == 2:
                    back = 0 if not sp["parentSpanId"] else 40_000
                    sp["attributes"] += attrs(slobudget__arrival_ns=int(sp["startTimeUnixNano"]) - back, slobudget__arrival_source="ebpf" if back else "start")
        lines.append(json.dumps(d))
    open(shadow, "w", encoding="utf-8").write("\n".join(lines) + "\n")
    stamped = evaluate(run, SIM, cg, recon_bin=None, do_recon=False)
    assert all(same(plain[k], stamped[k]) for k in plain if k not in ("timing", "peak_rss_mb")), [k for k in plain if not same(plain[k], stamped[k])]
    v = stamped["arrival_estimate_vs_stamp"]
    assert set(stamped) - set(plain) == {"arrival_estimate_vs_stamp"} and v["share_stamped"] == 1.0 and v["server_spans"] == v["server_spans_with_the_attribute"] == 2400 and v["calls"] == 1800
    assert v["healthy"]["around_it_ms"]["p50"] == 0.0 and v["healthy"]["estimate_minus_stamp_ms"]["p50"] == pytest.approx(0.04, abs=1e-6)      # the edge's median takes the handler's wait for travel
    with open(os.path.join(run, "arrival-vs-estimate.json"), encoding="utf-8") as f:
        cmp = json.load(f)
    assert len(cmp["edges"]) == 3 and cmp["all"]["window"]["estimate_minus_stamp_ms"]["n"] > 0
    assert "c" in arrival_estimate.report([cmp])


def test_the_burst_is_a_window_too(tmp_path):
    t, meta, healthy, _ = shadow(tmp_path)
    g, Calibrator = graph(tmp_path)
    f = meta.pop("fault")
    meta["burst"] = {"start": f["start"], "end": f["end"]}
    r = arrivals.compare(t, meta, healthy, t.own, g, Calibrator, {"r": 0.012})
    assert r["window"] == "burst" and r["culprit"] is None and r["all"]["window"]["calls"] == 400
    del meta["burst"]
    r = arrivals.compare(t, meta, healthy, t.own, g, Calibrator, {"r": 0.012})
    assert r["window"] is None and r["all"]["window"]["calls"] == 0 and r["all"]["window"]["share_off_by_over_1ms"] is None
    json.dumps(r, allow_nan=False)


def test_the_drift_has_the_centrals_graph(tmp_path, monkeypatch):
    """The old truths keep one graph for every run; the table the drift compares with the central's has the central's."""
    import fixture
    from sloeval import runinfo
    from sloeval.evaluate import evaluate
    run = fixture.make_run(str(tmp_path), arm="chosen")
    dep = tmp_path / "deploy"
    dep.mkdir()
    g = dict(fixture.CALLGRAPH)
    (dep / "callgraph.json").write_text(json.dumps(g))
    (dep / "callgraph.stages.json").write_text(json.dumps({**g, "stages": {"r": {"b": [["c", "d"]]}}}))
    monkeypatch.setattr(runinfo, "DEPLOY", str(dep))
    cg = str(dep / "callgraph.json")
    plain = evaluate(run, SIM, cg, recon_bin=None, do_recon=False)
    mp = os.path.join(run, "meta.json")
    m = json.load(open(mp))
    json.dump({**m, "callgraph": "callgraph.stages.json"}, open(mp, "w"))
    staged = evaluate(run, SIM, cg, recon_bin=None, do_recon=False)
    drift = [k for k in plain if plain[k] != staged[k] and not (plain[k] != plain[k] and staged[k] != staged[k]) and k not in ("timing", "peak_rss_mb")]
    assert drift and all(k.startswith("t_drift") or k == "oracle_T_ms" for k in drift), drift
    assert plain["ws_callgraph"] == staged["ws_callgraph"] == "callgraph.stages.json" and staged["truth_callgraph"] == "callgraph.json"


def test_the_drift_has_the_runs_measure(tmp_path):
    """A run whose central counted from the arrivals: the drift's table and the split of the lost outliers count from them too, and nothing else does."""
    import fixture
    from sloeval.evaluate import evaluate
    run = fixture.make_run(str(tmp_path), arm="chosen")
    cg = os.path.join(run, "callgraph.json")
    shadow, lines = os.path.join(run, "shadow.jsonl"), []
    for ln in open(shadow, encoding="utf-8"):
        d = json.loads(ln)
        for rs in d["resourceSpans"]:
            for sp in rs["scopeSpans"][0]["spans"]:
                if sp["kind"] == 2 and sp["parentSpanId"]:
                    sp["attributes"] += attrs(slobudget__arrival_ns=int(sp["startTimeUnixNano"]) - 700_000, slobudget__arrival_source="ebpf")
        lines.append(json.dumps(d))
    open(shadow, "w", encoding="utf-8").write(chr(10).join(lines))
    plain = evaluate(run, SIM, cg, recon_bin=None, do_recon=False)
    mp = os.path.join(run, "meta.json")
    m = json.load(open(mp))
    json.dump({**m, "sampler_env": {"own_measure": "within_service"}}, open(mp, "w"))
    within = evaluate(run, SIM, cg, recon_bin=None, do_recon=False)
    differ = {k for k in plain if plain[k] != within[k] and not (plain[k] != plain[k] and within[k] != within[k]) and k not in ("timing", "peak_rss_mb")}
    assert {"oracle_T_ms", "t_drift_pre"} <= differ and all(k.startswith(("t_drift", "ref_", "loss", "refusal")) or k == "oracle_T_ms" for k in differ), differ


def test_services_covered_and_where_the_volume_went(tmp_path):
    import fixture
    from sloeval.evaluate import evaluate
    run = fixture.make_run(str(tmp_path), arm="chosen")
    r = evaluate(run, SIM, os.path.join(run, "callgraph.json"), recon_bin=None, do_recon=False)
    # the fault: 150 traces in 3 periods (the last one's spans end in the fourth); c is the only service with true outliers, all but one kept
    assert r["ws_services_with_an_outlier_per_period"] == 1.0 and r["ws_services_covered"] == 1.0 and r["ws_services_covered_per_period"] == 1.0
    kept_c, kept_b, kept_a = 149, 50, 150
    assert r["ws_kept_window"] == pytest.approx(kept_a + kept_b + kept_c, abs=2)
    assert r["ws_kept_culprit_outliers"] == pytest.approx(149 / r["ws_kept_window"], abs=0.005) and r["ws_kept_other_outliers"] == 0.0
    assert r["ws_kept_culprit_outliers"] + r["ws_kept_other_outliers"] + r["ws_kept_ordinary"] == pytest.approx(1.0)
    assert r["ws_culprit_kept_per_period"] == pytest.approx(149 / 3, abs=0.4) and r["ws_culprit_kept_as_outlier_per_period"] == r["ws_culprit_kept_per_period"]
    assert r["ws_culprit_outliers_per_period"] == pytest.approx(50, abs=0.4) and r["n_need_window"] == 10.0
    head = evaluate(fixture.make_run(str(tmp_path), arm="head"), SIM, os.path.join(run, "callgraph.json"), recon_bin=None, do_recon=False)
    assert "n_need_window" not in head and "ws_culprit_kept_as_outlier_per_period" not in head and head["ws_services_covered"] == 1.0
    # no culprit: the shares of the others only
    mp = os.path.join(run, "meta.json")
    m = json.load(open(mp))
    m["fault"]["culprit"], m["scenario"] = None, "late_everywhere"
    json.dump(m, open(mp, "w"))
    r = evaluate(run, SIM, os.path.join(run, "callgraph.json"), recon_bin=None, do_recon=False)
    assert r["ws_kept_culprit_outliers"] != r["ws_kept_culprit_outliers"] and r["ws_kept_other_outliers"] > 0.4 and r["ws_services_covered"] == 1.0
    assert "ws_culprit_kept_per_period" not in r


def test_report_tool_without_a_stamp(tmp_path, capsys):
    sys.argv = ["x", str(tmp_path)]
    assert arrival_estimate.main() == 1 and "no arrival-vs-estimate.json" in capsys.readouterr().out
