"""The parts of the campaign harness for runs with the arrival stamp: sanity.py's check, render.py's settings, the queue lines."""
import datetime
import json
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import make_queue2  # noqa: E402
import sanity  # noqa: E402

T0 = 1_795_500_000.0


def line(t, **kw):
    ts = datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return f"{ts}\tinfo\tslobudgetprocessor/processor.go:1\tslobudget arrival\t{json.dumps({'period': 1, **kw})}\n"


def run_dir(tmp_path, lines, **meta):
    for node, text in lines.items():
        (tmp_path / f"{'tee' if meta.get('tee') else 'agent'}-{node}.log").write_text("".join(text))
    return str(tmp_path), {"t0": T0, "t1": T0 + 600, "arrival_pin": "/sys/fs/bpf/slobudget", **meta}


GOOD = [line(T0 - 20, hit=0, empty=500), line(T0 + 5, hit=980, root=100, empty=5, not_stored=3), line(T0 + 10, hit=990, root=90, overwritten=1), line(T0 + 700, hit=0, empty=99)]


def test_arrivals_of_the_load_only(tmp_path):
    assert sanity.arrival_checks(*run_dir(tmp_path, {"node-1": GOOD, "node-2": GOOD})) == []


@pytest.mark.parametrize("bad, says", [([line(T0 + 5, hit=900, empty=100)], "900 of 1000"), ([line(T0 + 5, hit=0, root=40)], "0 of 0"), ([], "0 of 0"),
                                       ([line(T0 + 5, hit=1000, bpf_errors=2)], "2 errors"), ([line(T0 + 5, hit=10, no_table=990)], "10 of 1000")])
def test_what_is_missing(tmp_path, bad, says):
    probs = sanity.arrival_checks(*run_dir(tmp_path, {"node-1": GOOD, "node-2": bad}))
    assert len(probs) == 1 and probs[0].startswith("agent-node-2.log") and says in probs[0]


def test_no_logs(tmp_path):
    assert "no logs of the stamp" in sanity.arrival_checks(*run_dir(tmp_path, {}))[0]


def test_the_tee_stamps_in_a_campaign_run(tmp_path):
    (tmp_path / "agent-node-1.log").write_text(line(T0 + 5, hit=0, no_table=1000))          # the agent has no table of its own there
    assert sanity.arrival_checks(*run_dir(tmp_path, {"node-1": GOOD, "node-2": GOOD}, tee=True)) == []


def test_clock_step(tmp_path):
    lines = {"node-1": [line(T0 + 5, hit=1000, clock_step=0.2)]}
    assert sanity.arrival_checks(*run_dir(tmp_path, lines, sampler_env={"own_measure": None})) == []
    probs = sanity.arrival_checks(*run_dir(tmp_path, lines, sampler_env={"own_measure": "within_service"}))
    assert len(probs) == 1 and "stepped by 200.0 ms" in probs[0]
    assert sanity.arrival_checks(*run_dir(tmp_path, {"node-1": [line(T0 + 5, hit=1000, clock_step=-2e-5)]}, sampler_env={"own_measure": "within_service"})) == []


def render(*args, **env):
    e = {k: v for k, v in os.environ.items() if k not in ("OWN_MEASURE", "OUTLIER_TRIGGER", "OWN_MARGIN", "SERVER_HOLD")}
    return subprocess.run([sys.executable, os.path.join(HERE, "..", "render.py"), "--volume", "0.12", "--image", "IMG", *args], capture_output=True, text=True, env={**e, **env})


def test_render_own_measure():
    on = render("--arm", "chosen", "--arrival", "PIN", "--no-tee", OWN_MEASURE="within_service")
    assert on.returncode == 0 and on.stdout.count("own_measure: within_service") == 2       # agents and central
    assert "arrival_table: PIN" in on.stdout and "kind: DaemonSet\nmetadata:\n  name: slobudget-arrival" in on.stdout and '"1048576"' in on.stdout
    assert "slobudgetarrival" not in on.stdout and on.stdout.count("capabilities: {add: [BPF]}") == 1
    # with a tee: the stamp is in front of its fork, once, and the agent has no table of its own
    tee = render("--arm", "chosen", "--arrival", "PIN", OWN_MEASURE="within_service")
    assert tee.returncode == 0 and tee.stdout.count("own_measure: within_service") == 2 and tee.stdout.count("arrival_table: PIN") == 1
    t = tee.stdout
    assert t.count("slobudgetarrival:") == 1 and "processors: [slobudgetarrival]" in t and "exporters: [otlp/agent, otlp/shadow]" in t
    assert t.count("capabilities: {add: [BPF]}") == 1 and t.index("capabilities: {add: [BPF]}") < t.index("- name: otelcol-pb-es-ctr")     # the tee's container comes first
    assert t.count("name: bpffs, mountPath: /sys/fs/bpf, readOnly: true") == 1
    cfg = render("--arm", "chosen", "--arrival", "PIN", "--print", "tee").stdout
    assert "slobudgetarrival" in cfg and "traces/agent" not in cfg
    assert "slobudgetarrival" not in render("--arm", "chosen", "--print", "tee").stdout
    assert render("--arm", "chosen", OWN_MEASURE="within_service").returncode != 0          # without the arrivals it would silently be handler
    plain = render("--arm", "chosen")
    assert plain.returncode == 0 and "own_measure" not in plain.stdout and "arrival" not in plain.stdout
    assert render("--arm", "head", "--arrival", "PIN").returncode != 0
    assert "own_measure" not in render("--arm", "head", OWN_MEASURE="within_service").stdout


def test_queue_lines():
    j = make_queue2.jobs("T")
    assert [len(j[k]) for k in "abc"] == [57, 21, 21]
    assert all(sorted(ln.split()[2:4] for ln in j[k] if " noisy_neighbour " in ln) == [["0.12", "1"], ["0.12", "2"], ["0.12", "3"]] for k in "abc")
    lines = sum(j.values(), [])
    ids = [w[7:] for ln in lines for w in ln.split() if w.startswith("RUN_ID=")]
    assert len(set(ids)) == len(lines)
    for ln in lines:            # nothing can land in a run of the campaign: v<pct>-<scenario>-<arm>-s<seed>
        arm, sc, vol, seed = ln.split()[:4]
        assert f"v{round(float(vol) * 100)}-{sc}-{arm}-s{seed}" not in ids and "VARIANT=" in ln and "CALLGRAPH=deploy/callgraph.stages.json" in ln
    assert all("ARRIVAL=1 OWN_MEASURE=within_service" in ln for ln in j["a"] + j["c"]) and not any("ARRIVAL" in ln or "OWN_MEASURE" in ln for ln in j["b"])
    # c is a's 12 % runs with the arm threshold (no budget, no pool, no N_need) and nothing else changed
    a12 = {tuple(ln.split()[1:]) for ln in j["a"] if ln.split()[2] == "0.12"}
    for ln in j["c"]:
        w = ln.split()
        same = [x.replace("-threshold-ebpf-", "-ebpf-") if x.startswith("RUN_ID=") else x for x in w[1:]]
        assert w[0] == "threshold" and tuple(same) in a12


def test_a_partial_export_is_a_problem(tmp_path):
    meta = {"scenario": "steady", "rps": 100, "dur_s": 600}
    good = {"vol_ratio": 1.0, "shadow_spans": 1_380_000, "volume_source": "gate", "kept_per_period": 1380.0, "gate_periods": 119, "export_spans": 165_000}

    def probs(**kw):
        (tmp_path / "summary.json").write_text(json.dumps({**good, **kw}))
        return sanity.summary_checks(str(tmp_path), meta)
    assert probs() == [] and probs(volume_source="export", export_spans=10) == []
    for n in (0, 120_000):
        assert len(probs(export_spans=n)) == 1 and f"has {n} spans, the central admitted about 164220" in probs(export_spans=n)[0]


def test_a_failed_chunk_fails_the_export(tmp_path, monkeypatch, capsys):
    sys.path.insert(0, os.path.join(HERE, "..", "..", "..", "..", "slo-budget-sim", "tools"))
    import jaeger_export
    monkeypatch.setattr(jaeger_export.time, "sleep", lambda s: None)
    answers = {"a": [{"traceID": "1", "spans": [1, 2]}], "b": None}

    def urlopen(url, timeout):
        svc = "a" if "service=a" in url else "b"
        if answers[svc] is None:
            raise OSError("no answer")
        import io
        return io.BytesIO(json.dumps({"data": answers[svc]}).encode())
    monkeypatch.setattr(jaeger_export.urllib.request, "urlopen", urlopen)
    argv = ["x", "--base", "http://j", "--service", "a,b", "--start", "0", "--end", "60", "--out", str(tmp_path / "e.json")]
    monkeypatch.setattr(sys, "argv", argv)
    assert jaeger_export.main() == 1 and capsys.readouterr().out.splitlines()[-1] == "FAILED: b, 0-60: no answer"
    assert not (tmp_path / "e.json").exists()
    answers["b"] = []
    assert jaeger_export.main() == 0 and len(json.load(open(tmp_path / "e.json"))["data"]) == 1
