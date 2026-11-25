"""arrival_check.py and the parts of tables.py that read the arrival program's logs."""
import copy
import datetime
import json
import math
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import arrival_check as ac  # noqa: E402
import tables  # noqa: E402

NOW = 1_795_500_000.0


def stamp(ago):
    return datetime.datetime.fromtimestamp(NOW - ago, datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def pod(name, node, ready=True, restarts=0, deleting=False, containers=("otelcol-pb-es-ctr",)):
    return {"metadata": {"name": name, **({"deletionTimestamp": "x"} if deleting else {})}, "spec": {"nodeName": node, "containers": [{"name": c} for c in containers]},
            "status": {"phase": "Running", "containerStatuses": [{"ready": ready, "restartCount": restarts}]}}


def line(interfaces, ago=3.0, **kw):
    return json.dumps({"time": stamp(ago), "msg": "counters", "interfaces": interfaces, "ids": 10, "errors": 0, **kw})


DOC = {"items": [pod("composepost-service-pb-es-ctr-1", "node-1"), pod("urlshorten-service-pb-es-ctr-1", "node-1"), pod("wrk2api-service-pb-es-ctr-x", "node-1"),
                 pod("old-service-pb-es-ctr-1", "node-1", deleting=True), pod("tracepressure-service-pb-es-ctr-x", "node-3"), pod("media-service-pb-es-ctr-1", "node-2"),
                 pod("slobudget-arrival-a", "node-1"), pod("slobudget-arrival-b", "node-2"), pod("otelcol-pb-es-ctr-a", "node-1"), pod("otelcol-pb-es-ctr-b", "node-2"),
                 pod("post-db-pb-es-ctr-1", "node-3")]}
AGENT = ('2026 info x arrival table open {}\n2026 info x slobudget arrival {"hit": 98, "root": 50, "empty": 2}\n'
         '2026 info x slobudget arrival {"hit": 0, "root": 0}\n')       # an idle period after the load
LOGS = {"slobudget-arrival-a": '{"msg":"loaded"}\nplain\n[1]\n' + line(1, 30) + "\n" + line(2, 2), "slobudget-arrival-b": line(1),
        "otelcol-pb-es-ctr-a": AGENT, "otelcol-pb-es-ctr-b": AGENT}


def check(logs=None, doc=DOC, traffic=True):
    tees = {p["metadata"]["name"] for p in doc["items"] if len(p["spec"]["containers"]) > 1}

    def of(pod, container=None):        # with a tee in the pod it is the tee that stamps
        assert container == (("tee" if pod in tees else "otelcol-pb-es-ctr") if pod.startswith("otelcol") else None)
        return {**LOGS, **(logs or {})}[pod]
    return ac.problems(ac.pods_of(doc), of, NOW, traffic)


def test_expected_counts_grpc_service_pods():
    assert ac.expected(ac.pods_of(DOC)) == {"node-1": 2, "node-2": 1}


def test_all_is_well():
    assert check() == [] and check(traffic=False) == []


@pytest.mark.parametrize("logs, says", [
    ({"slobudget-arrival-b": line(1, 40)}, "40 s old"),
    ({"slobudget-arrival-b": line(0)}, "at 0 interfaces, 1 service pods"),
    ({"slobudget-arrival-b": line(1, ids=0)}, "found 0 ids"),
    ({"slobudget-arrival-b": line(1, errors=3)}, "3 errors"),
    ({"slobudget-arrival-b": ""}, "no counters"),
    ({"otelcol-pb-es-ctr-a": AGENT + "2026 warn x arrival table not open {}"}, "has not opened the table"),
    ({"otelcol-pb-es-ctr-a": "x"}, "has not opened the table"),
    ({"otelcol-pb-es-ctr-a": 'x arrival table open {}\nx slobudget arrival {"hit": 3, "no_table": 50}'}, "3 of the 53 lookups"),
    ({"otelcol-pb-es-ctr-a": "x arrival table open {}"}, "0 of the 0 lookups"),
])
def test_what_is_missing(logs, says):
    bad = check(logs)
    assert len(bad) == 1 and says in bad[0]


def test_campaign_run_with_a_tee():
    doc = copy.deepcopy(DOC)
    for p in doc["items"]:
        if p["metadata"]["name"].startswith("otelcol"):
            p["spec"]["containers"] = [{"name": "tee"}, {"name": "otelcol-pb-es-ctr"}]
    assert check(doc=doc) == []
    assert "has not opened the table" in check({"otelcol-pb-es-ctr-a": "x"}, doc=doc)[0]


def test_lookups_are_asked_for_with_traffic_only():
    assert check({"otelcol-pb-es-ctr-a": "x arrival table open {}"}, traffic=False) == []


def test_a_last_period_with_a_few_lookups():
    few = 'x slobudget arrival {"hit": 15, "not_stored": 2}\n'         # one split id among the load's last lookups
    good, old = 'x slobudget arrival {"hit": 1990, "empty": 8}\n', 'x slobudget arrival {"hit": 0, "no_table": 5000}\n'
    assert check({"otelcol-pb-es-ctr-a": "x arrival table open {}\n" + old + good + few}) == []
    assert "17 lookups" in check({"otelcol-pb-es-ctr-a": "x arrival table open {}\n" + few})[0]
    assert "5017 lookups" in check({"otelcol-pb-es-ctr-a": "x arrival table open {}\n" + good + old + few})[0]


def test_restart_missing_loader_no_pods():
    doc = copy.deepcopy(DOC)
    doc["items"][6]["status"]["containerStatuses"][0]["restartCount"] = 1
    assert "1 restarts" in check(doc=doc)[0]
    assert check(doc={"items": [p for p in DOC["items"] if not p["metadata"]["name"].startswith("slobudget-arrival")]}) == ["node-1: no arrivald", "node-2: no arrivald"]
    assert check(doc={"items": []}) == ["no service pods"]


def test_age():
    assert ac.age(stamp(7.25), NOW) == pytest.approx(7.25, abs=1e-3)
    assert ac.age("2026-11-23T22:43:01.123456789-06:00", 0) == pytest.approx(ac.age("2026-11-24T04:43:01.123456Z", 0), abs=1e-3)
    for bad in ("", "yesterday", "2026-11-23"):
        with pytest.raises(ValueError):
            ac.age(bad, 0)


def test_arrival_share(tmp_path):
    assert math.isnan(tables.arrival_share(str(tmp_path)))
    (tmp_path / "arrival-agent-node-1.log").write_text('2026 x slobudget arrival {"period":1,"hit":90,"root":7,"empty":5,"overwritten":5}\n2026 x slobudget arrival {"hit":100}\n')
    assert tables.arrival_share(str(tmp_path)) == pytest.approx(0.95)


def test_program_cost_inside_the_load(tmp_path):
    t0 = NOW - 100
    lines = ["not json", line(3, 200, run_ns=10 ** 9, runs=1)] + [line(3, s, run_ns=3_000_000, runs=1500) for s in (90, 85, 80)] + [line(3, -50, run_ns=10 ** 9, runs=1)]
    for n in (1, 2):
        (tmp_path / f"arrival-node-{n}.log").write_text("\n".join(lines) + "\n")
    (tmp_path / "arrival-node-2.previous.log").write_text("\n".join(lines) + "\n")
    assert all(math.isnan(x) for x in tables.program(str(tmp_path), t0, NOW))      # one node's log is missing
    (tmp_path / "arrival-node-3.log").write_text("\n".join(lines) + "\n")
    cores, ns = tables.program(str(tmp_path), t0, NOW)
    assert cores == pytest.approx(3 * 9e6 / 1e9 / 95) and ns == pytest.approx(2000)


def test_node_cpu_over_its_own_interval(tmp_path):
    for name, x in (("cpu-start.json", 1), ("cpu-end.json", 3)):
        (tmp_path / name).write_text(json.dumps({"t": x * 10.0, "containers": {"default/slobudget-arrival-x/arrivald": {"cpu_ns": x * 10 ** 9, "t": 1.0}},
                                                 "nodes": {k: {"cpu_ns": x * 10 ** 10, "t": x * 8.0} for k in ("node-0", "node-1", "node-2", "node-3")}}))
    groups, secs = tables.cpu_by_group(str(tmp_path))
    assert groups == {"arrivald": 2.0, "node_cores": pytest.approx(3 * 20 / 16)} and secs == 20.0
    (tmp_path / "cpu-start.json").write_text(json.dumps({"t": 10.0, "containers": {}}))         # a snapshot from before the nodes were taken
    assert tables.cpu_by_group(str(tmp_path))[0] == {}


def test_a_row_per_e_run(tmp_path, capsys, monkeypatch):
    t0 = NOW - 300
    rd = tmp_path / "E-r100-1"
    rd.mkdir()
    (rd / "collected").write_text("")
    (rd / "meta.json").write_text(json.dumps({"config": "E", "rate": 100, "rep": 1, "t0": t0, "t1": NOW}))
    (rd / "wrk.log").write_text("  50.000%   10.00ms\n  99.000%  20.00ms\n30000 requests in 5.00m\nRequests/sec:  100.00\n")
    for name, t in (("cpu-start.json", t0), ("cpu-end.json", NOW)):
        (rd / name).write_text(json.dumps({"t": t, "containers": {}, "nodes": {}}))
    iso = lambda t: datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    for n in (1, 2, 3):
        bad = 400 if n == 3 else 2
        (rd / f"arrival-agent-node-{n}.log").write_text(f'{iso(t0 - 20)}\tinfo\tx\tslobudget arrival\t{{"hit": 0, "empty": 999}}\n{iso(t0 + 9)}\tinfo\tx\tslobudget arrival\t{{"hit": 1000, "empty": {bad}}}\n')
        (rd / f"arrival-node-{n}.log").write_text(line(4, 200, packets=50, run_ns=100_000, runs=40) + "\n")
    monkeypatch.setattr(sys, "argv", ["x", str(tmp_path)])
    assert tables.main() == 0
    out = capsys.readouterr().out
    assert "| E-r100-1 | **FLAG** 88.13 % | empty 404 | 71.43 % |" in out and "4 pods, packets 50, ids 10; 2,500 ns a run" in out


def test_frequency_and_differences(tmp_path, capsys, monkeypatch):
    t0 = NOW - 300

    def run(cfg, rep, p99, app_ns, ghz=None, invalid=None):
        rd = tmp_path / f"{cfg}-r100-{rep}"
        rd.mkdir()
        (rd / "collected").write_text("")
        (rd / "meta.json").write_text(json.dumps({"config": cfg, "rate": 100, "rep": rep, "t0": t0, "t1": NOW}))
        (rd / "wrk.log").write_text(f"  50.000%   10.00ms\n  99.000%  {p99:.2f}ms\n30000 requests in 5.00m\nRequests/sec:  100.00\n")
        for name, t, ns in (("cpu-start.json", t0, 0), ("cpu-end.json", NOW, app_ns)):
            (rd / name).write_text(json.dumps({"t": t, "containers": {"default/a-service-pb-es-ctr-x/a-service-pb-es-ctr": {"cpu_ns": ns, "t": t}}, "nodes": {}}))
        if ghz:
            for n in ("node-1", "node-2", "node-3"):
                (rd / f"freq-{n}.txt").write_text("".join(f"{t0 + s} {ghz * 1e6:.0f} {ghz * 1e6 + 2e5:.0f}\n" for s in (-10, 5, 100, 200, 400)))
        if invalid:
            (rd / "INVALID").write_text(invalid)
    run("B", 1, 13.0, 30e9, 2.3); run("B", 2, 14.0, 31e9, 2.3)
    run("C", 1, 14.0, 36e9, 2.5); run("C", 2, 16.0, 36e9)
    run("F", 1, 14.5, 33e9, 2.5); run("E", 1, 15.0, 37e9, 2.5, invalid="a test")
    monkeypatch.setattr(sys, "argv", ["x", str(tmp_path)])
    assert tables.main() == 0
    out = capsys.readouterr().out
    assert "| C - B | 100 | 2 | +0.00 (+0.00-+0.00) | +1.50 (+1.00-+2.00) |" in out and "| C - F | 100 | 1 | +0.00 | -0.50 |" in out and "| E - C |" not in out
    b = [ln for ln in out.splitlines() if ln.startswith("| B: ")][0]
    assert b.startswith("| B: SDK head sampling 12 % | 100 | 2 |") and b.endswith("| 2.40 (2.40-2.40) |")            # GHz: the mean over the CPUs, 2.3 and 2.5 GHz
    assert "Left out as invalid: E-r100-1 (a test)" in out and "No frequency data" in out and "C-r100-2" in out.split("No frequency data")[1]
