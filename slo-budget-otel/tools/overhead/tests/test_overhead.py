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


def pod(name, node, ready=True, restarts=0, deleting=False):
    return {"metadata": {"name": name, **({"deletionTimestamp": "x"} if deleting else {})}, "spec": {"nodeName": node},
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
    return ac.problems(ac.pods_of(doc), {**LOGS, **(logs or {})}.get, NOW, traffic)


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


def test_lookups_are_asked_for_with_traffic_only():
    assert check({"otelcol-pb-es-ctr-a": "x arrival table open {}"}, traffic=False) == []


def test_restart_missing_loader_no_pods():
    doc = copy.deepcopy(DOC)
    doc["items"][6]["status"]["containerStatuses"][0]["restartCount"] = 1
    assert "1 restarts" in check(doc=doc)[0]
    assert check(doc={"items": [p for p in DOC["items"] if not p["metadata"]["name"].startswith("slobudget-arrival")]}) == ["node-1: no arrivald", "node-2: no arrivald"]
    assert check(doc={"items": []}) == ["no service pods"]


def test_age():
    assert ac.age(stamp(7.25), NOW) == pytest.approx(7.25, abs=1e-3)
    assert ac.age("2026-11-23T22:43:01.123456789-06:00", 0) == pytest.approx(ac.age("2026-11-24T04:43:01.123456Z", 0))
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
    (tmp_path / "arrival-node-1.log").write_text("\n".join(lines) + "\n")
    cores, ns = tables.program(str(tmp_path), t0, NOW)
    assert cores == pytest.approx(9e6 / 1e9 / 95) and ns == pytest.approx(2000)


def test_node_cpu_over_its_own_interval(tmp_path):
    for name, x in (("cpu-start.json", 1), ("cpu-end.json", 3)):
        (tmp_path / name).write_text(json.dumps({"t": x * 10.0, "containers": {"default/slobudget-arrival-x/arrivald": {"cpu_ns": x * 10 ** 9, "t": 1.0}},
                                                 "nodes": {k: {"cpu_ns": x * 10 ** 10, "t": x * 8.0} for k in ("node-0", "node-1", "node-2", "node-3")}}))
    groups, secs = tables.cpu_by_group(str(tmp_path))
    assert groups == {"arrivald": 2.0, "node_cores": pytest.approx(3 * 20 / 16)} and secs == 20.0
    (tmp_path / "cpu-start.json").write_text(json.dumps({"t": 10.0, "containers": {}}))         # a snapshot from before the nodes were taken
    assert tables.cpu_by_group(str(tmp_path))[0] == {}
