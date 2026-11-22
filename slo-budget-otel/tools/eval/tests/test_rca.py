"""Stage 3 root-cause ranking: the vendored RCAEval methods and the adapter (sloeval/rca.py)."""
from __future__ import annotations

import os
import random
import sys

import numpy as np
import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("tqdm")
pytest.importorskip("dateutil")

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from rcaeval import microrank as mr  # noqa: E402
from sloeval import rca  # noqa: E402
from sloeval.spans import SERVER, CLIENT, SpanTable  # noqa: E402

pageRank = mr.pageRank


# RCAEval a30feeb RCAEval/e2e/microrank.py trace_pagerank, verbatim: the reference for the patch.
def trace_pagerank_original(operation_operation, operation_trace, trace_operation, pr_trace, anomaly):
    operation_length = len(operation_operation)
    trace_length = len(operation_trace)

    p_ss = np.zeros((operation_length, operation_length), dtype=np.float32)
    p_sr = np.zeros((operation_length, trace_length), dtype=np.float32)
    p_rs = np.zeros((trace_length, operation_length), dtype=np.float32)

    pr = np.zeros((trace_length, 1), dtype=np.float32)

    node_list = []
    for key in operation_operation.keys():
        node_list.append(key)

    trace_list = []
    for key in operation_trace.keys():
        trace_list.append(key)

    for operation in operation_operation:
        child_num = len(operation_operation[operation])

        for child in operation_operation[operation]:
            p_ss[node_list.index(child)][node_list.index(
                operation)] = 1.0 / child_num

    for trace_id in operation_trace:
        child_num = len(operation_trace[trace_id])
        for child in operation_trace[trace_id]:
            p_sr[node_list.index(child)][trace_list.index(trace_id)] \
                = 1.0 / child_num

    for operation in trace_operation:
        child_num = len(trace_operation[operation])

        for child in trace_operation[operation]:
            p_rs[trace_list.index(child)][node_list.index(operation)] \
                = 1.0 / child_num

    kind_list = np.zeros(len(trace_list))
    p_srt = p_sr.T
    for i in range(len(trace_list)):
        index_list = [i]
        if kind_list[i] != 0:
            continue
        n = 0
        for j in range(i, len(trace_list)):
            if (p_srt[i] == p_srt[j]).all():
                index_list.append(j)
                n += 1
        for index in index_list:
            kind_list[index] = n

    num_sum_trace = 0
    kind_sum_trace = 0
    if not anomaly:
        for trace_id in pr_trace:
            num_sum_trace += 1.0 / kind_list[trace_list.index(trace_id)]
        for trace_id in pr_trace:
            pr[trace_list.index(trace_id)] = 1.0 / \
                kind_list[trace_list.index(trace_id)] / num_sum_trace
    else:
        for trace_id in pr_trace:
            kind_sum_trace += 1.0 / kind_list[trace_list.index(trace_id)]
            num_sum_trace += 1.0 / len(pr_trace[trace_id])
        for trace_id in pr_trace:
            pr[trace_list.index(trace_id)] = 1.0 / (kind_list[trace_list.index(trace_id)] / kind_sum_trace * 0.5
                                                    + 1.0 / len(pr_trace[trace_id])) / num_sum_trace * 0.5

    result = pageRank(p_ss, p_sr, p_rs, pr, operation_length, trace_length)

    weight = {}
    sum = 0
    for operation in operation_operation:
        sum += result[node_list.index(operation)][0]

    trace_num_list = {}
    for operation in operation_operation:
        trace_num_list[operation] = 0
        i = node_list.index(operation)
        for j in range(len(trace_list)):
            if p_sr[i][j] != 0:
                trace_num_list[operation] += 1

    for operation in operation_operation:
        weight[operation] = result[node_list.index(
            operation)][0] * sum / len(operation_operation)

    return weight, trace_num_list


def _random_spans(seed: int, n_traces: int = 60) -> "pd.DataFrame":
    """Traces over a few operations with random subsets kept (partial traces, repeated shapes)."""
    rnd = random.Random(seed)
    ops = ["a_x", "b_y", "c_z", "d_w", "e_v"]
    rows = []
    sid = 0
    for k in range(n_traces):
        chain = [op for op in ops if rnd.random() < 0.7] or ["a_x"]
        parent = None
        for op in chain:
            sid += 1
            svc, name = op.split("_")
            rows.append({"traceID": f"t{k}", "spanID": f"s{sid}", "parentSpanID": parent, "serviceName": svc,
                         "methodName": name, "operationName": name, "startTime": k * 1000, "duration": 100 + rnd.random()})
            parent = f"s{sid}" if rnd.random() < 0.8 else f"missing{sid}"
    df = pd.DataFrame(rows)
    df["operation"] = df["serviceName"] + "_" + df["methodName"]
    return df


@pytest.mark.parametrize("seed", [1, 2, 3])
@pytest.mark.parametrize("anomaly", [False, True])
def test_patched_trace_pagerank_matches_the_original(seed, anomaly, capsys):
    graph = mr.get_pagerank_graph(_random_spans(seed))
    w_new, n_new = mr.trace_pagerank(*graph, anomaly)
    w_old, n_old = trace_pagerank_original(*graph, anomaly)
    capsys.readouterr()
    assert n_new == n_old
    assert w_new.keys() == w_old.keys()
    for op in w_old:
        assert w_new[op] == pytest.approx(w_old[op], rel=1e-12, abs=1e-15)


def test_kept_ancestor_skips_dropped_spans():
    # 0 <- 1 <- 2 <- 3, and 4 a root; kept: 0, 3, 4
    pidx = np.array([-1, 0, 1, 2, -1])
    kept = np.array([True, False, False, True, True])
    assert rca.kept_ancestor(pidx, kept, np.array([3, 4, 0])).tolist() == [0, -1, -1]


def _table(rows) -> SpanTable:
    """rows: (trace, span id, parent id, service, name, kind, start_ns, end_ns)."""
    svcs = sorted({r[3] for r in rows})
    names = sorted({r[4] for r in rows})
    traces = sorted({r[0] for r in rows})
    t = SpanTable(
        sid=np.array([r[1] for r in rows], dtype=np.uint64), pid=np.array([r[2] for r in rows], dtype=np.uint64),
        trace=np.array([traces.index(r[0]) for r in rows], dtype=np.int64),
        start=np.array([r[6] for r in rows], dtype=np.int64), end=np.array([r[7] for r in rows], dtype=np.int64),
        kind=np.array([r[5] for r in rows], dtype=np.int8), svc=np.array([svcs.index(r[3]) for r in rows], dtype=np.int32),
        cls=np.zeros(len(rows), dtype=np.int32), route=np.zeros(len(rows), dtype=np.int32),
        err=np.zeros(len(rows), dtype=bool), bdepth=np.full(len(rows), -1, dtype=np.int16),
        services=svcs, classes=["r|any"], routes=["r"], n_traces=len(traces),
        name=np.array([names.index(r[4]) for r in rows], dtype=np.int32), names=names)
    return t.finish()


def test_recon_ancestor_walks_up_through_placeholders():
    t = _table([("t", 1, 0, "a", "A", SERVER, 0, 10), ("t", 2, 1, "a", "Ac", CLIENT, 1, 9),
                ("t", 3, 2, "b", "B", SERVER, 2, 8), ("t", 4, 99, "c", "C", SERVER, 3, 4)])
    kept_ids = {1, 3, 4}
    # the reconstruction: 3's parent 2 (dropped) became a placeholder under 1; 4's parent 99 is a
    # placeholder chain 99 -> 98 that ends at a synthetic root 97
    parent = {1: 0, 3: 2, 2: 1, 4: 99, 99: 98, 98: 97, 97: 0}
    rows = np.array([0, 2, 3])
    assert rca.recon_ancestor(t, rows, kept_ids, parent).tolist() == [-1, 0, -1]


def test_noisy_neighbour_truth_leaves_out_services_on_every_node():
    meta = {"scenario": "noisy_neighbour", "fault": {"culprit": "socialgraph"},
            "placement": {"socialgraph": "node-2", "text": "node-2", "wrk2api": "node-2", "composepost": "node-1"}}
    assert rca.truth(meta) == ("node", ["socialgraph", "text"])
    meta["placement_nodes"] = {"socialgraph": ["node-2"], "text": ["node-2"], "wrk2api": ["node-1", "node-2", "node-3"],
                               "composepost": ["node-1"]}
    assert rca.truth(meta) == ("node", ["socialgraph", "text"])
    assert rca.truth({"scenario": "cascade", "fault": {"culprit": "poststorage"}}) == ("culprit", ["poststorage"])


def _chain_run(n: int = 400, seed: int = 5):
    """Traces a -> (a client) -> b -> (b client) -> c every 0.5 s; from trace n/2 on, c's own time
    grows by 3 ms.  c is steady (sd 0.1 ms), a and b are noisy (sd 1.5 ms) and every call adds a
    transit gap (sd 0.4 ms), so under the 3-sigma rule c's spans are abnormal and the client spans
    above it less often."""
    rng = np.random.default_rng(seed)
    t0 = 1_000_000.0
    rows = []
    sid = 1
    for k in range(n):
        ts = int((t0 + 0.5 * k) * 1e9)
        c_own = rng.normal(2.0, 0.1) + (3.0 if k >= n // 2 else 0.0)
        b_own = abs(rng.normal(1.5, 1.5))
        a_own = abs(rng.normal(1.5, 1.5))
        c_d = c_own
        bc_d = c_d + abs(rng.normal(0.3, 0.4))
        b_d = bc_d + b_own
        ac_d = b_d + abs(rng.normal(0.3, 0.4))
        a_d = ac_d + a_own
        ids = list(range(sid, sid + 5))
        sid += 5
        spec = [("a", "A", SERVER, 0, a_d, 0), ("a", "Ac", CLIENT, ids[0], ac_d, 0.05),
                ("b", "B", SERVER, ids[1], b_d, 0.1), ("b", "Bc", CLIENT, ids[2], bc_d, 0.15),
                ("c", "C", SERVER, ids[3], c_d, 0.2)]
        for i, (svc, name, kind, par, dur_ms, off_ms) in enumerate(spec):
            st = ts + int(off_ms * 1e6)
            rows.append((f"t{k}", ids[i], par, svc, name, kind, st, st + int(dur_ms * 1e6)))
    t = _table(rows)
    fs = t0 + 0.5 * (n // 2) - 0.01
    meta = {"run_id": "test", "arm": "head", "scenario": "saturated", "volume_frac": 0.12, "seed": 1, "t0": t0,
            "fault": {"start": fs, "end": t0 + 0.5 * n + 1, "culprit": "c"}, "placement": {"a": "n1", "b": "n1", "c": "n2"}}
    return meta, t


def test_rca_ranks_the_culprit_on_the_shadow_and_on_kept_spans():
    meta, t = _chain_run()
    kept_traces = np.unique(t.trace)[::3]                  # every third trace, whole (head-like)
    kidx = np.flatnonzero(np.isin(t.trace, kept_traces))
    out = rca.evaluate_run(meta, t, kidx, None, variants=("shadow", "true", "true_ref"))
    for v in ("shadow", "true", "true_ref"):
        r = out["variants"][v]
        # c's server spans tie with b's client spans into c (RCAEval's mapping: b); the callee mapping: c
        assert r["tracerca"]["rank_callee"] == 1, (v, r["tracerca"])
        assert r["tracerca"]["rank"] in (1, 2), (v, r["tracerca"])
        assert "error" not in r["microrank"], r["microrank"]
        assert r["microrank"]["services"], r["microrank"]
    assert out["variants"]["shadow"]["normal_set"]["spans_per_trace"] == pytest.approx(5.0)
    assert out["variants"]["true"]["normal_set"]["traces"] < out["variants"]["shadow"]["normal_set"]["traces"]
    assert out["variants"]["true_ref"]["normal_set"] == out["variants"]["shadow"]["normal_set"]


def test_a_method_that_cannot_rank_is_recorded_as_a_miss():
    meta, t = _chain_run()
    # keep only fault-window c spans: no operation has a normal-period span
    end_s = t.end / 1e9
    kidx = np.flatnonzero((end_s >= meta["fault"]["start"]) & (np.array(t.services)[t.svc] == "c"))
    out = rca.evaluate_run(meta, t, kidx, None, variants=("true",))
    r = out["variants"]["true"]["tracerca"]
    assert r["rank"] is None and r["rr"] == 0.0 and "error" in r
