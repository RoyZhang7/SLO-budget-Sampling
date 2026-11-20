"""Generator invariants and the Jaeger round trip."""
import json

import pytest

from slo_budget_sim.jaeger import load_jaeger, normalize_service, to_jaeger
from slo_budget_sim.metrics import CorpusArrays
from slo_budget_sim.model import CallGraph
from slo_budget_sim.synthetic import ROUTE_TREES, build_scenario, generate, scenario
from slo_budget_sim.ttable import oracle_table


def _count(node):
    return 1 + sum(_count(c) for c in node[1])


def test_generator_is_deterministic_and_well_formed():
    cfg = scenario("saturated", seed=3, duration=6.0, rate=40.0)
    a = generate(cfg)
    b = generate(cfg)
    assert [(s.service, s.start, s.duration) for s in a.spans] == [(s.service, s.start, s.duration) for s in b.spans]
    for s in a.spans:
        if s.parent_idx >= 0:
            p = a.spans[s.parent_idx]
            assert p.start <= s.start and s.end <= p.end + 1e-12     # containment
            assert s.depth == p.depth + 1
        assert s.own > 0 and s.duration >= s.own
    per_route = {}
    for r in a.roots:
        per_route.setdefault(a.spans[r].route, set()).add(len(_subtree(a, r)))
    for route, sizes in per_route.items():
        assert sizes == {_count(ROUTE_TREES[route])}


def _subtree(corpus, idx):
    out, stack = [], [idx]
    while stack:
        i = stack.pop()
        out.append(i)
        stack.extend(corpus.spans[i].children)
    return out


def test_fault_inflates_only_the_target_service():
    cfg = scenario("saturated", seed=5, duration=8.0, rate=60.0)
    cfg.faults[0].start = 4.0
    c = generate(cfg)
    import numpy as np
    def med(svc, lo, hi):
        return np.median([s.own for s in c.spans if s.service == svc and lo <= s.start < hi])
    assert med("socialgraph", 4.0, 8.0) / med("socialgraph", 0.0, 4.0) == pytest.approx(5.0, rel=0.3)
    assert med("poststorage", 4.0, 8.0) / med("poststorage", 0.0, 4.0) == pytest.approx(1.0, rel=0.3)


def test_learned_graph_matches_declared():
    corpus, reference = build_scenario("steady", seed=2, duration=4.0, rate=50.0)
    learned = CallGraph.learn(reference.spans)
    assert learned.root == corpus.graph.root
    for key, kids in corpus.graph.children.items():
        assert learned.children.get(key, []) == kids
    # the real ComposePost trace: composepost and text fan out in parallel, hometimeline does not
    assert learned.parallel[("compose_post", "composepost")] is True
    assert learned.parallel[("compose_post", "text")] is True
    assert learned.parallel.get(("compose_post", "hometimeline"), False) is False
    assert corpus.graph.children[("compose_post", "composepost")] == sorted(
        ["hometimeline", "usertimeline", "poststorage", "text", "uniqueid", "media", "user"])


def test_jaeger_round_trip(tmp_path):
    corpus, _ = build_scenario("mixed_size", seed=4, duration=3.0, rate=40.0)
    path = tmp_path / "traces.json"
    path.write_text(json.dumps(to_jaeger(corpus)), encoding="utf-8")
    loaded = load_jaeger(path, period=corpus.period, routes_slo=corpus.routes_slo)
    assert len(loaded.spans) == len(corpus.spans)
    assert len(loaded.roots) == len(corpus.roots)
    by_id = {s.span_id: s for s in loaded.spans}
    for s in corpus.spans:
        l = by_id[s.span_id]
        assert l.service == s.service and l.host == s.host
        assert l.duration == pytest.approx(s.duration, abs=2e-6)
        assert l.status_ok == s.status_ok
        assert l.depth == s.depth
        assert l.req_class == s.req_class
        assert (l.parent_idx < 0) == (s.parent_idx < 0)
        if s.parent_idx >= 0:
            assert loaded.spans[l.parent_idx].span_id == corpus.spans[s.parent_idx].span_id
    assert loaded.graph.parallel[("compose_post", "composepost")] is True


def test_loader_tolerates_bridges_tags_and_odd_shapes():
    doc = {"data": [{"traceID": "abc", "processes": {"p1": {"serviceName": "unknown_service:wrk2api_service_pb_es_proc",
                                                             "tags": [{"key": "hostname", "value": "node-3"}]},
                                                      "p2": {"serviceName": "composepost_service_pb_es_proc", "tags": []}},
                     "spans": [
                         {"traceID": "abc", "spanID": "1", "operationName": "Wrk2APIServiceServer_ComposePost",
                          "references": [], "startTime": 1_000_000, "duration": 5000, "processID": "p1",
                          "tags": [{"key": "_br", "type": "string", "value": "BFwcc"}, {"key": "_d", "type": "int64", "value": 0},
                                   {"key": "http.request_content_length", "type": "int64", "value": 200_000}]},
                         {"traceID": "abc", "spanID": "2", "operationName": "ComposePost",
                          "references": [{"refType": "CHILD_OF", "traceID": "abc", "spanID": "1"}],
                          "startTime": 1_000_500, "duration": 3000, "processID": "p2",
                          "tags": [{"key": "otel.status_code", "type": "string", "value": "ERROR"},
                                   {"key": "_d", "type": "int64", "value": 1}]},
                         {"traceID": "abc", "spanID": "3", "operationName": "Orphan",
                          "references": [{"refType": "CHILD_OF", "traceID": "abc", "spanID": "missing"}],
                          "startTime": 1_000_600, "duration": 100, "processID": "p2", "tags": []},
                     ]}]}
    c = load_jaeger(doc)
    svc = {s.span_id: s for s in c.spans}
    assert svc[1].service == "wrk2api" and svc[1].host == "node-3"
    assert svc[2].service == "composepost" and not svc[2].status_ok and svc[2].depth == 1
    assert svc[1].req_class == "Wrk2APIServiceServer_ComposePost|large"
    assert svc[3].is_root                   # dangling parent becomes a root
    assert svc[1].duration == pytest.approx(0.005)
    assert c.routes_slo["Wrk2APIServiceServer_ComposePost"] > 0


def test_normalize_service():
    assert normalize_service("unknown_service:composepost_service_pb_es_proc") == "composepost"
    assert normalize_service("userid_service_v_es_proc") == "userid"
    assert normalize_service("unknown_service:post_storage_service_pb_es_proc") == "poststorage"
    assert normalize_service("frontend") == "frontend"


def _jaeger_span(trace, sid, parent, op, proc, start_us, dur_us, kind):
    refs = [{"refType": "CHILD_OF", "traceID": trace, "spanID": parent}] if parent else []
    return {"traceID": trace, "spanID": sid, "operationName": op, "references": refs, "startTime": start_us,
            "duration": dur_us, "processID": proc, "tags": [{"key": "span.kind", "type": "string", "value": kind},
                                                           {"key": "_br", "type": "string", "value": "x"}]}


def test_loader_folds_client_spans_into_transit():
    """wrk2api SERVER -> wrk2api CLIENT -> composepost SERVER -> composepost CLIENT -> text SERVER,
    128-bit trace id, BOM-free JSON object: folding leaves one span per call and
    turns half of (client - server) into the callee's transit."""
    t = "c58b6f9b8eb8d2608fc053f1b8c7dbfe"
    export = {"data": [{"traceID": t, "processes": {
        "p1": {"serviceName": "unknown_service:wrk2api_service_pb_es_proc", "tags": []},
        "p2": {"serviceName": "unknown_service:composepost_service_pb_es_proc", "tags": []},
        "p3": {"serviceName": "unknown_service:text_service_pb_es_proc", "tags": []}},
        "spans": [
            _jaeger_span(t, "a1", None, "Wrk2APIServiceServer_ComposePost", "p1", 1_000_000, 10_000, "server"),
            _jaeger_span(t, "a2", "a1", "ComposePostServiceClient_ComposePost", "p1", 1_000_500, 9_000, "client"),
            _jaeger_span(t, "a3", "a2", "ComposePostServiceServer_ComposePost", "p2", 1_001_000, 8_000, "server"),
            _jaeger_span(t, "a4", "a3", "TextServiceClient_ComposeText", "p2", 1_002_000, 3_000, "client"),
            _jaeger_span(t, "a5", "a4", "TextServiceServer_ComposeText", "p3", 1_002_400, 2_200, "server"),
        ]}]}
    corpus = load_jaeger(export, period=1.0)
    by_svc = {s.service: s for s in corpus.spans}
    assert set(by_svc) == {"wrk2api", "composepost", "text"}
    assert len(corpus.spans) == 3 and len(corpus.roots) == 1
    assert corpus.spans[by_svc["composepost"].parent_idx].service == "wrk2api"
    assert corpus.spans[by_svc["text"].parent_idx].service == "composepost"
    assert by_svc["composepost"].transit == pytest.approx(0.0005)          # (9 - 8 ms) / 2
    assert by_svc["text"].transit == pytest.approx(0.0004)                 # (3 - 2.2 ms) / 2
    assert by_svc["composepost"].own == pytest.approx(0.0058)              # 8 ms - 2.2 ms child
    assert all(s.route == "Wrk2APIServiceServer_ComposePost" for s in corpus.spans)
    assert corpus.graph.root["Wrk2APIServiceServer_ComposePost"] == "wrk2api"
    # metrics cope with the 128-bit trace id
    arrays = CorpusArrays(corpus, oracle_table(corpus))
    assert arrays.n_traces == 1 and arrays.root_idx.tolist() == [by_svc["wrk2api"].idx] * 3
    # keeping client spans gives the Stage 1 behaviour: five spans, client spans belong to the caller
    kept = load_jaeger(export, period=1.0, fold_client_spans=False)
    assert len(kept.spans) == 5
    assert sorted(s.service for s in kept.spans) == ["composepost", "composepost", "text", "wrk2api", "wrk2api"]
