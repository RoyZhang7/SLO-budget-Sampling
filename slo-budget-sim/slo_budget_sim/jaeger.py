"""Jaeger JSON (query API / UI export shape) loader and writer.

Expected shape:
    {"data": [{"traceID": ..., "spans": [{"traceID", "spanID", "operationName",
               "references": [{"refType": "CHILD_OF", "spanID": ...}], "startTime" (us),
               "duration" (us), "tags": [{"key", "type", "value"}], "processID"}],
               "processes": {"p1": {"serviceName": ..., "tags": [...]}}}]}

Unknown tags (for example the bridges `_br` / `_d` payloads) are ignored.
Service names such as `unknown_service:composepost_service_pb_es_proc` are
normalised to `composepost`.  The request route is the root span's operation
name; the payload bucket comes from a size tag when one exists.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np

from .model import CallGraph, Corpus, Span

_SERVICE_RE = re.compile(r"^(?:unknown_service:)?(?P<name>[A-Za-z0-9_-]+?)(?:_service)?(?:_[a-z]+_es)?(?:_proc)?$")
SIZE_TAGS = ("sim.bucket", "payload.bucket", "http.request_content_length", "payload.bytes",
             "request.size", "message.uncompressed_size")
HOST_TAGS = ("host.name", "hostname", "k8s.node.name", "net.host.name", "ip")


def normalize_service(name: str) -> str:
    """`unknown_service:post_storage_service_pb_es_proc` -> `poststorage` (the
    generator's spelling); names that do not fit the pattern pass through."""
    m = _SERVICE_RE.match(name)
    return m.group("name").replace("_", "") if m else name


def _tags(obj) -> dict:
    return {t.get("key"): t.get("value") for t in obj.get("tags", []) or []}


def _bucket(tags: dict, thresholds=(1024, 65536)) -> str:
    for k in SIZE_TAGS:
        if k in tags:
            v = tags[k]
            if isinstance(v, str) and not v.isdigit():
                return v
            try:
                n = int(v)
            except (TypeError, ValueError):
                return "any"
            if n < thresholds[0]:
                return "small"
            if n < thresholds[1]:
                return "medium"
            return "large"
    return "any"


def _status_ok(tags: dict) -> bool:
    if str(tags.get("otel.status_code", "")).upper() == "ERROR":
        return False
    if tags.get("error") in (True, "true", "True"):
        return False
    try:
        if int(tags.get("http.status_code", 0)) >= 500:
            return False
    except (TypeError, ValueError):
        pass
    return True


def _id(x) -> int:
    if isinstance(x, int):
        return x
    try:
        return int(str(x), 16)
    except ValueError:
        return hash(x) & 0x7FFFFFFFFFFFFFFF


def load_jaeger(source, period: float = 5.0, routes_slo: dict[str, float] | None = None,
                slo_factor: float = 1.5, name: str | None = None, fold_client_spans: bool = True) -> Corpus:
    """Load a Jaeger export.  With `fold_client_spans` (default) a CLIENT span is
    folded into the call it represents: its children are re-parented to its
    parent (the caller's SERVER span) and half of (client - server duration)
    becomes the callee's one-way transit time.  The corpus then has one span per
    call, as the generator does; the caller's own time still excludes the call
    because the callee's span is its child.  Set it to False to sample client
    spans as spans of the calling service."""
    if isinstance(source, (str, Path)):
        with open(source, "r", encoding="utf-8-sig") as f:     # tolerate a BOM from Windows tooling
            obj = json.load(f)
        name = name or Path(source).stem
    else:
        obj = source
        name = name or "jaeger"
    traces = obj.get("data") if isinstance(obj, dict) else obj
    if isinstance(obj, dict) and traces is None:
        traces = obj.get("traces", [])

    spans: list[Span] = []
    roots: list[int] = []
    for tr in traces:
        procs = tr.get("processes", {}) or {}
        raw = tr.get("spans", []) or []
        by_id: dict[int, int] = {}
        recs = []
        for sp in raw:
            sid = _id(sp["spanID"])
            parent = None
            for ref in sp.get("references", []) or []:
                if ref.get("refType") in ("CHILD_OF", "FOLLOWS_FROM") and ref.get("spanID") is not None:
                    parent = _id(ref["spanID"])
                    if ref.get("refType") == "CHILD_OF":
                        break
            proc = procs.get(sp.get("processID"), {}) or {}
            ptags = _tags(proc)
            service = normalize_service(proc.get("serviceName") or sp.get("serviceName") or "unknown")
            host = next((str(ptags[k]) for k in HOST_TAGS if k in ptags), f"host:{service}")
            tags = _tags(sp)
            recs.append([sid, parent, service, host, sp, tags, float(tags.get("sim.transit_us", 0)) / 1e6])
        ids = {r[0] for r in recs}
        if fold_client_spans:
            clients = {r[0]: r for r in recs if str(r[5].get("span.kind", "")).lower() == "client" and r[1] in ids}
            for r in recs:
                c = clients.get(r[1])
                if c is None or r[0] in clients:
                    continue
                r[1] = c[1]                              # re-parent to the client's parent
                gap = (float(c[4]["duration"]) - float(r[4]["duration"])) / 1e6
                r[6] = max(r[6], gap / 2.0) if gap > 0 else r[6]
            # a client span's remaining children (client spans themselves) are dropped with it
            recs = [r for r in recs if r[0] not in clients]
            ids = {r[0] for r in recs}
        sp_by_idx: dict[int, dict] = {}
        for sid, parent, service, host, sp, tags, transit in recs:
            if parent is not None and parent not in ids:
                parent = None                    # dangling parent: treat as a root
            by_id[sid] = len(spans)
            sp_by_idx[len(spans)] = sp
            spans.append(Span(len(spans), sid, _id(tr.get("traceID", sp.get("traceID", 0))),
                              -1 if parent is None else parent, service, host,
                              float(sp["startTime"]) / 1e6, float(sp["duration"]) / 1e6,
                              _status_ok(tags), 0, "", "", len(json.dumps(sp, separators=(",", ":"))),
                              transit, 0.0))
        # resolve parent span ids to indexes, route from the root
        first = len(spans) - len(recs)
        route_by_root = {}
        for s in spans[first:]:
            if s.parent_idx == -1:
                roots.append(s.idx)
                route_by_root[s.span_id] = s
            else:
                s.parent_idx = by_id[s.parent_idx]
        for s in spans[first:]:
            if s.parent_idx == -1:
                r = s
            else:
                r = s
                while r.parent_idx >= 0:
                    r = spans[r.parent_idx]
            rsp = sp_by_idx[r.idx]
            s.route = rsp.get("operationName", "root")
            s.req_class = f"{s.route}|{_bucket(_tags(rsp))}"

    t0 = min((s.start for s in spans), default=0.0)
    hosts = sorted({s.host for s in spans})
    corpus = Corpus(name, spans, roots, {}, CallGraph(dict(), dict(), dict()), hosts,
                    {h: 0.0 for h in hosts}, period, t0, meta={"source": "jaeger", "traces": len(traces)})
    corpus.finalize()
    corpus.graph = CallGraph.learn(spans)
    if routes_slo is None:
        routes_slo = {}
        for route in corpus.graph.root:
            d = np.array([spans[r].duration for r in roots if spans[r].route == route])
            if len(d):
                routes_slo[route] = float(np.quantile(d, 0.95) * slo_factor)
    corpus.routes_slo = dict(routes_slo)
    return corpus


def to_jaeger(corpus: Corpus) -> dict:
    """Export a corpus in the Jaeger API shape (used for round-trip tests and
    to hand synthetic traces to other tools).  Adds the bridges-style `_d`
    and `_br` tags so loaders must tolerate them."""
    by_trace: dict[int, list[Span]] = {}
    for s in corpus.spans:
        by_trace.setdefault(s.trace_id, []).append(s)
    data = []
    for tid, spans in by_trace.items():
        procs, pid_of = {}, {}
        for s in spans:
            if s.service not in pid_of:
                pid = f"p{len(pid_of) + 1}"
                pid_of[s.service] = pid
                procs[pid] = {"serviceName": f"unknown_service:{s.service}_service_pb_es_proc",
                              "tags": [{"key": "host.name", "type": "string", "value": s.host}]}
        out = []
        for s in spans:
            tags = [
                {"key": "otel.status_code", "type": "string", "value": "OK" if s.status_ok else "ERROR"},
                {"key": "_d", "type": "int64", "value": s.depth},
                {"key": "sim.transit_us", "type": "int64", "value": int(round(s.transit * 1e6))},
            ]
            bucket = s.req_class.split("|", 1)[1] if "|" in s.req_class else "any"
            if s.is_root:
                tags.append({"key": "sim.bucket", "type": "string", "value": bucket})
                tags.append({"key": "_br", "type": "string", "value": "BFwccaHpYGvvtrcN"})
            refs = [] if s.is_root else [{"refType": "CHILD_OF", "traceID": f"{tid:032x}",
                                          "spanID": f"{corpus.spans[s.parent_idx].span_id:016x}"}]
            out.append({"traceID": f"{tid:032x}", "spanID": f"{s.span_id:016x}",
                        "operationName": s.route if s.is_root else f"{s.service}.call",
                        "references": refs, "startTime": int(round(s.start * 1e6)),
                        "duration": int(round(s.duration * 1e6)), "tags": tags,
                        "processID": pid_of[s.service], "warnings": None})
        data.append({"traceID": f"{tid:032x}", "spans": out, "processes": procs})
    return {"data": data, "total": 0, "limit": 0, "offset": 0, "errors": None}
