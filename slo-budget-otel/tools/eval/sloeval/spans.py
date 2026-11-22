"""Span tables: the shadow (100 % ground truth) and the arm's kept set (the Jaeger export).

The shadow is the collector file exporter's OTLP JSON lines (one
ExportTraceServiceRequest per line), optionally zstd- or gzip-compressed.  It is
parsed line by line into flat numpy arrays (about 60 B per span), so a 10-minute
run (1.4 M spans) stays well under a gigabyte.  The export is the Jaeger query
API shape `{"data": [traces], ...}`; it is decoded trace by trace with
`raw_decode`, so only the file text and one trace are in memory at a time.

Ids: span ids are joined as unsigned 64-bit integers (random, unique within a
run); trace ids are only used for grouping inside the shadow.
"""
from __future__ import annotations

import base64
import binascii
import gzip
import io
import json
import subprocess
from array import array
from dataclasses import dataclass, field

import numpy as np

SERVER, CLIENT = 2, 3
_KIND = {"SPAN_KIND_SERVER": SERVER, "SPAN_KIND_CLIENT": CLIENT, "server": SERVER, "client": CLIENT,
         "SPAN_KIND_INTERNAL": 1, "SPAN_KIND_PRODUCER": 4, "SPAN_KIND_CONSUMER": 5}
DEFAULT_ROUTE = "compose_post"


def norm_service(name: str) -> str:
    """`unknown_service:post_storage_service_pb_es_proc` -> `poststorage` (as tools/cluster_metrics.py)."""
    n = name.split(":", 1)[-1]
    for suf in ("_service_pb_es_proc", "_pb_es_proc", "_proc"):
        if n.endswith(suf):
            n = n[: -len(suf)]
            break
    return n.replace("_", "")


def uvarint(raw: bytes) -> int:
    v, shift = 0, 0
    for b in raw:
        v |= (b & 0x7F) << shift
        if b < 0x80:
            return v
        shift += 7
    return v


def b64(s: str) -> bytes | None:
    try:
        return base64.b64decode(s + "=" * (-len(s) % 4))
    except (binascii.Error, ValueError):
        return None


def _id64(s: str) -> int:
    """A span id as an unsigned 64-bit integer: hex (OTLP JSON, Jaeger, leading zeros may be
    trimmed) or base64 of 8 bytes (protojson of raw bytes)."""
    if not s:
        return 0
    try:
        return int(s, 16) & 0xFFFFFFFFFFFFFFFF
    except ValueError:
        raw = b64(s)
        return int.from_bytes(raw[-8:], "big") if raw else 0


def open_lines(path: str):
    """Iterate the text lines of a plain, .gz or .zst file (zstd through the CLI)."""
    if path.endswith(".zst"):
        proc = subprocess.Popen(["zstd", "-dc", "--no-progress", path], stdout=subprocess.PIPE, bufsize=1 << 20)
        try:
            yield from io.TextIOWrapper(proc.stdout, encoding="utf-8")
        finally:
            proc.stdout.close()
            rc = proc.wait()
            if rc not in (0, None):
                raise RuntimeError(f"zstd -dc {path} exited {rc}")
    elif path.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            yield from f
    else:
        with open(path, encoding="utf-8") as f:
            yield from f


class _Codes:
    """String -> small integer code, with the reverse list."""

    def __init__(self):
        self.map: dict[str, int] = {}
        self.names: list[str] = []

    def code(self, s: str) -> int:
        c = self.map.get(s)
        if c is None:
            c = len(self.names)
            self.map[s] = c
            self.names.append(s)
        return c


@dataclass
class SpanTable:
    """Complete spans of a run (the shadow), flat arrays indexed by span."""
    sid: np.ndarray            # uint64 span id
    pid: np.ndarray            # uint64 parent span id (0: none)
    trace: np.ndarray          # int64 trace code
    start: np.ndarray          # int64 ns
    end: np.ndarray            # int64 ns
    kind: np.ndarray           # int8: 2 server, 3 client, else other
    svc: np.ndarray            # int32 service code
    cls: np.ndarray            # int32 class code (slo.class)
    route: np.ndarray          # int32 route code (slo.root)
    err: np.ndarray            # bool: status error
    bdepth: np.ndarray         # int16 depth from _br / _d (-1: none)
    services: list[str]
    classes: list[str]
    routes: list[str]
    n_traces: int
    duplicates: int = 0
    lines: int = 0
    name: np.ndarray = field(default=None, repr=False)        # int32 span-name code (shadow: name, export: operationName)
    names: list = field(default_factory=list)
    # derived by finish()
    pidx: np.ndarray = field(default=None, repr=False)        # parent index (-1: root or parent missing)
    depth: np.ndarray = field(default=None, repr=False)       # bridge depth, else depth by parent chain
    sparent: np.ndarray = field(default=None, repr=False)     # nearest server ancestor's index (-1: none)
    own: np.ndarray = field(default=None, repr=False)         # ns: duration minus the union of direct children
    is_root: np.ndarray = field(default=None, repr=False)
    missing_parent: int = 0

    @property
    def n(self) -> int:
        return len(self.sid)

    @property
    def dur(self) -> np.ndarray:
        return self.end - self.start

    def finish(self) -> "SpanTable":
        n = self.n
        order = np.argsort(self.sid, kind="stable")
        sid_s = self.sid[order]
        has_p = self.pid != 0
        pos = np.searchsorted(sid_s, self.pid)
        pos_c = np.minimum(pos, max(0, n - 1))
        found = has_p & (pos < n) & (sid_s[pos_c] == self.pid) if n else has_p
        pidx = np.full(n, -1, dtype=np.int64)
        pidx[found] = order[pos_c[found]]
        # a parent from another trace (id collision) does not count
        bad = found & (self.trace[np.maximum(pidx, 0)] != self.trace)
        pidx[bad] = -1
        self.pidx = pidx
        self.is_root = ~has_p
        self.missing_parent = int((has_p & (pidx < 0)).sum())
        # depth: the bridge's where present, else by the parent chain (roots 0)
        cd = np.full(n, -1, dtype=np.int64)
        cd[self.is_root] = 0
        for _ in range(64):
            m = (cd < 0) & (pidx >= 0)
            if not m.any():
                break
            pd = cd[pidx[m]]
            ok = pd >= 0
            idx = np.flatnonzero(m)[ok]
            if not len(idx):
                break
            cd[idx] = pd[ok] + 1
        self.depth = np.where(self.bdepth >= 0, self.bdepth.astype(np.int64), cd)
        # nearest server ancestor
        sp = pidx.copy()
        for _ in range(64):
            m = (sp >= 0) & (self.kind[np.maximum(sp, 0)] != SERVER)
            if not m.any():
                break
            sp[m] = pidx[sp[m]]
        self.sparent = sp
        # own time: duration minus the union of the direct children's intervals, clipped to the span
        self.own = _own_times(self.start, self.end, pidx)
        return self


def _own_times(start: np.ndarray, end: np.ndarray, pidx: np.ndarray) -> np.ndarray:
    dur = end - start
    own = dur.astype(np.int64).copy()
    kids = np.flatnonzero(pidx >= 0)
    if not len(kids):
        return own
    par = pidx[kids]
    order = np.lexsort((start[kids], par))
    kids, par = kids[order], par[order]
    ks, ke = start[kids], end[kids]
    covered = np.zeros(len(start), dtype=np.int64)
    # merge intervals per parent, clipped to the parent's interval
    cur_p, cs, ce, acc = -1, 0, 0, 0
    for k in range(len(kids)):
        p = int(par[k])
        s = max(int(ks[k]), int(start[p]))
        e = min(int(ke[k]), int(end[p]))
        if p != cur_p:
            if cur_p >= 0:
                covered[cur_p] = acc + (ce - cs if ce > cs else 0)
            cur_p, cs, ce, acc = p, s, e, 0
            continue
        if s > ce:
            acc += ce - cs if ce > cs else 0
            cs, ce = s, e
        elif e > ce:
            ce = e
    if cur_p >= 0:
        covered[cur_p] = acc + (ce - cs if ce > cs else 0)
    return np.maximum(own - covered, 0)


def _attr_value(v: dict):
    if "stringValue" in v:
        return v["stringValue"]
    if "bytesValue" in v:
        return b64(v["bytesValue"])
    if "intValue" in v:
        return int(v["intValue"])
    if "doubleValue" in v:
        return float(v["doubleValue"])
    if "boolValue" in v:
        return bool(v["boolValue"])
    return None


def read_shadow(path: str, default_route: str = DEFAULT_ROUTE) -> SpanTable:
    """Parse the shadow's OTLP JSON lines into a SpanTable (duplicates by span id dropped)."""
    sid, pid, trace, start, end = array("Q"), array("Q"), array("q"), array("q"), array("q")
    kind, err, bdepth = bytearray(), bytearray(), array("h")
    svc, cls, route, nm = array("i"), array("i"), array("i"), array("i")
    svcs, clss, routes, traces, nms = _Codes(), _Codes(), _Codes(), _Codes(), _Codes()
    seen: set[int] = set()
    dups = lines = 0
    for line in open_lines(path):
        line = line.strip()
        if not line:
            continue
        lines += 1
        obj = json.loads(line)
        for rs in obj.get("resourceSpans") or ():
            sname = "unknown"
            for a in (rs.get("resource") or {}).get("attributes") or ():
                if a.get("key") == "service.name":
                    sname = (a.get("value") or {}).get("stringValue", sname)
            sc = svcs.code(norm_service(sname))
            for ss in rs.get("scopeSpans") or rs.get("instrumentationLibrarySpans") or ():
                for sp in ss.get("spans") or ():
                    s_id = _id64(sp.get("spanId", ""))
                    if s_id in seen:
                        dups += 1
                        continue
                    seen.add(s_id)
                    c_cls, c_route, depth = "", "", -1
                    for a in sp.get("attributes") or ():
                        k = a.get("key")
                        if k == "slo.class":
                            c_cls = (a.get("value") or {}).get("stringValue", "")
                        elif k == "slo.root":
                            c_route = (a.get("value") or {}).get("stringValue", "")
                        elif k == "_br" or k == "_d":
                            raw = _attr_value(a.get("value") or {})
                            if isinstance(raw, (bytes, bytearray)) and raw:
                                depth = uvarint(raw)
                            elif isinstance(raw, int):
                                depth = raw
                    r = c_route or default_route
                    sid.append(s_id)
                    pid.append(_id64(sp.get("parentSpanId", "") or ""))
                    trace.append(traces.code(sp.get("traceId", "")))
                    start.append(int(sp.get("startTimeUnixNano", 0)))
                    end.append(int(sp.get("endTimeUnixNano", 0)))
                    k = sp.get("kind", 0)
                    kind.append(_KIND.get(k, 0) if isinstance(k, str) else int(k) & 0x7F)
                    st = (sp.get("status") or {}).get("code", 0)
                    err.append(1 if st in (2, "STATUS_CODE_ERROR") else 0)
                    bdepth.append(depth if 0 <= depth < 32000 else -1)
                    svc.append(sc)
                    nm.append(nms.code(sp.get("name", "")))
                    cls.append(clss.code(c_cls or f"{r}|any"))
                    route.append(routes.code(r))
    t = SpanTable(
        sid=np.frombuffer(sid, dtype=np.uint64).copy() if len(sid) else np.zeros(0, np.uint64),
        pid=np.frombuffer(pid, dtype=np.uint64).copy() if len(pid) else np.zeros(0, np.uint64),
        trace=np.frombuffer(trace, dtype=np.int64).copy() if len(trace) else np.zeros(0, np.int64),
        start=np.frombuffer(start, dtype=np.int64).copy() if len(start) else np.zeros(0, np.int64),
        end=np.frombuffer(end, dtype=np.int64).copy() if len(end) else np.zeros(0, np.int64),
        kind=np.frombuffer(bytes(kind), dtype=np.int8).copy(),
        svc=np.frombuffer(svc, dtype=np.int32).copy() if len(svc) else np.zeros(0, np.int32),
        cls=np.frombuffer(cls, dtype=np.int32).copy() if len(cls) else np.zeros(0, np.int32),
        route=np.frombuffer(route, dtype=np.int32).copy() if len(route) else np.zeros(0, np.int32),
        err=np.frombuffer(bytes(err), dtype=np.uint8).astype(bool),
        bdepth=np.frombuffer(bdepth, dtype=np.int16).copy() if len(bdepth) else np.zeros(0, np.int16),
        services=svcs.names, classes=clss.names, routes=routes.names, n_traces=len(traces.names),
        duplicates=dups, lines=lines,
        name=np.frombuffer(nm, dtype=np.int32).copy() if len(nm) else np.zeros(0, np.int32), names=nms.names)
    return t.finish()



def own_client(t: SpanTable) -> np.ndarray:
    """Stage 2d own time (ns): duration minus the union of the CLIENT children's intervals
    (PRODUCER and other children are not subtracted), clipped to the span."""
    child_is_client = t.kind == CLIENT
    pidx_c = np.where(child_is_client, t.pidx, -1)
    return _own_times(t.start, t.end, pidx_c)


def read_export_table(path: str, default_route: str = DEFAULT_ROUTE) -> SpanTable:
    """A complete SpanTable from a Jaeger export (query-API shape).  Times are the export's
    microseconds (start times may be shifted by Jaeger's clock-skew adjuster; durations are
    not); kinds come from the span.kind tag, parents from the CHILD_OF reference.  Synthetic
    (reconstruction) spans are skipped."""
    sid, pid, trace, start, end = array("Q"), array("Q"), array("q"), array("q"), array("q")
    kind, err, bdepth = bytearray(), bytearray(), array("h")
    svc, cls, route, nm = array("i"), array("i"), array("i"), array("i")
    svcs, clss, routes, traces, nms = _Codes(), _Codes(), _Codes(), _Codes(), _Codes()
    seen: set[int] = set()
    dups = 0
    for tr in _iter_export_traces(path):
        procs = tr.get("processes") or {}
        tcode = traces.code(tr.get("traceID", ""))
        for sp in tr.get("spans") or ():
            tags = {t.get("key"): t.get("value") for t in sp.get("tags") or ()}
            if tags.get("synthetic") in (True, "true"):
                continue
            s_id = _id64(sp.get("spanID", ""))
            if s_id in seen:
                dups += 1
                continue
            seen.add(s_id)
            par = 0
            for ref in sp.get("references") or ():
                if ref.get("refType") == "CHILD_OF":
                    par = _id64(ref.get("spanID", ""))
                    break
            r = str(tags.get("slo.root") or default_route)
            st_us = int(sp.get("startTime", 0))
            sid.append(s_id)
            pid.append(par)
            trace.append(tcode)
            start.append(st_us * 1000)
            end.append((st_us + int(sp.get("duration", 0))) * 1000)
            kind.append(_KIND.get(str(tags.get("span.kind", "")), 0))
            e = tags.get("error")
            err.append(1 if e in (True, "true") or tags.get("otel.status_code") == "ERROR" else 0)
            bdepth.append(-1)
            svc.append(svcs.code(norm_service((procs.get(sp.get("processID")) or {}).get("serviceName", "?"))))
            nm.append(nms.code(str(sp.get("operationName", ""))))
            cls.append(clss.code(str(tags.get("slo.class") or f"{r}|any")))
            route.append(routes.code(r))
    t = SpanTable(
        sid=np.frombuffer(sid, dtype=np.uint64).copy() if len(sid) else np.zeros(0, np.uint64),
        pid=np.frombuffer(pid, dtype=np.uint64).copy() if len(pid) else np.zeros(0, np.uint64),
        trace=np.frombuffer(trace, dtype=np.int64).copy() if len(trace) else np.zeros(0, np.int64),
        start=np.frombuffer(start, dtype=np.int64).copy() if len(start) else np.zeros(0, np.int64),
        end=np.frombuffer(end, dtype=np.int64).copy() if len(end) else np.zeros(0, np.int64),
        kind=np.frombuffer(bytes(kind), dtype=np.int8).copy(),
        svc=np.frombuffer(svc, dtype=np.int32).copy() if len(svc) else np.zeros(0, np.int32),
        cls=np.frombuffer(cls, dtype=np.int32).copy() if len(cls) else np.zeros(0, np.int32),
        route=np.frombuffer(route, dtype=np.int32).copy() if len(route) else np.zeros(0, np.int32),
        err=np.frombuffer(bytes(err), dtype=np.uint8).astype(bool),
        bdepth=np.frombuffer(bdepth, dtype=np.int16).copy() if len(bdepth) else np.zeros(0, np.int16),
        services=svcs.names, classes=clss.names, routes=routes.names, n_traces=len(traces.names),
        duplicates=dups, lines=0,
        name=np.frombuffer(nm, dtype=np.int32).copy() if len(nm) else np.zeros(0, np.int32), names=nms.names)
    return t.finish()

@dataclass
class KeptSet:
    """The arm's kept spans (real spans of the export), with the tags the metrics read."""
    sid: np.ndarray            # uint64
    p: np.ndarray              # float sampling.p (nan: absent)
    admit: np.ndarray          # int64 slobudget.admit_ns (-1: absent)
    reason: list[str]
    service: list[str]         # short service name from the export's process
    end_us: np.ndarray         # int64 start + duration from the export (adjusted by Jaeger's skew adjuster)
    traces: int
    synthetic_skipped: int = 0
    duplicates: int = 0


def _iter_export_traces(path: str):
    """Yield the traces of a Jaeger export ({"data": [...]} or a bare list, plain or .zst / .gz)
    one at a time."""
    if path.endswith((".zst", ".gz")):
        text = "".join(open_lines(path))
    else:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    dec = json.JSONDecoder()
    i = 0
    n = len(text)
    while i < n and text[i] in " \t\r\n":
        i += 1
    if i < n and text[i] == "{":
        k = text.find('"data"', i)
        if k < 0:
            return
        i = text.index("[", k) + 1
    elif i < n and text[i] == "[":
        i += 1
    else:
        return
    while True:
        while i < n and text[i] in " \t\r\n,":
            i += 1
        if i >= n or text[i] == "]":
            return
        obj, i = dec.raw_decode(text, i)
        yield obj


def read_export(path: str) -> KeptSet:
    sid, admit, end_us = array("Q"), array("q"), array("q")
    p = array("d")
    reason: list[str] = []
    service: list[str] = []
    seen: set[int] = set()
    traces = synth = dups = 0
    for tr in _iter_export_traces(path):
        traces += 1
        procs = tr.get("processes") or {}
        for s in tr.get("spans") or ():
            tags = {t.get("key"): t.get("value") for t in s.get("tags") or ()}
            if tags.get("synthetic") in (True, "true"):
                synth += 1
                continue
            v = _id64(s.get("spanID", ""))
            if v in seen:
                dups += 1
                continue
            seen.add(v)
            sid.append(v)
            pv = tags.get("sampling.p")
            try:
                p.append(float(pv) if pv is not None else float("nan"))
            except (TypeError, ValueError):
                p.append(float("nan"))
            av = tags.get("slobudget.admit_ns")
            try:
                admit.append(int(av) if av is not None else -1)
            except (TypeError, ValueError):
                admit.append(-1)
            reason.append(str(tags.get("slobudget.reason") or ""))
            service.append(norm_service((procs.get(s.get("processID")) or {}).get("serviceName", "?")))
            end_us.append(int(s.get("startTime", 0)) + int(s.get("duration", 0)))
    return KeptSet(
        sid=np.frombuffer(sid, dtype=np.uint64).copy() if len(sid) else np.zeros(0, np.uint64),
        p=np.frombuffer(p, dtype=np.float64).copy() if len(p) else np.zeros(0),
        admit=np.frombuffer(admit, dtype=np.int64).copy() if len(admit) else np.zeros(0, np.int64),
        reason=reason, service=service,
        end_us=np.frombuffer(end_us, dtype=np.int64).copy() if len(end_us) else np.zeros(0, np.int64),
        traces=traces, synthetic_skipped=synth, duplicates=dups)


def join(shadow: SpanTable, kept: KeptSet):
    """Index of every kept span in the shadow (-1: not in the shadow)."""
    order = np.argsort(shadow.sid, kind="stable")
    sid_s = shadow.sid[order]
    pos = np.searchsorted(sid_s, kept.sid)
    n = len(sid_s)
    pos_c = np.minimum(pos, max(0, n - 1))
    ok = (pos < n) & (sid_s[pos_c] == kept.sid) if n else np.zeros(len(kept.sid), dtype=bool)
    idx = np.full(len(kept.sid), -1, dtype=np.int64)
    idx[ok] = order[pos_c[ok]]
    return idx
