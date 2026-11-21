#!/usr/bin/env python3
"""What a shed trace looks like in Jaeger: per trace the spans kept, whether the
root survived, orphans (spans whose parent is not in the trace), the query
API's warnings, and whether the bridges payload can place each orphan.

    python3 tools/jaeger_orphans.py export.json [--times times.json] [--out summary.json] [--stringify export-str.json]

Every server span of the pb deployment carries `_br` = base64 of
[1 byte depth][anchor prefix: the first bytes of the nearest checkpoint
ancestor's span id][bloom of the ancestors in the window]; client spans carry
`_d` (varint depth).  An orphan is "placed" when its anchor span is in the kept
set: the hole between them is depth(orphan) - depth(anchor) - 1 missing spans.
`--stringify` writes a copy of the export with every tag value as a string
(the bridges `pb_jaeger_recon` loader only accepts string tags).
"""
from __future__ import annotations

import argparse
import base64
import collections
import json
import statistics


def b64(s: str) -> bytes | None:
    try:
        return base64.b64decode(s + "=" * (-len(s) % 4))
    except Exception:                 # noqa: BLE001
        return None


def varint(raw: bytes) -> int:
    v, shift = 0, 0
    for b in raw:
        v |= (b & 0x7F) << shift
        if b < 0x80:
            break
        shift += 7
    return v


def depth_and_anchor(tags: dict, prefix_len: int) -> tuple[int | None, bytes | None]:
    br = tags.get("_br")
    if isinstance(br, str) and br:
        raw = b64(br)
        if raw and len(raw) >= 1 + prefix_len:
            return raw[0], raw[1:1 + prefix_len]
        if raw:
            return raw[0], None
    d = tags.get("_d")
    if isinstance(d, str) and d:
        raw = b64(d)
        if raw:
            return varint(raw), None
    if isinstance(d, int):
        return d, None
    return None, None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("export")
    ap.add_argument("--times", help="times.json of the run (hog window) to split the counts")
    ap.add_argument("--prefix-len", type=int, default=8, help="anchor prefix bytes in _br (pb_jaeger_recon default 8)")
    ap.add_argument("--out")
    ap.add_argument("--stringify", help="write a copy of the export with string tag values here")
    a = ap.parse_args()
    obj = json.load(open(a.export, encoding="utf-8-sig"))
    traces = obj.get("data", obj)
    times = json.load(open(a.times)) if a.times else {}
    hog = (times.get("hog_start"), times.get("hog_end"))

    n_traces = len(traces)
    spans_per_trace, orphans_per_trace = [], []
    with_root = roots_pinned = 0
    warnings = collections.Counter()
    reasons = collections.Counter()
    kinds = collections.Counter()
    p_hist = collections.Counter()
    by_service = collections.Counter()
    outlier_by_service = collections.Counter()
    dropped_children = []
    in_hog = collections.Counter()
    orphan_depth = collections.Counter()
    placement = collections.Counter()
    hole_sizes = collections.Counter()
    br_len = collections.Counter()
    root_depth0 = collections.Counter()
    for tr in traces:
        spans = tr.get("spans", [])
        ids = {s["spanID"] for s in spans}
        prefixes = {}
        info = {}
        has_root = False
        orphans = 0
        t_start = min((s["startTime"] for s in spans), default=0) / 1e6
        window = "hog" if hog[0] and hog[0] <= t_start < hog[1] else "normal"
        in_hog[window] += 1
        for s in spans:
            tags = {t["key"]: t.get("value") for t in s.get("tags", [])}
            d, anchor = depth_and_anchor(tags, a.prefix_len)
            info[s["spanID"]] = (tags, d, anchor)
            if isinstance(tags.get("_br"), str):
                raw = b64(tags["_br"])
                br_len[len(raw) if raw else -1] += 1
            try:
                sid_bytes = bytes.fromhex(s["spanID"].rjust(16, "0"))
            except ValueError:
                sid_bytes = b""
            prefixes[sid_bytes[:a.prefix_len]] = s["spanID"]
        for s in spans:
            tags, d, anchor = info[s["spanID"]]
            kinds[tags.get("span.kind")] += 1
            parent = None
            for ref in s.get("references", []) or []:
                if ref.get("refType") == "CHILD_OF":
                    parent = ref.get("spanID")
            svc = tr["processes"].get(s.get("processID"), {}).get("serviceName", "?")
            by_service[svc] += 1
            if parent is None:
                has_root = True
                root_depth0[d] += 1
                if tags.get("slobudget.root_pinned") in (True, "true"):
                    roots_pinned += 1
            elif parent not in ids:
                orphans += 1
                orphan_depth[d if d is not None else "?"] += 1
                if anchor is None:
                    placement["no anchor in payload"] += 1
                elif anchor in prefixes:
                    aid = prefixes[anchor]
                    ad = info[aid][1]
                    placement["anchor kept: placed"] += 1
                    if d is not None and ad is not None:
                        hole_sizes[d - ad - 1] += 1
                else:
                    placement["anchor missing: not placeable by the path bridge"] += 1
            for w in s.get("warnings") or []:
                warnings[w.split("=")[0].split(" by ")[0][:60]] += 1
            r = tags.get("slobudget.reason")
            if r:
                reasons[r] += 1
                if r == "outlier":
                    outlier_by_service[svc] += 1
            p = tags.get("sampling.p")
            if p is not None:
                try:
                    p_hist[f"{float(p):.2f}"] += 1
                except (TypeError, ValueError):
                    pass
            dc = tags.get("slobudget.dropped_children")
            if dc is not None:
                try:
                    dropped_children.append(int(dc))
                except (TypeError, ValueError):
                    pass
        spans_per_trace.append(len(spans))
        orphans_per_trace.append(orphans)
        with_root += has_root

    def q(xs, f):
        return sorted(xs)[int(f * (len(xs) - 1))] if xs else None

    summary = {
        "traces": n_traces, "spans": sum(spans_per_trace),
        "spans_per_trace": {"mean": statistics.mean(spans_per_trace) if spans_per_trace else None, "p50": q(spans_per_trace, 0.5), "max": max(spans_per_trace, default=0)},
        "traces_with_root": with_root, "traces_with_root_frac": with_root / n_traces if n_traces else None,
        "roots_pinned": roots_pinned, "root_depth_byte": dict(root_depth0),
        "orphans_total": sum(orphans_per_trace),
        "orphans_per_trace": dict(sorted(collections.Counter(orphans_per_trace).items())),
        "traces_with_orphans_frac": sum(1 for o in orphans_per_trace if o) / n_traces if n_traces else None,
        "orphan_depth": dict(sorted(orphan_depth.items(), key=lambda kv: str(kv[0]))),
        "orphan_placement_by_bridge_payload": dict(placement),
        "hole_sizes_when_placed": dict(sorted(hole_sizes.items())),
        "br_payload_bytes": dict(br_len),
        "jaeger_warnings": dict(warnings),
        "kept_reasons": dict(reasons), "span_kinds": dict(kinds), "sampling_p_hist": dict(sorted(p_hist.items())),
        "spans_by_service": dict(by_service), "outlier_keeps_by_service": dict(outlier_by_service),
        "dropped_children_on_kept_parents": dict(sorted(collections.Counter(dropped_children).items())),
        "traces_by_window": dict(in_hog),
    }
    text = json.dumps(summary, indent=1)
    print(text)
    if a.out:
        open(a.out, "w", encoding="utf-8").write(text)
    if a.stringify:
        for tr in traces:
            for s in tr.get("spans", []):
                for t in s.get("tags", []) or []:
                    if not isinstance(t.get("value"), str):
                        t["value"] = json.dumps(t.get("value"))
                        t["type"] = "string"
        json.dump({"data": traces}, open(a.stringify, "w", encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
