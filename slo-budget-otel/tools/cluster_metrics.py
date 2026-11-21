#!/usr/bin/env python3
"""Metrics of a cluster run from what reached Jaeger plus the central's status:

    python3 tools/cluster_metrics.py results/cluster-X/export.json results/cluster-X/central-status.json results/cluster-X/times.json [--out m.json]

Per period (5 s, aligned to the central's periods): spans kept (by kind), kept
as outliers, roots (pinned / plan), kept per service; against the central's
history: admitted span units, V*, B, N_need, frozen services, the table.

Outlier share = kept non-root spans kept as outliers / kept non-root spans
(the arm's own flag).  Precision is judged against the healthy table: the
central's route-level T per service averaged over the periods before the fault
window; a kept span is a "truth" outlier when its duration exceeds that T (or
its status is an error).  Evidence time = first kept outlier span of a service
on the hogged node after the hog start.  Volume = kept spans per period / V*.
"""
from __future__ import annotations

import argparse
import collections
import json
import statistics

NODE2_SERVICES = ("socialgraph", "media", "user", "userid", "wrk2api")   # services placed on node-2 (the hogged node)


def norm(name: str) -> str:
    n = name.split(":", 1)[-1]
    for suf in ("_service_pb_es_proc", "_pb_es_proc", "_proc"):
        if n.endswith(suf):
            n = n[: -len(suf)]
    return n.replace("_", "")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("export")
    ap.add_argument("status")
    ap.add_argument("times")
    ap.add_argument("--period", type=float, default=5.0)
    ap.add_argument("--hog-services", default=",".join(NODE2_SERVICES))
    ap.add_argument("--out")
    a = ap.parse_args()
    traces = json.load(open(a.export, encoding="utf-8-sig")).get("data", [])
    status = json.load(open(a.status))
    times = json.load(open(a.times))
    hog0, hog1, t0, t1 = times["hog_start"], times["hog_end"], times["t0"], times["t1"]
    hog_svcs = set(a.hog_services.split(","))

    # healthy table: route-level T per service over the history periods before the hog
    hist = status.get("history", [])
    healthy: dict[str, list[float]] = collections.defaultdict(list)
    for h in hist:
        p_start = h["period"] * a.period
        if t0 <= p_start < hog0 - a.period and h.get("table_ms"):
            for svc, T in h["table_ms"].items():
                healthy[svc].append(T)
    T_healthy = {svc: statistics.median(v) / 1000.0 for svc, v in healthy.items() if v}

    per = collections.defaultdict(lambda: collections.Counter())
    per_svc = collections.defaultdict(lambda: collections.Counter())
    first_evidence = {}
    truth_pos = truth_tot = 0
    kept_nonroot = kept_out_nonroot = 0
    for tr in traces:
        procs = tr.get("processes", {})
        for s in tr.get("spans", []):
            tags = {t["key"]: t.get("value") for t in s.get("tags", [])}
            svc = norm(procs.get(s.get("processID"), {}).get("serviceName", "?"))
            end = (s["startTime"] + s["duration"]) / 1e6
            k = int(end // a.period)
            if tags.get("synthetic") in (True, "true"):  # reconstruction placeholders: not sampled volume
                per[k]["synthetic"] += 1
                continue
            kind = tags.get("span.kind", "?")
            is_root = not any(r.get("refType") == "CHILD_OF" for r in s.get("references", []) or [])
            reason = tags.get("slobudget.reason", "")
            c = per[k]
            c["spans"] += 1
            c[f"kind_{kind}"] += 1
            if is_root:
                c["roots"] += 1
                if tags.get("slobudget.root_pinned") in (True, "true"):
                    c["roots_pinned"] += 1
            else:
                kept_nonroot += 1
                if reason == "outlier":
                    kept_out_nonroot += 1
                    c["outliers"] += 1
                    per_svc[svc]["outliers"] += 1
                    if svc in hog_svcs and end >= hog0 and (svc not in first_evidence or end - hog0 < first_evidence[svc]):
                        first_evidence[svc] = end - hog0
            per_svc[svc]["kept"] += 1
            if reason == "outlier" and kind == "server" and svc in T_healthy:
                truth_tot += 1
                err = str(tags.get("otel.status_code", "")).upper() == "ERROR"
                if s["duration"] / 1e6 > T_healthy[svc] or err:
                    truth_pos += 1
    periods = []
    hist_by = {h["period"]: h for h in hist}
    for k in sorted(per):
        h = hist_by.get(k, {})
        row = {"period": k, "t": round(k * a.period - t0, 1), "window": "hog" if hog0 <= k * a.period < hog1 else "normal", **per[k],
               "admitted": h.get("admitted"), "cap_dropped": h.get("cap_dropped"), "v_star": h.get("v_star"), "B": h.get("b"),
               "n_need": h.get("n_need"), "frozen": h.get("frozen"), "requests": h.get("requests"), "spans_total": h.get("spans")}
        if h.get("v_star"):
            row["vol_ratio"] = round(per[k]["spans"] / h["v_star"], 3)
        periods.append(row)
    run = [r for r in periods if 0 <= r["t"] < t1 - t0]
    def mean(key, rows):
        v = [r[key] for r in rows if isinstance(r.get(key), (int, float))]
        return statistics.mean(v) if v else None
    summary = {
        "traces": len(traces), "spans_kept": sum(r["spans"] for r in run),
        "kept_per_period_mean": mean("spans", run), "v_star_mean": mean("v_star", run), "vol_ratio_mean": mean("vol_ratio", run),
        "vol_ratio_normal": mean("vol_ratio", [r for r in run if r["window"] == "normal"]),
        "vol_ratio_hog": mean("vol_ratio", [r for r in run if r["window"] == "hog"]),
        "cap_dropped_total": sum(r["cap_dropped"] or 0 for r in run),
        "synthetic_spans": sum(r.get("synthetic", 0) for r in run),
        "outlier_share_nonroot": kept_out_nonroot / kept_nonroot if kept_nonroot else None,
        "outlier_share_normal": None, "outlier_share_hog": None,
        "precision_vs_healthy_table": truth_pos / truth_tot if truth_tot else None, "precision_n": truth_tot,
        "evidence_s_by_service": {s: round(v, 2) for s, v in sorted(first_evidence.items(), key=lambda kv: kv[1])},
        "roots_kept": sum(r.get("roots", 0) for r in run), "roots_pinned": sum(r.get("roots_pinned", 0) for r in run),
        "B_mean": mean("B", run), "n_need": run[-1]["n_need"] if run else None,
        "frozen_periods": [(r["t"], r["frozen"]) for r in run if r.get("frozen")],
        "T_healthy_ms": {s: round(v * 1000, 2) for s, v in sorted(T_healthy.items())},
        "kept_by_service": {s: dict(c) for s, c in sorted(per_svc.items())},
        "periods": periods,
    }
    for w in ("normal", "hog"):
        rows = [r for r in run if r["window"] == w]
        nr = sum(r.get("spans", 0) - r.get("roots", 0) for r in rows)
        summary[f"outlier_share_{w}"] = sum(r.get("outliers", 0) for r in rows) / nr if nr else None
    text = json.dumps(summary, indent=1, default=str)
    print(text[:6000])
    if a.out:
        open(a.out, "w", encoding="utf-8").write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
