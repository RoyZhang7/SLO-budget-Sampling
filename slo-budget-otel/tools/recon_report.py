#!/usr/bin/env python3
"""Roots x reconstruction report from a Jaeger export of one cluster run.

  python3 tools/recon_report.py results/cluster-*/export.json --times results/cluster-*/times.json \
      --status results/cluster-*/central-status.json --out recon.json --sample sample-trace.txt

Reports, over the run window: real and synthetic spans (per period, against V*), root share,
orphans as Jaeger counts them (spans whose parent is not in the trace = its "invalid parent
span IDs" warnings), orphans per trace, traces with a (real / any) root, traces the UI renders
as one tree (one parentless span, every parent present), whether the placeholders change
Jaeger's latency figures (trace durations; per-service span durations and self times over
real spans versus all spans), the propagated slo.* fields on every span, and one sample
trace as the query API returns it (tree order, service, operation, kind, duration, parent,
synthetic, warnings).
"""
from __future__ import annotations

import argparse
import collections
import json
import statistics

SLO_KEYS = ("slo.budget", "slo.root", "slo.class", "slo.root_host")


def norm(name: str) -> str:
    n = name.split(":", 1)[-1]
    for suf in ("_service_pb_es_proc", "_service"):
        if n.endswith(suf):
            n = n[: -len(suf)]
    return n.replace("_", "")


def pct(v, q):
    if not v:
        return None
    v = sorted(v)
    i = min(len(v) - 1, int(round(q * (len(v) - 1))))
    return v[i]


def union_len(iv):
    if not iv:
        return 0
    iv = sorted(iv)
    tot = 0
    cs, ce = iv[0]
    for s, e in iv[1:]:
        if s > ce:
            tot += ce - cs
            cs, ce = s, e
        elif e > ce:
            ce = e
    return tot + (ce - cs)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("export")
    ap.add_argument("--times")
    ap.add_argument("--status", help="central-status.json: requests per period for the root share")
    ap.add_argument("--period", type=float, default=5.0)
    ap.add_argument("--v-star", type=float, default=1380.0)
    ap.add_argument("--out")
    ap.add_argument("--sample", help="write one sample trace (API view) here")
    ap.add_argument("--jaeger", default="http://192.168.134.21:16686")
    a = ap.parse_args()
    obj = json.load(open(a.export, encoding="utf-8-sig"))
    traces = obj.get("data", obj)
    times = json.load(open(a.times)) if a.times else {}
    t0, t1 = times.get("t0", 0), times.get("t1", 1e18)

    requests = None
    if a.status:
        hist = json.load(open(a.status)).get("history", [])
        requests = sum(h.get("requests", 0) for h in hist if t0 <= h["period"] * a.period < t1)

    per = collections.defaultdict(collections.Counter)
    n_tr = 0
    real_spans = synth = 0
    orphans_total = orphans_real = 0
    orphans_hist = collections.Counter()
    with_real_root = with_any_root = one_tree = one_tree_real_only = 0
    warnings = collections.Counter()
    dur_differs = 0
    svc_dur = collections.defaultdict(list)       # service -> durations (real)
    svc_dur_all = collections.defaultdict(list)   # incl. synthetic
    svc_self = collections.Counter()              # self time sums (µs), real
    svc_self_all = collections.Counter()
    synth_self = 0.0
    synth_by_kind = collections.Counter()
    synth_real_id = collections.Counter()
    bag_all4 = bag_any = 0
    bag_by_svc = collections.defaultdict(lambda: [0, 0])
    classes = collections.Counter()
    root_hosts = collections.Counter()
    budgets = collections.Counter()
    sample = None
    sample_score = None
    adjusted_traces = 0
    op_dur = collections.defaultdict(list)      # (service, operation) -> durations, real spans
    op_dur_all = collections.defaultdict(list)  # incl. synthetic spans with the same operation name
    for tr in traces:
        spans = tr.get("spans", [])
        procs = tr.get("processes", {})
        if not spans:
            continue
        starts = [s["startTime"] for s in spans]
        if not (t0 * 1e6 - 5e6 <= min(starts) <= t1 * 1e6 + 30e6):
            continue
        n_tr += 1
        info = {}
        for s in spans:
            tags = {t["key"]: t.get("value") for t in s.get("tags", [])}
            parent = None
            for ref in s.get("references", []) or []:
                if ref.get("refType") == "CHILD_OF":
                    parent = ref.get("spanID")
            svc = norm(procs.get(s.get("processID"), {}).get("serviceName", "?"))
            info[s["spanID"]] = dict(tags=tags, parent=parent, svc=svc, syn=tags.get("synthetic") in (True, "true"),
                                     start=s["startTime"], end=s["startTime"] + s["duration"], dur=s["duration"],
                                     kind=tags.get("span.kind", "?"), op=s.get("operationName", ""), warn=s.get("warnings") or [])
        ids = set(info)
        real_ids = {i for i, x in info.items() if not x["syn"]}
        orphans = orphans_r = 0
        roots_real = roots_any = 0
        children = collections.defaultdict(list)
        if any(any(w.startswith("This span's timestamps were adjusted") for w in x["warn"]) for x in info.values()):
            adjusted_traces += 1
        for i, x in info.items():
            k = int((x["end"] / 1e6) // a.period)
            op_dur_all[(x["svc"], x["op"])].append(x["dur"])
            if not x["syn"]:
                op_dur[(x["svc"], x["op"])].append(x["dur"])
            if x["syn"]:
                synth += 1
                per[k]["synthetic"] += 1
                synth_by_kind[x["kind"]] += 1
                synth_real_id[str(x["tags"].get("slobudget.recon.real_id"))] += 1
            else:
                real_spans += 1
                per[k]["real"] += 1
                svc_dur[x["svc"]].append(x["dur"])
                have = [k2 for k2 in SLO_KEYS if x["tags"].get(k2) not in (None, "")]
                bag_by_svc[x["svc"]][1] += 1
                if len(have) == 4:
                    bag_all4 += 1
                    bag_by_svc[x["svc"]][0] += 1
                    classes[x["tags"]["slo.class"]] += 1
                    root_hosts[x["tags"]["slo.root_host"]] += 1
                    budgets[x["tags"]["slo.budget"]] += 1
                if have:
                    bag_any += 1
            svc_dur_all[x["svc"]].append(x["dur"])
            for w in x["warn"]:
                warnings[w.split("=")[0][:40]] += 1
            if x["parent"] is None:
                roots_any += 1
                if not x["syn"]:
                    roots_real += 1
            else:
                children[x["parent"]].append(i)
                if x["parent"] not in ids:
                    orphans += 1
                if x["parent"] not in real_ids and not x["syn"]:
                    orphans_r += 1
        # self times
        for i, x in info.items():
            kids = [(info[c]["start"], info[c]["end"]) for c in children.get(i, [])]
            self_all = x["dur"] - union_len(kids)
            svc_self_all[x["svc"]] += self_all
            if x["syn"]:
                synth_self += self_all
            else:
                rk = [(info[c]["start"], info[c]["end"]) for c in children.get(i, []) if not info[c]["syn"]]
                svc_self[x["svc"]] += x["dur"] - union_len(rk)
        orphans_total += orphans
        orphans_real += orphans_r
        orphans_hist[min(orphans, 4)] += 1
        if roots_real:
            with_real_root += 1
        if roots_any:
            with_any_root += 1
        if roots_any == 1 and orphans == 0:
            one_tree += 1
        if roots_real == 1 and orphans_r == 0:
            one_tree_real_only += 1
        # trace duration with and without placeholders
        r_start = min(x["start"] for x in info.values() if not x["syn"]) if real_ids else 0
        r_end = max(x["end"] for x in info.values() if not x["syn"]) if real_ids else 0
        a_start = min(x["start"] for x in info.values())
        a_end = max(x["end"] for x in info.values())
        if (a_end - a_start) != (r_end - r_start):
            dur_differs += 1
        # sample: a trace with a root, 4 to 12 spans, the most placeholders (ties: more spans)
        if roots_any and 4 <= len(info) <= 12:
            score = (synth and sum(1 for x in info.values() if x["syn"]), len(info))
            if sample is None or score > sample_score:
                sample, sample_score = (tr["traceID"], info, children), score

    run = {k: c for k, c in per.items() if t0 <= k * a.period < t1}
    real_pp = [c["real"] for c in run.values()]
    syn_pp = [c["synthetic"] for c in run.values()]
    svc_table = {}
    for svc in sorted(svc_dur_all):
        d, da = svc_dur.get(svc, []), svc_dur_all[svc]
        svc_table[svc] = {"n_real": len(d), "n_all": len(da),
                          "mean_ms_real": round(statistics.mean(d) / 1000, 3) if d else None,
                          "mean_ms_all": round(statistics.mean(da) / 1000, 3) if da else None,
                          "p95_ms_real": round(pct(d, 0.95) / 1000, 3) if d else None,
                          "p95_ms_all": round(pct(da, 0.95) / 1000, 3) if da else None,
                          "self_s_real": round(svc_self[svc] / 1e6, 3), "self_s_all": round(svc_self_all[svc] / 1e6, 3)}
    # per-operation figures of the real operations: changed by placeholders only when a
    # placeholder reuses a real operation name
    ops_changed = sum(1 for k in op_dur if len(op_dur_all[k]) != len(op_dur[k]))
    out = {
        "traces": n_tr, "spans_real": real_spans, "spans_synthetic": synth,
        "traces_retimed_by_jaeger_skew_adjustment": adjusted_traces,
        "real_operations": len(op_dur), "real_operations_with_synthetic_spans": ops_changed,
        "synthetic_share_of_export": round(synth / (real_spans + synth), 4) if real_spans + synth else None,
        "synthetic_by_kind": dict(synth_by_kind), "synthetic_real_id": dict(synth_real_id),
        "real_spans_per_period_mean": round(statistics.mean(real_pp), 1) if real_pp else None,
        "vol_over_vstar_real": round(statistics.mean(real_pp) / a.v_star, 3) if real_pp else None,
        "exported_over_vstar_incl_synthetic": round((statistics.mean(real_pp) + statistics.mean(syn_pp)) / a.v_star, 3) if real_pp else None,
        "synthetic_per_period_mean": round(statistics.mean(syn_pp), 1) if syn_pp else None,
        "requests": requests,
        "traces_with_real_root": with_real_root, "traces_with_any_root": with_any_root,
        "traces_with_real_root_frac": round(with_real_root / n_tr, 4) if n_tr else None,
        "traces_with_any_root_frac": round(with_any_root / n_tr, 4) if n_tr else None,
        "root_share_of_requests": round(with_real_root / requests, 4) if requests else None,
        "orphans": orphans_total, "orphans_without_placeholders": orphans_real,
        "orphans_per_trace": {str(k) if k < 4 else "4+": v for k, v in sorted(orphans_hist.items())},
        "traces_with_orphans_frac": round(1 - orphans_hist[0] / n_tr, 4) if n_tr else None,
        "traces_one_tree": one_tree, "traces_one_tree_frac": round(one_tree / n_tr, 4) if n_tr else None,
        "traces_one_tree_without_placeholders_frac": round(one_tree_real_only / n_tr, 4) if n_tr else None,
        "jaeger_warnings": dict(warnings),
        "trace_duration_changed_by_placeholders": dur_differs,
        "synthetic_self_time_s": round(synth_self / 1e6, 3),
        "per_service_latency": svc_table,
        "baggage": {"spans_with_all_4": bag_all4, "spans_with_any": bag_any, "real_spans": real_spans,
                    "all_4_frac": round(bag_all4 / real_spans, 4) if real_spans else None,
                    "by_service_all4_frac": {s: round(v[0] / v[1], 3) for s, v in sorted(bag_by_svc.items()) if v[1]},
                    "classes": dict(classes.most_common(12)), "root_hosts": dict(root_hosts), "budgets": dict(sorted(budgets.items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else 0))},
    }
    text = json.dumps(out, indent=1)
    print(text[:5000])
    if a.out:
        open(a.out, "w", encoding="utf-8").write(text)
    if a.sample and sample:
        tid, info, children = sample
        lines = [f"trace {tid}  ({len(info)} spans, {sum(1 for x in info.values() if x['syn'])} synthetic)  {a.jaeger}/trace/{tid}",
                 "GET /api/traces/" + tid + "  -> spans in tree order (depth-first); * = synthetic placeholder; ! = parent missing (Jaeger warning)"]
        roots = [i for i, x in info.items() if x["parent"] is None]
        orphan_roots = [i for i, x in info.items() if x["parent"] is not None and x["parent"] not in info]
        t_start = min(x["start"] for x in info.values())

        def walk(i, depth):
            x = info[i]
            mark = "*" if x["syn"] else " "
            miss = "!" if (x["parent"] is not None and x["parent"] not in info) else " "
            warn = "; ".join(w[:70] for w in x["warn"])
            lines.append(f"{mark}{miss} {'  ' * depth}{x['svc']:13s} {x['op'][:44]:44s} {x['kind']:6s} "
                         f"+{(x['start'] - t_start) / 1000:7.2f}ms {x['dur'] / 1000:8.2f}ms  id={i} parent={x['parent']}"
                         + (f"  budget={x['tags'].get('slo.budget')} class={x['tags'].get('slo.class')} host={x['tags'].get('slo.root_host')}" if x['tags'].get('slo.budget') else "")
                         + (f"  warnings: {warn}" if warn else ""))
            for c in sorted(children.get(i, []), key=lambda c: info[c]["start"]):
                walk(c, depth + 1)
        for r in roots:
            walk(r, 0)
        for r in orphan_roots:
            walk(r, 1)
        open(a.sample, "w", encoding="utf-8").write("\n".join(lines) + "\n")
        print("\n".join(lines[:40]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
