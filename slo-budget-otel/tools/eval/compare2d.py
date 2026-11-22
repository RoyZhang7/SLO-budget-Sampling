#!/usr/bin/env python3
"""Stage 2d trigger comparison tables from the run summaries (tools/eval/eval_run.py).

    python3 tools/eval/compare2d.py /storage/royno7/stage3/runs [--seeds 1 2 3] [--gaps] [--md out.md]

Runs: 2d-v12-saturated-own-s<seed> and 2d-v12-saturated-dur-s<seed> (chosen, 12 %, socialgraph
hog minutes 4-7, the own vs the duration trigger, server_hold in both), 2d-v12-steady-own-s<seed>.
Each cell is the mean over the seeds present, with the seed range when it varies.  Precision
is given under three truths: the single-culprit truth (an outlier keep in the fault window is
right only on the culprit), the own-time truth from the shadow (own time over its oracle share,
or an error) and the duration truth (duration over the oracle T, or an error).  Caller keeps are
the outlier keeps on the culprit's server ancestors, which the own-time trigger is meant to
spare.  --gaps adds the gap per edge (tools/eval/gaps.py) before vs during the fault window from
each saturated run's shadow.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys

ROWS_SAT = [
    ("culprit recall (culprit truth outliers kept)", "recall_culprit", "{:.3f}"),
    ("faulty-span coverage (culprit spans kept)", "faulty_span_coverage", "{:.3f}"),
    ("culprit identified", "identified", "bool"),
    ("evidence after fault start, s", "evidence_flagged_s", "{:.1f}"),
    ("culprit outlier keeps per period", "culprit_outlier_keeps_per_period", "{:.1f}"),
    ("N_need in force", "n_need_eff", "{:.1f}"),
    ("outlier keeps in the fault window", "outlier_keeps_window", "{:.0f}"),
    ("  on the culprit", "outlier_keeps_culprit_window", "{:.0f}"),
    ("  on the culprit's callers", "outlier_keeps_callers_window", "{:.0f}"),
    ("  callers' share of the window's outlier keeps", "outlier_keeps_callers_window_frac", "{:.3f}"),
    ("  caller outlier keeps per period", "caller_outlier_keeps_per_period", "{:.1f}"),
    ("  caller keeps true under the own-time truth", "caller_keeps_truth_own", "{:.3f}"),
    ("  caller keeps true under the duration truth", "caller_keeps_truth_dur", "{:.3f}"),
    ("precision in the window, single-culprit truth", "outlier_precision_culprit_window", "{:.3f}"),
    ("precision in the window, own-time truth", "outlier_precision_own_window", "{:.3f}"),
    ("precision in the window, duration truth", "outlier_precision_window", "{:.3f}"),
    ("precision over the run, own-time truth", "outlier_precision_own", "{:.3f}"),
    ("precision over the run, duration truth", "outlier_precision", "{:.3f}"),
    ("orphans per kept outlier, duration truth", "orphans_per_kept_outlier", "{:.2f}"),
    ("orphans per kept outlier, own-time truth", "orphans_per_kept_outlier_own", "{:.2f}"),
    ("spans kept per request, max", "max_per_request", "{:.0f}"),
    ("spans kept per host and period, max", "max_per_host_period", "{:.0f}"),
    ("volume / V*", "vol_ratio", "{:.3f}"),
    ("periods over the cap", "over_cap_periods", "{:.1f}"),
    ("P95 error per service, mean", "p95_err_svc_mean", "{:.3f}"),
    ("P95 error per service, max", "p95_err_svc_max", "{:.3f}"),
    ("client spans after their parent's release (frac)", "hold_late_client_frac", "{:.4f}"),
    ("mean server hold, ms", "hold_ms_mean", "{:.0f}"),
    ("peak spans held per agent", "hold_peak_spans", "{:.0f}"),
    ("peak bytes held per agent", "hold_peak_bytes", "{:.0f}"),
    ("agent CPU per span, us", "agent_cpu_us_per_span", "{:.1f}"),
]
ROWS_STEADY = [
    ("outlier share of kept spans, own-time truth", "outlier_share_own", "{:.4f}"),
    ("outlier share of kept spans, duration truth", "outlier_share", "{:.4f}"),
    ("outlier keeps", "outlier_keeps", "{:.0f}"),
    ("precision, own-time truth", "outlier_precision_own", "{:.3f}"),
    ("precision, duration truth", "outlier_precision", "{:.3f}"),
    ("volume / V*", "vol_ratio", "{:.3f}"),
    ("spans kept per request, max", "max_per_request", "{:.0f}"),
    ("P95 error per service, mean", "p95_err_svc_mean", "{:.3f}"),
    ("P95 error per service, max", "p95_err_svc_max", "{:.3f}"),
    ("client spans after their parent's release (frac)", "hold_late_client_frac", "{:.4f}"),
    ("mean server hold, ms", "hold_ms_mean", "{:.0f}"),
    ("agent CPU per span, us", "agent_cpu_us_per_span", "{:.1f}"),
]


def load(runs: str, rid: str) -> dict | None:
    p = os.path.join(runs, rid, "summary.json")
    if not os.path.exists(p):
        return None
    with open(p) as f:
        return json.load(f)


def cell(vals, fmt):
    vs = [v for v in vals if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not vs:
        return "n/a"
    if fmt == "bool":
        return f"{sum(1 for v in vs if v)} of {len(vs)}"
    vs = [float(v) for v in vs]
    m = sum(vs) / len(vs)
    lo, hi = min(vs), max(vs)
    s = fmt.format(m)
    if len(vs) > 1 and fmt.format(lo) != fmt.format(hi):
        s += f" ({fmt.format(lo)}-{fmt.format(hi)})"
    return s


def table(title, rows, groups: dict[str, list[dict]]) -> str:
    names = list(groups)
    out = [f"### {title}", "", "| metric | " + " | ".join(f"{n} (n={len(groups[n])})" for n in names) + " |",
           "|---|" + "---|" * len(names)]
    for label, key, fmt in rows:
        out.append(f"| {label} | " + " | ".join(cell([s.get(key) for s in groups[n]], fmt) for n in names) + " |")
    return "\n".join(out) + "\n"


def by_service(groups: dict[str, list[dict]]) -> str:
    names = list(groups)
    svcs = sorted({k for g in groups.values() for s in g for k in (s.get("outlier_keeps_by_service_window") or {})})
    out = ["### outlier keeps in the fault window by service (mean over seeds)", "",
           "| service | " + " | ".join(names) + " |", "|---|" + "---|" * len(names)]
    for svc in svcs:
        vals = []
        for n in names:
            xs = [float((s.get("outlier_keeps_by_service_window") or {}).get(svc, 0)) for s in groups[n]]
            vals.append(f"{sum(xs) / len(xs):.0f}" if xs else "n/a")
        out.append(f"| {svc} | " + " | ".join(vals) + " |")
    return "\n".join(out) + "\n"


def gap_tables(runs: str, rids: list[str]) -> str:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import gaps as G                                                # noqa: E402
    out = []
    for rid in rids:
        rd = os.path.join(runs, rid)
        meta = json.load(open(os.path.join(rd, "meta.json")))
        f = meta.get("fault") or {}
        shadow = next((os.path.join(rd, n) for n in ("shadow.jsonl.zst", "shadow.jsonl.gz", "shadow.jsonl")
                       if os.path.exists(os.path.join(rd, n))), None)
        if not f or not shadow:
            continue
        t = G.load(shadow)
        pre = G.analyse(t, (float(meta["t0"]) + 30, float(f["start"])), meta.get("placement") or {})
        dur = G.analyse(t, (float(f["start"]) + 5, float(f["end"])), meta.get("placement") or {})
        out += [f"### {rid}: gap p95 and callee server p95 (us), before vs during the fault", "",
                "| edge | hosts | gap before | gap during | server before | server during |", "|---|---|---|---|---|---|"]
        for e in sorted(pre["edges"]):
            a, b = pre["edges"][e], dur["edges"].get(e, {})
            out.append(f"| {e} | {a['hosts']} | {a['gap_p95_us']:.0f} | {b.get('gap_p95_us', float('nan')):.0f} | "
                       f"{a['server_p95_us']:.0f} | {b.get('server_p95_us', float('nan')):.0f} |")
        out.append("")
        out += ["| service | own p95 before | own p95 during | dur p95 before | dur p95 during |", "|---|---|---|---|---|"]
        for s in sorted(pre["services"]):
            a, b = pre["services"][s], dur["services"].get(s, {})
            out.append(f"| {s} | {a['own_p95_us']:.0f} | {b.get('own_p95_us', float('nan')):.0f} | "
                       f"{a['dur_p95_us']:.0f} | {b.get('dur_p95_us', float('nan')):.0f} |")
        out.append("")
    return "\n".join(out) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("runs")
    ap.add_argument("--seeds", type=int, nargs="+", default=[1, 2, 3])
    ap.add_argument("--gaps", action="store_true")
    ap.add_argument("--md")
    a = ap.parse_args(argv)
    sat = {"own trigger": [], "duration trigger": []}
    steady = {"own trigger": []}
    present = []
    for s in a.seeds:
        for name, rid in (("own trigger", f"2d-v12-saturated-own-s{s}"), ("duration trigger", f"2d-v12-saturated-dur-s{s}")):
            x = load(a.runs, rid)
            if x:
                sat[name].append(x)
                present.append(rid)
        x = load(a.runs, f"2d-v12-steady-own-s{s}")
        if x:
            steady["own trigger"].append(x)
    callers = sorted({c for g in sat.values() for s in g for c in (s.get("culprit_callers") or [])})
    md = ["## Stage 2d trigger comparison (12 %, socialgraph hog minutes 4-7, chosen, server_hold 200 ms)", "",
          f"Culprit callers (server ancestors in the shadow): {', '.join(callers) or 'n/a'}.", "",
          table("saturated", ROWS_SAT, sat), by_service(sat), table("steady", ROWS_STEADY, steady)]
    if a.gaps:
        md.append(gap_tables(a.runs, present))
    text = "\n".join(md)
    if a.md:
        with open(a.md, "w") as f:
            f.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
