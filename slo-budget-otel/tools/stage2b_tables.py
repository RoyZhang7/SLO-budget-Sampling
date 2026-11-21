#!/usr/bin/env python3
"""Markdown tables for the Stage 2b report from the pulled run directories.

  python3 tools/stage2b_tables.py results/cluster-*-ondemand results/cluster-*-ondemand-recon \
      results/cluster-*-pin results/cluster-*-pin-recon --burst results/cluster-*-burst

Each arm directory needs metrics.json (cluster_metrics.py), recon.json (recon_report.py),
arm.json and times.json; the burst directory needs central-status.json and times.json.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import statistics


def load(d: pathlib.Path, name: str):
    p = d / name
    return json.load(open(p, encoding="utf-8-sig")) if p.exists() else {}


def f(v, nd=3, pct=False):
    if v is None:
        return "-"
    if pct:
        return f"{100 * v:.1f} %"
    return f"{v:.{nd}f}" if isinstance(v, float) else str(v)


def arm_table(dirs):
    rows = []
    for d in dirs:
        d = pathlib.Path(d)
        m, r, a = load(d, "metrics.json"), load(d, "recon.json"), load(d, "arm.json")
        hist = {str(k if k < 4 else "4+"): v for k, v in ((int(k) if k != "4+" else 4, v) for k, v in r.get("orphans_per_trace", {}).items())}
        lat = r.get("per_service_latency", {})
        p95_delta = max((abs((x["p95_ms_all"] or 0) - (x["p95_ms_real"] or 0)) for x in lat.values()), default=0)
        self_delta = sum(abs(x["self_s_all"] - x["self_s_real"]) for x in lat.values())
        rows.append({
            "arm": f"{a.get('root_policy', '?')} / recon {'on' if a.get('recon') else 'off'}",
            "vol": f(m.get("vol_ratio_mean")), "exported": f(r.get("exported_over_vstar_incl_synthetic")),
            "roots": f"{m.get('roots_kept')} ({f(r.get('root_share_of_requests'), pct=True)}), pinned {m.get('roots_pinned')}",
            "orphans": f"{r.get('orphans')} ({f(r.get('orphans') / r['traces'] if r.get('traces') else None, 2)}/trace); 0/1/2/3/4+: "
                       + "/".join(str(hist.get(k, 0)) for k in ("0", "1", "2", "3", "4+")),
            "with_root": f"{f(r.get('traces_with_any_root_frac'), pct=True)} (real {f(r.get('traces_with_real_root_frac'), pct=True)})",
            "one_tree": f(r.get("traces_one_tree_frac"), pct=True),
            "outlier": f"{f(m.get('outlier_share_normal'), pct=True)} / {f(m.get('outlier_share_hog'), pct=True)}",
            "precision": f(m.get("precision_vs_healthy_table")),
            "synthetic": f"{r.get('spans_synthetic')} ({f(r.get('synthetic_per_period_mean'), 0)}/period)",
            "jaeger": f"durations changed {r.get('trace_duration_changed_by_placeholders')}; p95 shift max {p95_delta:.3f} ms; synthetic self {r.get('synthetic_self_time_s')} s (real-span figures identical)",
            "traces": r.get("traces"), "evidence": m.get("evidence_s_by_service", {}).get("socialgraph"),
            "B": f(m.get("B_mean"), 2),
        })
    hdr = ["arm", "vol/V* (sampled)", "exported/V* incl. placeholders", "roots kept (share of requests), pinned", "orphans as Jaeger counts them (per trace; 0/1/2/3/4+)",
           "traces with a root (real)", "one tree", "outlier share normal / hog", "precision", "placeholders", "Jaeger latency figures", "traces", "evidence socialgraph s", "B"]
    keys = ["arm", "vol", "exported", "roots", "orphans", "with_root", "one_tree", "outlier", "precision", "synthetic", "jaeger", "traces", "evidence", "B"]
    print("| " + " | ".join(hdr) + " |")
    print("|" + "---|" * len(hdr))
    for row in rows:
        print("| " + " | ".join(str(row[k]) for k in keys) + " |")


def burst_table(d, period=5.0, v_star=1380.0, cap=1725.0):
    d = pathlib.Path(d)
    st, times = load(d, "central-status.json"), load(d, "times.json")
    t0, b0, b1 = times["t0"], times.get("burst_start", 0), times.get("burst_end", 0)
    hist = st.get("history", [])
    print(f"\nBurst run {d.name}: base {times.get('rps')} req/s, +{times.get('burst_rps')} req/s from t={b0 - t0:.0f} s to {b1 - t0:.0f} s; V* {v_star:.0f}, cap {cap:.0f} span units per period.\n")
    print("| t (s) | window | requests | spans seen | admitted | admitted/V* | cap drops | demand | B | N_need | plan p |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    rows = []
    for h in hist:
        t = h["period"] * period - t0
        if not (0 <= t < times["t1"] - t0):
            continue
        w = "BURST" if b0 <= h["period"] * period < b1 else ("after" if b1 and b1 <= h["period"] * period < b1 + 30 else "")
        rows.append((t, w, h))
    for t, w, h in rows:
        if w or abs(t - b0 + t0) <= 15 or t % 60 == 0:
            pp = (h.get("plan_p") or {})
            print(f"| {t:.0f} | {w} | {h.get('requests')} | {h.get('spans')} | {h.get('admitted'):.0f} | {h.get('admitted') / v_star:.3f} | {h.get('cap_dropped')} | {h.get('demand', 0):.0f} | {h.get('b'):.2f} | {h.get('n_need')} | {statistics.mean(pp.values()) if pp else '-'} |")
    adm = [h["admitted"] for _, w, h in rows if not w]
    adm_b = [h["admitted"] for _, w, h in rows if w == "BURST"]
    adm_a = [h["admitted"] for _, w, h in rows if w == "after"]
    bs = [h["b"] for _, w, h in rows if not w and _ < b0 - t0]
    print(f"\nadmitted/V*: base {statistics.mean(adm) / v_star:.3f}, burst periods {statistics.mean(adm_b) / v_star if adm_b else 0:.3f} (max {max(adm_b) / v_star if adm_b else 0:.3f}), the 30 s after {statistics.mean(adm_a) / v_star if adm_a else 0:.3f}; "
          f"cap drops in the burst {sum(h['cap_dropped'] for _, w, h in rows if w == 'BURST')}; B before {statistics.mean(bs) if bs else 0:.2f}, in the burst {min(h['b'] for _, w, h in rows if w == 'BURST') if adm_b else 0:.2f}..{max(h['b'] for _, w, h in rows if w == 'BURST') if adm_b else 0:.2f}, after {[round(h['b'], 2) for _, w, h in rows if w == 'after']}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("arms", nargs="*")
    ap.add_argument("--burst")
    a = ap.parse_args()
    if a.arms:
        arm_table(a.arms)
    if a.burst:
        burst_table(a.burst)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
