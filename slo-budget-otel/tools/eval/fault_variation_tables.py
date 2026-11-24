#!/usr/bin/env python3
"""Tables over fault_variation.py's per-run JSON: the share of requests over the SLO before and
during the fault, what tracks it across the runs, the SLO next to the healthy latencies, and the
arms against head before the fault.

    python3 tools/eval/fault_variation_tables.py <dir of *.json> [--md out.md] [--csv per_run.csv]   # needs scipy
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os

import numpy as np
from scipy import stats

ARMS = ["head", "tail", "reservoir", "canopy", "keep-parent", "chosen"]
SCEN = ["saturated", "cascade", "cold_start"]


def load(d):
    rows = []
    for p in sorted(glob.glob(os.path.join(d, "*.json"))):
        r = json.load(open(p, encoding="utf-8"))
        if r.get("error"):
            print("ERROR", r["run_id"], r["error"])
            continue
        if r.get("share_during_by_end") is not None:     # the campaign's definition: roots that END in the window
            r["share_during_by_start"] = r.get("share_during")
            r["share_during"] = r["share_during_by_end"]
            r["diff_share"] = r["share_during"] - r.get("share_pre", 0.0)
        rows.append(r)
    return rows


def f(x, nd=1, unit=""):
    return "-" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x:.{nd}f}{unit}"


def spear(x, y):
    x, y = np.asarray(x, float), np.asarray(y, float)
    m = np.isfinite(x) & np.isfinite(y)
    if m.sum() < 4:
        return float("nan"), float("nan"), int(m.sum())
    r, p = stats.spearmanr(x[m], y[m])
    return float(r), float(p), int(m.sum())


def demean(rows, key, by="scenario"):
    """Residual of a column after removing its mean within `by` (scenario groups)."""
    v = np.array([r.get(key) if r.get(key) is not None else np.nan for r in rows], float)
    out = v.copy()
    for g in set(r[by] for r in rows):
        m = np.array([r[by] == g for r in rows])
        if np.isfinite(v[m]).any():
            out[m] = v[m] - np.nanmean(v[m])
    return out


def table_runs(rows):
    out = ["| run | start (EST) | over SLO before | during | diff (pts) | burst after the refresh (ms) | pre-fault p50 / p95 / p99 (ms, server side) | wrk2 whole run p50 / p95 / p99 (ms) | culprit own p95 pre / during / threshold (ms) | during / threshold | culprit node |",
           "|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        w = r.get("wrk") or {}
        out.append(f"| {r['run_id']} | {r['start_est'][5:16]} | {f(100 * (r.get('share_pre') or 0), 1, ' %')} | {f(100 * (r.get('share_during') or np.nan), 1, ' %')} | "
                   f"{f(100 * (r.get('diff_share') or np.nan), 1)} | {f(r.get('burst_after_refresh_ms'), 1)} | {f(r.get('pre_p50_ms'), 1)} / {f(r.get('pre_p95_ms'), 1)} / {f(r.get('pre_p99_ms'), 1)} | "
                   f"{f(w.get('p50'), 1)} / {f(w.get('p95'), 1)} / {f(w.get('p99'), 1)} | {f(r.get('culprit_own_p95_pre_ms'), 2)} / {f(r.get('culprit_own_p95_during_ms'), 2)} / {f(r.get('culprit_own_thr_ms'), 2)} | "
                   f"{f(r.get('culprit_own_p95_over_thr'), 2)} | {r.get('culprit_node') or '-'} |")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("dir")
    ap.add_argument("--md")
    ap.add_argument("--csv")
    a = ap.parse_args()
    rows = load(a.dir)
    rows = [r for r in rows if r["scenario"] in SCEN]
    arms_all = ARMS + sorted({r["arm"] for r in rows} - set(ARMS))      # and every arm-variant
    rows.sort(key=lambda r: (SCEN.index(r["scenario"]), arms_all.index(r["arm"]), r["seed"]))
    out = []
    out.append(f"## Per burner run ({len(rows)} runs: {sorted(set(r['scenario'] for r in rows))})\n")
    out += table_runs(rows)

    # the burner table in one layout, one definition (roots that start inside the fault window)
    out.append("\n## The burner table: share of requests over the SLO during the fault, by scenario, arm and seed\n")
    out.append("Definition (the campaign's `over_slo_frac_window`, also in every run's summary.json): requests (wrk2api root "
               "spans of the run's 100 % shadow) whose root ENDS in [fault start, fault end], duration over the SLO "
               "(23.4 ms, strictly), over all such requests.\n")
    for sc in SCEN:
        rs = [r for r in rows if r["scenario"] == sc]
        seeds = sorted({r["seed"] for r in rs})
        out.append(f"| {sc} | " + " | ".join(f"seed {sd}" for sd in seeds) + " | mean | share before the fault, mean |")
        out.append("|---|" + "---|" * (len(seeds) + 2))
        for arm in arms_all:
            cell = {r["seed"]: r for r in rs if r["arm"] == arm}
            if not cell:
                continue
            vals = [100 * cell[sd]["share_during"] for sd in seeds if sd in cell]
            pre = [100 * cell[sd]["share_pre"] for sd in seeds if sd in cell]
            out.append(f"| {arm} | " + " | ".join(f"{100 * cell[sd]['share_during']:.1f} %" if sd in cell else "-" for sd in seeds) +
                       f" | {np.mean(vals):.1f} % | {np.mean(pre):.2f} % |")
        out.append("")
    # correlation table ----------------------------------------------------------------------------
    out.append("\n## What tracks the share over the SLO during the fault\n")
    y = np.array([r.get("share_during") if r.get("share_during") is not None else np.nan for r in rows], float)
    preds = {
        "share over SLO before the fault": [r.get("share_pre") for r in rows],
        "pre-fault p50 (ms)": [r.get("pre_p50_ms") for r in rows],
        "pre-fault p95 (ms)": [r.get("pre_p95_ms") for r in rows],
        "pre-fault p99 (ms)": [r.get("pre_p99_ms") for r in rows],
        "wrk2 whole-run p50 (ms)": [(r.get("wrk") or {}).get("p50") for r in rows],
        "wrk2 whole-run p95 (ms)": [(r.get("wrk") or {}).get("p95") for r in rows],
        "culprit own p95 during / threshold": [r.get("culprit_own_p95_over_thr") for r in rows],
        "culprit own p95 during (ms)": [r.get("culprit_own_p95_during_ms") for r in rows],
        "start hour (EST, 0-24)": [r.get("start_hour_est") for r in rows],
        "run order (epoch)": [r.get("t0") for r in rows],
    }
    out.append("| candidate | Spearman r, all runs | p | r after removing the scenario mean | p | n |")
    out.append("|---|---|---|---|---|---|")
    ys = [r.get("share_during") for r in rows]
    for name, xs in preds.items():
        r_, p_, n_ = spear(xs, ys)
        # within-scenario (residuals)
        x_arr = np.array([x if x is not None else np.nan for x in xs], float)
        tmp = [dict(r, _x=x_arr[i]) for i, r in enumerate(rows)]
        xd = demean(tmp, "_x")
        yd = demean([dict(r, _y=ys[i]) for i, r in enumerate(rows)], "_y")
        r2_, p2_, n2_ = spear(xd, yd)
        out.append(f"| {name} | {r_:+.2f} | {p_:.2g} | {r2_:+.2f} | {p2_:.2g} | {n_} |")
    # placement: share_during by the culprit's node, per scenario
    out.append("\n### Share during the fault by the culprit's node\n")
    out.append("| scenario | node | runs | mean share during | min-max |")
    out.append("|---|---|---|---|---|")
    for sc in SCEN:
        for nd in sorted({r["culprit_node"] for r in rows if r["scenario"] == sc}):
            xs = [100 * r["share_during"] for r in rows if r["scenario"] == sc and r["culprit_node"] == nd and r.get("share_during") is not None]
            if xs:
                out.append(f"| {sc} | {nd} | {len(xs)} | {np.mean(xs):.1f} % | {min(xs):.1f}-{max(xs):.1f} % |")
    # where the variation sits: arm against seed, per scenario (additive decomposition of the cell values)
    out.append("\n### Where the variation sits (share over the SLO during the fault, per scenario)\n")
    out.append("Two-way layout of the runs of the six arms by three seeds; the percentages are shares of the total sum of squares.\n")
    out.append("| scenario | runs | arm | seed | residual | between-seed sd of the arm means (pts) | between-arm sd of the seed means (pts) |")
    out.append("|---|---|---|---|---|---|---|")
    for sc in SCEN:
        rs = [r for r in rows if r["scenario"] == sc and r.get("share_during") is not None]
        arms = [x for x in arms_all if any(r["arm"] == x for r in rs)]
        seeds = sorted({r["seed"] for r in rs})
        cell = {(r["arm"], r["seed"]): 100 * r["share_during"] for r in rs}
        arms = [x for x in arms if all((x, sd) in cell for sd in seeds)]          # complete arms only
        if not arms or len(seeds) < 2:
            continue
        M = np.array([[cell[(x, sd)] for sd in seeds] for x in arms])              # arm x seed
        g = M.mean()
        ss_t = ((M - g) ** 2).sum()
        ss_a = len(seeds) * ((M.mean(1) - g) ** 2).sum()
        ss_s = len(arms) * ((M.mean(0) - g) ** 2).sum()
        ss_r = max(0.0, ss_t - ss_a - ss_s)
        out.append(f"| {sc} | {M.size} | {100 * ss_a / ss_t:.0f} % | {100 * ss_s / ss_t:.0f} % | {100 * ss_r / ss_t:.0f} % | "
                   f"{M.mean(0).std(ddof=1):.1f} | {M.mean(1).std(ddof=1):.1f} |")
    # SLO vs healthy -------------------------------------------------------------------------------
    out.append("\n## The SLO next to the healthy latencies\n")
    slo = rows[0]["slo_ms"] if rows else float("nan")
    out.append(f"SLO: {slo:.1f} ms (compose_post)\n")
    out.append("| group | runs | share over SLO before the fault | pre-fault p50 | p95 | p99 (ms) | p95 / SLO | p99 / SLO |")
    out.append("|---|---|---|---|---|---|---|---|")
    groups = [("all burner runs (pre-fault)", rows)] + [(f"{sc}", [r for r in rows if r["scenario"] == sc]) for sc in SCEN]
    for name, rs in groups:
        if not rs:
            continue
        p50 = np.mean([r["pre_p50_ms"] for r in rs if r.get("pre_p50_ms")])
        p95 = np.mean([r["pre_p95_ms"] for r in rs if r.get("pre_p95_ms")])
        p99 = np.mean([r["pre_p99_ms"] for r in rs if r.get("pre_p99_ms")])
        sh = np.mean([r["share_pre"] for r in rs if r.get("share_pre") is not None])
        out.append(f"| {name} | {len(rs)} | {100 * sh:.2f} % | {p50:.1f} | {p95:.1f} | {p99:.1f} | {p95 / slo:.2f} | {p99 / slo:.2f} |")

    # arms vs head -----------------------------------------------------------------------------------
    out.append("\n## Does any arm run slower than head before a fault?  (paired by scenario and seed, pre-fault)\n")
    out.append("| arm | pairs | mean diff p50 (ms) [min, max] | mean diff p95 (ms) [min, max] | mean diff p99 (ms) [min, max] | pairs slower at p95 | Wilcoxon p (diff p95) | diff share over SLO before (pts) |")
    out.append("|---|---|---|---|---|---|---|---|")
    by = {(r["arm"], r["scenario"], r["seed"]): r for r in rows}
    for arm in arms_all:
        if arm == "head":
            continue
        dp50, dp95, dp99, dsh = [], [], [], []
        for sc in SCEN:
            for sd in sorted({r["seed"] for r in rows if r["scenario"] == sc}):
                h, x = by.get(("head", sc, sd)), by.get((arm, sc, sd))
                if not h or not x or h.get("pre_p95_ms") is None or x.get("pre_p95_ms") is None:
                    continue
                dp50.append(x["pre_p50_ms"] - h["pre_p50_ms"])
                dp95.append(x["pre_p95_ms"] - h["pre_p95_ms"])
                dp99.append(x["pre_p99_ms"] - h["pre_p99_ms"])
                dsh.append(100 * (x["share_pre"] - h["share_pre"]))
        if not dp95:
            continue
        try:
            wp = stats.wilcoxon(dp95).pvalue if any(abs(d) > 1e-9 for d in dp95) else float("nan")
        except ValueError:
            wp = float("nan")
        slower = sum(d > 0 for d in dp95)
        out.append(f"| {arm} | {len(dp95)} | {np.mean(dp50):+.2f} [{min(dp50):+.2f}, {max(dp50):+.2f}] | {np.mean(dp95):+.2f} [{min(dp95):+.2f}, {max(dp95):+.2f}] | "
                   f"{np.mean(dp99):+.2f} [{min(dp99):+.2f}, {max(dp99):+.2f}] | {slower}/{len(dp95)} | {wp:.2g} | {np.mean(dsh):+.2f} |")
    text = "\n".join(out) + "\n"
    print(text)
    if a.md:
        open(a.md, "w", encoding="utf-8").write(text)
    if a.csv and rows:
        keys = [k for k in rows[0] if k not in ("wrk", "placement", "culprit_coresident", "culprit_nodes")]
        with open(a.csv, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(keys + ["wrk_p50", "wrk_p95", "wrk_p99", "wrk_rps"])
            for r in rows:
                wk = r.get("wrk") or {}
                w.writerow([r.get(k) for k in keys] + [wk.get("p50"), wk.get("p95"), wk.get("p99"), wk.get("rps")])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
