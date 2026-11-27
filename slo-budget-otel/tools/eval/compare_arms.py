#!/usr/bin/env python3
"""Stage 3: one arm against another, scenario by scenario, from the run summaries (tables.py's rows).

    python3 tools/eval/compare_arms.py /storage/royno7/stage3/runs --volume 0.12 --base chosen-ebpf --other chosen-handler \
        --title "8a against 8b: what the measure buys"

For every scenario: the base arm's row, the other arm's row (means over seeds, the seed spread in parentheses as in
tables.py) and a difference row, other minus base, on the means (for a percentage column the difference is in points).
Two tables: (1) cost and volume, the duration truth (over the oracle T), the own-time truth in the fault window; (2) the common truth of all arms (within-service
latency on the estimated kernel receive time: what was found, which services were covered, where the kept spans went) and
the reconstruction coverage.  The difference is taken on the printed (rounded) means.  A key some runs lack (no evaluation on the common truth, no
reconstruction report) is averaged over the runs that have it and marked [n=k].  Runs with an INVALID file and the
2d-/invalid-/smoke-/pilot- runs are left out (tables.load).
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import tables  # noqa: E402

# (header, summary key, format, scale), picked from tables.py's column sets
COST_COLUMNS = [
    ("volume / target", "vol_ratio", "{:.2f}", 1.0),
    ("KB/period", "bytes_per_period", "{:.0f}", 1e-3),
    ("max/host/period", "max_per_host_period", "{:.0f}", 1.0),
    ("peak buffered", "buffer_peak_spans", "{:.0f}", 1.0),
    ("agent us/span", "agent_cpu_us_per_span", "{:.2f}", 1.0),
    ("outlier share (duration)", "outlier_share", "{:.1f}%", 100.0),
    ("precision (duration)", "outlier_precision", "{:.2f}", 1.0),
    ("culprit recall (duration)", "recall_culprit", "{:.2f}", 1.0),
    ("explained", "attrib_explained", "{:.2f}", 1.0),
    ("true loss", "attrib_loss", "{:.2f}", 1.0),
    ("P95 err mean", "p95_err_svc_mean", "{:.1f}%", 100.0),
    ("fault: prec. own", "outlier_precision_own_window", "{:.2f}", 1.0),
    ("fault: own recall", "recall_own_window", "{:.2f}", 1.0),
    ("fault: faulty-span coverage", "faulty_span_coverage", "{:.2f}", 1.0),
    ("fault: culprit keeps/period", "culprit_outlier_keeps_per_period", "{:.1f}", 1.0),
]
TRUTH_COLUMNS = [
    ("outlier share", "outlier_share_ws", "{:.1f}%", 100.0),
    ("precision", "outlier_precision_ws", "{:.2f}", 1.0),
    ("prec. in the window", "outlier_precision_ws_window", "{:.2f}", 1.0),
    ("recall in the window", "recall_ws_window", "{:.2f}", 1.0),
    ("culprit recall", "recall_culprit_ws", "{:.2f}", 1.0),
    ("evidence s", "evidence_ws_s", "{:.2f}", 1.0),
    ("over SLO with a culprit outlier kept", "over_slo_with_culprit_outlier_kept", "{:.1f}%", 100.0),
    ("services covered", "ws_services_covered", "{:.2f}", 1.0),
    ("struggling covered", "ws_struggling_covered", "{:.2f}", 1.0),
    ("kept: culprit's outliers", "ws_kept_culprit_outliers", "{:.1f}%", 100.0),
    ("kept: other services' outliers", "ws_kept_other_outliers", "{:.1f}%", 100.0),
    ("kept: ordinary spans", "ws_kept_ordinary", "{:.1f}%", 100.0),
    ("culprit's outliers, a period", "ws_culprit_outliers_per_period", "{:.0f}", 1.0),
    ("... kept", "ws_culprit_kept_per_period", "{:.1f}", 1.0),
    ("N_need (ours)", "n_need_window", "{:.0f}", 1.0),
    ("orphans/kept", "recon_orphans_frac_of_kept", "{:.0f}%", 100.0),
    ("one tree, synthesised", "recon_traces_one_tree_after_frac", "{:.1f}%", 100.0),
]


def _rounded(a: dict, fmt: str, scale: float) -> float:
    """the mean as the cell prints it (so the difference row agrees with the cells above it)"""
    return float(fmt.replace("%", "").format(a["mean"] * scale))


def diff_cell(a: dict, b: dict, fmt: str, scale: float, seeds: int) -> str:
    if a.get("mean") is None or b.get("mean") is None:
        return "-"
    d = _rounded(b, fmt, scale) - _rounded(a, fmt, scale)
    s = fmt.format(abs(d))
    out = "0" if s.strip("0.%") == "" else ("+" if d > 0 else "-") + s
    n = min(a["n"], b["n"])
    return out + (f" [n={n}]" if n < seeds else "")      # a difference over fewer seeds than the rows have


def value_cell(r: dict, key: str, fmt: str, scale: float) -> str:
    c = tables.cell(r[key], fmt, scale)
    n = r[key]["n"]
    return c + (f" [n={n}]" if n and n < len(r["seeds"]) else "")      # the key is missing in some of the arm's runs


def compare(rows_base: list[dict], rows_other: list[dict], columns, base: str, other: str, title: str, scenarios, seeds) -> str:
    hdr = ["scenario", "arm", "seeds"] + [c[0] for c in columns]
    lines = [f"### {title}", "", "| " + " | ".join(hdr) + " |", "|" + "---|" * len(hdr)]
    by = lambda rows: {r["scenario"]: r for r in rows}
    rb, ro = by(rows_base), by(rows_other)
    n = 0
    for sc in scenarios:
        a, b = rb.get(sc), ro.get(sc)
        if not a and not b:
            continue
        label = tables.SCENARIO_LABELS.get(sc, sc.replace("_", "-"))
        for arm, r in ((base, a), (other, b)):
            if not r:
                lines.append(f"| {label} | {arm} | 0 (missing {','.join(map(str, seeds))}) | " + " | ".join("-" for _ in columns) + " |")
                continue
            got = str(len(r["seeds"])) + (f" (missing {','.join(map(str, r['missing']))})" if r["missing"] else "")
            lines.append(f"| {label} | {arm} | {got} | " + " | ".join(value_cell(r, k, f, s) for _, k, f, s in columns) + " |")
        if a and b:
            lines.append(f"| {label} | **{other} - {base}** | | "
                         + " | ".join(f"**{diff_cell(a[k], b[k], f, s, min(len(a['seeds']), len(b['seeds'])))}**" for _, k, f, s in columns) + " |")
            n += 1
    if not n:
        lines.append("| (no scenario has both arms) | | | " + " | ".join("" for _ in columns) + " |")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs_root")
    ap.add_argument("--volume", type=float, required=True)
    ap.add_argument("--base", required=True, help="the reference arm (tables.py's arm label, e.g. chosen-ebpf)")
    ap.add_argument("--other", required=True, help="the arm compared with it (e.g. chosen-handler, threshold-ebpf)")
    ap.add_argument("--title", default="")
    ap.add_argument("--scenarios", nargs="+", default=list(tables.SCENARIOS))
    ap.add_argument("--seeds", nargs="+", type=int, default=[1, 2, 3])
    a = ap.parse_args()
    summaries = tables.load(a.runs_root)
    v = f"collection volume target {a.volume:.0%}"
    title = a.title or f"{a.other} against {a.base}"
    cost_b = tables.build(summaries, a.volume, a.scenarios, a.seeds, [a.base], COST_COLUMNS)
    cost_o = tables.build(summaries, a.volume, a.scenarios, a.seeds, [a.other], COST_COLUMNS)
    print(compare(cost_b, cost_o, COST_COLUMNS, a.base, a.other,
                  f"{title}: cost and volume, the duration truth, the own-time truth in the fault window; {v} (difference = {a.other} minus {a.base} on the "
                  f"printed means, points for %; [n=k]: the key is in k of the runs only)", a.scenarios, a.seeds))
    # every run: a key a run lacks (no common-truth evaluation, no reconstruction report) is left out of its mean and marked [n=k]
    tr_b = tables.build(summaries, a.volume, a.scenarios, a.seeds, [a.base], TRUTH_COLUMNS)
    tr_o = tables.build(summaries, a.volume, a.scenarios, a.seeds, [a.other], TRUTH_COLUMNS)
    print(compare(tr_b, tr_o, TRUTH_COLUMNS, a.base, a.other,
                  f"{title}: the common truth of all arms (within-service latency on the estimated kernel receive time) and the reconstruction coverage; {v}",
                  a.scenarios, a.seeds))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
