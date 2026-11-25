#!/usr/bin/env python3
"""Stage 3 tables from the run summaries (tools/eval/eval_run.py).

    python3 tools/eval/tables.py /storage/royno7/stage3/runs --volume 0.12 [--scenarios steady saturated] [--seeds 1 2 3] [--json]

One row per arm x scenario: the mean over seeds with the seed spread as (min-max) and the
number of seeds; a row is flagged when a seed in --seeds is missing, or when its runs ran
different collector images (meta.json digests; "images" column).  Rows of faulted scenarios
list each run's measured share of requests over the SLO in the fault window ("over SLO, by seed";
late_everywhere's spinner sets the severity only roughly) and, for the burner scenarios, where the
load's bursts fell in the burner's quota period ("burst after the refresh, by seed"; burst_phase.json of
burst_phase.py), which the over-SLO share follows.  Tables: (1) competitors
(chosen, head, tail, reservoir, canopy, hindsight / vaif when their runs exist), (2)
ablations (chosen, threshold, keep-parent, and every arm-variant of run_one.sh's VARIANT), (3) the fault window under
each truth (Stage 3: single culprit, own time, the culprit's node, duration), (4) the common truth of all arms
(the runs evaluated with it), (5) on it, the services covered per period of the window and where the kept spans went, (6)
reconstruction coverage.  The
Stage 2d series (run ids 2d-*, tools/eval/compare2d.py), invalid-*, smoke-* and pilot-* runs and runs with an
INVALID file are left out.  Last, one row per run with the arrival stamp: the share of its server spans with a parent whose
arrival the program gave (under 97 %: FLAG), the misses by cause, the lowest service, and per node the program's counters and time.
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "overhead"))
import arrival_stats  # noqa: E402

SCENARIOS = ("steady", "saturated", "noisy_neighbour", "cascade", "bursty", "cold_start", "late_everywhere")
SCENARIO_LABELS = {"noisy_neighbour": "noisy-neighbour"}
COMPETITORS = ("chosen", "head", "tail", "reservoir", "canopy", "hindsight", "vaif", "slo-budget+vaif")
ABLATIONS = ("chosen", "threshold", "keep-parent")

# (column header, summary key, format, scale)
COLUMNS = [
    ("volume / target", "vol_ratio", "{:.2f}", 1.0),
    ("KB/period", "bytes_per_period", "{:.0f}", 1e-3),
    ("periods>cap", "over_cap_periods", "{:.1f}", 1.0),
    ("max/request", "max_per_request", "{:.1f}", 1.0),
    ("max/host/period", "max_per_host_period", "{:.0f}", 1.0),
    ("peak buffered", "buffer_peak_spans", "{:.0f}", 1.0),
    ("outlier share", "outlier_share", "{:.1f}%", 100.0),
    ("precision", "outlier_precision", "{:.2f}", 1.0),
    ("evidence s", "evidence_s", "{:.2f}", 1.0),
    ("culprit recall", "recall_culprit", "{:.2f}", 1.0),
    ("identified", "identified", None, None),
    ("explained", "attrib_explained", "{:.2f}", 1.0),
    ("true loss", "attrib_loss", "{:.2f}", 1.0),
    ("P95 err mean", "p95_err_svc_mean", "{:.1f}%", 100.0),
    ("P95 err max", "p95_err_svc_max", "{:.1f}%", 100.0),
    ("agent us/span", "agent_cpu_us_per_span", "{:.2f}", 1.0),
]
FAULT_COLUMNS = [
    ("over-SLO in fault", "over_slo_frac_window", "{:.1f}%", 100.0),
    ("prec. culprit", "outlier_precision_culprit_window", "{:.2f}", 1.0),
    ("prec. own", "outlier_precision_own_window", "{:.2f}", 1.0),
    ("prec. node", "outlier_precision_node_window", "{:.2f}", 1.0),
    ("prec. duration", "outlier_precision_window", "{:.2f}", 1.0),
    ("culprit recall", "recall_culprit", "{:.2f}", 1.0),
    ("own recall in fault", "recall_own_window", "{:.2f}", 1.0),
    ("faulty-span coverage", "faulty_span_coverage", "{:.2f}", 1.0),
    ("culprit keeps/period", "culprit_outlier_keeps_per_period", "{:.1f}", 1.0),
    ("caller share", "outlier_keeps_callers_window_frac", "{:.2f}", 1.0),
]
# the common truth of all arms (sloeval/arrivals.py): within-service latency on the estimated kernel receive time
WS_COLUMNS = [
    ("volume / target", "vol_ratio", "{:.2f}", 1.0),
    ("healthy truth rate", "truth_rate_healthy_ws", "{:.2f}%", 100.0),
    ("outlier share", "outlier_share_ws", "{:.1f}%", 100.0),
    ("precision", "outlier_precision_ws", "{:.2f}", 1.0),
    ("prec. in the window", "outlier_precision_ws_window", "{:.2f}", 1.0),
    ("recall in the window", "recall_ws_window", "{:.2f}", 1.0),
    ("culprit recall", "recall_culprit_ws", "{:.2f}", 1.0),
    ("evidence s", "evidence_ws_s", "{:.2f}", 1.0),
    ("over SLO with a culprit outlier", "over_slo_with_culprit_truth_ws", "{:.1f}%", 100.0),
    ("... kept", "over_slo_with_culprit_outlier_kept", "{:.1f}%", 100.0),
]
# where the kept spans of the window went, on the common truth
VOLUME_COLUMNS = [
    ("services with a true outlier, a period", "ws_services_with_an_outlier_per_period", "{:.1f}", 1.0),
    ("... with one kept", "ws_services_covered_per_period", "{:.1f}", 1.0),
    ("services covered", "ws_services_covered", "{:.2f}", 1.0),
    ("struggling services, a period", "ws_struggling_per_period", "{:.2f}", 1.0),
    ("... with a true outlier kept", "ws_struggling_covered_per_period", "{:.2f}", 1.0),
    ("struggling covered", "ws_struggling_covered", "{:.2f}", 1.0),
    ("periods with a struggling one", "ws_periods_with_struggling", "{:.0f}%", 100.0),
    ("kept: culprit's outliers", "ws_kept_culprit_outliers", "{:.1f}%", 100.0),
    ("kept: other services' outliers", "ws_kept_other_outliers", "{:.1f}%", 100.0),
    ("kept: ordinary spans", "ws_kept_ordinary", "{:.1f}%", 100.0),
    ("culprit's outliers, a period", "ws_culprit_outliers_per_period", "{:.0f}", 1.0),
    ("... kept", "ws_culprit_kept_per_period", "{:.1f}", 1.0),
    ("... kept as outlier (ours)", "ws_culprit_kept_as_outlier_per_period", "{:.1f}", 1.0),
    ("N_need (ours)", "n_need_window", "{:.0f}", 1.0),
]
RECON_COLUMNS = [
    ("orphans/kept", "recon_orphans_frac_of_kept", "{:.0f}%", 100.0),
    ("placeable (known ids)", "recon_placeable_frac_of_orphans", "{:.1f}%", 100.0),
    ("hole = 1", "recon_hole_1_frac_of_orphans", "{:.1f}%", 100.0),
    ("one tree before", "recon_traces_one_tree_before_frac", "{:.1f}%", 100.0),
    ("one tree, known ids", "recon_traces_one_tree_after_known_ids_only_frac", "{:.1f}%", 100.0),
    ("one tree, synthesised", "recon_traces_one_tree_after_frac", "{:.1f}%", 100.0),
    ("placeholders/real span", "recon_placeholders_per_real_span", "{:.2f}", 1.0),
]


def lowest_service(s: dict) -> str:
    """of the 100 % export: the service with the smallest share of its requests stamped by the program"""
    svc = (s.get("arrival_estimate_vs_stamp") or {}).get("services") or {}
    if not svc:
        return "-"
    n, x = min(svc.items(), key=lambda kv: kv[1]["stamped"] / max(1, kv[1]["calls"]))
    return f"{n} {100 * x['stamped'] / max(1, x['calls']):.2f} % of {x['calls']:,}"


def load(root: str) -> list[dict]:
    out = []
    for p in sorted(glob.glob(os.path.join(root, "*", "summary.json"))):
        name = os.path.basename(os.path.dirname(p))
        if name.startswith(("2d-", "invalid-", "smoke-", "pilot-")) or os.path.exists(os.path.join(os.path.dirname(p), "INVALID")):
            continue
        with open(p, encoding="utf-8") as f:
            s = json.load(f)
        s["_dir"] = os.path.dirname(p)
        try:
            with open(os.path.join(s["_dir"], "burst_phase.json"), encoding="utf-8") as f:
                s["burst_after_refresh_ms"] = json.load(f)["after_refresh_ms"]
        except (OSError, ValueError, KeyError):
            pass
        try:
            with open(os.path.join(s["_dir"], "meta.json"), encoding="utf-8") as f:
                im = json.load(f).get("images") or {}
            s["_image"] = str(im.get("collector_digest") or im.get("collector") or "?")
        except (OSError, ValueError):
            s["_image"] = "?"
        out.append(s)
    return out


def _num(v):
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float)) and math.isfinite(v):
        return float(v)
    return None


def agg(rows: list[dict], key: str) -> dict:
    vals = [x for x in (_num(r.get(key)) for r in rows) if x is not None]
    if not vals:
        return {"mean": None, "min": None, "max": None, "n": 0}
    return {"mean": sum(vals) / len(vals), "min": min(vals), "max": max(vals), "n": len(vals)}


def cell(a: dict, fmt: str, scale: float) -> str:
    if a["mean"] is None:
        return "-"
    m = fmt.format(a["mean"] * scale)
    if a["n"] > 1 and a["max"] != a["min"]:
        lo, hi = fmt.format(a["min"] * scale), fmt.format(a["max"] * scale)
        return f"{m} ({lo}-{hi})"
    return m


def hole_dist(rows: list[dict]) -> str:
    """Mean over seeds of the full-reconstruction hole-length shares (1 / 2 / 3 / 4+)."""
    acc = {"1": [], "2": [], "3": [], "4+": []}
    for r in rows:
        h = r.get("recon_hole_hist_full") or {}
        tot = sum(h.values())
        if not tot:
            continue
        c = {"1": 0, "2": 0, "3": 0, "4+": 0}
        for k, v in h.items():
            try:
                kk = int(k)
            except ValueError:
                continue
            c[str(kk) if kk <= 3 else "4+"] += v
        for k in acc:
            acc[k].append(c[k] / tot)
    if not acc["1"]:
        return "-"
    return " / ".join(f"{100 * sum(v) / len(v):.0f}%" for v in acc.values())


def build(summaries, volume, scenarios, seeds, arms, columns):
    rows = []
    for arm in arms:
        for sc in scenarios:
            rs = [s for s in summaries if s.get("arm") == arm and s.get("scenario") == sc
                  and s.get("volume_frac") is not None and abs(float(s["volume_frac"]) - volume) < 1e-6
                  and (not seeds or int(s.get("seed", -1)) in seeds)]
            if not rs:
                continue
            got = sorted(int(s.get("seed", -1)) for s in rs)
            row = {"arm": arm, "scenario": sc, "seeds": got, "missing": sorted(set(seeds) - set(got)) if seeds else [],
                   "images": sorted({s.get("_image", "?") for s in rs}),
                   "over_slo_runs": [s.get("over_slo_frac_window") for s in sorted(rs, key=lambda x: int(x.get("seed", -1)))],
                   "burst_runs": [s.get("burst_after_refresh_ms") for s in sorted(rs, key=lambda x: int(x.get("seed", -1)))]}
            for _, key, _, _ in columns:
                if key == "identified":
                    vals = [s.get("identified") for s in rs if s.get("identified") is not None]
                    row[key] = {"k": int(sum(1 for v in vals if v)), "n": len(vals)}
                else:
                    row[key] = agg(rs, key)
            if columns is RECON_COLUMNS:
                row["hole_dist"] = hole_dist(rs)
            rows.append(row)
    return rows


def markdown(rows, columns, title, extra=None) -> str:
    if not rows:
        return f"### {title}\n\n(no runs)\n"
    per_run = any(_num(v) is not None for r in rows for v in r.get("over_slo_runs") or [])
    burst = any(_num(v) is not None for r in rows for v in r.get("burst_runs") or [])
    hdr = (["arm", "scenario", "seeds"] + [c[0] for c in columns] + ([extra[0]] if extra else [])
           + (["over SLO, by seed"] if per_run else []) + (["burst after the refresh, by seed (ms)"] if burst else []) + ["images"])
    lines = [f"### {title}", "", "| " + " | ".join(hdr) + " |", "|" + "---|" * len(hdr)]
    for r in rows:
        seeds = str(len(r["seeds"])) + (f" (missing {','.join(map(str, r['missing']))})" if r["missing"] else "")
        cells = [r["arm"], SCENARIO_LABELS.get(r["scenario"], r["scenario"].replace("_", "-")), seeds]
        for name, key, fmt, scale in columns:
            if key == "identified":
                v = r[key]
                cells.append(f"{v['k']}/{v['n']}" if v["n"] else "-")
            else:
                cells.append(cell(r[key], fmt, scale))
        if extra:
            cells.append(r.get(extra[1], "-"))
        if per_run:
            vals = [_num(v) for v in r.get("over_slo_runs") or []]
            cells.append(" / ".join(f"{100 * v:.1f}%" for v in vals if v is not None) or "-")
        if burst:
            vals = [_num(v) for v in r.get("burst_runs") or []]
            cells.append(" / ".join(f"{v:.1f}" if v is not None else "?" for v in vals) if any(v is not None for v in vals) else "-")
        ims = r.get("images") or []
        cells.append(ims[0].split(":")[-1][:12] if len(ims) == 1 else f"MIXED ({len(ims)})")
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs_root")
    ap.add_argument("--volume", type=float, required=True)
    ap.add_argument("--scenarios", nargs="+", default=list(SCENARIOS))
    ap.add_argument("--seeds", nargs="+", type=int, default=[1, 2, 3])
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    summaries = load(a.runs_root)
    comp = build(summaries, a.volume, a.scenarios, a.seeds, COMPETITORS, COLUMNS)
    arms_present = [x for x in dict.fromkeys(s.get("arm") for s in summaries) if x]
    variants = tuple(sorted(x for x in arms_present if x not in COMPETITORS + ABLATIONS))      # arm-variant (run_one.sh's VARIANT)
    abl = build(summaries, a.volume, a.scenarios, a.seeds, ABLATIONS + variants, COLUMNS)
    present = [x for x in COMPETITORS + ABLATIONS[1:] + variants if x in arms_present]
    rec = build(summaries, a.volume, a.scenarios, a.seeds, present, RECON_COLUMNS)
    with_ws = [s for s in summaries if s.get("recall_ws") is not None]
    ws = build(with_ws, a.volume, a.scenarios, a.seeds, [x for x in present if any(s.get("arm") == x for s in with_ws)], WS_COLUMNS) if with_ws else []
    with_volume = [s for s in summaries if s.get("ws_kept_window") is not None]
    vol = build(with_volume, a.volume, a.scenarios, a.seeds, [x for x in present if any(s.get("arm") == x for s in with_volume)], VOLUME_COLUMNS) if with_volume else []
    faulted = [sc for sc in a.scenarios if sc not in ("steady", "bursty")]
    flt = build(summaries, a.volume, faulted, a.seeds, present, FAULT_COLUMNS)
    if a.json:
        print(json.dumps({"volume": a.volume, "competitors": comp, "ablations": abl, "fault": flt, "recon": rec, "common_truth": ws, "common_truth_volume": vol}, indent=1))
        return 0
    v = f"collection volume target {a.volume:.0%}"
    print(markdown(comp, COLUMNS, f"Competitors, {v}"))
    print(markdown(abl, COLUMNS, f"Ablations, {v}"))
    print(markdown(flt, FAULT_COLUMNS, f"Fault window under each truth, {v}"))
    if ws:
        print(markdown(ws, WS_COLUMNS, f"Common truth of all arms: within-service latency on the estimated kernel receive time, {v} (window: the fault, bursty: the burst, steady: the run)"))
    if vol:
        print(markdown(vol, VOLUME_COLUMNS, f"Common truth: services covered in the window and where its kept spans went, {v} (struggling: at least 2 % of the service's spans of the period are true outliers; ordinary: every kept span that is no true outlier of a service, client spans and roots too)"))
    stamped = [s for s in summaries if s.get("arrival") and abs(float(s["volume_frac"]) - a.volume) < 1e-6 and s.get("scenario") in a.scenarios]
    if stamped:
        print(f"### Arrival stamp, {v}\n")
        print("\n".join(arrival_stats.rows([([os.path.basename(s["_dir"]), lowest_service(s)], s["arrival"]) for s in stamped], ["run", "lowest service"])) + "\n")
    print(markdown(rec, RECON_COLUMNS, f"Reconstruction coverage, {v}", extra=("hole length 1/2/3/4+", "hole_dist")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
