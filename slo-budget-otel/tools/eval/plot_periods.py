#!/usr/bin/env python3
"""Per-period time series of one cluster run (one row per 5-s period of the central) and its plot.

    python3 tools/eval/plot_periods.py series <run_dir> [-o <run_dir>/periods.json]
    python3 tools/eval/plot_periods.py plot <periods.json> -o <out.png> [--services] [--title ...]

`series` runs where the export is (node-0, where the raw exports stay).  It reads the central's
per-period history (central-status.json), the run's times (meta.json; times.json for the Stage 2
runs) and, when the run has an export, counts the outlier keeps per service and period: reasons
outlier and escalate, as the tables count them (metrics.FLAG_REASONS), binned by the agent's
admission stamp (the span's end when unstamped).  Their sum per period is the central's
kept_outlier but for a keep that straddles a period boundary.

`plot` (matplotlib) draws one panel per measure on a shared time axis, one 5-s step per period
(those at least half inside the load): requests per second (roots seen by the agents), spans
admitted against V* and the 1.25 V* cap, cap drops and the per-request budget B; with --services
also the outlier keeps per service and the periods each service's threshold was frozen.  The fault
or burst window is shaded.  Every plotted value goes to a CSV next to the PNG (same name, .csv).
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))

# light chart chrome and the one series hue (dataviz reference palette: slot 1 blue, slot 2 orange)
SURFACE, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
SERIES, WINDOW = "#2a78d6", "#eb6834"
SEQ = ["#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec", "#5598e7", "#3987e5", "#2a78d6",
       "#256abf", "#1c5cab", "#184f95", "#104281", "#0d366b"]           # blue 100 .. 700


def _load(path: str):
    return json.load(open(path)) if os.path.exists(path) else None


def call_order(graph: dict | None) -> list[str]:
    """The services in call-tree order (depth first from each route's root)."""
    out: list[str] = []
    for route, root in sorted(((graph or {}).get("root") or {}).items()):
        children = (graph.get("children") or {}).get(route) or {}
        stack = [root]
        while stack:
            s = stack.pop()
            if s not in out:
                out.append(s)
                stack.extend(reversed(children.get(s) or []))
    return out


def run_info(rd: str, status: dict) -> dict:
    """t0, t1, period, V*, cap and the fault or burst window (absolute times) of a run."""
    meta = _load(os.path.join(rd, "meta.json"))
    if meta:                                       # Stage 3 (tools/campaign/run_one.sh)
        f, b, w = meta.get("fault"), meta.get("burst"), None
        if f:
            w = {"kind": "fault", "start": f["start"], "end": f["end"],
                 "label": f"{f.get('kind', '?')} fault on {', '.join(f.get('targets') or [str(f.get('target'))])}"}
        elif b:
            w = {"kind": "burst", "start": b["start"], "end": b["end"], "label": f"burst +{b.get('extra_rps')} req/s"}
        return {"run": meta.get("run_id") or os.path.basename(rd), "t0": meta["t0"], "t1": meta["t1"],
                "period_s": meta.get("period_s", 5), "v_star": meta.get("v_star"), "cap": meta.get("cap"),
                "rps": meta.get("rps"), "culprit": (f or {}).get("culprit"), "window": w}
    t = _load(os.path.join(rd, "times.json"))      # Stage 2 (tools/run_cluster.sh)
    if not t:
        raise SystemExit(f"{rd}: neither meta.json nor times.json")
    w = None
    if t.get("burst_start"):
        w = {"kind": "burst", "start": t["burst_start"], "end": t["burst_end"], "label": f"burst +{t.get('burst_rps')} req/s"}
    elif t.get("hog_start"):
        w = {"kind": "fault", "start": t["hog_start"], "end": t["hog_end"], "label": "CPU hog fault"}
    return {"run": os.path.basename(rd), "t0": t["t0"], "t1": t["t1"], "period_s": 5, "v_star": status.get("v_star"),
            "cap": status.get("cap"), "rps": t.get("rps"), "culprit": None, "window": w}


def series(rd: str, out: str) -> int:
    st = json.load(open(os.path.join(rd, "central-status.json")))
    info = run_info(rd, st)
    P, t0 = float(info["period_s"]), float(info["t0"])
    rows = [{"period": h["period"], "t": round(h["period"] * P - t0, 3), "requests": h.get("requests", 0),
             "spans": h.get("spans", 0), "received": h.get("received", 0), "admitted": h.get("admitted", 0),
             "cap_dropped": h.get("cap_dropped", 0), "demand": h.get("demand"), "b": h.get("b"),
             "v_star": h.get("v_star"), "kept_total": h.get("kept_total", 0), "kept_outlier": h.get("kept_outlier", 0),
             "frozen": sorted(h.get("frozen") or [])} for h in st.get("history") or []]
    services = call_order(st.get("graph"))
    exp = next((p for p in (os.path.join(rd, n) for n in ("export.json.zst", "export.json.gz", "export.json"))
                if os.path.exists(p)), None)
    keeps = None
    if exp:
        sys.path.insert(0, HERE)
        from sloeval.metrics import FLAG_REASONS
        from sloeval.spans import read_export
        kept = read_export(exp)
        flag = set(FLAG_REASONS)
        by: dict[int, Counter] = defaultdict(Counter)
        for i, r in enumerate(kept.reason):
            if r in flag:
                ts = kept.admit[i] / 1e9 if kept.admit[i] >= 0 else kept.end_us[i] / 1e6
                by[int(math.floor(ts / P))][kept.service[i]] += 1
        known = {r["period"] for r in rows}
        for r in rows:
            r["outlier_keeps_by_service"] = dict(by.get(r["period"], {}))
        keeps = {"file": os.path.basename(exp), "reasons_counted": list(FLAG_REASONS), "kept_spans": len(kept.reason),
                 "kept_by_reason": dict(Counter(kept.reason)),
                 "outlier_keeps_outside_history": sum(sum(c.values()) for p, c in by.items() if p not in known)}
        services += sorted({s for c in by.values() for s in c} - set(services))
    services += sorted({s for r in rows for s in r["frozen"]} - set(services))
    w = info["window"]
    doc = {"run": info["run"], "source": os.path.abspath(rd), "t0": t0, "t1": info["t1"], "period_s": P,
           "v_star": info["v_star"], "cap": info["cap"], "rps": info["rps"], "culprit": info["culprit"],
           "window": dict(w, start=round(w["start"] - t0, 3), end=round(w["end"] - t0, 3)) if w else None,
           "services": services, "export": keeps, "rows": rows}
    with open(out, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=1)
    print(f"{out}: {len(rows)} periods, {len(services)} services, export {'counted' if keeps else 'absent'}")
    return 0


def _runs(flags: list[bool]) -> list[tuple[int, int]]:
    """[start, end) index ranges where flags is true."""
    out, a = [], None
    for i, f in enumerate(flags + [False]):
        if f and a is None:
            a = i
        elif not f and a is not None:
            out.append((a, i))
            a = None
    return out


def _thousands(v, _pos=None) -> str:
    return f"{v:,.0f}".replace(",", " ")


def plot(path: str, out: str, with_services: bool, title: str | None) -> int:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib import transforms
    from matplotlib.colors import LinearSegmentedColormap, PowerNorm
    from matplotlib.ticker import FuncFormatter, MultipleLocator

    d = json.load(open(path))
    P, dur = float(d["period_s"]), float(d["t1"]) - float(d["t0"])
    rows = [r for r in d["rows"] if -P / 2 <= r["t"] < dur - P / 2]   # periods at least half inside the load
    edges = np.append([r["t"] for r in rows], rows[-1]["t"] + P)
    svcs = list(d["services"])
    with_keeps = with_services and d.get("export") is not None
    panels = ["req", "adm", "cap", "b"] + (["keeps"] if with_keeps else []) + (["frozen"] if with_services else [])
    height = {"req": 1.0, "adm": 1.5, "cap": 0.8, "b": 1.0, "keeps": 0.2 * len(svcs) + 0.2, "frozen": 0.17 * len(svcs) + 0.2}
    plt.rcParams.update({"font.family": "sans-serif", "font.sans-serif": ["Segoe UI", "DejaVu Sans", "Arial"],
                         "axes.unicode_minus": False})
    fig, axes = plt.subplots(len(panels), 1, sharex=True, facecolor=SURFACE,
                             figsize=(11, sum(height[p] for p in panels) + 0.45 * len(panels) + 1.0),
                             gridspec_kw={"height_ratios": [height[p] for p in panels]})
    H = fig.get_figheight()
    fig.subplots_adjust(left=0.12, right=0.9, top=1 - 0.95 / H, bottom=0.5 / H, hspace=0.5)
    ax_of = dict(zip(panels, axes))

    def style(ax, ylabel: str, ttl: str, grid: bool = True):
        ax.set_facecolor(SURFACE)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
        for s in ("left", "bottom"):
            ax.spines[s].set_color(AXIS)
            ax.spines[s].set_linewidth(0.8)
        ax.tick_params(colors=AXIS, labelcolor=INK2, labelsize=8, length=3, width=0.6)
        if grid:
            ax.grid(axis="both", color=GRID, linewidth=0.6)
        ax.set_axisbelow(True)
        ax.set_ylabel(ylabel, color=INK2, fontsize=8.5)
        ax.set_title(ttl, loc="left", color=INK, fontsize=9.5, fontweight="semibold", pad=5)

    def margin_label(ax, y: float, text: str, color: str = INK2):
        tr = transforms.blended_transform_factory(ax.transAxes, ax.transData)
        ax.text(1.008, y, text, transform=tr, ha="left", va="center", fontsize=8, color=color, clip_on=False)

    w = d.get("window")
    if w:
        for p in panels:
            ax = ax_of[p]
            if p in ("keeps", "frozen"):                 # edges only: a wash would change the cells' colours
                for x in (w["start"], w["end"]):
                    ax.axvline(x, color=WINDOW, linewidth=1.0, zorder=3)
            else:
                ax.axvspan(w["start"], w["end"], color=WINDOW, alpha=0.10, linewidth=0, zorder=0)
        ax_of["req"].set_title(f"shaded: {w['label']}, t = {w['start']:.0f} to {w['end']:.0f} s", loc="right",
                               color=INK2, fontsize=8, pad=5)

    def stairs(ax, key: str):
        v = np.array([float(r.get(key) or 0.0) for r in rows])
        ax.stairs(v, edges, color=SERIES, linewidth=1.5, baseline=None, zorder=2)
        return v

    ax = ax_of["req"]
    style(ax, "req/s", "Requests per second (roots the agents saw, per 5-s period)")
    rps = np.array([r["requests"] / P for r in rows])
    ax.stairs(rps, edges, color=SERIES, linewidth=1.5, baseline=None, zorder=2)
    ax.set_ylim(0, max(1.0, rps.max()) * 1.18)

    ax = ax_of["adm"]
    style(ax, "spans", "Spans admitted per period, against V* and the 1.25 V* cap")
    adm = stairs(ax, "admitted")
    vs, cap = float(d["v_star"]), float(d["cap"])
    ax.axhline(vs, color=INK2, linewidth=0.9, zorder=1)
    ax.axhline(cap, color=INK, linewidth=0.9, zorder=1)
    margin_label(ax, vs, f"V* {_thousands(vs)}")
    margin_label(ax, cap, f"cap {_thousands(cap)}", INK)
    ax.set_ylim(0, max(cap, adm.max()) * 1.15)
    ax.yaxis.set_major_formatter(FuncFormatter(_thousands))

    ax = ax_of["cap"]
    style(ax, "spans", "Cap drops per period")
    drops = np.array([float(r.get("cap_dropped") or 0) for r in rows])
    if drops.max() > 0:
        ax.bar(edges[:-1] + P / 2, drops, width=0.8 * P, color=SERIES, linewidth=0, zorder=2)
        ax.set_ylim(0, drops.max() * 1.35)
        for i in np.flatnonzero(drops)[:4]:
            ax.text(edges[i] + P / 2, drops[i], f" {drops[i]:.0f}", ha="left", va="bottom", fontsize=8, color=INK2)
    else:
        ax.set_ylim(0, 1)
        ax.set_yticks([0, 1])
        ax.text(0.5, 0.5, "no cap drops in any period", transform=ax.transAxes, ha="center", va="center", fontsize=8.5, color=MUTED)

    ax = ax_of["b"]
    style(ax, "spans / request", "Per-request budget B")
    b = stairs(ax, "b")
    ax.set_ylim(0, b.max() * 1.18)

    if with_keeps:
        ax = ax_of["keeps"]
        style(ax, "", "Outlier keeps per service and period (reasons outlier and escalate)", grid=False)
        m = np.array([[float((r.get("outlier_keeps_by_service") or {}).get(s, 0)) for r in rows] for s in svcs])
        cmap = LinearSegmentedColormap.from_list("blue", SEQ)
        cmap.set_under(SURFACE)
        vmax = max(1.0, m.max())
        mesh = ax.pcolormesh(edges, np.arange(len(svcs) + 1), m, cmap=cmap, norm=PowerNorm(0.5, vmin=0.5, vmax=vmax),
                             shading="flat", zorder=1)
        for i in range(1, len(svcs)):
            ax.axhline(i, color=SURFACE, linewidth=1.2, zorder=2)
        cax = ax.inset_axes([1.012, 0.0, 0.012, 1.0])
        cb = fig.colorbar(mesh, cax=cax, extend="neither")
        cb.outline.set_visible(False)
        ticks = [t for t in (1, 2, 5, 10, 20, 50, 100, 200, 500) if t <= vmax]
        cb.set_ticks(ticks if ticks[-1] == vmax or len(ticks) < 2 else ticks + [vmax])
        cb.ax.tick_params(labelsize=7.5, colors=AXIS, labelcolor=INK2, length=2)
        cb.ax.yaxis.set_major_formatter(FuncFormatter(_thousands))
        totals = m.sum(axis=1)
        ax.set_title(f"total {_thousands(totals.sum())}", loc="right", color=INK2, fontsize=8, pad=5)

    if with_services:
        ax = ax_of["frozen"]
        style(ax, "", "Threshold frozen (each bar one interval of consecutive frozen periods)", grid=False)
        for i, s in enumerate(svcs):
            runs = _runs([s in (r.get("frozen") or []) for r in rows])
            if runs:
                ax.broken_barh([(edges[a], edges[e] - edges[a]) for a, e in runs], (i + 0.2, 0.6), color=SERIES, linewidth=0)
            n = sum(e - a for a, e in runs)
            margin_label(ax, i + 0.5, f"{len(runs)} / {n}" if runs else "-", INK2 if runs else MUTED)
        tr = transforms.blended_transform_factory(ax.transAxes, ax.transAxes)
        ax.text(1.008, 1.0, "intervals /\nperiods", transform=tr, ha="left", va="bottom", fontsize=7.5, color=MUTED)

    for p in ("keeps", "frozen"):
        if p in ax_of:
            ax = ax_of[p]
            ax.set_ylim(len(svcs), 0)
            ax.set_yticks(np.arange(len(svcs)) + 0.5)
            ax.set_yticklabels([f"{s} (culprit)" if s == d.get("culprit") else s for s in svcs], fontsize=8)
            ax.tick_params(axis="y", length=0)
    last = axes[-1]
    last.set_xlim(edges[0], edges[-1])
    last.xaxis.set_major_locator(MultipleLocator(60))
    last.set_xlabel("seconds since the load started (one step per 5-s period)", color=INK2, fontsize=8.5)
    fig.suptitle(title or d["run"], x=0.12, y=1 - 0.15 / H, ha="left", va="top", fontsize=11.5, fontweight="semibold", color=INK)
    fig.text(0.12, 1 - 0.45 / H, f"{d['run']}: V* {_thousands(vs)} spans per period, cap 1.25 V* = "
             f"{_thousands(cap)}, {d.get('rps') or '?'} req/s offered, {P:.0f}-s periods", ha="left", va="top",
             fontsize=8.5, color=INK2)
    fig.savefig(out, dpi=200, facecolor=SURFACE)
    plt.close(fig)

    csv_path = os.path.splitext(out)[0] + ".csv"
    keys = ["t", "period", "requests", "spans", "received", "admitted", "cap_dropped", "demand", "b", "v_star",
            "kept_total", "kept_outlier"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f)
        wr.writerow(["t_s", "period", "requests", "spans", "received", "admitted", "cap_dropped", "demand", "b", "v_star",
                     "kept_total", "kept_outlier_central", "req_per_s", "frozen"]
                    + ([f"outlier_keeps_{s}" for s in svcs] if with_keeps else []))
        for r in rows:
            wr.writerow([r.get(k) for k in keys] + [r["requests"] / P, " ".join(r.get("frozen") or [])]
                        + ([(r.get("outlier_keeps_by_service") or {}).get(s, 0) for s in svcs] if with_keeps else []))
    print(f"wrote {out} and {csv_path}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("series")
    s.add_argument("run_dir")
    s.add_argument("-o", "--out")
    p = sub.add_parser("plot")
    p.add_argument("series_json")
    p.add_argument("-o", "--out", required=True)
    p.add_argument("--services", action="store_true", help="add outlier keeps per service and the frozen intervals")
    p.add_argument("--title")
    a = ap.parse_args(argv)
    if a.cmd == "series":
        return series(a.run_dir, a.out or os.path.join(a.run_dir, "periods.json"))
    return plot(a.series_json, a.out, a.services, a.title)


if __name__ == "__main__":
    raise SystemExit(main())
