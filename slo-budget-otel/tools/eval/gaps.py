#!/usr/bin/env python3
"""Stage 2d measurement: the gap per call edge and the own time per service.

    python3 tools/eval/gaps.py --input healthy=runs/v12-steady-head-s1/shadow.jsonl.zst \\
        --input base=calib-base.json --input sg40=calib-sg40.json \\
        --placement runs/v12-steady-head-s1/meta.json --out gaps.json --md gaps.md

An input is LABEL=PATH, optionally LABEL=PATH@T0:T1 (epoch seconds; only calls whose CLIENT
span starts in the window).  PATH is a shadow (OTLP JSON lines, .zst/.gz) or a Jaeger export
(.json, query-API shape, e.g. the stock collector's 100 % data during a calibration).

For every SERVER span whose parent is a CLIENT span (caller -> callee):
  gap   = CLIENT duration - callee SERVER duration: request and response transit, queueing in
          the callee before its server span starts and after it ends.  A difference of two
          durations, each measured on one host, so clock skew does not enter it.
  start offset = server start - client start: skew-affected on cross-host edges (raw clocks in
          the shadow; Jaeger's skew adjuster shifts start times in an export).
Per service (SERVER spans): duration and the Stage 2d own time (duration minus the union of the
CLIENT children).  The gap is inside the caller's CLIENT interval, so the own-time trigger
charges it to nobody: neither the caller's own time nor the callee's server span holds it.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sloeval.spans import CLIENT, SERVER, own_client, read_export_table, read_shadow  # noqa: E402


def load(path: str):
    """A shadow (.jsonl, optionally .zst/.gz) or an uncompressed Jaeger export (.json)."""
    base = path[:-4] if path.endswith(".zst") else path[:-3] if path.endswith(".gz") else path
    if base.endswith(".json"):
        if base != path:
            raise SystemExit(f"decompress {path} first")
        return read_export_table(path)
    return read_shadow(path)


def q(x: np.ndarray, ps=(50, 95, 99)) -> list[float]:
    if not len(x):
        return [float("nan")] * len(ps)
    return [float(v) for v in np.percentile(x, ps)]


def analyse(t, window: tuple[float, float] | None, placement: dict) -> dict:
    dur = t.end - t.start
    own = own_client(t)
    srv = np.flatnonzero(t.kind == SERVER)
    par = t.pidx[srv]
    ok = par >= 0
    srv, par = srv[ok], par[ok]
    is_call = t.kind[par] == CLIENT
    srv, cli = srv[is_call], par[is_call]
    if window:
        lo, hi = int(window[0] * 1e9), int(window[1] * 1e9)
        w = (t.start[cli] >= lo) & (t.start[cli] < hi)
        srv, cli = srv[w], cli[w]
    gap = (dur[cli] - dur[srv]) / 1e3                       # us
    off = (t.start[srv] - t.start[cli]) / 1e3               # us, skew-affected across hosts
    edges = {}
    caller, callee = t.svc[cli], t.svc[srv]
    for a, b in sorted(set(zip(caller.tolist(), callee.tolist()))):
        m = (caller == a) & (callee == b)
        sa, sb = t.services[a], t.services[b]
        g = gap[m]
        p50, p95, p99 = q(g)
        cd50, cd95 = q(dur[cli[m]] / 1e3, (50, 95))
        sd50, sd95 = q(dur[srv[m]] / 1e3, (50, 95))
        edges[f"{sa}->{sb}"] = {
            "n": int(m.sum()), "gap_p50_us": p50, "gap_p95_us": p95, "gap_p99_us": p99,
            "gap_max_us": float(g.max()) if len(g) else float("nan"),
            "gap_neg_frac": float((g < 0).mean()) if len(g) else float("nan"),
            "client_p50_us": cd50, "client_p95_us": cd95, "server_p50_us": sd50, "server_p95_us": sd95,
            "gap_share_of_client_p50": float(np.median(g / np.maximum(dur[cli[m]] / 1e3, 1e-9))) if len(g) else float("nan"),
            "start_offset_p50_us": float(np.median(off[m])) if len(g) else float("nan"),
            "hosts": f"{placement.get(sa, '?')}->{placement.get(sb, '?')}",
        }
    services = {}
    s_all = np.flatnonzero(t.kind == SERVER)
    if window:
        s_all = s_all[(t.start[s_all] >= lo) & (t.start[s_all] < hi)]
    for c in sorted(set(t.svc[s_all].tolist())):
        m = s_all[t.svc[s_all] == c]
        d50, d95 = q(dur[m] / 1e3, (50, 95))
        o50, o95 = q(own[m] / 1e3, (50, 95))
        services[t.services[c]] = {"n": int(len(m)), "dur_p50_us": d50, "dur_p95_us": d95,
                                   "own_p50_us": o50, "own_p95_us": o95}
    return {"spans": int(t.n), "traces": int(t.n_traces), "calls": int(len(srv)),
            "missing_parent": int(t.missing_parent), "edges": edges, "services": services}


def md(results: dict) -> str:
    out = []
    labels = list(results)
    for lab in labels:
        r = results[lab]
        out.append(f"## {lab}: {r['traces']} traces, {r['calls']} calls\n")
        out.append("| edge | hosts | n | gap p50 us | gap p95 us | gap p99 us | gap < 0 | server p50 us | server p95 us | gap / client p50 | start offset p50 us |")
        out.append("|---|---|---|---|---|---|---|---|---|---|---|")
        for e, v in sorted(results[lab]["edges"].items(), key=lambda kv: -kv[1]["n"]):
            out.append(f"| {e} | {v['hosts']} | {v['n']} | {v['gap_p50_us']:.0f} | {v['gap_p95_us']:.0f} | {v['gap_p99_us']:.0f} | "
                       f"{v['gap_neg_frac']:.3f} | {v['server_p50_us']:.0f} | {v['server_p95_us']:.0f} | "
                       f"{v['gap_share_of_client_p50']:.2f} | {v['start_offset_p50_us']:.0f} |")
        out.append("\n| service | n | duration p50 us | duration p95 us | own p50 us | own p95 us |")
        out.append("|---|---|---|---|---|---|")
        for s, v in sorted(results[lab]["services"].items()):
            out.append(f"| {s} | {v['n']} | {v['dur_p50_us']:.0f} | {v['dur_p95_us']:.0f} | {v['own_p50_us']:.0f} | {v['own_p95_us']:.0f} |")
        out.append("")
    if len(labels) > 1:
        base = labels[0]
        out.append(f"## change against {base} (p95, us)\n")
        out.append("| edge | " + " | ".join(f"gap {lab}" for lab in labels) + " | " + " | ".join(f"server {lab}" for lab in labels) + " |")
        out.append("|---|" + "---|" * (2 * len(labels)))
        for e in sorted(results[base]["edges"]):
            g = [results[lab]["edges"].get(e, {}).get("gap_p95_us", float("nan")) for lab in labels]
            sv = [results[lab]["edges"].get(e, {}).get("server_p95_us", float("nan")) for lab in labels]
            out.append(f"| {e} | " + " | ".join(f"{x:.0f}" for x in g) + " | " + " | ".join(f"{x:.0f}" for x in sv) + " |")
        out.append("\n| service | " + " | ".join(f"own p95 {lab}" for lab in labels) + " | " + " | ".join(f"dur p95 {lab}" for lab in labels) + " |")
        out.append("|---|" + "---|" * (2 * len(labels)))
        for s in sorted(results[base]["services"]):
            o = [results[lab]["services"].get(s, {}).get("own_p95_us", float("nan")) for lab in labels]
            d = [results[lab]["services"].get(s, {}).get("dur_p95_us", float("nan")) for lab in labels]
            out.append(f"| {s} | " + " | ".join(f"{x:.0f}" for x in o) + " | " + " | ".join(f"{x:.0f}" for x in d) + " |")
    return "\n".join(out) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--input", action="append", required=True, help="LABEL=PATH[@T0:T1]")
    ap.add_argument("--placement", help="a run's meta.json (service -> node)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--md")
    a = ap.parse_args(argv)
    placement = {}
    if a.placement:
        placement = json.load(open(a.placement)).get("placement") or {}
    results = {}
    for spec in a.input:
        lab, _, rest = spec.partition("=")
        path, _, win = rest.partition("@")
        window = tuple(float(x) for x in win.split(":")) if win else None
        t = load(path)
        results[lab] = analyse(t, window, placement)
        results[lab]["source"] = path
        results[lab]["window"] = window
        print(f"{lab}: {results[lab]['traces']} traces, {results[lab]['calls']} calls, "
              f"{results[lab]['missing_parent']} spans with a missing parent", file=sys.stderr)
    with open(a.out, "w") as f:
        json.dump(results, f, indent=1)
    if a.md:
        with open(a.md, "w") as f:
            f.write(md(results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
