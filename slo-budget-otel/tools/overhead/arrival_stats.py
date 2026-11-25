"""What a run's logs say about the arrival stamp, per node (a node has one collector that stamps, one arrivald, one program at its service
pods' interfaces and one set of counters): the lookups of the server spans with a parent by outcome, from the stamping collector's period lines
(the tee of a campaign run, the agent of an overhead run), and the program's counters, runs and time, from arrivald's lines.

Used by the campaign's sanity check, the evaluation and both tables (standard library only).
"""
from __future__ import annotations

import datetime
import glob
import json
import os
import re

MISSES = ("no_table", "empty", "not_stored", "overwritten", "torn", "late")
COUNTERS = ("packets", "ids", "no_id", "overwritten", "capped", "cut", "bad_id", "repeats", "crowded", "errors", "paged", "paged_ids")
HIT_FLAG = 0.97         # a run under this is flagged


def _time(text: str) -> float:
    return datetime.datetime.fromisoformat(re.sub(r"(\.\d{6})\d*", r"\1", text).replace("Z", "+00:00")).timestamp()


def period_lines(path: str):
    """(time, fields) of a collector's "slobudget arrival" lines"""
    for ln in open(path, errors="replace"):
        if "slobudget arrival" not in ln or "{" not in ln:
            continue
        try:
            yield _time(ln.split(None, 1)[0]), json.loads(ln[ln.index("{"):])
        except ValueError:
            continue


def counter_lines(path: str):
    """(time, fields) of arrivald's "counters" lines"""
    for ln in open(path, errors="replace"):
        try:
            d = json.loads(ln)
            if isinstance(d, dict) and d.get("msg") == "counters":
                yield _time(d["time"]), d
        except (ValueError, KeyError, TypeError):
            continue


def lookups(path: str, t0: float, t1: float) -> dict:
    n = dict.fromkeys(("hit", "root", *MISSES), 0)
    errors, step, wait = 0, 0.0, 0.0
    for t, d in period_lines(path):
        if t0 <= t <= t1 + 5:
            for k in n:
                n[k] += d.get(k, 0)
            errors += d.get("bpf_errors", 0)
            step = max(step, abs(d.get("clock_step", 0.0)))
            wait += d.get("wait_us_mean", 0.0) * d.get("hit", 0)
    looked = n["hit"] + sum(n[k] for k in MISSES)
    return {**n, "looked_up": looked, "hit_rate": n["hit"] / looked if looked else None, "bpf_errors": errors, "clock_step_s": step,
            "wait_us_mean": wait / n["hit"] if n["hit"] else None}


def program(path: str, t0: float, t1: float) -> dict:
    """The counters over the load (the lines of it and the one after it); the time per run over the lines wholly inside the load."""
    n = dict.fromkeys(COUNTERS, 0)
    interfaces, ns, runs = 0, 0, 0
    for t, d in counter_lines(path):
        if t0 < t <= t1 + 5:
            for k in COUNTERS:
                n[k] += d.get(k, 0)
            interfaces = max(interfaces, d.get("interfaces", 0))
        if t0 + 5 <= t <= t1 and "run_ns" in d:
            ns += d["run_ns"]
            runs += d["runs"]
    return {**n, "interfaces": interfaces, "runs": runs, "ns_per_run": ns / runs if runs else None, "milli_cores": ns / 1e6 / (t1 - t0 - 5) if runs else None}


def stats(rd: str, t0: float, t1: float) -> dict | None:
    """Per node and for the run; None when no collector of the run stamped."""
    for prefix in ("tee-", "arrival-agent-", "agent-"):
        logs = {os.path.basename(p)[len(prefix):-4]: p for p in sorted(glob.glob(os.path.join(rd, prefix + "*.log")))}
        if any("slobudget arrival" in open(p, errors="replace").read() for p in logs.values()):     # a node without a line counts too
            break
    else:
        return None
    nodes = {}
    for node, p in logs.items():
        q = os.path.join(rd, f"arrival-{node}.log")
        nodes[node] = {"lookups": lookups(p, t0, t1), "program": program(q, t0, t1) if os.path.exists(q) else None}
    look = [n["lookups"] for n in nodes.values()]
    hit, looked = sum(x["hit"] for x in look), sum(x["looked_up"] for x in look)
    rates = [x["hit_rate"] for x in look]
    return {"nodes": nodes, "hit": hit, "looked_up": looked, "hit_rate": hit / looked if looked else None,
            "hit_rate_lowest_node": min(rates) if all(r is not None for r in rates) else None,
            "misses": {k: sum(x[k] for x in look) for k in MISSES}, "flagged": not looked or hit < HIT_FLAG * looked}


def pct(x, nd=2) -> str:
    return "-" if x is None else f"{100 * x:.{nd}f} %"


def misses(m: dict) -> str:
    return ", ".join(f"{k} {v:,}" for k, v in m.items() if v) or "none"


def node_text(n: dict) -> str:
    """one node in a table's cell or a note"""
    p = n["program"]
    if p is None:
        return f"{pct(n['lookups']['hit_rate'])}; no log of arrivald"
    other = ", ".join(f"{k} {p[k]:,}" for k in COUNTERS[2:] if p[k])
    return (f"{pct(n['lookups']['hit_rate'])} of {n['lookups']['looked_up']:,} (misses: {misses({k: n['lookups'][k] for k in MISSES})}); {p['interfaces']} pods, packets {p['packets']:,}, ids {p['ids']:,}"
            + (f", {other}" if other else "") + ("" if p["ns_per_run"] is None else f"; {p['ns_per_run']:,.0f} ns a run, {p['milli_cores']:.2f} milli-cores"))


def lines(s: dict) -> list[str]:
    """a run as text, the run first, then its nodes"""
    return ([f"arrival stamp: {pct(s['hit_rate'])} of the {s['looked_up']:,} server spans with a parent have the program's time" + (f" (FLAGGED: under {pct(HIT_FLAG, 0)})" if s["flagged"] else "")
             + f"; misses: {misses(s['misses'])}"] + [f"arrival stamp, {node}: {node_text(n)}" for node, n in sorted(s["nodes"].items())])


def rows(runs: list[tuple[list[str], dict]], head: list[str]) -> list[str]:
    """a markdown table, one row a run: (the run's own cells, its stats)"""
    nodes = sorted({n for _, s in runs for n in s["nodes"]})
    out = ["| " + " | ".join(head + ["hit rate", "misses by cause", "lowest node"] + [f"{n}: lookups; the program's counters and time" for n in nodes]) + " |", "|" + "---|" * (len(head) + 3 + len(nodes))]
    for cells, s in runs:
        out.append("| " + " | ".join(cells + [("**FLAG** " if s["flagged"] else "") + pct(s["hit_rate"]), misses(s["misses"]), pct(s["hit_rate_lowest_node"])]
                                     + [node_text(s["nodes"][n]) if n in s["nodes"] else "-" for n in nodes]) + " |")
    return out
