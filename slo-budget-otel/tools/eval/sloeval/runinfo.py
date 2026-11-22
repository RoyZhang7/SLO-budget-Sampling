"""Everything in a run directory besides the spans: meta.json, the cap gate's
periods, the central's status, the collectors' telemetry scrapes and logs."""
from __future__ import annotations

import glob
import json
import os
import re

PROM_LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+0-9.eEinfINFNa]+)(\s+\d+)?$')
PROM_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def load_json(path: str, default=None):
    if not os.path.exists(path):
        return default
    with open(path, encoding="utf-8-sig") as f:
        return json.load(f)


def load_meta(run_dir: str) -> dict:
    meta = load_json(os.path.join(run_dir, "meta.json"))
    if meta is None:
        raise FileNotFoundError(f"{run_dir}/meta.json")
    meta.setdefault("period_s", 5.0)
    meta.setdefault("margin", 0.1)
    meta.setdefault("slo_ms", {"compose_post": 23.4})
    meta.setdefault("arm_params", {})
    meta.setdefault("placement", {})
    return meta


def zap_records(path: str, message: str) -> list[dict]:
    """The JSON fields of every zap console line with the given message
    (`ts<TAB>level<TAB>caller<TAB>message<TAB>{json}`)."""
    out = []
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if message not in line:
                continue
            parts = line.rstrip("\n").split("\t", 4)
            if len(parts) == 5 and parts[3] == message:
                try:
                    rec = json.loads(parts[4])
                except json.JSONDecodeError:
                    continue
                rec.pop("resource", None)
                out.append(rec)
    return out


def load_gate(run_dir: str) -> list[dict]:
    """The cap gate's per-period records: gate.jsonl, else the `capgate period` lines of central.log."""
    path = os.path.join(run_dir, "gate.jsonl")
    recs: list[dict] = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    recs.append(json.loads(line))
    else:
        recs = zap_records(os.path.join(run_dir, "central.log"), "capgate period")
    by: dict[int, dict] = {}
    for r in recs:            # one record per period (the last wins if a period is logged twice)
        if "period" in r:
            by[int(r["period"])] = r
    return [by[k] for k in sorted(by)]


def load_status(run_dir: str) -> dict | None:
    return load_json(os.path.join(run_dir, "central-status.json"))


def parse_prom(path: str) -> list[tuple[str, dict, float]]:
    out = []
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = PROM_LINE.match(line)
            if not m:
                continue
            labels = dict(PROM_LABEL.findall(m.group(2) or ""))
            try:
                out.append((m.group(1), labels, float(m.group(3))))
            except ValueError:
                continue
    return out


def prom_sum(samples, name: str, pred=None) -> float | None:
    """Sum of the series named `name` (with or without the `_total` suffix) whose labels pass pred."""
    names = {name, name + "_total"} if not name.endswith("_total") else {name, name[: -len("_total")]}
    vals = [v for n, l, v in samples if n in names and (pred is None or pred(l))]
    return sum(vals) if vals else None


def agent_cpu(run_dir: str) -> dict:
    """Per node: delta process CPU seconds and delta accepted spans of the agent collector
    between the start and end scrapes."""
    out = {}
    for start in sorted(glob.glob(os.path.join(run_dir, "telemetry", "agent-*-start.prom"))):
        node = os.path.basename(start)[len("agent-"): -len("-start.prom")]
        end = start[: -len("-start.prom")] + "-end.prom"
        a, b = parse_prom(start), parse_prom(end)
        if not a or not b:
            continue
        cpu0, cpu1 = prom_sum(a, "otelcol_process_cpu_seconds"), prom_sum(b, "otelcol_process_cpu_seconds")
        otlp = lambda l: l.get("receiver", "").startswith("otlp")
        sp0 = prom_sum(a, "otelcol_receiver_accepted_spans", otlp)
        sp1 = prom_sum(b, "otelcol_receiver_accepted_spans", otlp)
        if sp0 is None and sp1 is not None:   # the start scrape precedes the first span: no series yet
            sp0 = 0.0
        if None in (cpu0, cpu1, sp0, sp1):
            continue
        out[node] = {"cpu_s": cpu1 - cpu0, "spans": sp1 - sp0}
    return out


def central_scrapes(run_dir: str) -> list[tuple[int, list]]:
    """Periodic central scrapes `telemetry/central-<unix>.prom`, sorted by time."""
    out = []
    for p in glob.glob(os.path.join(run_dir, "telemetry", "central-*.prom")):
        tag = os.path.basename(p)[len("central-"): -len(".prom")]
        if tag.isdigit():
            out.append((int(tag), parse_prom(p)))
    return sorted(out, key=lambda x: x[0])


def agent_period_records(run_dir: str, message: str) -> dict[str, list[dict]]:
    out = {}
    for p in sorted(glob.glob(os.path.join(run_dir, "agent-*.log"))):
        node = os.path.basename(p)[len("agent-"): -len(".log")]
        out[node] = zap_records(p, message)
    return out
