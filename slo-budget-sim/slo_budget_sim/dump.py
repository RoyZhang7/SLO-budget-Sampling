"""Replay dumps for the Go port's differential test.

    python -m slo_budget_sim.cli dump --scenario saturated --seed 1 --volume-frac 0.12 --out dumps/

writes `<scenario>-s<seed>-v<frac>.events.json` (the corpus with everything
`replay()` needs: spans in columns, call graph, SLOs, hosts, V*, cap, the
resolved arm configuration, the RNG seed) and `.decisions.json` (what this
Python replay kept and dropped: keep, p, reason per span, B per period).  The
Go harness (slo-budget-otel/slobudget) replays the events file and must
reproduce every decision bit for bit.
"""
from __future__ import annotations

import json
import math
import random
from dataclasses import asdict
from pathlib import Path

from .experiments import make_arm
from .model import Corpus
from .replay import RunResult, replay
from .synthetic import build_scenario
from .ttable import Calibrator, oracle_table

INF_AS = 1e300          # JSON has no inf; the Go side maps values >= 1e300 back to +inf


def _graph(corpus: Corpus) -> dict:
    g = corpus.graph
    children: dict[str, dict[str, list[str]]] = {}
    parallel: dict[str, dict[str, bool]] = {}
    for (route, svc), kids in g.children.items():
        children.setdefault(route, {})[svc] = list(kids)
    for (route, svc), par in g.parallel.items():
        parallel.setdefault(route, {})[svc] = bool(par)
    out = {"root": dict(g.root), "children": children, "parallel": parallel}
    if g.stages:
        stages: dict[str, dict[str, list[list[str]]]] = {}
        for (route, svc), st in g.stages.items():
            stages.setdefault(route, {})[svc] = [list(group) for group in st]
        out["stages"] = stages
    return out


def _oracle_table(oracle: Calibrator) -> dict:
    out: dict[str, dict[str, dict[str, float]]] = {}
    for (route, cls, svc), T in oracle.table.items():
        out.setdefault(route, {}).setdefault(cls, {})[svc] = T
    return out


def events_dump(corpus: Corpus, cfg, v_star: float, cap: float, rng_seed: int, arm: str, scenario: str, seed: int,
                oracle: Calibrator | None = None) -> dict:
    spans = corpus.spans
    config = {k: (INF_AS if isinstance(v, float) and math.isinf(v) else v) for k, v in asdict(cfg).items()}
    out = {
        "name": corpus.name, "scenario": scenario, "seed": seed, "period": corpus.period, "t0": corpus.t0,
        "v_star": v_star, "cap": cap, "rng_seed": rng_seed, "routes_slo": dict(corpus.routes_slo),
        "graph": _graph(corpus), "hosts": list(corpus.hosts), "host_skew": dict(corpus.host_skew),
        "services": corpus.services(), "arm": arm, "config": config,
        "spans": {
            "span_id": [s.span_id for s in spans], "trace_id": [s.trace_id for s in spans],
            "parent_idx": [s.parent_idx for s in spans], "service": [s.service for s in spans],
            "host": [s.host for s in spans], "start": [s.start for s in spans], "duration": [s.duration for s in spans],
            "status_ok": [bool(s.status_ok) for s in spans], "depth": [s.depth for s in spans],
            "route": [s.route for s in spans], "req_class": [s.req_class for s in spans],
            "nbytes": [s.nbytes for s in spans], "transit": [s.transit for s in spans], "own": [s.own for s in spans],
        },
    }
    if any(s.wait for s in spans):
        out["spans"]["wait"] = [s.wait for s in spans]
    if oracle is not None and cfg.calib_source == "oracle":
        out["oracle"] = _oracle_table(oracle)
    return out


def decisions_dump(res: RunResult, n_need: float) -> dict:
    n = res.n_spans
    keep = [False] * n
    p = [0.0] * n
    reason = list(res.drop_reason) if len(res.drop_reason) == n else [""] * n
    for idx, pv, why in zip(res.kept_idx.tolist(), res.kept_p.tolist(), res.kept_reason):
        keep[idx], p[idx], reason[idx] = True, pv, why
    periods = []
    for d, st in zip(res.diagnostics, res.periods):
        periods.append({"period": d["period"], "B": d["B"], "collector_volume": d["collector_volume"], "kept": d["kept"],
                        "cost_post": st.cost_post})
    return {"keep": keep, "p": p, "reason": reason, "n_need": INF_AS if math.isinf(n_need) else n_need, "periods": periods}


def set_stages(corpus: Corpus, stages: dict[str, dict[str, list[list[str]]]]) -> None:
    """Give a corpus's graph sequential stages (route -> service -> callees by stage); every callee must be one of the
    service's callees."""
    for route, by_svc in stages.items():
        for svc, st in by_svc.items():
            kids = set(corpus.graph.children.get((route, svc), []))
            named = [c for group in st for c in group]
            if not kids or len(named) != len(set(named)) or not set(named) <= kids:
                raise ValueError(f"stages of ({route}, {svc}) do not match its callees {sorted(kids)}: {st}")
            corpus.graph.stages[(route, svc)] = [list(group) for group in st]


def add_waits(corpus: Corpus, seed: int) -> None:
    """Give every span but the roots a wait between its request's arrival at the pod and its start: lognormal, median
    0.3 ms, 20 times that for the spans that carry the corpus's fault."""
    rng = random.Random(seed)
    onsets = corpus.fault.onsets if corpus.fault else {}
    for s in corpus.spans:
        w = 0.0003 * math.exp(0.6 * rng.gauss(0.0, 1.0))
        lo, hi = onsets.get(s.service, (math.inf, math.inf))
        s.wait = 0.0 if s.parent_idx < 0 else w * (20.0 if lo <= s.start < hi else 1.0)


def dump_replay(scenario: str, seed: int, volume_frac: float, out_dir: Path, arm: str = "chosen-v2",
                overrides: dict | None = None, cap_factor: float = 1.25,
                stages: dict[str, dict[str, list[list[str]]]] | None = None, waits: bool = False) -> tuple[Path, Path]:
    """Build the scenario, replay the arm exactly as the experiments do and write both files.  `stages` gives the
    corpus's and its reference's call graph sequential stages, `waits` their spans arrivals (add_waits)."""
    corpus, reference = build_scenario(scenario, seed)
    if stages:
        for c in (corpus, reference):
            set_stages(c, stages)
    if waits:
        add_waits(corpus, 7000 + seed)
        add_waits(reference, 8000 + seed)
    sampler = make_arm(arm, overrides or {})
    oracle = oracle_table(reference, q=0.95, use_class=True, within=getattr(getattr(sampler, "cfg", None), "own_measure", "handler") == "within_service")
    v_star = volume_frac * len(corpus.spans) * corpus.period / max(corpus.period, corpus.t_end - corpus.t0)
    cap = cap_factor * v_star
    rng_seed = 1000 + seed
    res = replay(corpus, sampler, v_star, cap, seed=rng_seed, oracle=oracle)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{scenario}-s{seed}-v{volume_frac:g}" + ("-stages" if stages else "") + ("-waits" if waits else "")
    ev, de = out_dir / f"{stem}.events.json", out_dir / f"{stem}.decisions.json"
    with open(ev, "w", encoding="utf-8") as f:
        json.dump(events_dump(corpus, sampler.cfg, v_star, cap, rng_seed, arm, scenario, seed, oracle), f)
    with open(de, "w", encoding="utf-8") as f:
        json.dump(decisions_dump(res, sampler.n_need), f)
    return ev, de
