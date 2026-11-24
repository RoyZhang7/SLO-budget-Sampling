"""Command line: generate corpora, replay one arm, run experiments."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from .experiments import ALL_ARMS, EXPERIMENTS, make_arm, resummarize, run_experiment, write_csv
from .jaeger import load_jaeger, to_jaeger
from .metrics import CorpusArrays, compute
from .replay import replay
from .dump import dump_replay
from .synthetic import ALL_SCENARIOS, SCENARIOS, build_scenario
from .ttable import oracle_table


def _parse_overrides(items: list[str]) -> dict:
    out = {}
    for it in items or []:
        k, v = it.split("=", 1)
        if v.lower() in ("true", "false"):
            out[k] = v.lower() == "true"
        else:
            try:
                out[k] = int(v)
            except ValueError:
                try:
                    out[k] = float(v)
                except ValueError:
                    out[k] = v
    return out


def _needs_arrivals(overrides: dict, corpus=None) -> None:
    """within_service reads Span.wait, which only `dump --waits` sets: generated and loaded spans have none."""
    if overrides.get("own_measure", "handler") != "handler" and not any(s.wait for s in (corpus.spans if corpus else ())):
        raise SystemExit("own_measure=within_service: no span has an arrival, it would be the handler's own time")


def cmd_generate(a) -> int:
    corpus, reference = build_scenario(a.scenario, a.seed)
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(to_jaeger(corpus), f)
    if a.reference:
        with open(a.reference, "w", encoding="utf-8") as f:
            json.dump(to_jaeger(reference), f)
    print(f"{a.scenario}: {len(corpus.spans)} spans, {len(corpus.roots)} requests, "
          f"{corpus.n_periods} periods -> {out}")
    return 0


def cmd_replay(a) -> int:
    if a.jaeger:
        fold = not a.keep_client_spans
        corpus = load_jaeger(a.jaeger, period=a.period, fold_client_spans=fold)
        reference = (load_jaeger(a.reference, period=a.period, routes_slo=corpus.routes_slo, fold_client_spans=fold)
                     if a.reference else corpus)
    else:
        corpus, reference = build_scenario(a.scenario, a.seed)
    _needs_arrivals(_parse_overrides(a.set), corpus)
    oracle = oracle_table(reference)
    arrays = CorpusArrays(corpus, oracle)
    v_star = a.volume_frac * len(corpus.spans) * corpus.period / max(corpus.period, corpus.t_end - corpus.t0)
    cap = a.cap_factor * v_star
    rows = []
    for arm in a.arm:
        sampler = make_arm(arm, _parse_overrides(a.set))
        res = replay(corpus, sampler, v_star, cap, seed=a.seed, oracle=oracle)
        m = compute(corpus, arrays, res)
        rows.append(m)
        print(f"{arm:14s} kept/period {m['kept_per_period']:.0f} (V*={v_star:.0f}) roots {m['root_share']:.2f} "
              f"outlier share {m['outlier_share']:.2f} precision {m['outlier_precision']:.2f} "
              f"max/req {m['max_per_request']:.1f} ctx {m['ctx_sufficient']:.2f} "
              f"culprit {m['culprit_est']} evidence {m['evidence_s']} "
              f"P95err/svc {m['p95_err_svc_mean']:.3f} [{m['elapsed_s']}s]")
    if a.out:
        write_csv(rows, Path(a.out))
    return 0


def cmd_dump(a) -> int:
    stages = None
    if a.stages:                 # a call graph file with sequential stages (its "stages" key), or that object itself
        with open(a.stages, "r", encoding="utf-8-sig") as f:
            obj = json.load(f)
        stages = obj.get("stages", obj)
    for sc in a.scenario:
        for sd in a.seeds:
            ev, de = dump_replay(sc, sd, a.volume_frac, Path(a.out), a.arm, _parse_overrides(a.set), stages=stages, waits=a.waits)
            print(f"{sc} seed {sd}: {ev.name} {ev.stat().st_size // 1024} KB, {de.name} {de.stat().st_size // 1024} KB")
    return 0


def cmd_exp(a) -> int:
    results = Path(a.results)
    results.mkdir(parents=True, exist_ok=True)
    exps = list(EXPERIMENTS) if a.exp == "all" else [a.exp]
    extra = _parse_overrides(a.set)
    _needs_arrivals(extra)
    for exp in exps:
        t = time.perf_counter()
        rows = run_experiment(exp, results, a.seeds, a.jobs, extra or None, a.volumes, fresh=a.fresh)
        print(f"{exp}: {len(rows)} runs in {time.perf_counter() - t:.0f}s -> {results / exp}.csv / .md")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="slo-budget-sim", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="write a synthetic scenario as Jaeger JSON")
    g.add_argument("--scenario", choices=ALL_SCENARIOS, required=True)
    g.add_argument("--seed", type=int, default=1)
    g.add_argument("--out", required=True)
    g.add_argument("--reference", help="also write the healthy reference corpus")
    g.set_defaults(func=cmd_generate)

    r = sub.add_parser("replay", help="replay one or more arms on a scenario or a Jaeger export")
    src = r.add_mutually_exclusive_group(required=True)
    src.add_argument("--scenario", choices=ALL_SCENARIOS)
    src.add_argument("--jaeger", help="Jaeger JSON export")
    r.add_argument("--reference", help="Jaeger JSON of a healthy run (oracle table); default: the corpus itself")
    r.add_argument("--keep-client-spans", action="store_true",
                   help="sample CLIENT spans as spans of the caller instead of folding them into the call")
    r.add_argument("--arm", nargs="+", default=["chosen"], choices=ALL_ARMS)
    r.add_argument("--set", nargs="*", metavar="KEY=VALUE", help="BudgetConfig overrides, e.g. n_need=10")
    r.add_argument("--seed", type=int, default=1)
    r.add_argument("--period", type=float, default=5.0)
    r.add_argument("--volume-frac", type=float, default=0.25)
    r.add_argument("--cap-factor", type=float, default=1.25)
    r.add_argument("--out", help="CSV path")
    r.set_defaults(func=cmd_replay)

    rs = sub.add_parser("resummarize", help="rewrite results/<exp>.md from the existing CSV")
    rs.add_argument("exp", choices=list(EXPERIMENTS))
    rs.add_argument("--results", default="results")
    rs.set_defaults(func=lambda a: (resummarize(a.exp, Path(a.results)), 0)[1])

    d = sub.add_parser("dump", help="write replay dumps (events + Python decisions) for the Go port's differential test")
    d.add_argument("--scenario", choices=ALL_SCENARIOS, nargs="+", default=list(SCENARIOS))
    d.add_argument("--seeds", type=int, nargs="+", default=[1, 2, 3])
    d.add_argument("--volume-frac", type=float, default=0.12)
    d.add_argument("--arm", default="chosen-v2", choices=ALL_ARMS)
    d.add_argument("--set", nargs="*", metavar="KEY=VALUE", help="BudgetConfig overrides")
    d.add_argument("--stages", metavar="FILE", help="call graph file whose \"stages\" the corpora's graphs get (sequential stages per service)")
    d.add_argument("--waits", action="store_true", help="give the spans a wait from the request's arrival to their start (own_measure=within_service reads it)")
    d.add_argument("--out", default="dumps")
    d.set_defaults(func=cmd_dump)

    for name in list(EXPERIMENTS) + ["all"]:
        e = sub.add_parser(name, help=f"run {name}" if name != "all" else "run every experiment",
                           description="Resumable: rows already in results/<exp>.csv for the same (scenario, seed, arm, "
                                       "label, volume, overrides, workload / scenario tag) are kept, the rest is run and "
                                       "the CSV is rewritten after every chunk of runs.")
        e.add_argument("--results", default="results")
        e.add_argument("--seeds", type=int, nargs="+", default=[1, 2, 3])
        e.add_argument("--jobs", type=int, default=1, help="parallel worker processes")
        e.add_argument("--volumes", type=float, nargs="+", metavar="FRAC",
                       help="run every job of the experiment at these volume targets (fractions of spans), e.g. 0.25 0.12 0.05")
        e.add_argument("--fresh", action="store_true", help="rerun every job even if results/<exp>.csv already has its row")
        e.add_argument("--set", nargs="*", metavar="KEY=VALUE",
                       help="BudgetConfig overrides applied to every budget arm of the experiment, e.g. root_policy=record")
        e.set_defaults(func=cmd_exp, exp=name)

    a = p.parse_args(argv)
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
