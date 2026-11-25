#!/usr/bin/env python3
"""Print the queue lines of the runs after the overhead experiment (decisions of 2026-11-24), in this order:

a. our arm with the eBPF arrival stamp (own_measure within_service), all scenarios, 25 / 12 / 5 %;
b. a on the handler's own time, 12 % only: the program attached and the lookup on as in a, only the outlier test (agents and central)
   does not read the arrival;
c. threshold-only, 12 % only: a without the budget, the pool and N_need (the arm `threshold`), everything else the same.

noisy_neighbour is at 12 % only (a, b and c); the other scenarios are the campaign's.

All on the new image with the staged call graph, each under a label (variant) and run id of its own, so that no earlier run is
overwritten or pooled with them.
Printed as comments (`#hold `) unless --go: the queue reads its file before every job, and a must not start before it is confirmed.

  python3 tools/campaign/make_queue2.py --tag <new image> [--go] [--only a b c]
"""
import argparse

SEEDS = (1, 2, 3)
ALL = ("saturated", "cascade", "steady", "bursty", "cold_start", "late_everywhere")       # a fault first: the first run shows the stamp in one
AT_12 = ("saturated", "cascade", "steady", "noisy_neighbour", "bursty", "cold_start", "late_everywhere")


def jobs(tag: str) -> dict[str, list[str]]:
    cg = "CALLGRAPH=deploy/callgraph.stages.json"
    out: dict[str, list[str]] = {"a": [], "b": [], "c": []}
    for vol, scs in ((0.25, ALL), (0.12, AT_12), (0.05, ALL)):
        for seed in SEEDS:
            for sc in scs:
                out["a"].append(f"chosen {sc} {vol} {seed} IMAGE_TAG={tag} {cg} ARRIVAL=1 OWN_MEASURE=within_service VARIANT=ebpf RUN_ID=v{round(vol * 100)}-{sc}-ebpf-s{seed}")
    for seed in SEEDS:
        for sc in AT_12:
            out["b"].append(f"chosen {sc} 0.12 {seed} IMAGE_TAG={tag} {cg} ARRIVAL=1 VARIANT=handler RUN_ID=v12-{sc}-handler-s{seed}")
    for seed in SEEDS:
        for sc in AT_12:
            out["c"].append(f"threshold {sc} 0.12 {seed} IMAGE_TAG={tag} {cg} ARRIVAL=1 OWN_MEASURE=within_service VARIANT=ebpf RUN_ID=v12-{sc}-threshold-ebpf-s{seed}")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tag", required=True)
    ap.add_argument("--go", action="store_true")
    ap.add_argument("--only", nargs="+", choices=["a", "b", "c"], default=["a", "b", "c"])
    a = ap.parse_args()
    for part, lines in jobs(a.tag).items():
        if part in a.only:
            print(f"# {part}. {len(lines)} runs")
            print("\n".join(("" if a.go else "#hold ") + ln for ln in lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
