#!/usr/bin/env python3
"""Print the Stage 3 campaign queue (decisions of 2026-11-21, after the 2d trigger comparison):

1. the chosen arm's completed Stage 3 runs again on the new image (freeze_release 3 and the
   N_need = +Inf fix): steady and noisy_neighbour at 12 % (noisy_neighbour is the unconfined
   spinner on socialgraph, the saturated scenario as it ran until then);
2. 12 %: bursty (no fault), every arm;
3. keep-parent's completed runs again on the new image (it shares the chosen sampler);
4. 12 %: the scenarios with the confined burner (saturated, cascade, cold_start), every arm,
   then late_everywhere (after the burner block: its spinner moves under a CPU limit, which
   is calibrated meanwhile);
5. 25 % and 5 %: every scenario (noisy_neighbour is 12 % only), every arm;
6. the threshold ablation at 12 % only, every scenario but noisy_neighbour, 3 seeds.

Seed by seed inside a block, so a one-seed table exists early.  The baseline arms stay on the
campaign image (--old-tag), the chosen family (chosen, threshold, keep-parent) moves to --new-tag.
queue.sh skips a job whose run directory is `collected`, so jobs already run may stay listed;
superseded runs are moved out of runs/ before they are rerun.

  python3 tools/campaign/make_queue.py --old-tag 20261121-011955 --new-tag <tag>
"""
import argparse

ARMS = ["chosen", "head", "tail", "reservoir", "canopy", "keep-parent"]
CHOSEN_FAMILY = {"chosen", "threshold", "keep-parent"}
SEEDS = [1, 2, 3]
NO_BURNER = ["bursty"]
BURNER = ["saturated", "cascade", "cold_start"]
ALL = ["steady", "saturated", "cascade", "bursty", "cold_start", "late_everywhere"]


def job(arm, sc, vol, seed, old, new):
    return f"{arm} {sc} {vol} {seed} IMAGE_TAG={new if arm in CHOSEN_FAMILY else old}"


def block(vol, scenarios, arms, old, new):
    return [job(arm, sc, vol, seed, old, new) for seed in SEEDS for sc in scenarios for arm in arms]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old-tag", required=True, help="campaign image for the baseline arms")
    ap.add_argument("--new-tag", required=True, help="image for the chosen family")
    a = ap.parse_args()
    o, n = a.old_tag, a.new_tag
    lines = ["# Stage 3 campaign (decisions 2026-11-21): arm scenario volume seed [ENV=...]"]
    lines += ["# 1. the chosen arm's completed runs on the new image"]
    lines += [job("chosen", sc, 0.12, s, o, n) for sc in ("steady", "noisy_neighbour") for s in SEEDS]
    lines += ["# 2. 12 %: bursty"] + block(0.12, NO_BURNER, ARMS, o, n)
    lines += ["# 3. keep-parent's completed runs on the new image"]
    lines += [job("keep-parent", sc, 0.12, s, o, n) for sc in ("steady", "noisy_neighbour") for s in SEEDS]
    lines += ["# 4. 12 %: the confined burner (saturated, cascade, cold_start)"] + block(0.12, BURNER, ARMS, o, n)
    lines += ["# 4b. 12 %: late_everywhere"] + block(0.12, ["late_everywhere"], ARMS, o, n)
    lines += ["# 5a. 25 %"] + block(0.25, ALL, ARMS, o, n)
    lines += ["# 5b. 5 %"] + block(0.05, ALL, ARMS, o, n)
    lines += ["# 6. threshold ablation, 12 % only"] + block(0.12, ALL, ["threshold"], o, n)
    print("\n".join(lines))


if __name__ == "__main__":
    main()
