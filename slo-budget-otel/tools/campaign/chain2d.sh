#!/usr/bin/env bash
# Stage 2d chain on node-0, started once under nohup next to the queue runner:
#   nohup bash tools/campaign/chain2d.sh > /dev/null 2>&1 &
#
#  1. pause the queue runner after its current run (a marker line in $STAGE3/STOP) and wait
#     until no run is in progress;
#  2. calib3 under the postprocess lock (the node quiet): spinner levels for late_everywhere;
#     the level closest to 25 % over-SLO requests goes into faults.env (FAULT_LEVEL_ALL,
#     FAULT_DUTY_ALL);
#  3. build the 2d image under the lock (module tests, microbenchmark -> bench-2d.txt; one
#     retry) and validate the 2d agent / central configs (own and duration trigger);
#  4. write the queue: the 2d trigger comparison first (chosen, saturated 12 %, seeds 1-3, own
#     vs duration trigger, server_hold 200 ms in both; chosen steady 12 %, seeds 1-3, own),
#     then the jobs already queued, then the whole Stage 3 campaign (make_queue.py: baselines
#     on the campaign image, chosen family on the 2d image), duplicates dropped;
#  5. resume the runner (the marker is removed only when it is the only STOP line).
# If the build or the validation fails twice, only the baseline arms are queued and the runner
# resumes, so the cluster does not idle.  Progress: $STAGE3/chain2d.log.
set -u
MOD=$HOME/slo-budget-otel
STAGE3=/storage/royno7/stage3
Q=$STAGE3/queue.txt
LOG=$STAGE3/chain2d.log
MARK="chain2d: paused for the 2d build"
log() { echo "$(date '+%F %T') $*" >> "$LOG"; }

log "chain2d start (pid $$): calib3, 2d build, 2d series first, then the whole campaign"
grep -qx "$MARK" "$STAGE3/STOP" 2>/dev/null || echo "$MARK" >> "$STAGE3/STOP"
while pgrep -f 'tools/campaign/run_one.sh' > /dev/null; do sleep 20; done
cd "$MOD" || exit 1
OLDTAG=$(cat build/IMAGE_TAG)

# 2. calib3 ---------------------------------------------------------------------------------
log "queue paused; calib3 (late_everywhere levels) under the postprocess lock"
BEFORE=$(ls -dt results/calib-* 2>/dev/null | head -1)
PHASES="base;spin all 5;spin all 6;spin all 7;spin all 8 0.75;spin all 8 0.9" \
    flock "$STAGE3/postprocess.lock" bash tools/hog/calibrate.sh > "$HOME/calib3.log" 2>&1 \
    || log "calib3 failed: $(tail -2 "$HOME/calib3.log" | tr '\n' ' ')"
CAL=$(ls -dt results/calib-* 2>/dev/null | head -1)
[ "$CAL" = "$BEFORE" ] && CAL=/nonexistent             # calib3 wrote nothing: keep the current level
PICK=$(python3 - "$CAL/calib.json" <<'PY'
import json, sys
try:
    rows = json.load(open(sys.argv[1]))
except (OSError, ValueError):
    rows = []
best = None
for r in rows:
    ph = r.get("phase", "").split()
    if len(ph) >= 3 and ph[0] == "spin" and ph[1] == "all" and r.get("over_slo") is not None:
        cand = (abs(float(r["over_slo"]) - 0.25), ph[2], ph[3] if len(ph) > 3 else "", float(r["over_slo"]))
        if best is None or cand[0] < best[0]:
            best = cand
print("" if best is None else f"{best[1]} {best[2] or '-'} {best[3]:.3f}")
PY
)
if [ -n "$PICK" ]; then
    read -r LV DU OS <<< "$PICK"
    [ "$DU" = "-" ] && DU=""
    python3 - "$MOD/tools/campaign/faults.env" "$LV" "$DU" "$OS" "$CAL" <<'PY'
import os, sys
path, lv, du, os_frac, cal = sys.argv[1:6]
lines = open(path).read().splitlines()
out = [ln for ln in lines if not ln.startswith("FAULT_LEVEL_ALL=") and not ln.startswith("FAULT_DUTY_ALL=")]
i = next((k for k, ln in enumerate(lines) if ln.startswith("FAULT_LEVEL_ALL=")), len(out))
note = f"# calib3 {os.path.basename(cal)}: closest to 25 % over-SLO = {float(os_frac) * 100:.1f} %"
out[i:i] = [f"FAULT_LEVEL_ALL={lv}                   {note}", f"FAULT_DUTY_ALL={du}"]
tmp = path + ".new"
open(tmp, "w").write("\n".join(out) + "\n")
os.replace(tmp, path)
PY
    log "late_everywhere level: $LV threads, duty ${DU:-1.0}, over-SLO $OS (from $CAL)"
else
    log "calib3 gave no usable all-services phase; late_everywhere keeps FAULT_LEVEL_ALL=$(grep '^FAULT_LEVEL_ALL=' tools/campaign/faults.env | cut -d= -f2 | cut -d' ' -f1)"
fi

# 3. build and validate --------------------------------------------------------------------------
ok=0
for try in 1 2; do
    if flock "$STAGE3/postprocess.lock" bash tools/build-node0.sh > "$STAGE3/build-2d.log" 2>&1; then ok=1; break; fi
    log "2d build attempt $try failed: $(tail -3 "$STAGE3/build-2d.log" | tr '\n' ' ' | cut -c1-300)"
done
NEWTAG=$(cat build/IMAGE_TAG)
if [ $ok = 1 ] && [ "$NEWTAG" != "$OLDTAG" ]; then
    cp build/bench.txt "$STAGE3/bench-2d.txt"
    log "image $NEWTAG pushed; microbenchmark in $STAGE3/bench-2d.txt"
    BIN=$MOD/build/otelcol-slobudget/otelcol-slobudget
    export NODE_NAME=node-1
    mkdir -p "$STAGE3/tmp"
    for trig in own duration; do
        for kind in agent central; do
            f=$STAGE3/tmp/2d-$trig-$kind.yaml
            OUTLIER_TRIGGER=$trig python3 tools/campaign/render.py --arm chosen --volume 0.12 --image x --print "$kind" > "$f" || ok=0
            sed -i -e "s#/etc/slobudget/callgraph.json#$MOD/deploy/callgraph.json#" "$f"
            "$BIN" validate --config "$f" > "$f.validate" 2>&1 || { ok=0; log "2d config invalid ($trig $kind): $(tail -2 "$f.validate" | tr '\n' ' ' | cut -c1-300)"; }
        done
    done
    for arm in threshold keep-parent; do
        f=$STAGE3/tmp/2d-$arm-agent.yaml
        python3 tools/campaign/render.py --arm "$arm" --volume 0.12 --image x --print agent > "$f" || ok=0
        "$BIN" validate --config "$f" > "$f.validate" 2>&1 || { ok=0; log "2d config invalid ($arm agent)"; }
    done
    [ $ok = 1 ] && log "2d configs valid (chosen own / duration, threshold, keep-parent)"
else
    ok=0
fi

# 4. the queue ------------------------------------------------------------------------------------
T=$(mktemp "$STAGE3/queue.XXXXXX")
{
    if [ $ok = 1 ]; then
        echo "# Stage 2d trigger comparison, run first (image $NEWTAG, server_hold 200 ms in both arms)"
        for s in 1 2 3; do
            echo "chosen saturated 0.12 $s IMAGE_TAG=$NEWTAG OUTLIER_TRIGGER=own RUN_ID=2d-v12-saturated-own-s$s"
            echo "chosen saturated 0.12 $s IMAGE_TAG=$NEWTAG OUTLIER_TRIGGER=duration RUN_ID=2d-v12-saturated-dur-s$s"
        done
        echo "# the own trigger in steady state (healthy outlier share, volume), 12 %"
        for s in 1 2 3; do
            echo "chosen steady 0.12 $s IMAGE_TAG=$NEWTAG OUTLIER_TRIGGER=own RUN_ID=2d-v12-steady-own-s$s"
        done
    fi
    cat "$Q"
    python3 tools/campaign/make_queue.py --old-tag "$OLDTAG" --new-tag "$NEWTAG"
} > "$T.raw"
python3 - "$T.raw" "$T" "$ok" <<'PY'
import sys
src, dst, ok = sys.argv[1], sys.argv[2], sys.argv[3] == "1"
seen, out = set(), []
for line in open(src).read().splitlines():
    body = line.split("#", 1)[0].strip()
    if not body:
        out.append(line)
        continue
    tok = body.split()
    rid = next((t[len("RUN_ID="):] for t in tok[4:] if t.startswith("RUN_ID=")), None)
    if rid is None:
        rid = "v%d-%s-%s-s%s" % (round(float(tok[2]) * 100), tok[1], tok[0], tok[3])
    if rid in seen:
        continue
    if not ok and tok[0] in ("chosen", "threshold", "keep-parent"):
        continue                                   # no 2d image: baseline arms only
    seen.add(rid)
    out.append(line)
open(dst, "w").write("\n".join(out) + "\n")
print(len(seen))
PY
rm -f "$T.raw"
N=$(grep -vc '^\s*#' "$T")
chmod 644 "$T"
mv "$T" "$Q"
if [ $ok = 1 ]; then
    log "queue written: the 2d series first, then the whole campaign ($N lines)"
else
    log "2d image unavailable: queued the baseline arms only ($N lines); the chosen family waits"
fi

# 5. resume -------------------------------------------------------------------------------------
if [ "$(grep -cvx "$MARK" "$STAGE3/STOP" 2>/dev/null)" = 0 ]; then
    rm -f "$STAGE3/STOP"
    log "queue resumed"
else
    log "STOP holds other reasons, the queue stays paused: $(grep -vx "$MARK" "$STAGE3/STOP" | tail -2 | tr '\n' ' ')"
fi
log "chain2d done"
