#!/usr/bin/env bash
# The overhead experiment's runs in order, after the campaign's queue is empty (node-0, nohup):
#   tools/overhead/overhead_queue.sh <knee rate>
# 5 repeats x (100 req/s, the knee rate) x (A B C D), interleaved: rep 1 A B C D at 100, A B C D at
# the knee, rep 2 ...  A run already `collected` is skipped, a failed one retried once.  At the end
# the services' env is reset to the campaign's (OTEL_SAMPLE_RATIO and SLO_BUDGET_PORT unset,
# BRIDGE_KIND=pb).  With the eBPF arrival program as configuration E:
#   CONFIGS="A B C D E" IMAGE_TAG=<collector image with arrivald> tools/overhead/overhead_queue.sh <knee rate>
# C and D then run on that image too, and the queue refuses when collected ones ran on another.
set -u
cd "$(dirname "$0")/../.."
MOD=$(pwd)
: "${STAGE3:=/storage/royno7/stage3}"; : "${CONFIGS:=A B C D}"
KNEE=$1
LOG=$STAGE3/overhead/queue.log
mkdir -p "$STAGE3/overhead"
log() { echo "$(date '+%F %T') $*" >> "$LOG"; }
if [ -n "$(grep -v '^#' "$STAGE3/queue.txt" | while read -r a s v e rest; do
        [ -n "$a" ] || continue
        id=$(echo "$rest" | tr ' ' '\n' | sed -n 's/^RUN_ID=//p'); [ -n "$id" ] || id="v$(python3 -c "print(round($v*100))")-$s-$a-s$e"
        [ -f "$STAGE3/runs/$id/collected" ] || { echo pending; break; }
    done)" ]; then
    echo "the campaign's queue still has jobs: not starting"; exit 1
fi
case "$CONFIGS" in *E*)
    [ -n "${IMAGE_TAG:-}" ] || { echo "config E needs IMAGE_TAG (a collector image with arrivald)"; exit 1; }
    export IMAGE_TAG
    other=$(python3 - "$STAGE3/overhead" "otelcol-slobudget:$IMAGE_TAG" <<'PY'
import glob, json, os, sys
for m in sorted(glob.glob(os.path.join(sys.argv[1], "[CDE]-r*-*", "meta.json"))):
    if os.path.exists(os.path.join(os.path.dirname(m), "collected")) and not json.load(open(m))["images"]["collector"].endswith(sys.argv[2]):
        print(os.path.basename(os.path.dirname(m)), end=" ")
PY
)
    [ -z "$other" ] || { echo "collected on another image: $other(move them away, or use their IMAGE_TAG)"; exit 1; } ;;
esac
log "overhead queue start (knee $KNEE, configs $CONFIGS, image ${IMAGE_TAG:-the default})"
for rep in 1 2 3 4 5; do
    for rate in 100 "$KNEE"; do
        for cfg in $CONFIGS; do
            rd=$STAGE3/overhead/$cfg-r$rate-$rep
            [ -f "$rd/collected" ] && continue
            for try in 1 2; do
                log "start $cfg rate $rate rep $rep (try $try)"
                if bash "$MOD/tools/overhead/run_overhead.sh" "$cfg" "$rate" "$rep" > "$STAGE3/overhead/last-run.out" 2>&1; then
                    log "collected $cfg rate $rate rep $rep"; break
                fi
                log "FAILED $cfg rate $rate rep $rep: $(cat "$rd/failed" 2>/dev/null)"
            done
        done
    done
done
for w in $(kubectl get deploy,ds -o name | grep -- '-service-pb-es-ctr' | grep -v tracepressure); do
    kubectl set env "$w" OTEL_SAMPLE_RATIO- SLO_BUDGET_PORT- BRIDGE_KIND=pb >> "$LOG" 2>&1
done
log "overhead queue done; app env reset"
