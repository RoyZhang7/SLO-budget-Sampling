#!/usr/bin/env bash
# The overhead experiment's runs in order, after the campaign's queue is empty (node-0, nohup):
#   tools/overhead/overhead_queue.sh <knee rate>
# 5 repeats x (100 req/s, the knee rate) x (A B C D), interleaved: rep 1 A B C D at 100, A B C D at
# the knee, rep 2 ...  A run already `collected` is skipped, a failed one retried once (a failed try's
# directory goes to failed/).  It stops when a configuration has lost two runs or four tries in a row
# failed, and before the next run when overhead/STOP is there.  However it ends, the services' env is
# reset to the campaign's (OTEL_SAMPLE_RATIO and SLO_BUDGET_PORT unset, BRIDGE_KIND=pb).  The high rate is
# dropped from the remaining repeats when a repeat (the second or a later one) is disturbed at it: the data stores
# over STORES_MAX ms of CPU a request in any run, or A's p99 over twice its p99 at 100 req/s (rates.txt then says
# so, and the disturbed runs get an INVALID note).  With the eBPF arrival program as configuration E:
#   CONFIGS="A B C D E" IMAGE_TAG=<collector image with arrivald> tools/overhead/overhead_queue.sh <knee rate>
# C and D then run on that image too, and the queue refuses when collected ones ran on another.
set -u
cd "$(dirname "$0")/../.."
MOD=$(pwd)
: "${STAGE3:=/storage/royno7/stage3}"; : "${CONFIGS:=A B C D}"
KNEE=$1
: "${RATES:=100 $KNEE}"       # RATES=<rate>: that rate only (once more, after a wider sweep)
LOG=$STAGE3/overhead/queue.log
mkdir -p "$STAGE3/overhead"
log() { echo "$(date '+%F %T') $*" >> "$LOG"; }
: "${STORES_MAX:=7}"
[ -f "$STAGE3/overhead/rates.txt" ] && RATES=$(cat "$STAGE3/overhead/rates.txt")
disturbed() {   # repeat $1 at the high rate: writes INVALID into its disturbed runs; exit 0 when there were any, 1 when not, 2 when the check itself failed (a Python error)
    MOD=$MOD python3 - "$STAGE3/overhead" "$1" "$KNEE" "$STORES_MAX" <<'PY'
import json, os, sys
sys.path.insert(0, os.environ["MOD"] + "/tools/overhead")
import tables
root, rep, knee, smax = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4])
def run(cfg, rate):
    rd = f"{root}/{cfg}-r{rate}-{rep}"
    if not os.path.exists(f"{rd}/collected"):
        return None
    m = json.load(open(f"{rd}/meta.json"))
    w = tables.wrk(f"{rd}/wrk.log")
    cpu, secs = tables.cpu_by_group(rd)
    return rd, w, 1000 * cpu.get("stores", 0.0) / max(1, w["requests"])
bad = []
a100, ahigh = run("A", 100), run("A", knee)
if a100 and ahigh and ahigh[1].get("p99", 0) > 2 * a100[1].get("p99", float("inf")):
    bad.append((ahigh[0], f"A's p99 {ahigh[1]['p99']:.1f} ms, over twice its {a100[1]['p99']:.1f} ms at 100 req/s"))
for cfg in "ABCDEF":
    r = run(cfg, knee)
    if r and r[2] > smax:
        bad.append((r[0], f"the data stores took {r[2]:.1f} ms of CPU a request, over {smax:g}"))
for rd, why in bad:
    with open(f"{rd}/INVALID", "a") as f:
        f.write(f"disturbed at {knee} req/s: {why}\n")
print("; ".join(f"{os.path.basename(rd)}: {why}" for rd, why in bad))
sys.exit(0 if bad else 3)
PY
    case $? in 0) return 0 ;; 3) return 1 ;; *) return 2 ;; esac
}
if [ -n "$(grep -v '^#' "$STAGE3/queue.txt" | while read -r a s v e rest; do
        [ -n "$a" ] || continue
        id=$(echo "$rest" | tr ' ' '\n' | sed -n 's/^RUN_ID=//p'); [ -n "$id" ] || id="v$(python3 -c "print(round($v*100))")-$s-$a-s$e"
        [ -f "$STAGE3/runs/$id/collected" ] || { echo pending; break; }
    done)" ]; then
    echo "the campaign's queue still has jobs: not starting"; exit 1
fi
case "$CONFIGS" in *E*|*F*)
    [ -n "${IMAGE_TAG:-}" ] || { echo "configs E and F need IMAGE_TAG (the collector image of C and D)"; exit 1; }
    export IMAGE_TAG
    other=$(python3 - "$STAGE3/overhead" "otelcol-slobudget:$IMAGE_TAG" <<'PY'
import glob, json, os, sys
for m in sorted(glob.glob(os.path.join(sys.argv[1], "[CDEF]-r*-*", "meta.json"))):
    if os.path.exists(os.path.join(os.path.dirname(m), "collected")) and not ((json.load(open(m)).get("images") or {}).get("collector") or "").endswith(sys.argv[2]):
        print(os.path.basename(os.path.dirname(m)), end=" ")
PY
)
    [ -z "$other" ] || { echo "collected on another image: $other(move them away, or use their IMAGE_TAG)"; exit 1; } ;;
esac
reset_env() {
    local bad="" ws
    ws=$(kubectl get deploy,ds -o name | grep -- '-service-pb-es-ctr' | grep -v tracepressure)
    [ -n "$ws" ] || bad=" (no workloads)"
    for w in $ws; do
        kubectl set env "$w" OTEL_SAMPLE_RATIO- SLO_BUDGET_PORT- BRIDGE_KIND=pb >> "$LOG" 2>&1 || bad="$bad $w"
    done
    if [ -z "$bad" ]; then log "app env reset"; else log "app env NOT reset:$bad"; fi
}
trap reset_env EXIT
trap 'exit 1' HUP INT TERM
log "overhead queue start (knee $KNEE, configs $CONFIGS, image ${IMAGE_TAG:-the default})"
declare -A lost
row=0
for rep in 1 2 3 4 5; do
    for rate in $RATES; do
        for cfg in $CONFIGS; do
            rd=$STAGE3/overhead/$cfg-r$rate-$rep
            [ -f "$rd/collected" ] && continue
            [ -f "$STAGE3/overhead/STOP" ] && { log "overhead/STOP is there"; exit 1; }
            got=0
            for try in 1 2; do
                log "start $cfg rate $rate rep $rep (try $try)"
                if bash "$MOD/tools/overhead/run_overhead.sh" "$cfg" "$rate" "$rep" > "$STAGE3/overhead/last-run.out" 2>&1; then
                    log "collected $cfg rate $rate rep $rep"; got=1; row=0; break
                fi
                row=$((row + 1))
                sleep "${PAUSE:-60}"
                log "FAILED $cfg rate $rate rep $rep: $(cat "$rd/failed" 2>/dev/null)"
                mkdir -p "$STAGE3/overhead/failed" && mv "$rd" "$STAGE3/overhead/failed/$cfg-r$rate-$rep-$(date +%d%H%M%S)"    # a failed try keeps its logs
            done
            [ $got = 1 ] || lost[$cfg]=$((${lost[$cfg]:-0} + 1))
            if [ "${lost[$cfg]:-0}" -ge 2 ] || [ $row -ge 4 ]; then log "stopping: $cfg has lost ${lost[$cfg]:-0} runs, $row tries in a row failed"; exit 1; fi
        done
    done
    if [ "$rep" -ge 2 ] && [ "${DROP_HIGH:-1}" = 1 ] && [ "$KNEE" != 100 ] && case " $RATES " in *" $KNEE "*) true ;; *) false ;; esac; then
        why=$(disturbed "$rep" 2>&1); rc=$?
        if [ $rc = 0 ]; then
            RATES=100
            echo "$RATES" > "$STAGE3/overhead/rates.txt"
            log "repeat $rep disturbed at $KNEE req/s ($why): the remaining repeats run at 100 only"
        elif [ $rc != 1 ]; then
            log "WARNING: the disturbance check failed after repeat $rep: $(echo "$why" | tail -1)"
        fi
    fi
done
log "overhead queue done"
