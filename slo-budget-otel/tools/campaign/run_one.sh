#!/usr/bin/env bash
# One Stage 3 run on node-0:  tools/campaign/run_one.sh <arm> <scenario> <volume_frac> <seed>
#
# reseed -> 30 s app warm-up on the stock collector -> shadow collector (100 %, node-0 file)
# -> the arm's central + agents (fresh, empty table) -> wrk2 ComposePost 10 min at 100 req/s,
# started on a 5-s period boundary, with the scenario's seeded fault or burst -> drain,
# telemetry, status, logs -> stop the shadow, restore the stock DaemonSet, remove the central
# -> postprocess in the background (export, compression, evaluation; serialized).
# Writes /storage/royno7/stage3/runs/<run_id>/ (meta.json, run.log, collected / failed markers).
# Faults (faults.env): saturated, cascade and cold_start burn CPU inside the target container
# under a CPU limit on it (confined to the target); noisy_neighbour (the unconfined spinner on
# socialgraph, the saturated scenario before 2026-11-22) spins unconfined; late_everywhere spins in
# every service pod, unconfined (spin; spinf: plus a seventh thread spinning a fraction f of every
# 10 ms) or under a CPU limit per pod (burn).
# meta.json records the image digests the pods ran (images.*_digest) and the source commit of
# the synced tree (images.source_commit and source_tree, the commit's tree hash, which a reworded
# commit keeps; from SOURCE.json; source_dirty lists files that differ).
set -u
cd "$(dirname "$0")/../.."
MOD=$(pwd)
# shellcheck disable=SC1090
source ~/bridges-env.sh 2>/dev/null || true
ARM=$1; SCENARIO=$2; VOL=$3; SEED=$4
: "${STAGE3:=/storage/royno7/stage3}"
: "${DUR:=600}" ; : "${RPS:=100}" ; : "${WARMUP:=30}" ; : "${TEE:=1}"
: "${URL:=${WRK2API_URL:-http://10.10.1.1:22903}}"
: "${LUA:=$HOME/blueprint-docc-mod/examples/dsb_sn/scripts/compose-post.lua}"
: "${IMAGE_TAG:=$(cat "$MOD/build/IMAGE_TAG")}"
IMG=10.10.1.1:30000/otelcol-slobudget:$IMAGE_TAG
# shellcheck disable=SC1091
source "$MOD/tools/campaign/faults.env"
VPCT=$(python3 -c "print(round($VOL*100))")
RUN_ID=${RUN_ID:-v${VPCT}-${SCENARIO}-${ARM}-s${SEED}}
RD=$STAGE3/runs/$RUN_ID
mkdir -p "$RD/telemetry"
rm -f "$RD/failed" "$RD/collected"
log() { echo "$(date '+%F %T') $*" | tee -a "$RD/run.log"; }
fail() { log "FAILED: $*"; echo "$*" > "$RD/failed"; restore; exit 1; }
SHADOW_BIN=$MOD/build/otelcol-slobudget/otelcol-slobudget
shadow_stop() {
    if [ -f "$RD/shadow.pid" ]; then
        pid=$(cat "$RD/shadow.pid")
        kill -TERM "$pid" 2>/dev/null
        for _ in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
        kill -9 "$pid" 2>/dev/null
        rm -f "$RD/shadow.pid"
    fi
}
restore() {
    shadow_stop
    kubectl replace -f "$STAGE3/otelcol-ds.stock.yaml" >> "$RD/run.log" 2>&1
    kubectl rollout status ds/otelcol-pb-es-ctr --timeout=300s >> "$RD/run.log" 2>&1
    kubectl delete deploy/slobudget-central svc/slobudget-central cm/slobudget-central-config cm/slobudget-callgraph \
        --ignore-not-found >> "$RD/run.log" 2>&1
    log "restored: $(kubectl get ds otelcol-pb-es-ctr -o jsonpath='{.spec.template.spec.containers[0].image}')"
}
scrape() {   # scrape <label>: every collector's Prometheus endpoint
    for line in $(kubectl get pods -l io.kompose.service=otelcol-pb-es-ctr -o jsonpath='{range .items[*]}{.spec.nodeName}={.status.podIP}{" "}{end}'); do
        node=${line%%=*}; ip=${line#*=}
        curl -s -m 3 "http://$ip:8888/metrics" > "$RD/telemetry/agent-$node-$1.prom"
        [ "$TEE" = 1 ] && curl -s -m 3 "http://$ip:8889/metrics" > "$RD/telemetry/tee-$node-$1.prom"
    done
    cip=$(kubectl get pod -l app=slobudget-central -o jsonpath='{.items[0].status.podIP}')
    [ -n "$cip" ] && curl -s -m 3 "http://$cip:8888/metrics" > "$RD/telemetry/central-$1.prom"
}

log "run $RUN_ID: arm=$ARM scenario=$SCENARIO volume=$VOL seed=$SEED image=$IMG tee=$TEE"
[ "$(kubectl get ds otelcol-pb-es-ctr -o jsonpath='{.spec.template.spec.containers[0].image}')" = "$STOCK_COLLECTOR_IMAGE" ] || fail "the stock collector is not in place"

# 1. reseed + warm-up (stock collector) ------------------------------------------------
log "reseed"
bash "$MOD/tools/reseed.sh" > "$RD/reseed.log" 2>&1 || fail "reseed: $(tail -1 "$RD/reseed.log")"
log "$(tail -1 "$RD/reseed.log")"
log "warm-up ${WARMUP}s"
wrk -t2 -c10 -d"${WARMUP}"s -s "$LUA" "$URL" -R"$RPS" > "$RD/wrk-warmup.log" 2>&1

# 2. shadow collector + the arm ------------------------------------------------------------
if [ "$TEE" = 1 ]; then
    sed "s#__PATH__#$RD/shadow.jsonl#" "$MOD/tools/campaign/shadow-config.yaml" > "$RD/shadow-config.yaml"
    nohup "$SHADOW_BIN" --config "$RD/shadow-config.yaml" > "$RD/shadow.log" 2>&1 &
    echo $! > "$RD/shadow.pid"
    for _ in $(seq 1 30); do ss -ltn 2>/dev/null | grep -q ':14317 ' && break; sleep 1; done
    ss -ltn | grep -q ':14317 ' || fail "shadow collector did not start: $(tail -3 "$RD/shadow.log")"
fi
TT=""; [ "$ARM" = reservoir ] && TT="--ttable $STAGE3/reservoir-ttable.json"
NT=""; [ "$TEE" = 1 ] || NT="--no-tee"
[ "${CAPGATE:-1}" = 1 ] || NT="$NT --no-capgate"
python3 "$MOD/tools/campaign/render.py" --arm "$ARM" --volume "$VOL" --seed "$SEED" --image "$IMG" $NT $TT \
    --shares "$RESERVOIR_SHARES" --tail-wait "$TAIL_WAIT" > "$RD/manifests.yaml" || fail "render"
kubectl apply -f "$RD/manifests.yaml" >> "$RD/run.log" 2>&1 || fail "apply"
kubectl rollout status deploy/slobudget-central --timeout=180s >> "$RD/run.log" 2>&1 || fail "central did not come up"
kubectl rollout status ds/otelcol-pb-es-ctr --timeout=300s >> "$RD/run.log" 2>&1 || fail "agents did not roll"
sleep 15
NREADY=$(kubectl get pods -l io.kompose.service=otelcol-pb-es-ctr --no-headers | grep -c Running)
[ "$NREADY" = 3 ] || fail "agents running: $NREADY"
# the image digests the pods run (container=imageID per container)
AGENT_IMAGES=$(kubectl get pods -l io.kompose.service=otelcol-pb-es-ctr -o jsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.name}={.imageID}{" "}{end}{end}')
CENTRAL_IMAGES=$(kubectl get pods -l app=slobudget-central -o jsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.name}={.imageID}{" "}{end}{end}')
APP_IMAGEID=$(kubectl get pods -l io.kompose.service=composepost-service-pb-es-ctr -o jsonpath='{.items[0].status.containerStatuses[0].imageID}')
SHADOW_SHA=""; [ "$TEE" = 1 ] && SHADOW_SHA=$(sha256sum "$SHADOW_BIN" | cut -d' ' -f1)
log "image digests: agents $AGENT_IMAGES; central $CENTRAL_IMAGES"
scrape start

# 3. load, fault, burst ----------------------------------------------------------------------
DELTA=$(python3 -c "import random; print(round(random.Random(1000 + $SEED).uniform(0, 30), 1))")
FAULT_TARGET=""; FAULT_AT=0; BURST_AT=0; CULPRIT=null
case "$SCENARIO" in
    steady) ;;
    saturated) FAULT_TARGET=socialgraph; FAULT_AT=240; CULPRIT='"socialgraph"'; FAULT_MODE=burn ;;
    cascade) FAULT_TARGET=post-storage; FAULT_AT=240; CULPRIT='"poststorage"'; FAULT_MODE=burn ;;
    cold_start) FAULT_TARGET=socialgraph; FAULT_AT=120; CULPRIT='"socialgraph"'; FAULT_MODE=burn ;;
    noisy_neighbour) FAULT_TARGET=socialgraph; FAULT_AT=240; CULPRIT='"socialgraph"'; FAULT_MODE=spin ;;
    late_everywhere) FAULT_TARGET=all; FAULT_AT=240; FAULT_MODE=${LATE_EVERYWHERE_MODE:-spin} ;;
    bursty) BURST_AT=360 ;;
    *) fail "unknown scenario $SCENARIO" ;;
esac
LEVEL=""; FEXTRA=""; FAULT_LIMIT=""; DUTY=""
if [ -n "$FAULT_TARGET" ]; then   # the fault size, before the load starts (a failure here leaves no load behind)
    if [ "$FAULT_TARGET" = all ]; then LEVEL=$FAULT_LEVEL_ALL; else LEVEL=$(eval echo "\$FAULT_LEVEL_$(echo "$FAULT_TARGET" | tr '-' '_' | tr a-z A-Z)"); fi
    DUTY=""; [ "$FAULT_TARGET" = all ] && DUTY=${FAULT_DUTY_ALL:-}       # spinner duty cycle (empty: 1.0)
    FEXTRA=$DUTY; FAULT_LIMIT=""
    if [ "$FAULT_MODE" = burn ]; then    # threads and the container's CPU limit (quota_us/period_us)
        TU=$(echo "$FAULT_TARGET" | tr '-' '_' | tr a-z A-Z)
        v=BURN_THREADS_$TU; LEVEL=${!v:-}
        v=BURN_LIMIT_$TU; FAULT_LIMIT=${!v:-}
        [ -n "$LEVEL" ] && [ -n "$FAULT_LIMIT" ] || fail "no burner size for $FAULT_TARGET in faults.env"
        FEXTRA=$FAULT_LIMIT
    fi
    if [ "$FAULT_MODE" = spinf ]; then   # LEVEL full threads plus one spinning this fraction of every 10 ms
        FEXTRA=${FAULT_EXTRA_DUTY_ALL:-}
        [ -n "$FEXTRA" ] || fail "no FAULT_EXTRA_DUTY_ALL in faults.env"
    fi
fi
python3 -c "import time; t=time.time(); time.sleep((5 - t % 5) % 5 + 0.2)"     # first period complete
T0=$(python3 -c "import time; print(round(time.time(), 3))")
log "load: wrk -t2 -c10 -d${DUR}s -R$RPS seed $SEED (delta ${DELTA}s)"
RANDOM_SEED=$SEED wrk -t2 -c10 -d"${DUR}"s -L -s "$LUA" "$URL" -R"$RPS" > "$RD/wrk.log" 2>&1 &
WRK=$!
FPID=""; BPID=""
if [ -n "$FAULT_TARGET" ]; then
    ( python3 -c "import time; time.sleep(max(0, $T0 + $FAULT_AT + $DELTA - time.time()))"
      FS=$(python3 -c "import time; print(round(time.time(), 3))")
      "$MOD/tools/hog/fault.sh" "$FAULT_MODE" "$FAULT_TARGET" "$LEVEL" "$FAULT_FOR" $FEXTRA > "$RD/fault.log" 2>&1
      FA=$(python3 -c "import time; print(round(time.time(), 3))")
      echo "{\"start\": $FS, \"applied\": $FA, \"end\": $(python3 -c "print(round($FS + $FAULT_FOR, 3))"), \"mode\": \"$FAULT_MODE\", \"target\": \"$FAULT_TARGET\", \"level\": \"$LEVEL\", \"duty\": \"${DUTY:-1.0}\"}" > "$RD/fault.json"
    ) &
    FPID=$!
fi
if [ "$BURST_AT" -gt 0 ]; then
    ( python3 -c "import time; time.sleep(max(0, $T0 + $BURST_AT + $DELTA - time.time()))"
      BS=$(python3 -c "import time; print(round(time.time(), 3))")
      RANDOM_SEED=$((SEED + 100)) wrk -t2 -c20 -d"${BURST_FOR}"s -L -s "$LUA" "$URL" -R"$BURST_RPS" > "$RD/wrk-burst.log" 2>&1
      echo "{\"start\": $BS, \"end\": $(python3 -c "import time; print(round(time.time(), 3))"), \"extra_rps\": $BURST_RPS}" > "$RD/burst.json"
    ) &
    BPID=$!
fi
SCR=""
if [ "$ARM" = tail ]; then   # the tail buffer every 5 s
    cip=$(kubectl get pod -l app=slobudget-central -o jsonpath='{.items[0].status.podIP}')
    ( while kill -0 $WRK 2>/dev/null; do curl -s -m 2 "http://$cip:8888/metrics" | grep -E '^otelcol_processor_tail_sampling|^otelcol_process_memory_rss' > "$RD/telemetry/central-$(date +%s).prom"; sleep 5; done ) &
    SCR=$!
fi
wait $WRK
T1=$(python3 -c "import time; print(round(time.time(), 3))")
# wait for the fault / burst helpers only (the shadow collector is a background job too)
for p in $FPID $BPID $SCR; do wait "$p" 2>/dev/null; done
log "load done: $(grep -E 'requests in|Non-2xx|Requests/sec' "$RD/wrk.log" | tr -s ' ' | tr '\n' ' ')"

# 4. drain, collect -----------------------------------------------------------------------------
sleep 30
scrape end
cip=$(kubectl get pod -l app=slobudget-central -o jsonpath='{.items[0].status.podIP}')
case "$ARM" in chosen|threshold|keep-parent) curl -s -m 10 "http://$cip:8081/slobudget/status" > "$RD/central-status.json" ;; esac
kubectl logs deploy/slobudget-central --since=30m > "$RD/central.log" 2>&1
for p in $(kubectl get pods -l io.kompose.service=otelcol-pb-es-ctr -o jsonpath='{.items[*].metadata.name}'); do
    node=$(kubectl get pod "$p" -o jsonpath='{.spec.nodeName}')
    kubectl logs "$p" -c otelcol-pb-es-ctr --since=30m > "$RD/agent-$node.log" 2>&1
    [ "$TEE" = 1 ] && kubectl logs "$p" -c tee --since=30m > "$RD/tee-$node.log" 2>&1
done
RESTARTS=$(kubectl get pods -l io.kompose.service=otelcol-pb-es-ctr -o jsonpath='{range .items[*]}{.status.containerStatuses[*].restartCount}{" "}{end}')
PLACEMENT=$(kubectl get pods -o jsonpath='{range .items[*]}{.metadata.labels.io\.kompose\.service}={.spec.nodeName}{" "}{end}')
APPIMG=$(kubectl get deploy composepost-service-pb-es-ctr -o jsonpath='{.spec.template.spec.containers[0].image}')
restore

# 5. meta + postprocess ---------------------------------------------------------------------------
python3 - "$RD" <<PY
import json, os, sys
rd = sys.argv[1]
def load(n):
    p = os.path.join(rd, n)
    return json.load(open(p)) if os.path.exists(p) else None
fault = load("fault.json")
if fault:
    fault.update({"kind": "cpu-$FAULT_MODE", "culprit": $CULPRIT, "targets": ["$FAULT_TARGET".replace("post-storage", "poststorage")],
                  "limit": "${FAULT_LIMIT:-}" or None, "extra_duty": "${FEXTRA:-}" if "$FAULT_MODE" == "spinf" else None})
burst = load("burst.json")
def digests(text):   # "container=docker-pullable://repo@sha256:... ..." -> {container: [digests]}
    out = {}
    for item in text.split():
        name, _, image_id = item.partition("=")
        d = image_id.rpartition("@")[2] if "@" in image_id else image_id
        out.setdefault(name, [])
        if d not in out[name]:
            out[name].append(d)
    return out
agents = digests("$AGENT_IMAGES")
central = digests("$CENTRAL_IMAGES")
col, cen = agents.get("otelcol-pb-es-ctr", []), sum(central.values(), [])
images = {"collector": "$IMG", "app": "$APPIMG",
          "collector_digest": col[0] if len(col) == 1 else (col or None),       # a list if the agents disagree
          "central_digest": cen[0] if len(cen) == 1 else (cen or None), "agent_containers": agents,
          "app_digest": "$APP_IMAGEID".rpartition("@")[2] or None, "shadow_sha256": "$SHADOW_SHA" or None}
try:
    src = json.load(open("$MOD/SOURCE.json"))
except (OSError, ValueError):
    src = {}
images.update({"source_commit": src.get("commit"), "source_tree": src.get("tree"), "source_dirty": src.get("dirty"),
               "source_synced": src.get("synced")})
placement, placement_nodes = {}, {}
for item in "$PLACEMENT".split():
    lab, _, node = item.partition("=")
    if lab.endswith("-service-pb-es-ctr"):
        svc = lab[: -len("-service-pb-es-ctr")].replace("-", "")
        placement[svc] = node
        placement_nodes.setdefault(svc, set()).add(node)
meta = {"run_id": "$RUN_ID", "arm": "$ARM", "scenario": "$SCENARIO", "volume_frac": $VOL, "v_star": round($VOL * 11500),
        "cap": 1.25 * round($VOL * 11500), "seed": $SEED, "slo_ms": {"compose_post": 23.4}, "period_s": 5, "margin": 0.1,
        "t0": $T0, "t1": $T1, "delta_s": $DELTA, "fault": fault, "burst": burst, "placement": placement,
        "placement_nodes": {k: sorted(v) for k, v in placement_nodes.items()},
        "arm_params": {"head_r": $VOL, "tail_p": $VOL, "tail_decision_wait_s": float("$TAIL_WAIT".rstrip("s")),
                       "reservoir_shares": json.loads('$RESERVOIR_SHARES')},
        "images": images, "tee": $TEE == 1, "agent_restarts": "$RESTARTS".split(),
        "rps": $RPS, "dur_s": $DUR,
        "sampler_env": {"outlier_trigger": "${OUTLIER_TRIGGER:-}" or None, "own_margin": "${OWN_MARGIN:-}" or None,
                        "server_hold": "${SERVER_HOLD:-}" or None}}
json.dump(meta, open(os.path.join(rd, "meta.json"), "w"), indent=1)
PY
touch "$RD/collected"
log "collected; postprocess queued"
nohup bash "$MOD/tools/campaign/postprocess.sh" "$RD" >> "$RD/postprocess.log" 2>&1 &
exit 0
