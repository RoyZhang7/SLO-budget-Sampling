#!/usr/bin/env bash
# One Stage 3 run on node-0:  tools/campaign/run_one.sh <arm> <scenario> <volume_frac> <seed>
#
# reseed -> 30 s app warm-up on the stock collector -> shadow collector (100 %, node-0 file)
# -> the arm's central + agents (fresh, empty table) -> wrk2 ComposePost 10 min at 100 req/s,
# started on a 5-s period boundary, with the scenario's seeded fault or burst -> drain,
# telemetry, status, logs -> stop the shadow, restore the stock DaemonSet, remove the central
# -> meta.json -> sanity.py --pre (no export; a problem writes STOP) -> postprocess in the
# background (export, compression, evaluation, the full sanity check; serialized).
# Writes /storage/royno7/stage3/runs/<run_id>/ (meta.json, run.log, collected / failed markers).
# Faults (faults.env): saturated, cascade and cold_start burn CPU inside the target container
# under a CPU limit on it (confined to the target); noisy_neighbour (the unconfined spinner on
# socialgraph, the saturated scenario before 2026-11-22) spins unconfined; late_everywhere spins in
# every service pod, unconfined (spin; spinf: plus a seventh thread spinning a fraction f of every
# 10 ms) or under a CPU limit per pod (burn).
# ARRIVAL=1 (SLO-budget arms): the eBPF arrival stamp, arrivald's DaemonSet and the stamp in the tee (TEE=0: in the
# agents), so that the 100 % shadow carries slobudget.arrival_ns; the run fails unless the program is at every service
# pod before the load and the ids arrived in it (arrival_check.py; a second failure writes STOP), and arrivald goes at
# the end.  OWN_MEASURE=within_service: the agents' and the central's own time counts from the arrival.  CALLGRAPH:
# the central's call graph file (the evaluation's truth does not change with it).  VARIANT: a label that keeps the
# run apart from the plain arm in every table (arm-variant).  Any of these needs a VARIANT and a RUN_ID of its own.
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
: "${ARRIVAL:=0}" ; : "${ARRIVAL_PIN:=/sys/fs/bpf/slobudget}" ; : "${ARRIVAL_SLOTS:=}" ; : "${VARIANT:=}"
: "${CALLGRAPH:=$MOD/deploy/callgraph.json}"
case "$CALLGRAPH" in /*) ;; *) CALLGRAPH=$MOD/$CALLGRAPH ;; esac
: "${URL:=${WRK2API_URL:-http://10.10.1.1:22903}}"
: "${LUA:=$HOME/blueprint-docc-mod/examples/dsb_sn/scripts/compose-post.lua}"
: "${IMAGE_TAG:=$(cat "$MOD/build/IMAGE_TAG")}"
IMG=10.10.1.1:30000/otelcol-slobudget:$IMAGE_TAG
# shellcheck disable=SC1091
source "$MOD/tools/campaign/faults.env"
# the late_everywhere fault from the queue line (2026-11-26, the overshoot runs): LATE_MODE=spin LATE_LEVEL=7 override faults.env;
# LATE_MODE=burn takes its threads from LATE_LEVEL too and its per-pod CPU quota from LATE_LIMIT (quota_us/period_us, e.g.
# 62000/10000 = 6.2 CPUs, the calibration's 83.7 % over-SLO setting); fault.json records the mode, the level and the limit
[ -n "${LATE_MODE:-}" ] && LATE_EVERYWHERE_MODE=$LATE_MODE
[ -n "${LATE_LEVEL:-}" ] && { FAULT_LEVEL_ALL=$LATE_LEVEL; BURN_THREADS_ALL=$LATE_LEVEL; }
[ -n "${LATE_LIMIT:-}" ] && BURN_LIMIT_ALL=$LATE_LIMIT
if [ "$ARRIVAL" = 1 ] || [ -n "$VARIANT${OWN_MEASURE:-}${LATE_MODE:-}${LATE_LEVEL:-}${LATE_LIMIT:-}${EXPECT_BRIDGE_KIND:-}${PERIOD_DETAIL_LOG:-}${TABLE_SMOOTH:-}" ] \
        || [ "$(basename "$CALLGRAPH")" != callgraph.json ]; then   # not a plain arm: a label and a directory of its own
    { [ -n "$VARIANT" ] && [ -n "${RUN_ID:-}" ]; } || { echo "ARRIVAL, OWN_MEASURE, CALLGRAPH, LATE_*, EXPECT_BRIDGE_KIND, PERIOD_DETAIL_LOG, TABLE_SMOOTH and VARIANT need a VARIANT and a RUN_ID of their own"; exit 2; }
fi
# the overhead experiment has the cluster (a job can be appended while its runs are on): wait
while pgrep -f '^([^ ]*/)?bash [^ ]*tools/overhead/(run_overhead|overhead_queue|knee|live_check)\.sh' > /dev/null; do sleep 30; done
exec 8> "$STAGE3/run_one.lock"      # one run at a time (the processes left behind do not hold it: 8>&-)
flock 8
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
    # arrivald takes the program off on SIGTERM; the DaemonSet goes only when its pods are gone (none there: returns at once)
    kubectl delete ds/slobudget-arrival --ignore-not-found --cascade=foreground --wait --timeout=90s >> "$RD/run.log" 2>&1 \
        || log "WARNING: arrivald is not gone: $(kubectl get pods -l app=slobudget-arrival --no-headers 2>&1 | tr '\n' ' ')"
    log "restored: $(kubectl get ds otelcol-pb-es-ctr -o jsonpath='{.spec.template.spec.containers[0].image}')"
}
# the collectors' logs, and arrivald's; a kubectl error is not a log: up to three tries
logs_to() {     # logs_to <file> <kubectl logs arguments>
    local f=$1 _; shift
    for _ in 1 2 3; do kubectl logs "$@" --since=30m > "$f" 2>&1 && return; sleep 3; done
}
run_logs() {
    logs_to "$RD/central.log" deploy/slobudget-central
    for p in $(kubectl get pods -l io.kompose.service=otelcol-pb-es-ctr -o jsonpath='{.items[*].metadata.name}'); do
        node=$(kubectl get pod "$p" -o jsonpath='{.spec.nodeName}')
        logs_to "$RD/agent-$node.log" "$p" -c otelcol-pb-es-ctr
        [ "$TEE" = 1 ] && logs_to "$RD/tee-$node.log" "$p" -c tee
    done
    if [ "$ARRIVAL" = 1 ]; then
        for p in $(kubectl get pods -l app=slobudget-arrival -o jsonpath='{.items[*].metadata.name}'); do
            node=$(kubectl get pod "$p" -o jsonpath='{.spec.nodeName}')
            logs_to "$RD/arrival-$node.log" "$p"
            kubectl logs "$p" --previous > "$RD/arrival-$node.previous.log" 2>/dev/null || rm -f "$RD/arrival-$node.previous.log"
        done
    fi
}
# a defect of the stamp: the logs before the pods go; when the retry fails too, the queue halts
arrival_fail() {
    run_logs
    if [ "${SANITY:-1}" = 1 ] && [ "$(cat "$RD/tries" 2>/dev/null || echo 0)" -ge 2 ]; then echo "$RUN_ID (arrival): $*" >> "$STAGE3/STOP"; fi
    fail "$*"
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
[ -z "$(kubectl get ds,pods -l app=slobudget-arrival --no-headers 2>/dev/null)" ] || fail "arrivald is still there (an earlier run): kubectl delete ds/slobudget-arrival"
[ -f "$CALLGRAPH" ] || fail "no call graph $CALLGRAPH"
BADENV=$(kubectl get deploy,ds -o json | python3 -c "
import json, sys
for x in json.load(sys.stdin)['items']:
    n = x['metadata']['name']
    if '-service-pb-es-ctr' in n and 'tracepressure' not in n:
        e = {v['name']: v.get('value') for v in x['spec']['template']['spec']['containers'][0].get('env') or []}
        if e.get('BRIDGE_KIND', 'pb') != '${EXPECT_BRIDGE_KIND:-pb}' or 'OTEL_SAMPLE_RATIO' in e or 'SLO_BUDGET_PORT' in e:
            print(n, end=' ')
") || fail "kubectl get deploy,ds"
# EXPECT_BRIDGE_KIND=<kind> (the bridge-kind runs, 2026-11-26): the services must carry that BRIDGE_KIND instead of the campaign's pb
[ -z "$BADENV" ] || fail "the services' env is not the expected one (BRIDGE_KIND=${EXPECT_BRIDGE_KIND:-pb}, no OTEL_SAMPLE_RATIO / SLO_BUDGET_PORT): $BADENV"
if [ "$ARRIVAL" = 1 ]; then
    docker run --rm --entrypoint /arrivald "$IMG" -version > "$RD/arrivald-version" 2>&1 || arrival_fail "no arrivald in $IMG: $(tail -1 "$RD/arrivald-version")"
fi

# 1. reseed + warm-up (stock collector) ------------------------------------------------
log "reseed"
bash "$MOD/tools/reseed.sh" > "$RD/reseed.log" 2>&1 || fail "reseed: $(tail -1 "$RD/reseed.log")"
log "$(tail -1 "$RD/reseed.log")"
log "warm-up ${WARMUP}s"
wrk -t2 -c10 -d"${WARMUP}"s -s "$LUA" "$URL" -R"$RPS" > "$RD/wrk-warmup.log" 2>&1

# 2. shadow collector + the arm ------------------------------------------------------------
if [ "$TEE" = 1 ]; then
    sed "s#__PATH__#$RD/shadow.jsonl#" "$MOD/tools/campaign/shadow-config.yaml" > "$RD/shadow-config.yaml"
    nohup "$SHADOW_BIN" --config "$RD/shadow-config.yaml" > "$RD/shadow.log" 2>&1 8>&- &
    echo $! > "$RD/shadow.pid"
    for _ in $(seq 1 30); do ss -ltn 2>/dev/null | grep -q ':14317 ' && break; sleep 1; done
    ss -ltn | grep -q ':14317 ' || fail "shadow collector did not start: $(tail -3 "$RD/shadow.log")"
fi
TT=""; [ "$ARM" = reservoir ] && TT="--ttable $STAGE3/reservoir-ttable.json"
NT=""; [ "$TEE" = 1 ] || NT="--no-tee"
[ "${CAPGATE:-1}" = 1 ] || NT="$NT --no-capgate"
ARR=""; [ "$ARRIVAL" = 1 ] && ARR="--arrival $ARRIVAL_PIN${ARRIVAL_SLOTS:+ --arrival-slots $ARRIVAL_SLOTS}"
# shellcheck disable=SC2086
python3 "$MOD/tools/campaign/render.py" --arm "$ARM" --volume "$VOL" --seed "$SEED" --image "$IMG" $NT $TT $ARR \
    --callgraph "$CALLGRAPH" --shares "$RESERVOIR_SHARES" --tail-wait "$TAIL_WAIT" > "$RD/manifests.yaml" || fail "render"
kubectl apply -f "$RD/manifests.yaml" >> "$RD/run.log" 2>&1 || fail "apply"
kubectl rollout status deploy/slobudget-central --timeout=180s >> "$RD/run.log" 2>&1 || fail "central did not come up"
kubectl rollout status ds/otelcol-pb-es-ctr --timeout=300s >> "$RD/run.log" 2>&1 || fail "agents did not roll"
sleep 15
NREADY=$(kubectl get pods -l io.kompose.service=otelcol-pb-es-ctr --no-headers | grep -c Running)
[ "$NREADY" = 3 ] || fail "agents running: $NREADY"
if [ "$ARRIVAL" = 1 ]; then
    kubectl rollout status ds/slobudget-arrival --timeout=120s >> "$RD/run.log" 2>&1 || arrival_fail "arrivald did not come up"
    python3 "$MOD/tools/overhead/arrival_check.py" > "$RD/arrival-check.txt" 2>&1 || arrival_fail "arrival: $(tail -1 "$RD/arrival-check.txt")"
fi
# the image digests the pods run (container=imageID per container)
AGENT_IMAGES=$(kubectl get pods -l io.kompose.service=otelcol-pb-es-ctr -o jsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.name}={.imageID}{" "}{end}{end}')
CENTRAL_IMAGES=$(kubectl get pods -l app=slobudget-central -o jsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.name}={.imageID}{" "}{end}{end}')
APP_IMAGEID=$(kubectl get pods -l io.kompose.service=composepost-service-pb-es-ctr -o jsonpath='{.items[0].status.containerStatuses[0].imageID}')
SHADOW_SHA=""; [ "$TEE" = 1 ] && SHADOW_SHA=$(sha256sum "$SHADOW_BIN" | cut -d' ' -f1)
log "image digests: agents $AGENT_IMAGES; central $CENTRAL_IMAGES"
scrape start

# 3. load, fault, burst ----------------------------------------------------------------------
DELTA=$(python3 -c "import random; print(round(random.Random(1000 + $SEED).uniform(0, 30), 1))")
FAULT_TARGET=""; FAULT_AT=0; BURST_AT=0; CULPRIT=None   # a Python literal: pasted into the meta heredoc below
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
run_logs
if [ "$ARRIVAL" = 1 ]; then
    python3 "$MOD/tools/overhead/arrival_check.py" --traffic --wait 5 >> "$RD/arrival-check.txt" 2>&1 || arrival_fail "arrival, after the load: $(tail -1 "$RD/arrival-check.txt")"
fi
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
meta = {"run_id": "$RUN_ID", "arm": "$ARM", "variant": "$VARIANT" or None, "scenario": "$SCENARIO", "volume_frac": $VOL, "v_star": round($VOL * 11500),
        "cap": 1.25 * round($VOL * 11500), "seed": $SEED, "slo_ms": {"compose_post": 23.4}, "period_s": 5, "margin": 0.1,
        "t0": $T0, "t1": $T1, "delta_s": $DELTA, "fault": fault, "burst": burst, "placement": placement,
        "placement_nodes": {k: sorted(v) for k, v in placement_nodes.items()},
        "arm_params": {"head_r": $VOL, "tail_p": $VOL, "tail_decision_wait_s": float("$TAIL_WAIT".rstrip("s")),
                       "reservoir_shares": json.loads('$RESERVOIR_SHARES')},
        "images": images, "tee": $TEE == 1, "agent_restarts": "$RESTARTS".split(),
        "rps": $RPS, "dur_s": $DUR,
        "callgraph": os.path.basename("$CALLGRAPH"), "arrival_pin": "$ARRIVAL_PIN" if "$ARRIVAL" == "1" else None,
        "sampler_env": {"outlier_trigger": "${OUTLIER_TRIGGER:-}" or None, "own_margin": "${OWN_MARGIN:-}" or None,
                        "own_measure": "${OWN_MEASURE:-}" or None, "server_hold": "${SERVER_HOLD:-}" or None}}
json.dump(meta, open(os.path.join(rd, "meta.json"), "w"), indent=1)
PY
touch "$RD/collected"
# the checks that need no Jaeger export, at once: a problem halts the queue before the next run
# (the full check still follows the export, in postprocess.sh)
python3 "$MOD/tools/campaign/sanity.py" --pre "$RD" > "$RD/precheck.txt" 2>&1
if ! grep -q '^OK' "$RD/precheck.txt"; then
    log "pre-check failed: $(head -5 "$RD/precheck.txt" | tr '\n' ' ')"
    [ "${SANITY:-1}" = 1 ] && echo "$RUN_ID (pre-check): $(head -3 "$RD/precheck.txt" | tr '\n' ' ')" >> "$STAGE3/STOP"
fi
log "collected; postprocess queued"
nohup bash "$MOD/tools/campaign/postprocess.sh" "$RD" >> "$RD/postprocess.log" 2>&1 8>&- &
exit 0
