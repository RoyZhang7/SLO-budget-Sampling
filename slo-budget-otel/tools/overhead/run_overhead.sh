#!/usr/bin/env bash
# One run of the end-to-end overhead experiment (node-0, after the campaign's queue is empty):
#
#   tools/overhead/run_overhead.sh <A|B|C|D|E> <rate> <rep>
#
#   A  tracing off: OTEL_SAMPLE_RATIO=0 (the SDK records no span), the stock collector
#   B  SDK head sampling at 12 %: OTEL_SAMPLE_RATIO=0.12 (ParentBased(TraceIDRatioBased) at the
#      root, whole traces kept or dropped) into the stock collector -> Jaeger
#   C  record everything (ratio unset: AlwaysSample) into our agent at a collection volume target
#      of 12 % of the spans at <rate>, the share B keeps (the chosen arm of the campaign without the
#      tee and the shadow; 1 380 spans per 5 s at 100 req/s) -> the central on node-0 -> Jaeger
#   D  C with the Path Bridge off (BRIDGE_KIND=v: the SDK's vanilla processor, the same plain
#      OTLP export, no breadcrumbs), same image: a span carries no _br / _d
#   E  C with the eBPF arrival program attached: arrivald's DaemonSet keeps it at every service
#      pod's interface (tc ingress), the agents look every server span's arrival up in its table
#      and attach it.  Needs a collector image with arrivald in it (IMAGE_TAG, built from this
#      tree); C and D run on the same image, so that C and E differ only by the program
#
# The app images are the campaign's, the collector image is IMAGE_TAG's; only the env and the
# collector DaemonSet differ.  SLO_BUDGET_PORT=off in A and B: the budget issuer's span processor
# is then not registered with the tracer provider at all (trace.go adds it only when its
# constructor returns non-nil), and this run checks that in the pods' logs.  Unset in C, D and E.
# Fault-free.  Set the app env (rolls the services when it changes) -> reseed -> C / D / E: our
# agent and the central (E: and arrivald, until it is at every service pod) -> 30-s warm-up at
# <rate> -> CPU snapshot -> 5-min wrk2 load at <rate> req/s (exponential gaps, one connection per
# req/s, see knee.sh) -> CPU snapshot -> Path Bridge metrics lines and pod facts (E: the program's
# counters and the agents' lookups) -> restore the stock collector, remove arrivald.
# Writes $STAGE3/overhead/<cfg>-r<rate>-<rep>/ (meta.json, wrk.log, cpu-start.json, cpu-end.json).
set -u
cd "$(dirname "$0")/../.."
MOD=$(pwd)
# shellcheck disable=SC1090
source ~/bridges-env.sh 2>/dev/null || true
CFG=$1; RATE=$2; REP=$3
: "${STAGE3:=/storage/royno7/stage3}"; : "${DUR:=300}"; : "${WARMUP:=30}"
: "${URL:=${WRK2API_URL:-http://10.10.1.1:22903}}"
: "${LUA:=$HOME/blueprint-docc-mod/examples/dsb_sn/scripts/compose-post.lua}"
: "${IMAGE_TAG:=20261121-224737}"
: "${ARRIVAL_PIN:=/sys/fs/bpf/slobudget}"
IMG=10.10.1.1:30000/otelcol-slobudget:$IMAGE_TAG
# shellcheck disable=SC1091
source "$MOD/tools/campaign/faults.env"
RD=$STAGE3/overhead/$CFG-r$RATE-$REP
mkdir -p "$RD"
rm -f "$RD/failed" "$RD/collected"
log() { echo "$(date '+%F %T') $*" | tee -a "$RD/run.log"; }
ours() { case "$CFG" in C|D|E) return 0 ;; esac; return 1; }    # our agent and the central
restore() {
    if [ "$(kubectl get ds otelcol-pb-es-ctr -o jsonpath='{.spec.template.spec.containers[0].image}')" != "$STOCK_COLLECTOR_IMAGE" ]; then
        kubectl replace -f "$STAGE3/otelcol-ds.stock.yaml" >> "$RD/run.log" 2>&1
        kubectl rollout status ds/otelcol-pb-es-ctr --timeout=300s >> "$RD/run.log" 2>&1
    fi
    kubectl delete deploy/slobudget-central svc/slobudget-central cm/slobudget-central-config cm/slobudget-callgraph \
        --ignore-not-found >> "$RD/run.log" 2>&1
    # arrivald takes the program off on SIGTERM; the DaemonSet goes only when its pods are gone
    kubectl delete ds/slobudget-arrival --ignore-not-found --cascade=foreground --wait --timeout=90s >> "$RD/run.log" 2>&1 \
        || log "WARNING: arrivald is not gone: $(kubectl get pods -l app=slobudget-arrival --no-headers 2>&1 | tr '\n' ' ')"
    log "restored: $(kubectl get ds otelcol-pb-es-ctr -o jsonpath='{.spec.template.spec.containers[0].image}')"
}
# E: what arrivald and the agents said, before they are removed
arrival_logs() {
    for p in $(kubectl get pods -l app=slobudget-arrival -o jsonpath='{.items[*].metadata.name}'); do
        n=$(kubectl get pod "$p" -o jsonpath='{.spec.nodeName}')
        kubectl logs "$p" --since=12m > "$RD/arrival-$n.log" 2>&1
        kubectl logs "$p" --previous > "$RD/arrival-$n.previous.log" 2>/dev/null || rm -f "$RD/arrival-$n.previous.log"
    done
    for p in $(kubectl get pods -l io.kompose.service=otelcol-pb-es-ctr -o jsonpath='{.items[*].metadata.name}'); do
        kubectl logs "$p" --since=12m 2>/dev/null | grep -E "slobudget arrival|arrival table" > "$RD/arrival-agent-$(kubectl get pod "$p" -o jsonpath='{.spec.nodeName}').log"
    done
}
fail() { log "FAILED: $*"; echo "$*" > "$RD/failed"; [ "$CFG" = E ] && arrival_logs; restore; exit 1; }
# BRIDGE_KIND is explicit in every config: kubectl set env keeps the last value otherwise
case "$CFG" in
    A) ENVS="OTEL_SAMPLE_RATIO=0 SLO_BUDGET_PORT=off BRIDGE_KIND=pb" ;;
    B) ENVS="OTEL_SAMPLE_RATIO=0.12 SLO_BUDGET_PORT=off BRIDGE_KIND=pb" ;;
    C) ENVS="OTEL_SAMPLE_RATIO- SLO_BUDGET_PORT- BRIDGE_KIND=pb" ;;
    D) ENVS="OTEL_SAMPLE_RATIO- SLO_BUDGET_PORT- BRIDGE_KIND=v" ;;
    E) ENVS="OTEL_SAMPLE_RATIO- SLO_BUDGET_PORT- BRIDGE_KIND=pb" ;;
    *) echo "config A, B, C, D or E"; exit 2 ;;
esac
log "overhead run $CFG rate=$RATE rep=$REP env=[$ENVS] image=$IMG"
[ "$(kubectl get ds otelcol-pb-es-ctr -o jsonpath='{.spec.template.spec.containers[0].image}')" = "$STOCK_COLLECTOR_IMAGE" ] \
    || fail "the stock collector is not in place"
[ -z "$(kubectl get ds,pods -l app=slobudget-arrival --no-headers 2>/dev/null)" ] || fail "arrivald is still there (an earlier E run): kubectl delete ds/slobudget-arrival"
ARR=""
if [ "$CFG" = E ]; then
    ARR="--arrival $ARRIVAL_PIN"
    docker run --rm --entrypoint /arrivald "$IMG" -version > "$RD/arrivald-version" 2>&1 || fail "no arrivald in $IMG: $(tail -1 "$RD/arrivald-version")"
    # the kernel's verifier (node-0 runs the app nodes' kernel); nothing is attached
    docker run --rm --privileged --entrypoint /arrivald "$IMG" -check > "$RD/arrivald-check" 2>&1 || fail "the verifier: $(tail -3 "$RD/arrivald-check" | tr '\n' ' ')"
    log "arrivald: $(grep -m1 processed "$RD/arrivald-check")"
fi

# 1. the app's tracing config, then the reseed (it restarts the services too) ------------------
WORKLOADS=$(kubectl get deploy,ds -o name | grep -- '-service-pb-es-ctr' | grep -v tracepressure)
for w in $WORKLOADS; do
    # shellcheck disable=SC2086
    kubectl set env "$w" $ENVS >> "$RD/run.log" 2>&1 || fail "set env $w"
done
for w in $WORKLOADS; do
    kubectl rollout status "$w" --timeout=300s >> "$RD/run.log" 2>&1 || fail "rollout $w"
done
bash "$MOD/tools/reseed.sh" > "$RD/reseed.log" 2>&1 || fail "reseed: $(tail -1 "$RD/reseed.log")"
log "$(tail -1 "$RD/reseed.log")"

# 2. C / D / E: our agent (no tee, no shadow) and the central; E: arrivald ------------------------
if ours; then
    # shellcheck disable=SC2086
    python3 "$MOD/tools/campaign/render.py" --arm chosen --volume 0.12 --rate "$RATE" --seed "$REP" --image "$IMG" --no-tee $ARR \
        > "$RD/manifests.yaml" || fail "render"
    kubectl apply -f "$RD/manifests.yaml" >> "$RD/run.log" 2>&1 || fail "apply"
    kubectl rollout status deploy/slobudget-central --timeout=180s >> "$RD/run.log" 2>&1 || fail "central did not come up"
    kubectl rollout status ds/otelcol-pb-es-ctr --timeout=300s >> "$RD/run.log" 2>&1 || fail "agents did not roll"
    sleep 15
fi
if [ "$CFG" = E ]; then
    kubectl rollout status ds/slobudget-arrival --timeout=120s >> "$RD/run.log" 2>&1 || fail "arrivald did not come up"
    python3 "$MOD/tools/overhead/arrival_check.py" >> "$RD/run.log" 2>&1 || fail "arrival: $(tail -1 "$RD/run.log")"
fi
NREADY=$(kubectl get pods -l io.kompose.service=otelcol-pb-es-ctr --no-headers | grep -c Running)
[ "$NREADY" = 3 ] || fail "collectors running: $NREADY"
# the issuer processor: off (A, B) = "not registered" in every app pod's log, on (C, D, E) = polling
APP_PODS=$(kubectl get pods -o json | python3 -c "
import json, sys
for p in json.load(sys.stdin)['items']:
    n = p['metadata']['name']
    if '-service-pb-es-ctr' in n and 'tracepressure' not in n and not p['metadata'].get('deletionTimestamp') \
       and p['status'].get('phase') == 'Running':
        print('pod/' + n)
")
[ -n "$APP_PODS" ] || fail "no app pods"
for p in $APP_PODS; do
    if [ "$CFG" = A ] || [ "$CFG" = B ]; then
        kubectl logs "$p" 2>/dev/null | grep -q "slobudget issuer disabled" || fail "$p: the budget issuer is not disabled"
        kubectl logs "$p" 2>/dev/null | grep -q "slobudget issuer: polling" && fail "$p: the budget issuer is polling"
    else
        kubectl logs "$p" 2>/dev/null | grep -q "slobudget issuer: polling" || fail "$p: the budget issuer is not polling"
    fi
done
APP_ENV=$(kubectl get deploy composepost-service-pb-es-ctr -o jsonpath='{range .spec.template.spec.containers[0].env[*]}{.name}={.value}{" "}{end}' \
    | tr ' ' '\n' | grep -E '^(OTEL_SAMPLE_RATIO|SLO_BUDGET_PORT|BRIDGE_KIND)=' | tr '\n' ' ')
log "app env: $APP_ENV"

# 3. warm-up, CPU snapshot, load, CPU snapshot ----------------------------------------------------
LOAD="-t2 -c$RATE -D exp"
# shellcheck disable=SC2086
wrk $LOAD -d"${WARMUP}"s -s "$LUA" "$URL" -R"$RATE" > "$RD/wrk-warmup.log" 2>&1
if [ "$CFG" = E ]; then
    python3 "$MOD/tools/overhead/arrival_check.py" --traffic >> "$RD/run.log" 2>&1 || fail "arrival, after the warm-up: $(tail -1 "$RD/run.log")"
fi
python3 "$MOD/tools/overhead/cpu_snapshot.py" "$RD/cpu-start.json" || fail "cpu snapshot"
T0=$(python3 -c "import time; print(round(time.time(), 3))")
log "load: wrk $LOAD -d${DUR}s -R$RATE"
# shellcheck disable=SC2086
RANDOM_SEED=$REP wrk $LOAD -d"${DUR}"s -L -s "$LUA" "$URL" -R"$RATE" > "$RD/wrk.log" 2>&1
T1=$(python3 -c "import time; print(round(time.time(), 3))")
python3 "$MOD/tools/overhead/cpu_snapshot.py" "$RD/cpu-end.json" || fail "cpu snapshot"
log "load done: $(grep -E 'requests in|Non-2xx|Requests/sec' "$RD/wrk.log" | tr -s ' ' | tr '\n' ' ')"
if [ "$CFG" = E ]; then     # still there, and it answered to the end
    python3 "$MOD/tools/overhead/arrival_check.py" --traffic --wait 5 >> "$RD/run.log" 2>&1 || fail "arrival, after the load: $(tail -1 "$RD/run.log")"
    arrival_logs
fi

# 4. collect: the span processor's metrics lines (the Path Bridge's, or the vanilla one's in D), placement, image digests ----------------------------
mkdir -p "$RD/pb"
for p in $(kubectl get pods -o name | grep -- '-service-pb-es-ctr' | grep -v tracepressure); do
    kubectl logs "$p" --since=7m 2>/dev/null | grep -E "(pb|vanilla)_processor_metrics" > "$RD/pb/$(basename "$p").log"
done
PLACEMENT=$(kubectl get pods -o jsonpath='{range .items[*]}{.metadata.name}={.spec.nodeName}{" "}{end}')
AGENT_IMAGES=$(kubectl get pods -l io.kompose.service=otelcol-pb-es-ctr -o jsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.name}={.imageID}{" "}{end}{end}')
APP_IMAGEID=$(kubectl get pods -l io.kompose.service=composepost-service-pb-es-ctr -o jsonpath='{.items[0].status.containerStatuses[0].imageID}')
if ours; then kubectl logs deploy/slobudget-central --since=8m > "$RD/central.log" 2>&1; fi
restore

python3 - "$RD" <<PY
import json, os, sys
rd = sys.argv[1]
try:
    src = json.load(open("$MOD/SOURCE.json"))
except (OSError, ValueError):
    src = {}
meta = {"config": "$CFG", "rate": $RATE, "rep": $REP, "dur_s": $DUR, "t0": $T0, "t1": $T1, "load": "wrk $LOAD", "app_env": "$APP_ENV".split(),
        "volume_target_spans_per_period": round(0.12 * 11500 * ($RATE / 100)) if "$CFG" in ("C", "D", "E") else None,
        "arrival_pin": "$ARRIVAL_PIN" if "$CFG" == "E" else None,
        "placement": dict(x.split("=", 1) for x in "$PLACEMENT".split() if "=" in x),
        "images": {"collector": "$IMG" if "$CFG" in ("C", "D", "E") else "$STOCK_COLLECTOR_IMAGE", "agent_containers": "$AGENT_IMAGES".split(),
                   "app_digest": "$APP_IMAGEID".rpartition("@")[2] or None,
                   "source_commit": src.get("commit"), "source_tree": src.get("tree"), "source_dirty": src.get("dirty")}}
json.dump(meta, open(os.path.join(rd, "meta.json"), "w"), indent=1)
PY
touch "$RD/collected"
log "collected"
