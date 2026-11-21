#!/usr/bin/env bash
# Cluster run on node-0: deploy the central and switch the deployment's collector
# DaemonSet to the SLO-budget image, 10 min of wrk2 ComposePost at 100 req/s at
# V* = 12 % (fixed 1380), a CPU hog on socialgraph's node from minute 4 to 7, then
# export the traces, collect the collectors' logs and status, and restore the
# original DaemonSet.  The app images (with the SDK budget issuer) are not touched.
#
#   tools/run_cluster.sh                                  # on-demand roots, no reconstruction
#   ROOT_POLICY=pin RECON=1 ARM=pin-recon tools/run_cluster.sh
#   HOG_FOR=0 BURST_AT=480 BURST_FOR=60 BURST_RPS=200 ARM=burst tools/run_cluster.sh   # D: 3x for 60 s at minute 8
#   DUR=120 HOG_AT=40 HOG_FOR=30 tools/run_cluster.sh    # a short smoke run
set -u
cd "$(dirname "$0")/.."
# shellcheck disable=SC1090
source ~/bridges-env.sh 2>/dev/null || true
: "${DUR:=600}" ; : "${RPS:=100}" ; : "${HOG_AT:=240}" ; : "${HOG_FOR:=180}" ; : "${HOG_NODE:=node-2}" ; : "${HOG_PROCS:=40}"
: "${BURST_AT:=0}" ; : "${BURST_FOR:=60}" ; : "${BURST_RPS:=200}"
: "${ROOT_POLICY:=on_demand}" ; : "${RECON:=0}" ; : "${ARM:=}"
: "${URL:=${WRK2API_URL:-http://10.10.1.1:22903}}"
: "${LUA:=$HOME/blueprint-docc-mod/examples/dsb_sn/scripts/compose-post.lua}"
: "${JQ:=http://192.168.134.21:16686}"
# every service: the query API filters by service, a shed trace without a span of it would be missed
: "${SERVICE:=$(for n in wrk2api composepost hometimeline usertimeline post_storage text urlshorten usermention socialgraph uniqueid media user; do printf 'unknown_service:%s_service_pb_es_proc,' $n; done)}"
TAG=$(cat build/IMAGE_TAG)
IMG=10.10.1.1:30000/otelcol-slobudget:$TAG
PROCS="slobudgetcentral, batch"; [ "$RECON" = 1 ] && PROCS="slobudgetcentral, slobudgetrecon, batch"
R=results/cluster-$(date +%Y%m%d-%H%M%S)${ARM:+-$ARM}
mkdir -p "$R"
log() { echo "$(date '+%F %T') $*" | tee -a "$R/run.log"; }
restore() {
    log "restore: rollout undo of the otelcol DaemonSet, remove the central"
    kubectl rollout undo daemonset/otelcol-pb-es-ctr >> "$R/run.log" 2>&1
    kubectl rollout status daemonset/otelcol-pb-es-ctr --timeout=300s >> "$R/run.log" 2>&1
    kubectl delete -f deploy/central.yaml --ignore-not-found >> "$R/run.log" 2>&1
    kubectl get ds otelcol-pb-es-ctr -o jsonpath='{.spec.template.spec.containers[0].image}' >> "$R/run.log"; echo >> "$R/run.log"
}
log "run $R: image $IMG, $DUR s at $RPS rps, root_policy=$ROOT_POLICY recon=$RECON, hog on $HOG_NODE at ${HOG_AT}s for ${HOG_FOR}s, burst at ${BURST_AT}s for ${BURST_FOR}s at +$BURST_RPS rps"
echo "{\"root_policy\": \"$ROOT_POLICY\", \"recon\": $RECON, \"arm\": \"$ARM\", \"app_images\": \"$(kubectl get deploy composepost-service-pb-es-ctr -o jsonpath='{.spec.template.spec.containers[0].image}')\"}" > "$R/arm.json"
kubectl get ds otelcol-pb-es-ctr -o yaml > "$R/otelcol-ds.original.yaml"
kubectl get pods -o wide > "$R/pods.before.txt"

log "deploy: central (processors: $PROCS), agent config, headless peers service, SDK port, DaemonSet switch"
sed -e "s#__IMAGE__#$IMG#" -e "s#__CENTRAL_PROCESSORS__#$PROCS#" deploy/central.yaml | kubectl apply -f - | tee -a "$R/run.log"
sed -e "s#__ROOT_POLICY__#$ROOT_POLICY#" deploy/agent.yaml | kubectl apply -f - | tee -a "$R/run.log"
# the SDK's budget issuer polls the node-local agent at otelcol-pb-es-ctr:8082 (internalTrafficPolicy Local)
if ! kubectl get svc otelcol-pb-es-ctr -o jsonpath='{.spec.ports[*].port}' | grep -qw 8082; then
    kubectl patch svc otelcol-pb-es-ctr --type=json -p='[{"op":"add","path":"/spec/ports/-","value":{"name":"slobudget-sdk","port":8082,"protocol":"TCP","targetPort":8082}}]' | tee -a "$R/run.log"
fi
sed "s#__IMAGE__#$IMG#" deploy/otelcol-daemonset.slobudget.yaml | kubectl apply -f - | tee -a "$R/run.log"
if ! kubectl rollout status deploy/slobudget-central --timeout=180s >> "$R/run.log" 2>&1; then log "central did not come up"; kubectl describe deploy slobudget-central > "$R/central-describe.txt"; kubectl logs deploy/slobudget-central > "$R/central.log" 2>&1; restore; exit 1; fi
if ! kubectl rollout status ds/otelcol-pb-es-ctr --timeout=300s >> "$R/run.log" 2>&1; then log "agent DaemonSet did not roll"; kubectl describe ds otelcol-pb-es-ctr > "$R/ds-describe.txt"; restore; exit 1; fi
kubectl get pods -o wide | grep -E "otelcol|slobudget" | tee -a "$R/run.log"
CENTRAL=$(kubectl get svc slobudget-central -o jsonpath='{.spec.clusterIP}')
sleep 20
log "central status before load:"; curl -s "http://$CENTRAL:8081/slobudget/status" | head -c 800 | tee -a "$R/run.log"; echo
log "sdk endpoint from node-0 via the agents' pod IPs:"
for ip in $(kubectl get pods -l io.kompose.service=otelcol-pb-es-ctr -o jsonpath='{.items[*].status.podIP}'); do curl -s -m 2 "http://$ip:8082/slobudget/sdk" | tee -a "$R/run.log"; echo; done

T0=$(date +%s)
log "phase a: wrk -t2 -c10 -d${DUR}s -R$RPS ComposePost only"
wrk -t2 -c10 -d"${DUR}"s -L -s "$LUA" "$URL" -R"$RPS" > "$R/wrk.log" 2>&1 &
WRK=$!
HOG0=0; HOG1=0
if [ "$HOG_FOR" -gt 0 ]; then
    ( sleep "$HOG_AT"
      echo "$(date '+%F %T') cpu hog: $HOG_PROCS x 'yes' on $HOG_NODE for ${HOG_FOR}s" >> "$R/run.log"
      ssh -o BatchMode=yes "$HOG_NODE" "for i in \$(seq 1 $HOG_PROCS); do timeout $HOG_FOR yes > /dev/null & done; sleep 1; pgrep -c yes" >> "$R/run.log" 2>&1 ) &
    HOG0=$((T0 + HOG_AT)); HOG1=$((HOG0 + HOG_FOR))
fi
B0=0; B1=0
if [ "$BURST_AT" -gt 0 ]; then
    ( sleep "$BURST_AT"
      echo "$(date '+%F %T') burst: wrk -t2 -c20 -d${BURST_FOR}s -R$BURST_RPS on top of the base load" >> "$R/run.log"
      wrk -t2 -c20 -d"${BURST_FOR}"s -L -s "$LUA" "$URL" -R"$BURST_RPS" > "$R/wrk-burst.log" 2>&1 ) &
    B0=$((T0 + BURST_AT)); B1=$((B0 + BURST_FOR))
fi
wait $WRK
T1=$(date +%s)
wait
log "wrk finished: $(grep -E 'requests in|Non-2xx|Requests/sec' "$R/wrk.log" | tr '\n' ' ')"
[ "$BURST_AT" -gt 0 ] && log "burst finished: $(grep -E 'requests in|Non-2xx|Requests/sec' "$R/wrk-burst.log" | tr '\n' ' ')"
echo "{\"t0\": $T0, \"t1\": $T1, \"hog_start\": $HOG0, \"hog_end\": $HOG1, \"burst_start\": $B0, \"burst_end\": $B1, \"rps\": $RPS, \"burst_rps\": $BURST_RPS, \"dur\": $DUR, \"image\": \"$IMG\", \"root_policy\": \"$ROOT_POLICY\", \"recon\": $RECON}" > "$R/times.json"
sleep 30
log "collect: central status, collector logs"
curl -s "http://$CENTRAL:8081/slobudget/status" > "$R/central-status.json"
kubectl logs deploy/slobudget-central --since=30m > "$R/central.log" 2>&1
for p in $(kubectl get pods -l io.kompose.service=otelcol-pb-es-ctr -o jsonpath='{.items[*].metadata.name}'); do
    node=$(kubectl get pod "$p" -o jsonpath='{.spec.nodeName}')
    kubectl logs "$p" --since=30m > "$R/agent-$node.log" 2>&1
done
kubectl logs ds/wrk2api-service-pb-es-ctr --since=30m 2>/dev/null | grep -E "slobudget" | tail -20 > "$R/sdk-issuer.log"
log "export: $SERVICE from $((T0 - 5)) to $((T1 + 30))"
python3 "$HOME/slo-budget-sim/tools/jaeger_export.py" --base "$JQ" --service "$SERVICE" --start $((T0 - 5)) --end $((T1 + 30)) \
    --chunk 60 --limit 50000 --out "$R/export.json" > "$R/export.log" 2>&1
log "export: $(tail -1 "$R/export.log")"
restore
log "done: $R"
