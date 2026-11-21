#!/bin/bash
# Rebuild the DSB-SN pb_es service images (the live variant) with the SLO-budget issuer in
# the SDK and push them to the cluster registry.  Nothing is deployed here.
#   bash ~/bp-patch/rebuild_images.sh            # writes ~/bp-patch/IMAGE_TAG
set -uo pipefail
source ~/bridges-env.sh
cd "$BLUEPRINT_DIR/examples/dsb_sn" || exit 1
TAG=${TAG:-v$(date +%Y%m%d-%H%M%S)-slobgt}
echo "== $(date '+%T') wiring docker_pb_es -> build_slobgt (OT_BRIDGE=pb)"
export OT_BRIDGE=pb
rm -rf build_slobgt
if ! go run wiring/main.go -w docker_pb_es -o build_slobgt > ~/bp-patch/wiring.log 2>&1; then
    echo "wiring failed"; tail -30 ~/bp-patch/wiring.log; exit 1
fi
echo "== $(date '+%T') goimports"
"$GOPATH/bin/goimports" -w build_slobgt/docker || { echo "goimports failed"; exit 1; }
cd build_slobgt/docker || exit 1
echo "== sanity: issuer compiled into the copied runtime, request size in the HTTP handler"
echo "trace.go with issuer: $(grep -l NewSLOBudgetProcessor */*/runtime/plugins/otelcol/trace.go | wc -l) workspaces"
echo "issuer files: $(ls */*/runtime/plugins/otelcol/slobudget_processor.go | wc -l)"
echo "requestBytes in wrk2api handler: $(grep -c requestBytes wrk2api_service_pb_es_ctr/wrk2api_service_pb_es_proc/wrk2api_service_pb_es_proc/http/Wrk2APIService_HTTPServer.go)"
SVCS=$(ls -d *_service_pb_es_ctr)
echo "== $(date '+%T') building $(echo "$SVCS" | wc -w) images, tag $TAG, 5 in parallel"
build_one() {
    svc=$1; tag=$2
    img=10.10.1.1:30000/$(echo "$svc" | tr _ -):$tag
    if docker build -q -t "$img" "$svc" > "$HOME/bp-patch/build-$svc.log" 2>&1 && docker push -q "$img" >> "$HOME/bp-patch/build-$svc.log" 2>&1; then
        echo "ok   $img"
    else
        echo "FAIL $svc (see ~/bp-patch/build-$svc.log)"; tail -5 "$HOME/bp-patch/build-$svc.log"
    fi
}
export -f build_one
echo "$SVCS" | xargs -P 5 -I{} bash -c 'build_one {} '"$TAG"
echo "$TAG" > ~/bp-patch/IMAGE_TAG
echo "== $(date '+%T') registry tags for wrk2api:"
curl -s http://10.10.1.1:30000/v2/wrk2api-service-pb-es-ctr/tags/list
echo
echo "== done $TAG"
