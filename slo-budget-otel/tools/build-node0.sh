#!/usr/bin/env bash
# Build the SLO-budget collector on node-0 with the bridges fork's ocb builder
# and push the image to the cluster registry.
#
#   COLLECTOR_SRC=~/opentelemetry-collector-contrib REGISTRY=10.10.1.1:30000 tools/build-node0.sh
#
# Also runs the module's tests and the microbenchmark (ns/span and the rate-paced
# CPU / latency table) and writes them to build/bench.txt.
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck disable=SC1090
source ~/bridges-env.sh 2>/dev/null || true
: "${COLLECTOR_SRC:=$HOME/opentelemetry-collector-contrib}"
: "${REGISTRY:=10.10.1.1:30000}"
: "${TAG:=$(date +%Y%m%d-%H%M%S)}"
export PATH=$PATH:/usr/local/go/bin CGO_ENABLED=0 GOFLAGS=-mod=mod
HERE=$(pwd)
mkdir -p build
echo "== go test"
go test -count=1 ./... 2>&1 | tail -8
if [ "${SKIP_BENCH:-0}" != 1 ]; then   # SKIP_BENCH=1 while cluster runs are in progress (node-0 drives wrk2)
    echo "== benchmark (ns/span)"
    go test -run XXX -bench . -benchmem ./processor/slobudgetprocessor/ 2>&1 | grep -E "Benchmark|ok" | tee build/bench.txt
    echo "== rate-paced (1k, 10k spans/s)"
    SLOBUDGET_BENCH=1 go test -count=1 -run TestRatePaced -v ./processor/slobudgetprocessor/ 2>&1 | grep -E "^RATE" | tee -a build/bench.txt   # RATE-HOLD: server_hold footprint
fi
echo "== ocb"
sed -e "s#__SLOBUDGET_OTEL_DIR__#$HERE#g" -e "s#__COLLECTOR_SRC__#$COLLECTOR_SRC#g" builder/manifest.yaml > build/manifest.yaml
"$COLLECTOR_SRC/.tools/builder" --config build/manifest.yaml
ls -la build/otelcol-slobudget/otelcol-slobudget
build/otelcol-slobudget/otelcol-slobudget components | grep -E "slobudget|priority|configdiscovery|probabilistic|otlp|batch" | head -20
echo "== image"
cp builder/Dockerfile build/otelcol-slobudget/Dockerfile
docker build -q -t "$REGISTRY/otelcol-slobudget:$TAG" build/otelcol-slobudget
docker push -q "$REGISTRY/otelcol-slobudget:$TAG"
echo "$TAG" > build/IMAGE_TAG
echo "pushed $REGISTRY/otelcol-slobudget:$TAG"
