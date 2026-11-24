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
# campaign runs start their shadow collector from build/ and read build/IMAGE_TAG: not while one runs, and not
# while the queue has jobs left (a paused queue goes on with them)
if [ "${FORCE:-0}" != 1 ]; then
    : "${STAGE3:=/storage/royno7/stage3}"
    if pgrep -f '^([^ ]*/)?bash [^ ]*tools/(campaign/run_one|overhead/run_overhead|overhead/knee)\.sh' > /dev/null; then
        echo "a run is in progress (FORCE=1 to build anyway)"; exit 1
    fi
    if [ -f "$STAGE3/queue.txt" ] && [ -n "$(grep -v '^#' "$STAGE3/queue.txt" | while read -r a s v e rest; do
            [ -n "$a" ] || continue
            id=$(echo "$rest" | tr ' ' '\n' | sed -n 's/^RUN_ID=//p'); [ -n "$id" ] || id="v$(python3 -c "print(round($v*100))")-$s-$a-s$e"
            [ -f "$STAGE3/runs/$id/collected" ] || { echo pending; break; }
        done)" ]; then
        echo "the campaign's queue has jobs left (FORCE=1 to build anyway)"; exit 1
    fi
fi
HERE=$(pwd)
# the tree is a commit's: SOURCE.json (written when the tree is synced) names it and the files that differ from it
COMMIT=$(python3 -c "import json; s = json.load(open('SOURCE.json')); print(s['commit'] + ('+%d' % len(s['dirty']) if s['dirty'] else ''))" 2>/dev/null || echo unknown)
case "$COMMIT" in
    *+*|unknown) [ "${ALLOW_DIRTY:-0}" = 1 ] || { echo "the tree is not a commit's: $COMMIT (ALLOW_DIRTY=1 to build it anyway)"; exit 1; } ;;
esac
if [ -f build/IMAGE_TAG ]; then     # what earlier runs used (outside the synced tree)
    OLD=$HOME/otelcol-builds/$(cat build/IMAGE_TAG)
    [ -d "$OLD" ] || { mkdir -p "$HOME/otelcol-builds" && cp -a build "$OLD"; }
fi
mkdir -p build
echo "== go test"
go test -count=1 ./... 2>&1 | tail -8
(cd arrivald && go test -count=1 ./... 2>&1 | tail -4)
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
echo "== arrivald"
(cd arrivald && go build -trimpath -ldflags "-s -w" -o "$HERE/build/otelcol-slobudget/arrivald" .)
echo "== image"
cp builder/Dockerfile build/otelcol-slobudget/Dockerfile
docker build -q --label "slobudget.commit=$COMMIT" -t "$REGISTRY/otelcol-slobudget:$TAG" build/otelcol-slobudget
docker push -q "$REGISTRY/otelcol-slobudget:$TAG"
echo "$TAG" > build/IMAGE_TAG
echo "pushed $REGISTRY/otelcol-slobudget:$TAG (commit $COMMIT)"
