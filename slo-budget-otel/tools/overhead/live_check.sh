#!/usr/bin/env bash
# The first live check of the arrival program (node-0; after the campaign's queue is empty, before any
# measurement run): the program on one pod during a 30-s warm-up load, a capture at the same interface at the
# same time, and the comparison of what the program stored with the capture (live_check.py).
#
#   tools/overhead/live_check.sh [service]      default post-storage (the largest requests: most ids split
#                                               between packets, most packets of several fragments)
#
# The loader is the one in the collector image (IMAGE_TAG), run on the pod's node as root with -pid (that pod
# only) and a pin directory of its own; it takes the program off again at the end.  Nothing else on the cluster
# changes: the stock collector stays, no DaemonSet.  Attaching and detaching stop the pod's transmit queue for a
# moment (clsact on 5.15), before and after the load.
# Writes $STAGE3/overhead/live-<service>/ (the capture stays there, on node-0); exits 0 only when all holds.
set -u
cd "$(dirname "$0")/../.."
MOD=$(pwd)
# shellcheck disable=SC1090
source ~/bridges-env.sh 2>/dev/null || true
SVC=${1:-post-storage}
: "${STAGE3:=/storage/royno7/stage3}"; : "${WARMUP:=30}"; : "${RPS:=100}"
: "${URL:=${WRK2API_URL:-http://10.10.1.1:22903}}"
: "${LUA:=$HOME/blueprint-docc-mod/examples/dsb_sn/scripts/compose-post.lua}"
: "${IMAGE_TAG:?the collector image with arrivald in it}"
IMG=10.10.1.1:30000/otelcol-slobudget:$IMAGE_TAG
PIN=/sys/fs/bpf/slobudget-live
BIN=/tmp/slobudget-arrivald
RD=$STAGE3/overhead/live-$SVC
mkdir -p "$RD"
log() { echo "$(date '+%F %T') $*" | tee -a "$RD/run.log"; }
N() { ssh -n -o BatchMode=yes "$NODE" "$@"; }       # on the pod's node
die() { log "FAILED: $*"; exit 1; }
off() {     # the loader, with its own clean-up
    [ -n "${NODE:-}" ] || return 0
    N "sudo -n pkill -TERM -f '^$BIN -pid'" 2>/dev/null
    for _ in $(seq 1 20); do N "pgrep -f '^$BIN -pid'" > /dev/null 2>&1 || break; sleep 0.5; done
    N "sudo -n pkill -KILL -f '^$BIN -pid'" 2>/dev/null
}
bye() {     # however the script ends: what the loader left behind (after after.txt and pins.txt were taken), and the binary
    off
    [ -z "${NODE:-}" ] || N "[ ! -x $BIN ] || sudo -n $BIN -clean -pin $PIN; rm -f $BIN" >> "$RD/run.log" 2>&1
    [ -z "${CAP:-}" ] || wait "$CAP" 2>/dev/null
}
trap bye EXIT
trap 'exit 1' HUP INT TERM
pgrep -x wrk > /dev/null && { echo "a load is running"; exit 1; }
pgrep -f '^([^ ]*/)?bash [^ ]*tools/(campaign/run_one|overhead/(run_overhead|overhead_queue|knee))\.sh' > /dev/null && { echo "a run is in progress"; exit 1; }
[ -z "$(kubectl get ds,pods -l app=slobudget-arrival --no-headers 2>/dev/null)" ] || { echo "arrivald is there (an E run)"; exit 1; }
if [ "${FORCE:-0}" != 1 ] && [ -f "$STAGE3/queue.txt" ] && [ -n "$(grep -v '^#' "$STAGE3/queue.txt" | while read -r a s v e rest; do
        [ -n "$a" ] || continue
        id=$(echo "$rest" | tr ' ' '\n' | sed -n 's/^RUN_ID=//p'); [ -n "$id" ] || id="v$(python3 -c "print(round($v*100))")-$s-$a-s$e"
        [ -f "$STAGE3/runs/$id/collected" ] || { echo pending; break; }
    done)" ]; then
    echo "the campaign's queue has jobs left (FORCE=1 to check anyway)"; exit 1
fi

# the loader of the image
C=$(docker create "$IMG") || { echo "no image $IMG"; exit 1; }
docker cp -q "$C:/arrivald" "$RD/arrivald"; GOT=$?
docker rm "$C" > /dev/null
[ $GOT = 0 ] || { echo "no arrivald in $IMG"; exit 1; }
rm -f "$RD"/*.txt "$RD"/*.log "$RD"/dump*.csv "$RD"/capture.pcap "$RD"/live-check.*     # an earlier check's, only now
log "live check: $SVC, $("$RD/arrivald" -version), image $IMG"
# the pod
read -r POD NODE IP CTR < <(kubectl get pods -l "io.kompose.service=$SVC-service-pb-es-ctr" -o jsonpath='{range .items[0]}{.metadata.name} {.spec.nodeName} {.status.podIP} {.status.containerStatuses[0].containerID}{end}')
[ -n "${CTR:-}" ] || die "no pod of $SVC"
pod() { kubectl get pod "$POD" -o jsonpath='{.metadata.uid} {.status.containerStatuses[0].containerID} restarts={.status.containerStatuses[0].restartCount}{"\n"}'; }
PID=$(N "docker inspect -f '{{.State.Pid}}' ${CTR#docker://}")
[ "${PID:-0}" -gt 0 ] || die "no process of $POD"
log "pod $POD ($IP) on $NODE, process $PID"
pod > "$RD/pod.txt"
iface() { N "sudo -n nsenter -t $PID -n sh -c 'tc qdisc show dev eth0; echo ingress:; tc filter show dev eth0 ingress; echo egress:; tc filter show dev eth0 egress'" 2>&1; }
scp -q -o BatchMode=yes "$RD/arrivald" "$NODE:$BIN" || die "copy to $NODE"
N "pgrep -f '^$BIN -pid'" > /dev/null && die "a loader of an earlier check is still there"
iface > "$RD/before.txt"

# the capture at the pod's eth0 (its taps run right before the program), then the program
ssh -o BatchMode=yes "$NODE" "sudo -n nsenter -t $PID -n timeout -s INT $((WARMUP + 25)) tcpdump -i eth0 -Q in --time-stamp-precision=nano -n -s 0 -B 65536 -w - 'tcp dst port 12345'" \
    > "$RD/capture.pcap" 2> "$RD/capture.err" < /dev/null &
CAP=$!
for _ in $(seq 1 20); do grep -q "listening on" "$RD/capture.err" && break; sleep 0.25; done
grep -q "listening on" "$RD/capture.err" || die "no capture: $(tail -2 "$RD/capture.err" | tr '\n' ' ')"
ssh -o BatchMode=yes "$NODE" "sudo -n timeout -s TERM $((WARMUP + 300)) $BIN -pid $PID -stats -print 2s -pin $PIN" > "$RD/arrivald.log" 2>&1 < /dev/null &
for _ in $(seq 1 40); do grep -q '"attached","interfaces":1' "$RD/arrivald.log" && break; sleep 0.25; done
grep -q '"attached","interfaces":1' "$RD/arrivald.log" || die "not attached: $(tail -3 "$RD/arrivald.log" | tr '\n' ' ')"
iface > "$RD/during.txt"
log "attached; load ${WARMUP}s at $RPS req/s"
wrk -t2 -c10 -d"${WARMUP}"s -L -s "$LUA" "$URL" -R"$RPS" > "$RD/wrk.log" 2>&1
sleep 1
N "sudo -n $BIN -dump -pin $PIN" > "$RD/dump.csv" 2> "$RD/dump.err" || die "dump: $(cat "$RD/dump.err")"
# the same with the agent's rights: a container, not privileged, CAP_BPF, the bpf filesystem read-only, no seccomp profile (cri-dockerd's default)
N "docker run --rm --cap-add BPF --security-opt seccomp=unconfined -v /sys/fs/bpf:/sys/fs/bpf:ro --entrypoint /arrivald $IMG -dump -pin $PIN" \
    > "$RD/dump-container.csv" 2> "$RD/dump-container.err" || log "dump in a container: $(tail -2 "$RD/dump-container.err" | tr '\n' ' ')"
off
wait "$CAP"
CAP=""
iface > "$RD/after.txt"
N "sudo -n ls $PIN 2>/dev/null" > "$RD/pins.txt"
pod >> "$RD/pod.txt"
log "load: $(grep -E 'requests in|Non-2xx|Requests/sec' "$RD/wrk.log" | tr -s ' ' | tr '\n' ' '); capture: $(grep -E 'captured|dropped' "$RD/capture.err" | tr '\n' ' ')"
log "loader: $(grep -c '"counters"' "$RD/arrivald.log") counter lines, $(grep '"detached"' "$RD/arrivald.log" | tail -1 | cut -c1-120)"
N "pgrep -fa '^$BIN'; docker ps -a --filter ancestor=$IMG --format '{{.Names}} {{.Status}}' | grep -v k8s_" > "$RD/left.txt" 2>&1
grep -E "dropped by kernel" "$RD/capture.err" | grep -q "^0 " || die "the capture lost packets: $(grep dropped "$RD/capture.err" | tr '\n' ' ')"
python3 "$MOD/tools/overhead/live_check.py" "$RD" | tee -a "$RD/run.log"
exit "${PIPESTATUS[0]}"
