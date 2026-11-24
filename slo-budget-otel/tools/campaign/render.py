#!/usr/bin/env python3
"""Render the Kubernetes manifests of one Stage 3 run: the central collector (node-0), the
agent and tee ConfigMaps and the agent DaemonSet (tee + agent containers, or the agent alone
with --no-tee), for one arm and volume.

  python3 tools/campaign/render.py --arm chosen --volume 0.12 --seed 1 --image IMG [--no-tee] > m.yaml
  python3 tools/campaign/render.py --arm tail --volume 0.12 --image IMG --print central   # one collector config
  ... --arrival /sys/fs/bpf/slobudget      the eBPF arrival stamp: arrivald on every node, and the tee (before its fork, so that
                                           the 100 % shadow carries the stamp too) or, without a tee, the agent reads its table

Arms: chosen, threshold, keep-parent (the SLO-budget agent and central), head (stock
probabilistic_sampler at the agents, hash of the trace id), tail (stock tail_sampling at the
central), reservoir (top-K per host at the agents), canopy (per-service rate targets at the
central).  Every central ends in the cap gate (cap 1.25 V*, first come first served, stamps
slobudget.admit_ns), as the simulator's collector does for every arm (--no-capgate leaves it
out, for pilots on an image without it).  The tee receives the SDK's spans on :4317 and
copies them to the agent (localhost:4319) and to the shadow collector on node-0 (100 %,
separate storage).
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys

SPANS_PER_PERIOD = 11500          # 100 req/s x 23 spans x 5 s, the deployment's measured volume
SLO_MS = 23.4
SERVICE_PORT = 12345              # the services' gRPC port
ARRIVAL_SLOTS = 1 << 20           # 16 MiB a node; how many are needed: README, arrival stamp
SLOBUDGET_ARMS = ("chosen", "threshold", "keep-parent")
ARMS = SLOBUDGET_ARMS + ("head", "tail", "reservoir", "canopy")


def block(text: str, n: int) -> str:
    """Indent every non-empty line of text by n spaces."""
    pad = " " * n
    return "".join((pad + ln) if ln.strip() else ln for ln in text.splitlines(keepends=True))


PRIORITY = """priority:
  check_interval: 100ms
  soft_percentage: 50
  hard_percentage: 70
  cp_safety_factor: 1
  force_gc: true
  gc_soft_interval: 1s
  gc_ultrasoft_interval: 0s
"""


def telemetry(port: int, level: str = "detailed") -> str:
    return f"""telemetry:
  logs:
    level: INFO
  metrics:
    level: {level}
    readers:
      - pull:
          exporter:
            prometheus:
              host: 0.0.0.0
              port: {port}
"""


def sampler_overrides(arm: str) -> str:
    lines = ["root_policy: on_demand"]
    if arm == "threshold":
        lines += ["unbounded: true", "n_need: .inf", "n_need_mode: fixed"]
    if arm == "keep-parent":
        lines += ["keep_parent: true"]
    # Stage 2d: the trigger (own | duration) and its margin from the job's environment;
    # unset = the image defaults (own, own_margin 0.1)
    if os.environ.get("OUTLIER_TRIGGER"):
        lines += [f"outlier_trigger: {os.environ['OUTLIER_TRIGGER']}"]
    if os.environ.get("OWN_MARGIN"):
        lines += [f"own_margin: {float(os.environ['OWN_MARGIN']):g}"]
    # handler (default) | within_service (the own time counts from the request's arrival; needs --arrival)
    if os.environ.get("OWN_MEASURE"):
        lines += [f"own_measure: {os.environ['OWN_MEASURE']}"]
    return "".join(ln + "\n" for ln in lines)


def agent_config(arm: str, frac: float, v_star: float, seed: int, port: int, shares: dict, arrival: str = "") -> str:
    procs, pipe, extra_recv, extra_pipe = "", [], "", ""
    if arm in SLOBUDGET_ARMS:
        sb = """slobudget:
  period: 5s
  central: http://slobudget-central:8081
  host: ${env:NODE_NAME}
  default_route: compose_post
  root_hold: 2s
  notice_listen: ":7777"
  notice_port: 7777
  notice_peers: ["slobudget-agents"]
  client_spans: plan
  request_ttl: 10s
  stats_log: true
  budget_source: propagated
  sdk_listen: ":8082"
"""
        # Stage 2d: hold each non-root server span until its CLIENT children are in (own time)
        sb += f"  server_hold: {os.environ.get('SERVER_HOLD', '200ms')}\n"
        if arm == "keep-parent":
            sb += "  keep_parent_anchor: true\n"
        if arrival:
            sb += f"  arrival_table: {arrival}\n"
        sb += "  sampler:\n" + block(sampler_overrides(arm), 4)
        procs += sb
        pipe.append("slobudget")
    else:
        # the SDK's budget issuer polls :8082 in every arm, so that the app does the same
        # work and the spans carry the same slo.* attributes; nothing flows through here
        extra_recv = """otlp/sdkdummy:
  protocols:
    grpc:
      endpoint: 127.0.0.1:4399
"""
        procs += """slobudget/sdk:
  period: 5s
  central: ""
  standalone_v_star: 1000
  host: ${env:NODE_NAME}
  notice_listen: ""
  sdk_listen: ":8082"
  stats_log: false
"""
        extra_pipe = """traces/sdk:
  receivers: [otlp/sdkdummy]
  processors: [slobudget/sdk]
  exporters: [debug]
"""
        if arm == "head":
            procs += f"""probabilistic_sampler:
  sampling_percentage: {100 * frac:g}
  hash_seed: 22
  mode: hash_seed
"""
            pipe.append("probabilistic_sampler")
        elif arm == "reservoir":
            sh = "".join(f"    {n}: {v:.4f}\n" for n, v in sorted(shares.items()))
            procs += f"""reservoir:
  period: 5s
  v_star: {v_star:g}
  host: ${{env:NODE_NAME}}
  shares:
{sh}  ttable_file: /etc/slobudget/ttable.json
  seed: {seed}
"""
            pipe.append("reservoir")
    procs += PRIORITY
    pipe.append("priority")
    return f"""receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:{port}
        max_recv_msg_size_mib: 32
  configdiscovery:
    endpoint: ":8080"
    config_map:
      cpd: 2
{block(extra_recv, 2)}processors:
{block(procs, 2)}exporters:
  otlp:
    endpoint: slobudget-central:4317
    tls:
      insecure: true
    sending_queue:
      enabled: true
      num_consumers: 10
      queue_size: 1000
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 0
  debug:
    verbosity: basic
service:
{block(telemetry(8888), 2)}  pipelines:
    traces:
      receivers: [otlp]
      processors: [{', '.join(pipe)}]
      exporters: [otlp]
{block(extra_pipe, 4)}    logs/configdiscovery:
      receivers: [configdiscovery]
      processors: []
      exporters: [debug]
"""


def central_config(arm: str, frac: float, v_star: float, seed: int, tail_wait: str, capgate: bool) -> str:
    procs, pipe = "", []
    if arm in SLOBUDGET_ARMS:
        procs += f"""slobudgetcentral:
  period: 5s
  grace: 400ms
  listen: ":8081"
  v_star: {v_star:g}
  v_star_frac: {frac:g}
  cap_factor: 1.25
  refill_input: demand
  slo_ms:
    compose_post: {SLO_MS}
  default_route: compose_post
  count_kinds: all
  call_graph_file: /etc/slobudget/callgraph.json
  stats_log: true
  sampler:
{block(sampler_overrides(arm), 4)}"""
        pipe.append("slobudgetcentral")
    elif arm == "tail":
        procs += f"""tail_sampling:
  decision_wait: {tail_wait}
  num_traces: 100000
  expected_new_traces_per_sec: 600
  decision_cache:
    sampled_cache_size: 200000
    non_sampled_cache_size: 200000
  policies:
    - name: errors
      type: status_code
      status_code:
        status_codes: [ERROR]
    - name: slow
      type: ottl_condition
      ottl_condition:
        error_mode: ignore
        span:
          - 'IsRootSpan() and (end_time_unix_nano - start_time_unix_nano) > {int(SLO_MS * 1e6)}'
    - name: fill
      type: probabilistic
      probabilistic:
        sampling_percentage: {100 * frac:g}
"""
        pipe.append("tail_sampling")
    elif arm == "canopy":
        procs += f"""canopy:
  period: 5s
  v_star: {v_star:g}
  initial_rate: 0.15
  alpha: 0.5
  seed: {seed}
"""
        pipe.append("canopy")
    if capgate:
        procs += f"""capgate:
  period: 5s
  phase: 400ms
  v_star: {v_star:g}
  cap_factor: 1.25
"""
        pipe.append("capgate")
    procs += """batch:
  send_batch_size: 512
  timeout: 200ms
"""
    pipe.append("batch")
    return f"""receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
        max_recv_msg_size_mib: 32
processors:
{block(procs, 2)}exporters:
  otlp:
    endpoint: jaeger-pb-es-ctr:4317
    tls:
      insecure: true
    sending_queue:
      enabled: true
      num_consumers: 10
      queue_size: 5000
    retry_on_failure:
      enabled: true
      initial_interval: 1s
      max_interval: 10s
      max_elapsed_time: 0
service:
{block(telemetry(8888), 2)}  pipelines:
    traces:
      receivers: [otlp]
      processors: [{', '.join(pipe)}]
      exporters: [otlp]
"""


def tee_config(shadow: str, arrival: str = "") -> str:
    if arrival:         # one pipeline: every span is stamped once, and the agent and the shadow get the same stamp
        procs = f"""processors:
  slobudgetarrival:
    arrival_table: {arrival}
"""
        pipes = """    traces:
      receivers: [otlp]
      processors: [slobudgetarrival]
      exporters: [otlp/agent, otlp/shadow]
"""
    else:
        procs = ""
        pipes = """    traces/agent:
      receivers: [otlp]
      exporters: [otlp/agent]
    traces/shadow:
      receivers: [otlp]
      exporters: [otlp/shadow]
"""
    return f"""receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
        max_recv_msg_size_mib: 32
{procs}exporters:
  otlp/agent:
    endpoint: localhost:4319
    tls:
      insecure: true
    compression: none
    sending_queue:
      enabled: true
      num_consumers: 4
      queue_size: 5000
    retry_on_failure:
      enabled: true
      initial_interval: 100ms
      max_interval: 1s
      max_elapsed_time: 30s
  otlp/shadow:
    endpoint: {shadow}
    tls:
      insecure: true
    sending_queue:
      enabled: true
      num_consumers: 4
      queue_size: 20000
    retry_on_failure:
      enabled: true
      initial_interval: 1s
      max_interval: 5s
      max_elapsed_time: 120s
service:
{block(telemetry(8889, 'normal'), 2)}  pipelines:
{pipes}"""


def arrival_daemonset(image: str, pin: str, slots: int) -> str:
    """arrivald on every app node: loads the arrival program and keeps it at the service pods' interfaces."""
    return f"""apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: slobudget-arrival
  labels: {{app: slobudget-arrival}}
spec:
  selector:
    matchLabels: {{app: slobudget-arrival}}
  template:
    metadata:
      labels: {{app: slobudget-arrival}}
    spec:
      hostPID: true
      terminationGracePeriodSeconds: 20
      containers:
        - name: arrivald
          image: {image}
          imagePullPolicy: IfNotPresent
          command: ["/arrivald"]
          args: ["-port", "{SERVICE_PORT}", "-slots", "{slots}", "-pin", "{pin}", "-stats"]
          securityContext: {{privileged: true}}
          resources:          # a core: not throttled while it holds the RTNL lock (attach, detach)
            limits: {{cpu: "1", memory: 256Mi}}
            requests: {{cpu: 50m, memory: 64Mi}}
          volumeMounts:
            - {{name: bpffs, mountPath: /sys/fs/bpf}}
      volumes:
        - name: bpffs
          hostPath: {{path: /sys/fs/bpf, type: Directory}}
"""


def configmap(name: str, app: str, key: str, text: str) -> str:
    return f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: {name}
  labels: {{app: {app}}}
data:
  {key}: |
{block(text, 4)}"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--arm", required=True, choices=ARMS)
    ap.add_argument("--volume", type=float, required=True)
    ap.add_argument("--rate", type=float, default=100, help="entry rate (req/s) the target is a fraction of: V* = volume x 115 x rate spans per 5 s")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--image", required=True)
    ap.add_argument("--no-tee", action="store_true")
    ap.add_argument("--no-capgate", action="store_true")
    ap.add_argument("--shadow", default="10.10.1.1:14317")
    ap.add_argument("--tail-wait", default="5s")
    ap.add_argument("--callgraph", default=str(pathlib.Path(__file__).resolve().parents[2] / "deploy" / "callgraph.json"))
    ap.add_argument("--ttable", help="reservoir: JSON route -> service -> seconds")
    ap.add_argument("--shares", default='{"node-1": 0.47, "node-2": 0.30, "node-3": 0.23}')
    ap.add_argument("--arrival", default="", metavar="DIR", help="the eBPF arrival stamp: pin directory of arrivald's maps (SLO-budget arms)")
    ap.add_argument("--arrival-slots", type=int, default=ARRIVAL_SLOTS)
    ap.add_argument("--print", choices=["agent", "central", "tee"], help="print one collector config only")
    a = ap.parse_args()
    v_star = round(a.volume * SPANS_PER_PERIOD * (a.rate / 100))
    tee = not a.no_tee
    agent_port = 4319 if tee else 4317
    if a.arrival and a.arm not in SLOBUDGET_ARMS:
        raise SystemExit("--arrival: the agent of the SLO-budget arms reads the table")
    if os.environ.get("OWN_MEASURE") == "within_service" and a.arm in SLOBUDGET_ARMS and not a.arrival:
        raise SystemExit("OWN_MEASURE=within_service needs --arrival: without the arrivals it is the handler's own time")
    agent = agent_config(a.arm, a.volume, v_star, a.seed, agent_port, json.loads(a.shares), "" if tee else a.arrival)
    central = central_config(a.arm, a.volume, v_star, a.seed, a.tail_wait, not a.no_capgate)
    if a.print:
        sys.stdout.write({"agent": agent, "central": central, "tee": tee_config(a.shadow, a.arrival)}[a.print])
        return 0
    docs = [configmap("slobudget-central-config", "slobudget-central", "config.yaml", central),
            configmap("slobudget-callgraph", "slobudget-central", "callgraph.json", pathlib.Path(a.callgraph).read_text())]
    docs.append(f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: slobudget-central
  labels: {{app: slobudget-central}}
spec:
  replicas: 1
  selector:
    matchLabels: {{app: slobudget-central}}
  template:
    metadata:
      labels: {{app: slobudget-central}}
    spec:
      nodeSelector:
        kubernetes.io/hostname: node-0
      tolerations:
        - {{key: node-role.kubernetes.io/control-plane, operator: Exists, effect: NoSchedule}}
      containers:
        - name: otelcol
          image: {a.image}
          imagePullPolicy: IfNotPresent
          args: ["--config", "/etc/otelcol/config.yaml"]
          ports:
            - {{containerPort: 4317, name: otlp}}
            - {{containerPort: 8081, name: config}}
            - {{containerPort: 8888, name: metrics}}
          env:
            - {{name: GOMEMLIMIT, value: 3276MiB}}
          resources:
            requests: {{cpu: "2", memory: 4Gi}}
            limits: {{cpu: "2", memory: 4Gi}}
          volumeMounts:
            - {{name: config, mountPath: /etc/otelcol}}
            - {{name: callgraph, mountPath: /etc/slobudget}}
      volumes:
        - name: config
          configMap: {{name: slobudget-central-config}}
        - name: callgraph
          configMap: {{name: slobudget-callgraph}}
""")
    docs.append("""apiVersion: v1
kind: Service
metadata:
  name: slobudget-central
  labels: {app: slobudget-central}
spec:
  selector: {app: slobudget-central}
  ports:
    - {name: otlp, port: 4317, targetPort: 4317}
    - {name: config, port: 8081, targetPort: 8081}
    - {name: metrics, port: 8888, targetPort: 8888}
""")
    docs.append(configmap("slobudget-agent-config", "slobudget-agent", "config.yaml", agent))
    if a.arm == "reservoir":
        if not a.ttable:
            raise SystemExit("reservoir needs --ttable")
        docs.append(configmap("slobudget-ttable", "slobudget-agent", "ttable.json", pathlib.Path(a.ttable).read_text()))
    if tee:
        docs.append(configmap("slobudget-tee-config", "slobudget-agent", "config.yaml", tee_config(a.shadow, a.arrival)))
    docs.append("""apiVersion: v1
kind: Service
metadata:
  name: slobudget-agents
  labels: {app: slobudget-agent}
spec:
  clusterIP: None
  selector:
    io.kompose.service: otelcol-pb-es-ctr
  ports:
    - {name: notice, port: 7777, targetPort: 7777, protocol: UDP}
""")
    mounts = "            - {name: config, mountPath: /etc/otelcol}\n"
    vols = "        - name: config\n          configMap: {name: slobudget-agent-config}\n"
    if a.arm == "reservoir":
        mounts += "            - {name: ttable, mountPath: /etc/slobudget}\n"
        vols += "        - name: ttable\n          configMap: {name: slobudget-ttable}\n"
    sec = tee_sec = tee_mounts = ""
    if a.arrival:               # the container that stamps maps the pinned table: CAP_BPF opens it, reading needs nothing more
        bpffs = "            - {name: bpffs, mountPath: /sys/fs/bpf, readOnly: true}\n"
        cap = "          securityContext:\n            capabilities: {add: [BPF]}\n"
        if tee:
            tee_sec, tee_mounts = cap, bpffs
        else:
            sec = cap
            mounts += bpffs
        vols += "        - name: bpffs\n          hostPath: {path: /sys/fs/bpf, type: Directory}\n"
        docs.append(arrival_daemonset(a.image, a.arrival, a.arrival_slots))
    tee_c = ""
    if tee:
        vols += "        - name: tee-config\n          configMap: {name: slobudget-tee-config}\n"
        tee_c = f"""        - name: tee
          image: {a.image}
          imagePullPolicy: IfNotPresent
          args: ["--config", "/etc/otelcol-tee/config.yaml"]
          env:
            - {{name: GOMEMLIMIT, value: 1600MiB}}
          ports:
            - {{containerPort: 4317, protocol: TCP}}
            - {{containerPort: 8889, protocol: TCP}}
          resources:
            limits: {{cpu: "1", memory: 2Gi}}
            requests: {{cpu: "1", memory: 2Gi}}
{tee_sec}          volumeMounts:
            - {{name: tee-config, mountPath: /etc/otelcol-tee}}
{tee_mounts}"""
    docs.append(f"""apiVersion: apps/v1
kind: DaemonSet
metadata:
  labels:
    io.kompose.service: otelcol-pb-es-ctr
  name: otelcol-pb-es-ctr
  namespace: default
spec:
  selector:
    matchLabels:
      io.kompose.service: otelcol-pb-es-ctr
  template:
    metadata:
      labels:
        io.kompose.service: otelcol-pb-es-ctr
        slobudget: agent
    spec:
      containers:
{tee_c}        - name: otelcol-pb-es-ctr
          image: {a.image}
          imagePullPolicy: IfNotPresent
          args: ["--config", "/etc/otelcol/config.yaml"]
          env:
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            - name: POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - {{name: GOMEMLIMIT, value: 3276MiB}}
          ports:
            - {{containerPort: {agent_port}, protocol: TCP}}
            - {{containerPort: 8080, protocol: TCP}}
            - {{containerPort: 8082, protocol: TCP}}
            - {{containerPort: 8888, protocol: TCP}}
            - {{containerPort: 7777, protocol: UDP}}
          resources:
            limits: {{cpu: "1", memory: 4Gi}}
            requests: {{cpu: "1", memory: 4Gi}}
{sec}          volumeMounts:
{mounts}      volumes:
{vols}      hostname: otelcol-pb-es-ctr
      restartPolicy: Always
  updateStrategy:
    rollingUpdate:
      maxSurge: 0
      maxUnavailable: 3
    type: RollingUpdate
""")
    sys.stdout.write("---\n".join(d if d.endswith("\n") else d + "\n" for d in docs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
