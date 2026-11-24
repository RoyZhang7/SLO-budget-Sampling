# Image of the SLO-budget collector (same shape as the fork's cmd/otelcontribcol/Dockerfile):
# the static ocb binary on scratch, the config mounted at /etc/otelcol/config.yaml; and
# arrivald, the loader of the eBPF arrival program (its own DaemonSet runs it, render.py --arrival).
FROM alpine:3.20 AS prep
RUN apk --update add ca-certificates

FROM scratch
COPY --from=prep /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY otelcol-slobudget /otelcol-slobudget
COPY arrivald /arrivald
EXPOSE 4317 4318 8080 8081 7777/udp
ENTRYPOINT ["/otelcol-slobudget"]
CMD ["--config", "/etc/otelcol/config.yaml"]
