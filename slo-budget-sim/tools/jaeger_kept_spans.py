"""Server spans of a Jaeger JSON export, per service, with the SLO-budget stamps:
[duration_us, sampling.p, slobudget.reason, start_us].  Standard library only,
so it runs on the cluster's control node next to the export:

    python3 tools/jaeger_kept_spans.py export.json kept_spans.json

Output: {"traces": n, "spans": {service: [[duration_us, p | null, reason | "", start_us], ...]}}.
An unsampled export (no stamps) gives p null and reason "" for every span.
"""
import json
import re
import sys

SERVICE_RE = re.compile(r"^(?:unknown_service:)?(?P<name>[A-Za-z0-9_-]+?)(?:_service)?(?:_[a-z]+_es)?(?:_proc)?$")


def norm(name: str) -> str:
    m = SERVICE_RE.match(name)
    return m.group("name").replace("_", "") if m else name


def main(src: str, dst: str) -> int:
    with open(src, "r", encoding="utf-8") as f:
        data = json.load(f)["data"]
    out: dict = {}
    for tr in data:
        procs = {pid: norm(p.get("serviceName", "")) for pid, p in tr.get("processes", {}).items()}
        for sp in tr.get("spans", []):
            tags = {t.get("key"): t.get("value") for t in sp.get("tags", []) or []}
            if tags.get("span.kind") != "server":
                continue
            p = tags.get("sampling.p")
            out.setdefault(procs.get(sp.get("processID"), "?"), []).append(
                [int(sp["duration"]), float(p) if p is not None else None, tags.get("slobudget.reason") or "", int(sp["startTime"])])
    with open(dst, "w", encoding="utf-8") as f:
        json.dump({"traces": len(data), "spans": out}, f)
    print("traces", len(data), {k: len(v) for k, v in out.items()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1], sys.argv[2]))
