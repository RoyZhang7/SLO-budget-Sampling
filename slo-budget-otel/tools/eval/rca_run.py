#!/usr/bin/env python3
"""Stage 3 root-cause ranking of one run (sloeval/rca.py): RCAEval's TraceRCA and MicroRank on the
100 % shadow (the ceiling) and on the arm's kept spans with the reconstructed structure, the true
structure, and the true structure with the shadow's pre-fault spans as the normal reference.
Writes <run_dir>/rca.json; variants already there are kept unless named again.

    ~/rca-venv/bin/python3 tools/eval/rca_run.py /storage/royno7/stage3/runs/<run_id> [--variants shadow ...]

Only saturated, cascade, cold_start (single culprit) and noisy_neighbour (node-level truth) runs are
scored; other runs are skipped.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from sloeval.rca import METHODS, SCORED, VARIANTS, evaluate_run, recon_parents  # noqa: E402
from sloeval.spans import join, read_export, read_shadow  # noqa: E402


def _clean(o):
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    if isinstance(o, dict):
        return {k: _clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_clean(v) for v in o]
    if hasattr(o, "item"):
        return _clean(o.item())
    return o


def find(rd: str, *names: str) -> str | None:
    return next((os.path.join(rd, n) for n in names if os.path.exists(os.path.join(rd, n))), None)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run_dir")
    ap.add_argument("--variants", nargs="+", default=list(VARIANTS), choices=VARIANTS)
    ap.add_argument("--methods", nargs="+", default=list(METHODS), choices=METHODS)
    ap.add_argument("--out", help="default: <run_dir>/rca.json")
    ap.add_argument("--skip-existing", action="store_true", help="skip variants rca.json already holds")
    a = ap.parse_args()
    rd = a.run_dir
    meta = json.load(open(os.path.join(rd, "meta.json")))
    if meta.get("scenario") not in SCORED or not meta.get("fault"):
        print(f"{meta.get('run_id')}: scenario {meta.get('scenario')} is not scored")
        return 0
    out = a.out or os.path.join(rd, "rca.json")
    if a.skip_existing and os.path.exists(out):
        have = json.load(open(out)).get("variants") or {}
        a.variants = [v for v in a.variants if v not in have]
        if not a.variants:
            print(f"{meta.get('run_id')}: done")
            return 0
    t_start = time.time()
    shadow = find(rd, "shadow.jsonl.zst", "shadow.jsonl.gz", "shadow.jsonl")
    if shadow is None:
        raise SystemExit(f"{rd}: no shadow")
    t = read_shadow(shadow)
    kidx = None
    variants = list(a.variants)
    if any(v != "shadow" for v in variants):
        exp = find(rd, "export.json", "export.json.zst")
        if exp is None:
            raise SystemExit(f"{rd}: no export")
        kidx = join(t, read_export(exp))
    else:
        import numpy as np
        kidx = np.zeros(0, dtype=np.int64)
    recon = None
    if "recon" in variants:
        rp = find(rd, "export.recon.json", "export.recon.json.zst")
        recon = recon_parents(rp) if rp else None
    res = evaluate_run(meta, t, kidx, recon, variants=variants, methods=a.methods)
    if os.path.exists(out):
        old = json.load(open(out))
        old.setdefault("variants", {}).update(res["variants"])
        res["variants"] = old["variants"]
    res["seconds"] = round(time.time() - t_start, 1)
    tmp = out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(_clean(res), f, indent=1, sort_keys=True)
    os.replace(tmp, out)
    brief = {v: {m: (r.get(m) or {}).get("rank") for m in a.methods} for v, r in res["variants"].items() if v in variants}
    print(json.dumps({"run_id": res["run_id"], "truth": res["truth"], "ranks": brief, "seconds": res["seconds"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
