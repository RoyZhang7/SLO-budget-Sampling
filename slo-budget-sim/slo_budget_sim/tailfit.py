"""Own-time distributions beyond the lognormal, and how to fit them to real
server-span durations.

The workload JSON gives every service a lognormal fit (`median_ms`, `sigma`).
A service entry may carry a `dist` object that the generator uses instead:

    {"type": "lognormal", "median_ms": m, "sigma": s}
    {"type": "loglogistic", "scale_ms": a, "shape": b}
        X = a (U / (1 - U))^(1/b); log X is logistic with location log a and
        scale 1 / b: a lognormal-like body with a power-law tail of index b
    {"type": "lognormal_pareto", "median_ms": m, "sigma": s, "tail_q": q, "tail_alpha": a}
        a lognormal body; a draw above the body's q-quantile x_q is replaced by
        a Pareto draw x_q (1 - U)^(-1/a), so the tail mass is exactly 1 - q and
        the tail decays as a power law of index a

Fitting (`fit_candidates`): the body is fitted on the whole sample, the Pareto
index by maximum likelihood on the top 1 - q; the family is chosen by held-out
fit on the top 5 %: the samples are split in two halves, each candidate is
fitted on one and scored on the other half's top 5 % by the mean conditional
log-likelihood (density given the sample is above the half's P95) and by the
quantile error (mean |log Q_model(p) / Q_empirical(p)| over p from 0.95 to
0.999).

    python -m slo_budget_sim.tailfit results/remote/server_durations_us.json \
        --service usertimeline --out slo_budget_sim/workload_dsbsn_heavytail.json

writes the fitted workload with the `dist` entry added to the service (the
lognormal fit stays in `median_ms` / `sigma`) and prints the fit report.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import NormalDist

import numpy as np

_N = NormalDist()
_norm_cdf = np.vectorize(_N.cdf, otypes=[float])
_norm_inv = np.vectorize(_N.inv_cdf, otypes=[float])

DIST_TYPES = ("lognormal", "loglogistic", "lognormal_pareto")


def lognormal_spec(median_ms: float, sigma: float) -> dict:
    return {"type": "lognormal", "median_ms": float(median_ms), "sigma": float(sigma)}


def _check(spec: dict) -> str:
    t = spec.get("type", "lognormal")
    if t not in DIST_TYPES:
        raise ValueError(f"unknown dist type {t}; choose from {DIST_TYPES}")
    return t


# ---------------------------------------------------------------------------
# distribution functions (x in ms)
# ---------------------------------------------------------------------------
def splice_point_ms(spec: dict) -> float:
    """x_q of a lognormal_pareto spec: the body's tail_q quantile."""
    return spec["median_ms"] * math.exp(spec["sigma"] * _N.inv_cdf(spec["tail_q"]))


def quantile(spec: dict, p) -> np.ndarray:
    p = np.asarray(p, dtype=float)
    t = _check(spec)
    if t == "lognormal":
        return spec["median_ms"] * np.exp(spec["sigma"] * _norm_inv(p))
    if t == "loglogistic":
        return spec["scale_ms"] * (p / (1.0 - p)) ** (1.0 / spec["shape"])
    q, a = spec["tail_q"], spec["tail_alpha"]
    body = spec["median_ms"] * np.exp(spec["sigma"] * _norm_inv(np.minimum(p, q)))
    xq = splice_point_ms(spec)
    tail = xq * ((1.0 - p) / (1.0 - q)) ** (-1.0 / a)
    return np.where(p <= q, body, tail)


def cdf(spec: dict, x) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    t = _check(spec)
    if t == "lognormal":
        return _norm_cdf(np.log(x / spec["median_ms"]) / spec["sigma"])
    if t == "loglogistic":
        return 1.0 / (1.0 + (x / spec["scale_ms"]) ** (-spec["shape"]))
    q, a = spec["tail_q"], spec["tail_alpha"]
    xq = splice_point_ms(spec)
    body = _norm_cdf(np.log(x / spec["median_ms"]) / spec["sigma"])
    tail = q + (1.0 - q) * (1.0 - (xq / np.maximum(x, xq)) ** a)
    return np.where(x <= xq, body, tail)


def logpdf(spec: dict, x) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    t = _check(spec)
    if t == "lognormal":
        s = spec["sigma"]
        z = np.log(x / spec["median_ms"]) / s
        return -np.log(x) - math.log(s) - 0.5 * math.log(2 * math.pi) - 0.5 * z * z
    if t == "loglogistic":
        b = spec["shape"]
        z = b * np.log(x / spec["scale_ms"])
        return math.log(b) - np.log(x) + z - 2.0 * np.logaddexp(0.0, z)      # logistic density of log x, divided by x
    q, a = spec["tail_q"], spec["tail_alpha"]
    xq = splice_point_ms(spec)
    body = logpdf(lognormal_spec(spec["median_ms"], spec["sigma"]), x)
    tail = math.log(1.0 - q) + math.log(a) + a * math.log(xq) - (a + 1.0) * np.log(x)
    return np.where(x <= xq, body, tail)


def draw(spec: dict, rng: np.random.Generator, n: int) -> np.ndarray:
    """n draws in ms.  The lognormal path is `median * exp(sigma * standard_normal)`,
    the generator's and the late-everywhere replica's arithmetic."""
    t = _check(spec)
    if t == "lognormal":
        return spec["median_ms"] * np.exp(spec["sigma"] * rng.standard_normal(n))
    if t == "loglogistic":
        u = rng.random(n)
        return spec["scale_ms"] * (u / (1.0 - u)) ** (1.0 / spec["shape"])
    x = spec["median_ms"] * np.exp(spec["sigma"] * rng.standard_normal(n))
    xq = splice_point_ms(spec)
    tail = x > xq
    if tail.any():
        x[tail] = xq * (1.0 - rng.random(int(tail.sum()))) ** (-1.0 / spec["tail_alpha"])
    return x


def drawer(spec: dict, z_next, u_next):
    """Per-span draw (seconds) for the generator from its pools of standard
    normals and uniforms.  Lognormal: `median * exp(sigma * z)` exactly as
    before, so the default workload's corpora do not change; the other types
    take their uniforms from `u_next`."""
    t = _check(spec)
    if t == "lognormal":
        med, sig = spec["median_ms"] / 1000.0, spec["sigma"]
        return lambda: med * math.exp(sig * z_next())
    if t == "loglogistic":
        a, inv_b = spec["scale_ms"] / 1000.0, 1.0 / spec["shape"]

        def ll():
            u = u_next()
            return a * (u / (1.0 - u)) ** inv_b
        return ll
    med, sig = spec["median_ms"] / 1000.0, spec["sigma"]
    xq, inv_a = splice_point_ms(spec) / 1000.0, -1.0 / spec["tail_alpha"]

    def lp():
        x = med * math.exp(sig * z_next())
        if x > xq:
            return xq * (1.0 - u_next()) ** inv_a
        return x
    return lp


# ---------------------------------------------------------------------------
# fitting
# ---------------------------------------------------------------------------
def fit_lognormal(x_ms: np.ndarray) -> dict:
    lv = np.log(x_ms)
    return lognormal_spec(math.exp(float(lv.mean())), float(lv.std(ddof=1)))


def fit_loglogistic(x_ms: np.ndarray, iters: int = 200) -> dict:
    """Maximum likelihood of the logistic distribution of log x (location mu,
    scale s) by damped Newton on the score equations mean(2F - 1) = 0 and
    mean(z (2F - 1)) = 1."""
    y = np.log(x_ms)
    mu, s = float(np.median(y)), max(1e-6, float(y.std(ddof=1)) * math.sqrt(3.0) / math.pi)

    def score(mu, s):
        z = (y - mu) / s
        g = 2.0 / (1.0 + np.exp(-z)) - 1.0
        return np.array([g.mean(), (z * g).mean() - 1.0])

    for _ in range(iters):
        f = score(mu, s)
        if np.abs(f).max() < 1e-10:
            break
        h = 1e-6
        J = np.column_stack([(score(mu + h, s) - f) / h, (score(mu, s + h) - f) / h])
        try:
            step = np.linalg.solve(J, -f)
        except np.linalg.LinAlgError:
            break
        lam = 1.0
        while lam > 1e-4:
            mu2, s2 = mu + lam * step[0], s + lam * step[1]
            if s2 > 0 and np.abs(score(mu2, s2)).sum() < np.abs(f).sum():
                mu, s = mu2, s2
                break
            lam *= 0.5
        else:
            break
    return {"type": "loglogistic", "scale_ms": float(math.exp(mu)), "shape": float(1.0 / s)}


def fit_lognormal_pareto(x_ms: np.ndarray, q: float = 0.95) -> dict:
    """Body: the empirical median, with sigma chosen so that the body's
    q-quantile is the empirical q-quantile (the splice is continuous in the
    CDF); tail: Pareto index by maximum likelihood on the samples above x_q,
    alpha = n_tail / sum(log(x_i / x_q))."""
    med = float(np.median(x_ms))
    xq = float(np.quantile(x_ms, q))
    sigma = math.log(xq / med) / _N.inv_cdf(q)
    tail = x_ms[x_ms > xq]
    alpha = len(tail) / float(np.log(tail / xq).sum()) if len(tail) else 10.0
    return {"type": "lognormal_pareto", "median_ms": med, "sigma": float(sigma), "tail_q": float(q),
            "tail_alpha": float(alpha)}


TAIL_PS = (0.95, 0.96, 0.97, 0.98, 0.99, 0.995, 0.999)


def tail_scores(spec: dict, held_out_ms: np.ndarray, top: float = 0.95) -> dict:
    """Held-out fit of the top (1 - top) of `held_out_ms`: mean conditional
    log-likelihood of those samples given they exceed the held-out `top`
    quantile, and the mean absolute log quantile error over TAIL_PS."""
    x95 = float(np.quantile(held_out_ms, top))
    tail = held_out_ms[held_out_ms > x95]
    surv = max(1e-300, 1.0 - float(cdf(spec, x95)))
    cll = float((logpdf(spec, tail) - math.log(surv)).mean()) if len(tail) else math.nan
    qm = quantile(spec, TAIL_PS)
    qe = np.quantile(held_out_ms, TAIL_PS)
    return {"tail_cll": cll, "qq_err": float(np.abs(np.log(qm / qe)).mean()), "n_tail": int(len(tail))}


def fit_candidates(x_ms: np.ndarray, splice_qs=(0.90, 0.925, 0.95), seed: int = 1) -> dict:
    """Fit the lognormal, the log-logistic and the spliced lognormal + Pareto
    (one per splice quantile) on one random half, score each on the other
    half's top 5 %, then refit the winner (and every candidate) on the full
    sample.  Returns {"chosen": name, "candidates": {name: {"spec", "held_out"}}}."""
    rng = np.random.default_rng(seed)
    idx = rng.permutation(len(x_ms))
    a, b = x_ms[idx[: len(idx) // 2]], x_ms[idx[len(idx) // 2:]]
    fits = {"lognormal": fit_lognormal, "loglogistic": fit_loglogistic}
    for q in splice_qs:
        fits[f"lognormal_pareto@{q}"] = (lambda q=q: (lambda x: fit_lognormal_pareto(x, q)))()
    out = {}
    for name, fn in fits.items():
        sa, sb = tail_scores(fn(a), b), tail_scores(fn(b), a)
        held = {k: 0.5 * (sa[k] + sb[k]) for k in ("tail_cll", "qq_err")}
        held["n_tail"] = sa["n_tail"] + sb["n_tail"]
        out[name] = {"spec": fn(x_ms), "held_out": held}
    chosen = max(out, key=lambda n: out[n]["held_out"]["tail_cll"])
    return {"chosen": chosen, "candidates": out}


def over_threshold(spec: dict, T_ms: float) -> float:
    return float(1.0 - cdf(spec, T_ms))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("samples", help="JSON {\"durations_us\": {service: [us, ...]}} (tools: extract server-span durations)")
    ap.add_argument("--service", default="usertimeline")
    ap.add_argument("--workload", default=str(Path(__file__).with_name("workload_dsbsn.json")))
    ap.add_argument("--out", default=str(Path(__file__).with_name("workload_dsbsn_heavytail.json")))
    ap.add_argument("--variant", default="heavytail")
    ap.add_argument("--family", default=None, help="force a candidate instead of the held-out choice")
    ap.add_argument("--reference-variant", default=None,
                    help="the healthy reference (SLO, oracle T) comes from this workload variant, e.g. fitted")
    ap.add_argument("--note", default="", help="appended to the workload's timings note")
    ap.add_argument("--thresholds-ms", type=float, nargs="*", default=[9.1, 9.3],
                    help="report the modeled fraction over these T (ms) against the sample")
    a = ap.parse_args(argv)
    with open(a.samples, "r", encoding="utf-8") as f:
        data = json.load(f)
    x = np.array(data["durations_us"][a.service], dtype=float) / 1000.0
    x = x[x > 0]
    res = fit_candidates(x)
    chosen = a.family or res["chosen"]
    print(f"{a.service}: {len(x)} server spans, median {np.median(x):.3f} ms, P95 {np.quantile(x, 0.95):.3f} ms, "
          f"P99 {np.quantile(x, 0.99):.3f} ms, max {x.max():.1f} ms")
    for name, c in res["candidates"].items():
        h = c["held_out"]
        qs = quantile(c["spec"], TAIL_PS)
        print(f"  {name:22s} held-out tail CLL {h['tail_cll']:8.4f}  QQ err {h['qq_err']:.4f}  "
              f"Q95/99/99.9 {qs[0]:.2f}/{qs[4]:.2f}/{qs[6]:.2f} ms  "
              + "  ".join(f"P(>{T})={100 * over_threshold(c['spec'], T):.2f}%" for T in a.thresholds_ms)
              + f"  spec {json.dumps({k: (round(v, 4) if isinstance(v, float) else v) for k, v in c['spec'].items()})}")
    print("  empirical            " + " " * 42 + f"Q95/99/99.9 {np.quantile(x, 0.95):.2f}/{np.quantile(x, 0.99):.2f}/"
          f"{np.quantile(x, 0.999):.2f} ms  " + "  ".join(f"P(>{T})={100 * float((x > T).mean()):.2f}%" for T in a.thresholds_ms))
    print(f"  chosen: {chosen}")
    with open(a.workload, "r", encoding="utf-8") as f:
        wl = json.load(f)
    spec = res["candidates"][chosen]["spec"]
    wl["services"][a.service]["dist"] = {k: (round(v, 6) if isinstance(v, float) else v) for k, v in spec.items()}
    wl["variant"] = a.variant
    if a.reference_variant:
        wl["reference_variant"] = a.reference_variant
    wl["timings"] = (wl.get("timings", "") + f"; {a.service}: {chosen} refit of {len(x)} server-span durations from "
                     f"{Path(a.samples).name} (held-out top-5% choice), lognormal kept in median_ms / sigma"
                     + (f"; {a.note}" if a.note else ""))
    Path(a.out).write_bytes((json.dumps(wl, indent=1) + "\n").encode())
    print(f"-> {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
