#!/usr/bin/env python3
"""The in-service filter's proof by replay: agent(filter(all spans)) must keep exactly the spans agent(all spans) keeps, both with the plan
coin of the span id (plan_coin: hash) and every other draw a function of the span id too (coin_source: span).

    python3 tools/filter_replay.py --synthetic saturated [--seed 1] [--volume 0.12]       a generated corpus (the simulator's)
    python3 tools/filter_replay.py --run /storage/royno7/stage3/runs/v12-steady-ebpf-s1   a cluster run's 100 % export (node-0; measure13.build)
    python3 tools/filter_replay.py --corpus FILE.pkl ...                                   a corpus pickled by another tool
    ... --dump DIR                                                                         the filtered corpus's events/decisions for the Go differential test

The agent run (A) is the simulator's chosen-v2 sampler on every span (own trigger on the within-service latency, Span.wait), recording the
thresholds b and the plan rates in force after each period.  Both agent runs use coin_source = span: every draw (the request budget's
fraction, the outlier coin beyond N_need, the credit coin) is a function of the span id and the draw's number, so that two runs over
different span sets make the same decision for the same span; what the filter changes is then only what it drops.  The filter (F) sees every span in the same order with the tables ONE PERIOD
STALE (what the agent had in the period before: its poll is at most a second behind the agent, which applies the central's update at the
period's start) and the higher plan rate of its two newest tables times the headroom 1.05; it passes roots, errors, spans over b WITHOUT
the margin, spans the coin selects, and spans without an arrival (--no-arrival SHARE of them, at random); it counts what it drops per
service and stamps the count on the next span it passes, and what is still uncounted at a period's end goes to the agent as a count
record at that boundary (--no-count-records: it rides on the next passed span instead, which shows the drift that costs).  The agent run on the passed spans (B) must keep the same spans with the same reasons.  Reported: the spans
F drops that A keeps (must be none), what F passes split into roots, plan-selected and outliers, in the healthy and the fault windows,
and the share of the spans it passes.
"""
from __future__ import annotations

import argparse
import copy
import json
import os
import pickle
import random
import sys

for _p in ("/users/royno7/slo-budget-sim", os.environ.get("SLO_SIM")):      # SLO_SIM: a simulator copy to use first (node-0: ~/an14/sim)
    if _p:
        sys.path.insert(0, _p)
from slo_budget_sim.replay import replay  # noqa: E402
from slo_budget_sim.sampler import BudgetSampler, coin_u, make_budget_arm  # noqa: E402


RATE_HEADROOM = 1.05        # sdk/filter RateHeadroom
ARM = dict(plan_coin="hash", coin_source="span")


class Recorder(BudgetSampler):
    """the sampler with the tables in force after every period written down"""

    def start_run(self, ctx):
        super().start_run(ctx)
        self.tables = {}        # period -> (b by (route, class, service), plan rate by service)
        self.frozen = {}        # period -> the services frozen at its end

    def on_period_end(self, period, volume):
        super().on_period_end(period, volume)
        self.tables[period] = (dict(self.cal.b), dict(self._plan_p))
        self.frozen[period] = set(self._frozen)


def stale_tables(tables, period, lag):
    """the b table the filter has in `period`: the one computed at the end of period - 1 - lag (None before any)"""
    k = period - 1 - lag
    return tables.get(k) if k in tables else None


def filter_corpus(corpus, tables, margin_free=True, lag=1, no_arrival=0.0, seed=0, period=5.0, flush=True):
    """the spans the filter passes, in a corpus of their own (indexes renumbered), and the per-span outcome.  flush: the dropped counts
    still pending at a period's end go to the agent as count records at that boundary (corpus.count_records); without it they ride on
    the next passed span"""
    rng = random.Random(seed)
    spans = corpus.spans
    order = sorted(range(len(spans)), key=lambda i: (spans[i].start + spans[i].duration, i))
    passed, why, dropped, bf, pf, carry = [], {}, {}, {}, {}, {}
    cur = None
    for i in order:
        s = spans[i]
        k = int((s.start + s.duration - corpus.t0) // period)
        if flush and cur is not None and k != cur:
            carry[cur] = dict(dropped)
            dropped = {}
        cur = k
        t_now = stale_tables(tables, k, lag)
        t_prev = stale_tables(tables, k, lag + 1)
        if s.parent_idx < 0:
            reason = "root"
        elif not s.status_ok:
            reason = "error"
        elif no_arrival and rng.random() < no_arrival:
            reason = "no_arrival"
        else:
            b = None
            if t_now:
                b = t_now[0].get((s.route, s.req_class, s.service)) or t_now[0].get((s.route, "*", s.service))
            bf[i] = b
            if b is None:
                reason = "no_threshold"
            elif s.own + s.wait > b:
                reason = "outlier"
            else:
                rates = [t[1].get(s.service) for t in (t_now, t_prev) if t]
                p = max((r for r in rates if r is not None), default=None)
                pf[i] = p
                if p is None or p >= 1.0 or coin_u(s.span_id) < min(1.0, RATE_HEADROOM * p):
                    reason = "plan" if p is not None and p < 1.0 else "no_rate"
                else:
                    reason = None
        if reason is None:
            dropped[s.service] = dropped.get(s.service, 0) + 1
            why[i] = "dropped"
            continue
        why[i] = reason
        passed.append((i, dropped.pop(s.service, 0)))
    new = {i: n for n, (i, _) in enumerate(passed)}

    def ancestor(i):
        """the nearest passed ancestor: a span whose parent the filter dropped is still not a root (the agent sees its parent id), and
        roots always pass, so there is one"""
        j = spans[i].parent_idx
        while j >= 0 and j not in new:
            j = spans[j].parent_idx
        return new[j] if j >= 0 else -1

    out = copy.copy(corpus)
    out.spans = []
    for n, (i, before) in enumerate(passed):
        s = copy.copy(spans[i])
        s.idx = n
        s.parent_idx = ancestor(i)
        s.children = [new[c] for c in s.children if c in new]
        s.dropped_before = before
        out.spans.append(s)
    out.roots = [new[r] for r in corpus.roots if r in new]
    if flush and cur is not None:
        carry[cur] = dict(dropped)
    hosts = {s.service: s.host for s in spans}
    out.count_records = {per: [(svc, hosts.get(svc, ""), n) for svc, n in recs.items() if n > 0] for per, recs in carry.items()}
    out.count_records = {per: recs for per, recs in out.count_records.items() if recs}
    return out, why, new, bf, pf


def classify(sampler, period, lag, svc):
    """why the filter's table could differ from the agent's: cold start (no table yet at the filter's lag), the service's freeze was released
    between the two tables, or an ordinary period"""
    first = min(sampler.tables) if sampler.tables else None
    k_f, k_a = period - 1 - lag, period - 1
    if first is None or k_f < first:
        return "cold_start"
    if svc in sampler.frozen.get(k_f, set()) and svc not in sampler.frozen.get(k_a, set()):
        return "freeze_release"
    return "ordinary"


def run(corpus, v_star, cap, seed, lag, no_arrival, flush=True, dump=None, name="corpus", **cfg):
    sampler = Recorder(make_budget_arm("chosen-v2", **ARM, **cfg).cfg, name="chosen-v2")
    a = replay(corpus, sampler, v_star, cap, seed=seed)
    kept_a = {int(i): r for i, r in zip(a.kept_idx, a.kept_reason)}
    fc, why, new, bf, pf = filter_corpus(corpus, sampler.tables, lag=lag, no_arrival=no_arrival, seed=seed, period=corpus.period, flush=flush)
    sb = BudgetSampler(make_budget_arm("chosen-v2", **ARM, **cfg).cfg, name="chosen-v2")
    b = replay(fc, sb, v_star, cap, seed=seed)
    if dump:
        from slo_budget_sim.dump import decisions_dump, events_dump
        os.makedirs(dump, exist_ok=True)
        for tag, c, res, sm in (("all", corpus, a, sampler), ("filtered", fc, b, sb)):
            stem = os.path.join(dump, f"{name}-{tag}")
            json.dump(events_dump(c, sm.cfg, v_star, cap, seed, "chosen-v2", name, 0), open(stem + ".events.json", "w"))
            json.dump(decisions_dump(res, sm.n_need), open(stem + ".decisions.json", "w"))
    kept_b = {int(i): r for i, r in zip(b.kept_idx, b.kept_reason)}
    back = {n: i for i, n in new.items()}
    kept_b_orig = {back[n]: r for n, r in kept_b.items()}
    spans = corpus.spans
    dropped_but_kept = sorted(i for i in kept_a if why.get(i) == "dropped")
    # the dropped-but-kept spans: when, and what slack on the filter's threshold would have passed them
    det = []
    for i in dropped_but_kept:
        s = spans[i]
        p = int((s.start + s.duration - corpus.t0) // corpus.period)
        x, b_f = s.own + s.wait, bf.get(i)
        t_a = sampler.tables.get(p - 1)
        b_a = (t_a[0].get((s.route, s.req_class, s.service)) or t_a[0].get((s.route, "*", s.service))) if t_a else None
        p_a = t_a[1].get(s.service) if t_a else None
        p_f, u = pf.get(i), coin_u(s.span_id)
        cls = classify(sampler, p, lag, s.service)
        if kept_a[i] == "plan" and p_f is not None and p_a is not None and p_a > p_f:
            cls = "plan_rate_rose" + ("_at_cold_start" if sampler.tables and p - 1 - lag <= min(sampler.tables) else "")
        det.append({"idx": i, "period": p, "service": s.service, "agent_reason": kept_a[i], "class": cls, "x_ms": 1e3 * x,
                    "b_filter_ms": 1e3 * b_f if b_f else None, "b_agent_ms": 1e3 * b_a if b_a else None, "slack": (1 - x / b_f) if b_f else None,
                    "p_filter": p_f, "p_agent": p_a, "coin": u, "rate_headroom_needed": (u / p_f) if p_f else None})
    by_class = {}
    for d in det:
        by_class[d["class"]] = by_class.get(d["class"], 0) + 1
    heads = sorted(d["rate_headroom_needed"] for d in det if d.get("rate_headroom_needed") is not None and d["class"].startswith("plan_rate_rose"))
    slacks = sorted(d["slack"] for d in det if d["slack"] is not None and not d["class"].startswith("plan_rate_rose"))
    slack_summary = {"spans": len(slacks), "max": slacks[-1] if slacks else None, "p90": slacks[int(0.9 * (len(slacks) - 1))] if slacks else None,
                     "median": slacks[len(slacks) // 2] if slacks else None} if slacks else {}
    head_summary = {"spans": len(heads), "max": heads[-1], "p90": heads[int(0.9 * (len(heads) - 1))], "median": heads[len(heads) // 2]} if heads else {}
    differ = sorted((spans[i].start + spans[i].duration, i) for i in set(kept_a) | set(kept_b_orig) if kept_a.get(i) != kept_b_orig.get(i))
    first = [{"idx": i, "period": int((t - corpus.t0) // corpus.period), "service": spans[i].service, "agent_on_all": kept_a.get(i), "agent_on_filtered": kept_b_orig.get(i), "filter": why.get(i)} for t, i in differ[:8]]
    differ = [i for _, i in differ]
    fault = getattr(corpus, "fault_window", None)

    def split(pred):
        c = {"roots": 0, "plan": 0, "outliers": 0, "errors": 0, "no_arrival": 0, "no_threshold": 0, "no_rate": 0, "dropped": 0, "spans": 0}
        for i, s in enumerate(spans):
            if not pred(s):
                continue
            c["spans"] += 1
            c[{"root": "roots", "outlier": "outliers", "error": "errors"}.get(why[i], why[i])] += 1
        c["passed_share"] = 1 - c["dropped"] / max(1, c["spans"])
        return c
    res = {"spans": len(spans), "kept_by_agent_on_all": len(kept_a), "kept_by_agent_on_filtered": len(kept_b), "passed": len(fc.spans), "passed_share": len(fc.spans) / len(spans),
           "dropped_by_filter_but_kept_by_agent": len(dropped_but_kept), "examples": dropped_but_kept[:10], "dropped_but_kept_by_class": by_class,
           "dropped_but_kept_slack": slack_summary, "dropped_but_kept_rate_headroom": head_summary, "flush": flush, "dropped_but_kept_details": det, "decisions_that_differ": len(differ), "differ_examples": differ[:10], "first_differences": first,
           "identical": not differ, "all": split(lambda s: True)}
    if fault:
        f0, f1 = fault
        res["fault"] = split(lambda s: f0 <= s.start + s.duration < f1)
        res["healthy"] = split(lambda s: s.start + s.duration < f0)
    return res


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--synthetic", help="a simulator scenario (saturated, cascade, steady, ...)")
    ap.add_argument("--corpus", help="a pickled Corpus (with fault_window = (start, end) when it has one)")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--volume", type=float, default=0.12)
    ap.add_argument("--lag", type=int, default=1, help="how many periods stale the filter's tables are (0: the agent's own)")
    ap.add_argument("--no-arrival", type=float, default=0.01, help="share of non-root spans without an arrival (exported)")
    ap.add_argument("--run", help="a cluster run directory (node-0): the corpus from its 100 %% export, the wait by the stamp")
    ap.add_argument("--no-count-records", action="store_true", help="no count record at the period's end: the counts ride on the next passed span")
    ap.add_argument("--dump", help="write <name>-all / <name>-filtered events and decisions here (the Go differential test)")
    ap.add_argument("--out")
    a = ap.parse_args()
    if a.synthetic:
        from slo_budget_sim.synthetic import build_scenario
        corpus, _ = build_scenario(a.synthetic, a.seed)
        try:
            from slo_budget_sim.dump import add_waits
            add_waits(corpus, a.seed)
        except Exception:                                             # noqa: BLE001 - a corpus without waits is fine for the proof
            pass
    elif a.run:
        sys.path.insert(0, os.path.expanduser("~/an13"))
        from measure13 import build  # noqa: E402
        import math
        t, corpus, _, waits, _, _ = build(os.path.basename(a.run.rstrip("/")))
        for sp, w in zip(corpus.spans, waits["iv"].tolist()):
            sp.wait = max(0.0, float(w))
        fw = (t.meta or {}).get("fault")
        if fw:    # the fault window in the corpus's clock (measure13: spans start at t0 floored to the period)
            base = math.floor(float(t.meta["t0"]) / 5.0) * 5
            corpus.fault_window = (float(fw["start"]) - base, float(fw["end"]) - base)
    else:
        corpus = pickle.load(open(a.corpus, "rb"))
    n = len(corpus.spans)
    dur = max(s.start + s.duration for s in corpus.spans) - corpus.t0
    v_star = a.volume * n * corpus.period / dur
    name = a.synthetic or (os.path.basename(a.run.rstrip("/")) if a.run else os.path.basename(a.corpus))
    res = run(corpus, v_star, 1.25 * v_star, 1000 + a.seed, a.lag, a.no_arrival, flush=not a.no_count_records, dump=a.dump, name=name, own_measure="within_service")
    res["source"] = name
    print(json.dumps(res, indent=1))
    if a.out:
        json.dump(res, open(a.out, "w"), indent=1)
    return 0 if res["identical"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
