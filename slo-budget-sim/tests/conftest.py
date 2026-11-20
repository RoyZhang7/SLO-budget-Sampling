import pytest

from slo_budget_sim.model import CallGraph, Corpus, Span


def chain_corpus(durations_ms, services=("a", "b", "c"), n_traces=1, period=1.0, slo_ms=12.0, hosts=None):
    """Linear chain a -> b -> c (sequential).  durations_ms are inclusive
    durations per service for every trace; traces arrive 10 ms apart."""
    spans, roots = [], []
    for k in range(n_traces):
        t0 = 0.010 * k
        parent = -1
        start = t0
        for depth, (svc, d) in enumerate(zip(services, durations_ms)):
            idx = len(spans)
            spans.append(Span(idx, idx + 1, k + 1, parent, svc, (hosts or {}).get(svc, "h1"), start, d / 1000.0,
                              True, depth, "r", "r|any", 300))
            if parent < 0:
                roots.append(idx)
            parent = idx
            start += 0.0002              # callee starts 0.2 ms after its caller
    graph = CallGraph({"r": services[0]},
                      {("r", s): ([services[i + 1]] if i + 1 < len(services) else []) for i, s in enumerate(services)},
                      {("r", s): False for s in services})
    corpus = Corpus("chain", spans, roots, {"r": slo_ms / 1000.0}, graph, sorted({s.host for s in spans}),
                    {h: 0.0 for h in {s.host for s in spans}}, period, 0.0)
    corpus.finalize()
    return corpus


@pytest.fixture
def chain():
    return chain_corpus([6.0, 2.0, 1.0])
