"""SLO-budget span sampling: offline trace-replay simulator (Stage 1).

Modules
    model       spans, call graph, corpus
    ttable      T_i table: weighted quantiles, Erms square-root split, calibration
    sampler     the sampler interface and the chosen mechanism (BudgetSampler)
    baselines   threshold, reservoir, keep-the-parent, uniform head, Canopy-style
    replay      event replay (span start/end in time order), periods, collector cap
    metrics     recall, evidence time, culprit identification, 1/p-reweighted P95
    synthetic   DSB-SN-like scenario generator
    jaeger      Jaeger JSON loader / writer
    experiments experiments 1-5
    cli         command line
"""

__version__ = "0.1.0"
