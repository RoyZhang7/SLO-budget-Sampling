# rcaeval: RCAEval's TraceRCA and MicroRank, vendored

From RCAEval (https://github.com/phamquiluan/RCAEval, commit a30feeb, MIT, `LICENSE`):
`RCAEval/e2e/tracerca.py` (unchanged) and `RCAEval/e2e/microrank.py`.  The package itself is not
installed: importing `RCAEval.e2e` pulls every method's dependencies (torch and others), and these
two files need only numpy, pandas, tqdm and python-dateutil.

Changes to microrank.py, all inside `trace_pagerank` and marked `slobudget`, are for speed only
(the 100 % shadow has about 20 000 traces per window, where the original's list lookups and its
pairwise grouping of identical traces are quadratic):

- `node_list.index(x)` / `trace_list.index(x)` -> positions from a dict (lists of unique keys, so
  the same values);
- the loop that gives every trace the number of traces with an identical `p_sr` column ->
  `np.unique(axis=0, return_counts=True)` (exact comparison, the same counts);
- the per-operation count of non-zero `p_sr` entries -> `np.count_nonzero`.

`tests/test_rca.py` checks the patched `trace_pagerank` against the original loops.

How the two methods read the data (as RCAEval implements them; both take `inject_time` in the
unit of `startTime`, and both divide `duration` by 1000 to get ms):

- Normal and anomalous periods: spans whose `startTime + duration` is before `inject_time` are the
  normal period, the rest the anomalous one.
- TraceRCA labels spans, not traces: a span of the anomalous period is abnormal when its duration
  is at least its operation's normal mean + 3 standard deviations.  Operations are ranked by the
  harmonic mean of support (the operation's share of abnormal spans) and confidence (the share of
  its spans that are abnormal).  Parents and roots are never read.
- MicroRank labels traces through their spans: a trace counts as anomalous when one of its spans in
  the anomalous period is abnormal by the same 3-sigma rule, and as normal when one is not (a trace
  can be both); the root's latency is not consulted, so a partial trace without its root is
  labelled by the spans it has.  The two PageRank graphs are built from every span of each period
  (the anomaly labels enter only as trace counts in the DStar2 spectrum).  Call edges come from
  parent-child pairs present in the data, and in RCAEval's refactor an operation keeps only the
  children of the last of its spans that is read (the child list is reset per span); they enter
  the PageRank with weight alpha = 0.01 against the trace-operation coverage.  It returns the top
  11 operations.
- Both index the normal period's statistics by operation, so an operation that appears only in
  the anomalous period raises KeyError (sloeval/rca.py records the failure).
