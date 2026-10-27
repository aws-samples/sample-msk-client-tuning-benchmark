#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Aggregate MultiGroupConsumer E2E_LATENCY lines into a markdown table.

Usage: latency_report.py <run_label> <log_glob> [<log_glob> ...] [--stage poll|processed|both]
  e.g. kubectl -n msk-bench logs -l role=consumer --prefix --tail=-1 > /tmp/run-baseline.log
       latency_report.py "baseline" '/tmp/run-baseline*.log'

Line format (one per group, stage and window):
  E2E_LATENCY group=<g> stage=<s> count=<n> p50=<ms> p90=<ms> p99=<ms> p999=<ms> max=<ms> window_s=<s>

Method (approximation, documented on purpose): windows are combined per (group type, stage)
with count-weighted means of each window's percentile; max is the true max of window maxima.
A weighted mean of percentiles is not an exact percentile of the union, but it is stable when
windows are similar; for exact tails rerun with one long window (--latency-report-sec large).
Group type: names containing 'bcast' (e.g. bench-bcast-*) = broadcast; everything else = heavy.
Windows with count=0 are ignored. Latency includes clock skew between producer and consumer nodes.
"""
import glob, re, sys
from collections import defaultdict

LINE = re.compile(r"E2E_LATENCY group=(\S+) stage=(\S+) count=(\d+) p50=(\d+) p90=(\d+) p99=(\d+) p999=(\d+) max=(\d+)")

def kind(group):
    g = group.lower()
    return "broadcast" if "bcast" in g else "heavy"

def main(argv):
    stage = "both"
    if "--stage" in argv:
        i = argv.index("--stage"); stage = argv[i + 1]; del argv[i:i + 2]
    if len(argv) < 2:
        sys.exit(__doc__)
    label, globs = argv[0], argv[1:]
    files = sorted({f for g in globs for f in glob.glob(g)})
    if not files:
        sys.exit(f"ERROR: no files match {globs}")
    acc = defaultdict(lambda: {"n": 0, "w": [0.0] * 4, "max": 0, "windows": 0})
    for f in files:
        with open(f, errors="replace") as fh:
            for line in fh:
                m = LINE.search(line)
                if not m:
                    continue
                g, st, n = m.group(1), m.group(2), int(m.group(3))
                if n == 0 or (stage != "both" and st != stage):
                    continue
                a = acc[(kind(g), st)]
                a["n"] += n; a["windows"] += 1
                for k in range(4):
                    a["w"][k] += n * int(m.group(4 + k))
                a["max"] = max(a["max"], int(m.group(8)))
    if not acc:
        sys.exit(f"ERROR: no non-empty E2E_LATENCY lines in {len(files)} file(s)")
    print("| Run | Group type | Stage | Samples | p50 ms | p90 ms | p99 ms | p99.9 ms | max ms |")
    print("|---|---|---|---|---|---|---|---|---|")
    for (k, st), a in sorted(acc.items()):
        p = [round(x / a["n"]) for x in a["w"]]
        print(f"| {label} | {k} | {st} | {a['n']:,} | {p[0]} | {p[1]} | {p[2]} | {p[3]} | {a['max']} |")

if __name__ == "__main__":
    main(sys.argv[1:])
