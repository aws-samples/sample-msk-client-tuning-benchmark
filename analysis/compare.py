#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Compare two measurement windows recorded by measure_cluster.py and check that the comparison is fair.

Usage:
  compare.py <baseline-label> <candidate-label> [--file snapshots.jsonl] [--max-rate-diff-pct 5]

Reads the most recent record for each label from the JSON-lines file written by measure_cluster.py,
prints a side-by-side table (mean across brokers and the worst broker), and runs quality checks:

  * Message rate: MessagesInPerSec must match within --max-rate-diff-pct. Compression changes on-wire
    bytes, so runs are compared at equal message rate, not equal bytes.
  * Replication health: UnderReplicatedPartitions must be 0 in both windows.
  * Completeness: the primary metric (RequestHandlerAvgIdlePercent) must be present in both windows.

Exit code 0 means the comparison passed every check (OK). Exit code 1 means at least one check failed
(SUSPECT): fix the run instead of explaining the numbers. Exit code 2 means a usage or input error.
"""
import argparse
import json
import os
import sys

# (key prefix or key, label, worst-broker direction: "min" = lower is worse, "max" = higher is worse)
ROWS = [
    ("rh", "RequestHandlerAvgIdlePercent", "min"),
    ("np", "NetworkProcessorAvgIdlePercent", "min"),
    ("cpu_user", "CpuUser (%)", "max"),
    ("cpu_sys", "CpuSystem (%)", "max"),
    ("produce_ms", "ProduceTotalTimeMsMean (ms)", "max"),
    ("fetch_ms", "FetchConsumerTotalTimeMsMean (ms)", "max"),
    ("vql", "VolumeQueueLength", "max"),
]
TOTALS = [
    ("msgs_in_per_s", "MessagesInPerSec (sum)"),
    ("bytes_in_MiBps", "BytesInPerSec (MiB/s, sum)"),
    ("bytes_out_MiBps", "BytesOutPerSec (MiB/s, sum)"),
]


def load(path, label):
    found = None
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if rec.get("label") == label:
                found = rec
    return found


def fmt(v, nd=3):
    return "-" if v is None else ("%.*f" % (nd, v))


def delta(a, b, points=False):
    if a is None or b is None:
        return "-"
    if points:
        # *IdlePercent metrics are fractions (1.0 = fully idle); report the change in percentage points.
        return "%+.1f pts" % ((b - a) * 100.0)
    if a is None or b is None:
        return "-"
    if a == 0:
        return "%+.3f" % (b - a)
    return "%+.1f%%" % ((b - a) / abs(a) * 100.0)


def main(argv):
    here = os.path.dirname(os.path.abspath(__file__))
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("baseline")
    p.add_argument("candidate")
    p.add_argument("--file", default=os.path.join(here, "snapshots.jsonl"))
    p.add_argument("--max-rate-diff-pct", type=float, default=5.0)
    a = p.parse_args(argv)
    if not os.path.exists(a.file):
        print("ERROR: %s not found (run measure_cluster.py first)" % a.file, file=sys.stderr)
        return 2
    base, cand = load(a.file, a.baseline), load(a.file, a.candidate)
    for name, rec in ((a.baseline, base), (a.candidate, cand)):
        if rec is None:
            print("ERROR: no record with label %r in %s" % (name, a.file), file=sys.stderr)
            return 2

    w = 44
    print("%-*s %14s %14s %10s" % (w, "metric", a.baseline[:14], a.candidate[:14], "change"))
    print("-" * (w + 41))
    for key, label, worst in ROWS:
        bm, cm = base.get(key + "_mean"), cand.get(key + "_mean")
        if bm is None and cm is None:
            continue
        bw, cw = base.get(key + "_" + worst), cand.get(key + "_" + worst)
        pts = key in ("rh", "np")
        print("%-*s %14s %14s %10s" % (w, label + " - mean", fmt(bm), fmt(cm), delta(bm, cm, pts)))
        print("%-*s %14s %14s %10s" % (w, label + " - worst broker", fmt(bw), fmt(cw), delta(bw, cw, pts)))
    for key, label in TOTALS:
        bv, cv = base.get(key), cand.get(key)
        print("%-*s %14s %14s %10s" % (w, label, fmt(bv, 1), fmt(cv, 1), delta(bv, cv)))
    for key, label in (("time_lag_max_s", "EstimatedMaxTimeLag max (s)"), ("partitions_per_broker_max", "PartitionCount max/broker")):
        bv, cv = base.get(key), cand.get(key)
        if bv is not None or cv is not None:
            print("%-*s %14s %14s %10s" % (w, label, fmt(bv, 1), fmt(cv, 1), delta(bv, cv)))

    problems = []
    bm, cm = base.get("msgs_in_per_s"), cand.get("msgs_in_per_s")
    if not bm or not cm:
        problems.append("MessagesInPerSec missing or zero in one window")
    else:
        diff = abs(cm - bm) / bm * 100.0
        if diff > a.max_rate_diff_pct:
            problems.append("message rates differ by %.1f%% (> %.1f%%): runs are not comparable"
                            % (diff, a.max_rate_diff_pct))
    for name, rec in ((a.baseline, base), (a.candidate, cand)):
        urp = rec.get("under_replicated_max")
        if urp is None:
            problems.append("%s: UnderReplicatedPartitions not recorded (re-measure with the current measure_cluster.py)" % name)
        elif urp > 0:
            problems.append("%s: UnderReplicatedPartitions max = %s (must be 0)" % (name, urp))
        if rec.get("rh_mean") is None:
            problems.append("%s: RequestHandlerAvgIdlePercent missing (is enhanced monitoring PER_BROKER or higher?)" % name)

    print()
    if problems:
        print("RESULT: SUSPECT")
        for msg in problems:
            print("  - " + msg)
        return 1
    print("RESULT: OK (equal message rate within %.1f%%, no under-replicated partitions)" % a.max_rate_diff_pct)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
