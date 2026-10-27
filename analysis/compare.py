#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Compare two measurement windows recorded by measure_cluster.py and check that the comparison is fair.

Usage:
  compare.py <baseline-label> <candidate-label> [--file snapshots.jsonl] [--max-rate-diff-pct 5]
             [--format text|markdown]

Example:
  compare.py aws-tuned-20260101T100000Z aws-tuned-snappy-20260101T100000Z --format markdown > ab.md

Reads the most recent record for each label from the JSON-lines file written by measure_cluster.py
(only records with "source": "measure_cluster", or, in older files without that tag, records that
have an rh_mean key; snap.py records are ignored), prints a side-by-side table (mean across brokers and the worst broker), and runs quality checks:

--format text (default) prints a fixed-width table for a terminal. --format markdown prints a GitHub-flavoured
Markdown table plus the OK/SUSPECT result, suitable to paste into a doc, an issue or a GitHub Discussion. Both
formats print only the labels, the cluster name and the window times, never an ARN or an AWS account ID.

  * Message rate: MessagesInPerSec must match within --max-rate-diff-pct. Compression changes on-wire
    bytes, so runs are compared at equal message rate, not equal bytes.
  * Replication health: UnderReplicatedPartitions must be 0 in both windows.
  * Completeness: the primary metric (RequestHandlerAvgIdlePercent) must be present in both windows.

Exit code 0 means the comparison passed every check (OK). Exit code 1 means at least one check failed
(SUSPECT): fix the run instead of explaining the numbers. Exit code 2 means a usage or input error
(missing or unreadable file, invalid JSON, label not found).
"""
import argparse
import json
import os
import re
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
    ("produce_throttle_ms", "ProduceThrottleTime (ms)", "max"),
    ("fetch_throttle_ms", "FetchThrottleTime (ms)", "max"),
    ("request_throttle_ms", "RequestThrottleTime (ms)", "max"),
]
TOTALS = [
    ("msgs_in_per_s", "MessagesInPerSec (sum)"),
    ("bytes_in_MiBps", "BytesInPerSec (MiB/s, sum)"),
    ("bytes_out_MiBps", "BytesOutPerSec (MiB/s, sum)"),
]


class InputError(Exception):
    """The snapshots file cannot be read or contains invalid JSON."""


def is_measure_record(rec):
    # measure_cluster.py tags its records with "source"; older files without the tag are recognised
    # by the rh_mean key, which only measure_cluster.py writes (snap.py records are skipped).
    if not isinstance(rec, dict):
        return False
    if "source" in rec:
        return rec["source"] == "measure_cluster"
    return "rh_mean" in rec


def load(path, label):
    found = None
    try:
        with open(path) as fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError as e:
                    raise InputError("%s line %d is not valid JSON (%s)" % (path, lineno, e.msg))
                if is_measure_record(rec) and rec.get("label") == label:
                    found = rec
    except (OSError, UnicodeDecodeError) as e:
        raise InputError("cannot read %s (%s)" % (path, e))
    return found


def fmt(v, nd=3):
    return "-" if v is None else ("%.*f" % (nd, v))


def delta(a, b, points=False):
    if a is None or b is None:
        return "-"
    if points:
        # *IdlePercent metrics are fractions (1.0 = fully idle); report the change in percentage points.
        return "%+.1f pts" % ((b - a) * 100.0)
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
    p.add_argument("--format", choices=("text", "markdown"), default="text",
                   help="output format (default text); markdown is safe to paste publicly (no ARNs or account IDs)")
    a = p.parse_args(argv)
    if not os.path.exists(a.file):
        print("ERROR: %s not found (run measure_cluster.py first)" % a.file, file=sys.stderr)
        return 2
    try:
        base, cand = load(a.file, a.baseline), load(a.file, a.candidate)
    except InputError as e:
        print("ERROR: %s" % e, file=sys.stderr)
        return 2
    for name, rec in ((a.baseline, base), (a.candidate, cand)):
        if rec is None:
            print("ERROR: no measure_cluster.py record with label %r in %s" % (name, a.file), file=sys.stderr)
            return 2

    rows = []  # (metric, baseline value, candidate value, change)
    for key, label, worst in ROWS:
        bm, cm = base.get(key + "_mean"), cand.get(key + "_mean")
        if bm is None and cm is None:
            continue
        bw, cw = base.get(key + "_" + worst), cand.get(key + "_" + worst)
        pts = key in ("rh", "np")
        rows.append((label + " - mean", fmt(bm), fmt(cm), delta(bm, cm, pts)))
        rows.append((label + " - worst broker", fmt(bw), fmt(cw), delta(bw, cw, pts)))
    for key, label in TOTALS:
        bv, cv = base.get(key), cand.get(key)
        rows.append((label, fmt(bv, 1), fmt(cv, 1), delta(bv, cv)))
    for key, label in (("time_lag_max_s", "EstimatedMaxTimeLag max (s)"), ("partitions_per_broker_max", "PartitionCount max/broker")):
        bv, cv = base.get(key), cand.get(key)
        if bv is not None or cv is not None:
            rows.append((label, fmt(bv, 1), fmt(cv, 1), delta(bv, cv)))

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

    ok_msg = "equal message rate within %.1f%%, no under-replicated partitions" % a.max_rate_diff_pct
    if a.format == "markdown":
        print_markdown(a, base, cand, rows, problems, ok_msg)
    else:
        print_text(a, rows, problems, ok_msg)
    return 1 if problems else 0


def print_text(a, rows, problems, ok_msg):
    w = 44
    print("%-*s %14s %14s %10s" % (w, "metric", a.baseline[:14], a.candidate[:14], "change"))
    print("-" * (w + 41))
    for row in rows:
        print("%-*s %14s %14s %10s" % ((w,) + row))
    print()
    if problems:
        print("RESULT: SUSPECT")
        for msg in problems:
            print("  - " + msg)
    else:
        print("RESULT: OK (%s)" % ok_msg)


_ARN = re.compile(r"arn:aws[a-zA-Z-]*:[^\s|`]*")
_ACCOUNT_ID = re.compile(r"(?<!\d)\d{12}(?!\d)")


def md(text):
    """Plain text safe for a Markdown table cell, with any ARN or 12-digit account ID masked."""
    text = _ACCOUNT_ID.sub("<account-id>", _ARN.sub("<arn>", str(text)))
    for ch in ("\\", "|", "`", "*", "_", "<", ">"):
        text = text.replace(ch, "\\" + ch)
    return text.replace("\n", " ")


def print_markdown(a, base, cand, rows, problems, ok_msg):
    # Only the cluster NAME and window times are printed (measure_cluster.py records hold no ARN or account ID).
    cluster = base.get("cluster") or cand.get("cluster")
    print("### A/B comparison: %s vs %s" % (md(a.baseline), md(a.candidate)))
    print()
    if cluster:
        print("Cluster: %s" % md(cluster))
        print()
    for name, rec in ((a.baseline, base), (a.candidate, cand)):
        if rec.get("start") and rec.get("end"):
            print("- %s: %s to %s (%s brokers)" % (md(name), md(rec["start"]), md(rec["end"]),
                                                   md(rec.get("brokers", "?"))))
    print()
    print("| Metric | %s | %s | Change |" % (md(a.baseline), md(a.candidate)))
    print("|---|---:|---:|---:|")
    for metric, bv, cv, ch in rows:
        print("| %s | %s | %s | %s |" % (md(metric), bv, cv, ch))
    print()
    print("Idle metrics are fractions (1.0 = fully idle); their change is in percentage points.")
    print()
    if problems:
        print("**Result: SUSPECT** (fix the run before reading the numbers)")
        print()
        for msg in problems:
            print("- " + md(msg))
    else:
        print("**Result: OK** (%s)" % ok_msg)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
