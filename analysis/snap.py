#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Quick cluster snapshot from Amazon CloudWatch (5-minute periods) for one window.

Usage:
  snap.py <label> <start-iso> <end-iso>
      [--cluster NAME] [--num-brokers N] [--topic-prefix P] [--num-topics N]
      [--groups g1,g2] [--region REGION] [--profile PROFILE] [--out FILE]

Defaults come from env (CLUSTER_NAME=load-benchmark, NUM_BROKERS=3, TOPIC_PREFIX=load-bench,
NUM_TOPICS=12, GROUPS=group-0). Region/profile default to the AWS CLI's own resolution.
Throughput (bytes/messages) is summed across brokers; idle %, CPU and latency are the mean
across brokers. Prints the record and appends it as one JSON line to --out
(default: snap-snapshots.jsonl next to this script). The default is deliberately a separate file
from measure_cluster.py's snapshots.jsonl: snap.py records use different keys and resolution, and
compare.py ignores them.
"""
import argparse
import json
import os
import re
import statistics as st
import sys

import boto3


# Arguments are validated so that only well-formed values reach the Amazon CloudWatch API calls.
_PATTERNS = {
    "cluster": r"[A-Za-z0-9][A-Za-z0-9-]{0,63}",
    "start": r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?",
    "end": r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?",
    "region": r"[a-z]{2}(-[a-z]+)+-\d",
    "profile": r"[A-Za-z0-9._-]{1,64}",
    "topic_prefix": r"[A-Za-z0-9._-]{1,200}",
    "groups": r"[A-Za-z0-9._-]+(,[A-Za-z0-9._-]+)*",
}


def _validate(parser, args):
    for name, pattern in _PATTERNS.items():
        value = getattr(args, name, None)
        if value is not None and not re.fullmatch(pattern, value):
            parser.error("invalid %s: %r" % (name.replace("_", "-"), value))


def parse_args(argv):
    env = os.environ.get
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("label")
    p.add_argument("start")
    p.add_argument("end")
    p.add_argument("--cluster", default=env("CLUSTER_NAME", "load-benchmark"))
    p.add_argument("--num-brokers", type=int, default=int(env("NUM_BROKERS", "3")))
    p.add_argument("--topic-prefix", default=env("TOPIC_PREFIX", "load-bench"))
    p.add_argument("--num-topics", type=int, default=int(env("NUM_TOPICS", "12")))
    p.add_argument("--groups", default=env("GROUPS", "group-0"))
    p.add_argument("--region", default=None)
    p.add_argument("--profile", default=None)
    p.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "snap-snapshots.jsonl"))
    a = p.parse_args(argv)
    _validate(p, a)
    return a


def main(argv):
    a = parse_args(argv)
    S, E = a.start, a.end
    cw = boto3.Session(region_name=a.region, profile_name=a.profile).client("cloudwatch")

    def get(name, dims, stat):
        resp = cw.get_metric_statistics(Namespace="AWS/Kafka", MetricName=name,
                                        Dimensions=[{"Name": k, "Value": v} for k, v in dims],
                                        StartTime=S, EndTime=E, Period=300, Statistics=[stat])
        return [p[stat] for p in resp["Datapoints"]]

    C = ("Cluster Name", a.cluster)
    brokers = range(1, a.num_brokers + 1)

    def per_broker(name, stat="Average", agg="sum"):
        vals = []
        for b in brokers:
            v = get(name, [C, ("Broker ID", str(b))], stat)
            vals.append(st.mean(v) if v else 0)
        return sum(vals) if agg == "sum" else st.mean(vals) if agg == "mean" else max(vals)

    r = {"label": a.label, "cluster": a.cluster, "start": S, "end": E}
    r["bytes_in_MBps"] = per_broker("BytesInPerSec") / 1048576
    r["bytes_out_MBps"] = per_broker("BytesOutPerSec") / 1048576
    r["msgs_in_per_s"] = per_broker("MessagesInPerSec")
    r["cpu_user_sys_pct_mean"] = per_broker("CpuUser", agg="mean") + per_broker("CpuSystem", agg="mean")
    r["req_handler_idle_mean"] = per_broker("RequestHandlerAvgIdlePercent", agg="mean")
    r["net_proc_idle_mean"] = per_broker("NetworkProcessorAvgIdlePercent", agg="mean")
    r["produce_total_ms_mean"] = per_broker("ProduceTotalTimeMsMean", agg="mean")
    r["fetch_consumer_total_ms_mean"] = per_broker("FetchConsumerTotalTimeMsMean", agg="mean")
    r["volume_write_ops_sum"] = per_broker("VolumeWriteOps")
    r["volume_write_MB_sum"] = per_broker("VolumeWriteBytes") / 1048576
    r["volume_queue_len_mean"] = per_broker("VolumeQueueLength", agg="mean")
    r["connection_count_sum"] = per_broker("ConnectionCount", "Maximum")
    r["conn_creation_rate_max"] = per_broker("ConnectionCreationRate", "Maximum", agg="max")
    groups = [g for g in a.groups.split(",") if g]
    for g in groups:
        lag, tl = [], []
        for t in range(1, a.num_topics + 1):
            d = [C, ("Consumer Group", g), ("Topic", f"{a.topic_prefix}-{t}")]
            v = get("SumOffsetLag", d, "Average")
            lag.append(st.mean(v) if v else 0)
            v = get("EstimatedMaxTimeLag", d, "Maximum")
            tl.append(max(v) if v else 0)
        sfx = "" if g == groups[0] else "_" + g.replace("-", "")
        r["sum_offset_lag" + sfx] = sum(lag)
        r["max_time_lag_s" + sfx] = max(tl) if tl else 0
    print(json.dumps(r, indent=1))
    with open(a.out, "a") as fh:
        fh.write(json.dumps(r) + "\n")


if __name__ == "__main__":
    main(sys.argv[1:])
