#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Summarize one steady-state window of an Amazon MSK cluster from Amazon CloudWatch.

Usage:
  measure_cluster.py <cluster-name> <label> <start-utc> <end-utc> [broker_ids_csv]
      [--region REGION] [--profile PROFILE] [--out FILE]

  start/end are ISO-8601 UTC, e.g. 2026-01-01T10:00:00Z; end must be after start.
  broker_ids_csv defaults to 1..NUM_BROKERS (env NUM_BROKERS, default 3).
  Region/profile default to the AWS SDK's own resolution (AWS_REGION, AWS_PROFILE, config).
  Requires boto3: python3 -m pip install -r requirements.txt

For each broker-level metric it takes the mean of the 1-minute Averages per broker, then
reports the mean across brokers AND the min/max broker (the worst broker is the min for the
*IdlePercent metrics and the max for latency/CPU/queue metrics). Throughput is summed across
brokers so runs can be compared at equal MessagesInPerSec. Consumer lag comes from a
CloudWatch Metrics Insights query. The full record (tagged "source": "measure_cluster") is
appended as one JSON line to --out (default: snapshots.jsonl next to this script), which is the
file compare.py reads.
"""
import argparse
import datetime as dt
import json
import os
import re
import statistics as st
import sys

import boto3
from botocore.exceptions import BotoCoreError, ClientError


# Arguments are validated so that only well-formed values reach the Amazon CloudWatch API calls and the
# Metrics Insights query string.
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


def _parse_utc(value):
    """Parse an ISO-8601 timestamp accepted by _PATTERNS; a missing offset means UTC."""
    v = value.replace("Z", "+00:00")
    v = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", v)
    v = re.sub(r"\.(\d+)", lambda m: "." + (m.group(1) + "000000")[:6], v)
    t = dt.datetime.fromisoformat(v)
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("cluster")
    p.add_argument("label")
    p.add_argument("start")
    p.add_argument("end")
    p.add_argument("broker_ids", nargs="?", default=None, help="comma-separated broker IDs")
    p.add_argument("--region", default=None)
    p.add_argument("--profile", default=None)
    p.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "snapshots.jsonl"))
    a = p.parse_args(argv)
    _validate(p, a)
    try:
        if _parse_utc(a.end) <= _parse_utc(a.start):
            p.error("end (%s) must be after start (%s)" % (a.end, a.start))
    except ValueError as e:
        p.error("invalid start/end timestamp: %s" % e)
    if a.broker_ids and not re.fullmatch(r"[0-9]+(,[0-9]+)*", a.broker_ids):
        p.error("broker_ids must be comma-separated integers")
    if a.broker_ids:
        a.ids = a.broker_ids.split(",")
    else:
        a.ids = [str(i) for i in range(1, int(os.environ.get("NUM_BROKERS", "3")) + 1)]
    return a


def main(argv):
    a = parse_args(argv)
    C, S, E, ids = a.cluster, a.start, a.end, a.ids
    cw = boto3.Session(region_name=a.region, profile_name=a.profile).client("cloudwatch")

    def gms(metric, b, stat="Average", extra=()):
        dims = [{"Name": "Cluster Name", "Value": C}, {"Name": "Broker ID", "Value": b}]
        dims += [{"Name": n, "Value": v} for n, v in extra]
        resp = cw.get_metric_statistics(Namespace="AWS/Kafka", MetricName=metric, Dimensions=dims,
                                        StartTime=S, EndTime=E, Period=60, Statistics=[stat])
        return [p[stat] for p in resp["Datapoints"]]

    def per_broker(m, stat="Average", extra=()):
        return {b: (st.mean(v) if (v := gms(m, b, stat, extra)) else None) for b in ids}

    # "source" lets compare.py tell these records apart from other JSON lines (for example snap.py output).
    r = {"source": "measure_cluster", "label": a.label, "cluster": C, "start": S, "end": E, "brokers": len(ids)}
    for m, k in [("RequestHandlerAvgIdlePercent", "rh"), ("NetworkProcessorAvgIdlePercent", "np"),
                 ("CpuUser", "cpu_user"), ("CpuSystem", "cpu_sys"), ("ProduceTotalTimeMsMean", "produce_ms"),
                 ("FetchConsumerTotalTimeMsMean", "fetch_ms"), ("VolumeQueueLength", "vql"),
                 ("ProduceThrottleTime", "produce_throttle_ms"), ("FetchThrottleTime", "fetch_throttle_ms"),
                 ("RequestThrottleTime", "request_throttle_ms")]:
        pb = per_broker(m)
        vals = [v for v in pb.values() if v is not None]
        r[k + "_mean"] = st.mean(vals) if vals else None
        r[k + "_min"] = min(vals) if vals else None
        r[k + "_max"] = max(vals) if vals else None
        if k == "rh":
            r["rh_per_broker"] = {b: round(v, 3) for b, v in pb.items() if v is not None}
    for m, k in [("BytesInPerSec", "bytes_in_MiBps"), ("BytesOutPerSec", "bytes_out_MiBps"),
                 ("MessagesInPerSec", "msgs_in_per_s")]:
        pb = per_broker(m)
        r[k] = sum(v for v in pb.values() if v) / (1048576 if "bytes" in k else 1)
    urp = per_broker("UnderReplicatedPartitions", "Maximum")
    r["under_replicated_max"] = max((v for v in urp.values() if v is not None), default=None)
    pc = per_broker("PartitionCount", "Maximum")
    r["partitions_per_broker_max"] = max((v for v in pc.values() if v), default=None)
    ic = per_broker("ClientConnectionCount", "Sum", (("Client Authentication", "IAM"),))
    r["iam_conns_mean_max_broker"] = max((v for v in ic.values() if v), default=None)

    def insights(metric):
        q = [{"Id": "a", "Period": 300, "Expression":
              f"SELECT MAX({metric}) FROM SCHEMA(\"AWS/Kafka\", \"Cluster Name\",\"Consumer Group\",Topic) "
              f"WHERE \"Cluster Name\" = '{C}' GROUP BY \"Consumer Group\", Topic ORDER BY MAX() DESC LIMIT 100"}]  # nosec B608 - CloudWatch Metrics Insights, not SQL; C is validated in parse_args
        values, n, token = [], 0, None
        try:
            while True:
                kw = {"MetricDataQueries": q, "StartTime": S, "EndTime": E}
                if token:
                    kw["NextToken"] = token
                resp = cw.get_metric_data(**kw)
                for x in resp["MetricDataResults"]:
                    n += 1
                    values.extend(x["Values"])
                token = resp.get("NextToken")
                if not token:
                    break
        except (BotoCoreError, ClientError) as e:
            print("WARN lag query failed:", str(e)[:200])
            return None, []
        return n, values

    n, lag = insights("SumOffsetLag")
    _, tl = insights("EstimatedMaxTimeLag")
    r["lag_pairs_reported"] = n
    r["sum_offset_lag_max"] = max(lag) if lag else None
    r["time_lag_max_s"] = max(tl) if tl else None
    r["time_lag_p50_s"] = st.median(tl) if tl else None

    print(json.dumps({k: (round(v, 3) if isinstance(v, float) else v) for k, v in r.items()
                      if k not in ("rh_per_broker", "source")},
                     indent=1))
    print("rh_per_broker", r.get("rh_per_broker"))
    with open(a.out, "a") as fh:
        fh.write(json.dumps(r) + "\n")


if __name__ == "__main__":
    main(sys.argv[1:])
