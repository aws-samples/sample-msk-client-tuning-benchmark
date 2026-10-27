#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Read-only health check of an existing Amazon MSK provisioned cluster over a time window.

Usage:
  diagnose.py --cluster-arn ARN [--hours 3 | --start ISO --end ISO]
              [--region REGION] [--profile PROFILE] [--format text|markdown|json]

Example:
  python3 analysis/diagnose.py \\
      --cluster-arn arn:aws:kafka:us-east-1:111122223333:cluster/load-benchmark/abcd1234-ab12-cd34-ef56-abcdef123456-2 \\
      --hours 3 --format markdown > diagnose.md

It calls kafka:DescribeClusterV2 and kafka:ListNodes once each (broker type, instance type, broker count,
Kafka version, enhanced monitoring level, broker IDs) and cloudwatch:GetMetricData for the AWS/Kafka
per-broker metrics of the window (default: the last 3 hours, 1-minute data, so the window must start within
the last 15 days). Nothing is changed on the cluster. Run it against your non-production or performance-test
cluster while a representative load is running.

Each finding prints a status (OK / WARN / ATTENTION, or SKIP when a check cannot run), the measured value,
the threshold and where it comes from (an AWS documentation URL, or "heuristic"), and one line on what to do.
Output never contains the cluster ARN or the AWS account ID, only the cluster name.

Exit codes: 0 = ran, nothing needs attention (only OK / SKIP); 1 = ran, at least one WARN or ATTENTION;
2 = usage or input error, or the AWS calls failed (credentials, permissions, cluster not found).
"""
import argparse
import datetime as dt
import json
import math
import re
import statistics as st
import sys

# Same validation rules as _PATTERNS in measure_cluster.py (kept as a copy so that --help and argument
# errors work without boto3 installed). Only well-formed values reach the AWS API calls.
_PATTERNS = {
    "cluster": r"[A-Za-z0-9][A-Za-z0-9-]{0,63}",
    "start": r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?",
    "end": r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?",
    "region": r"[a-z]{2}(-[a-z]+)+-\d",
    "profile": r"[A-Za-z0-9._-]{1,64}",
}
# arn:<partition>:kafka:<region>:<12-digit account>:cluster/<cluster name>/<cluster UUID>
_ARN = re.compile(r"arn:aws[a-z-]*:kafka:(?P<region>%s):\d{12}:cluster/(?P<name>%s)/[A-Za-z0-9-]{1,128}"
                  % (_PATTERNS["region"], _PATTERNS["cluster"]))

OK, WARN, ATTENTION, SKIP = "OK", "WARN", "ATTENTION", "SKIP"

# ---------------------------------------------------------------------------------------------------------
# Thresholds. Every AWS value below was checked against the cited AWS documentation page; values that only
# come from a blog post, or that this tool chose, are labelled "heuristic".
# ---------------------------------------------------------------------------------------------------------
DOC_METRICS = "https://docs.aws.amazon.com/msk/latest/developerguide/metrics-details.html"
DOC_METRICS_EXPRESS = "https://docs.aws.amazon.com/msk/latest/developerguide/metrics-details-express.html"
DOC_BEST_PRACTICES = "https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html"
DOC_QUOTAS = "https://docs.aws.amazon.com/msk/latest/developerguide/limits.html"
DOC_GET_METRIC_DATA = "https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_GetMetricData.html"
BLOG_EXPRESS = "https://aws.amazon.com/blogs/big-data/simplifying-kafka-operations-with-amazon-msk-express-brokers/"

# Monitoring levels in increasing order. RequestHandlerAvgIdlePercent, NetworkProcessorAvgIdlePercent,
# ConnectionCreationRate, IAMTooManyConnections and ProduceMessageConversionsPerSec are PER_BROKER metrics;
# CpuUser/CpuSystem, BytesInPerSec, MessagesInPerSec, PartitionCount, ClientConnectionCount,
# UnderReplicatedPartitions and UnderMinIsrPartitionCount are DEFAULT metrics. Source: DOC_METRICS.
LEVELS = ["DEFAULT", "PER_BROKER", "PER_TOPIC_PER_BROKER", "PER_TOPIC_PER_PARTITION"]

# Heuristic (AWS Big Data Blog, "Simplifying Kafka operations with Amazon MSK Express brokers", BLOG_EXPRESS):
# alarm when RequestHandlerAvgIdlePercent < 0.4 and NetworkProcessorAvgIdlePercent < 0.5. These are blog
# alarm thresholds, not documented limits.
RH_IDLE_MIN = 0.40
NP_IDLE_MIN = 0.50

# AWS documentation (DOC_BEST_PRACTICES, "Monitor CPU usage"): keep CpuUser + CpuSystem under 60% per broker;
# the same page advises against partition reassignment above 70%.
CPU_MAX_PCT = 60.0
CPU_REASSIGN_PCT = 70.0

# Heuristic (this tool): an average on-wire message below 1 KiB means the broker handles many records per
# byte, so producer batching (linger.ms, batch.size) matters more than bytes per second.
SMALL_MSG_BYTES = 1024

# AWS documentation (DOC_BEST_PRACTICES, "Right-size your cluster: Number of partitions per Standard broker"):
# instance type -> (recommended partitions per broker incl. replicas, maximum that supports update operations).
PARTITIONS_STANDARD = {
    "kafka.t3.small": (300, 300),
    "kafka.m5.large": (1000, 1500), "kafka.m5.xlarge": (1000, 1500),
    "kafka.m5.2xlarge": (2000, 3000),
    "kafka.m5.4xlarge": (4000, 6000), "kafka.m5.8xlarge": (4000, 6000), "kafka.m5.12xlarge": (4000, 6000),
    "kafka.m5.16xlarge": (4000, 6000), "kafka.m5.24xlarge": (4000, 6000),
    "kafka.m7g.large": (1000, 1500), "kafka.m7g.xlarge": (1000, 1500),
    "kafka.m7g.2xlarge": (2000, 3000),
    "kafka.m7g.4xlarge": (4000, 6000), "kafka.m7g.8xlarge": (4000, 6000), "kafka.m7g.12xlarge": (4000, 6000),
    "kafka.m7g.16xlarge": (4000, 6000),
}
# AWS documentation (DOC_QUOTAS, "Express broker partition quota"): (recommended, maximum per broker).
PARTITIONS_EXPRESS = {
    "express.m7g.large": (1000, 1500), "express.m7g.xlarge": (1000, 2000),
    "express.m7g.2xlarge": (2500, 4000), "express.m7g.4xlarge": (6000, 8000),
    "express.m7g.8xlarge": (12000, 16000), "express.m7g.12xlarge": (16000, 24000),
    "express.m7g.16xlarge": (20000, 32000),
}

# AWS documentation (DOC_QUOTAS, Standard and Express broker quota tables): maximum TCP connections per broker
# with IAM access control = 3000 (monitor ClientConnectionCount, Client Authentication=IAM, Sum, 1-minute
# period); maximum TCP connection rate per broker with IAM = 100 per second for M5, M7g and Express sizes and
# 4 per second for t3 (monitor ConnectionCreationRate, Sum, 1-minute period). Not enforced for non-IAM auth.
IAM_MAX_CONNECTIONS = 3000
IAM_MAX_CONN_RATE = 100.0
IAM_MAX_CONN_RATE_T3 = 4.0
# Heuristic (this tool): warn when the busiest broker reaches 80% of an IAM connection quota.
QUOTA_WARN_FRACTION = 0.8

# AWS documentation (DOC_GET_METRIC_DATA): 1-minute data points are kept for 15 days; one GetMetricData
# request can return up to 500 metrics and 100,800 data points.
ONE_MINUTE_RETENTION = dt.timedelta(days=15)
MAX_QUERIES_PER_CALL = 500
MAX_POINTS_PER_CALL = 100800

LINK_TUNING = "docs/client-tuning.md"
TC = {
    1: "docs/test-cases/tc1-baseline-vs-tuned-low-density.md",
    3: "docs/test-cases/tc3-build-up-to-production-density.md",
    5: "docs/test-cases/tc5-production-shaped-head-to-head.md",
    7: "docs/test-cases/tc7-one-consumer-per-partition-iam-connections.md",
}


# ---------------------------------------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------------------------------------
def _parse_utc(value):
    """Parse an ISO-8601 timestamp accepted by _PATTERNS; a missing offset means UTC."""
    v = value.replace("Z", "+00:00")
    v = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", v)
    v = re.sub(r"\.(\d+)", lambda m: "." + (m.group(1) + "000000")[:6], v)
    t = dt.datetime.fromisoformat(v)
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)


def parse_args(argv, now=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--cluster-arn", required=True, help="ARN of the MSK provisioned cluster (read-only access)")
    p.add_argument("--hours", default=None,
                   help="window length ending now, whole hours 1-336 (default 3); not with --start/--end")
    p.add_argument("--start", default=None, help="window start, ISO-8601 UTC, e.g. 2026-01-01T10:00:00Z")
    p.add_argument("--end", default=None, help="window end, ISO-8601 UTC; must be after --start")
    p.add_argument("--region", default=None, help="AWS Region (default: the Region in the cluster ARN)")
    p.add_argument("--profile", default=None, help="AWS named profile (default: the SDK's normal resolution)")
    p.add_argument("--format", choices=("text", "markdown", "json"), default="text",
                   help="output format (default text); markdown and json never contain ARNs or account IDs")
    a = p.parse_args(argv)

    m = _ARN.fullmatch(a.cluster_arn)
    if not m:
        p.error("invalid --cluster-arn (expected arn:aws:kafka:<region>:<account>:cluster/<name>/<uuid>)")
    a.cluster_name = m.group("name")
    if a.region is None:
        a.region = m.group("region")
    for name in ("region", "profile", "start", "end"):
        value = getattr(a, name)
        if value is not None and not re.fullmatch(_PATTERNS[name], value):
            p.error("invalid --%s: %r" % (name, value))
    if not re.fullmatch(_PATTERNS["cluster"], a.cluster_name):
        p.error("invalid cluster name in --cluster-arn")

    now = now or dt.datetime.now(dt.timezone.utc)
    if a.start or a.end:
        if a.hours is not None:
            p.error("use either --hours or --start/--end, not both")
        if not (a.start and a.end):
            p.error("--start and --end must be given together")
        try:
            a.t_start, a.t_end = _parse_utc(a.start), _parse_utc(a.end)
        except ValueError as e:
            p.error("invalid --start/--end timestamp: %s" % e)
        if a.t_end <= a.t_start:
            p.error("--end (%s) must be after --start (%s)" % (a.end, a.start))
        if a.t_end > now:
            a.t_end = now
            if a.t_end <= a.t_start:
                p.error("--start is in the future")
    else:
        hours = "3" if a.hours is None else a.hours
        if not re.fullmatch(r"[0-9]{1,3}", hours) or not 1 <= int(hours) <= 336:
            p.error("--hours must be a whole number from 1 to 336 (14 days)")
        a.t_end = now.replace(second=0, microsecond=0)
        a.t_start = a.t_end - dt.timedelta(hours=int(hours))
    if a.t_start < now - ONE_MINUTE_RETENTION:
        p.error("the window must start within the last 15 days (1-minute CloudWatch data retention, see %s)"
                % DOC_GET_METRIC_DATA)
    return a


# ---------------------------------------------------------------------------------------------------------
# AWS calls (read-only)
# ---------------------------------------------------------------------------------------------------------
def describe(kafka, arn):
    info = kafka.describe_cluster_v2(ClusterArn=arn)["ClusterInfo"]
    if info.get("ClusterType") != "PROVISIONED" or "Provisioned" not in info:
        raise InputError("this tool supports Amazon MSK provisioned clusters only (cluster type %s)"
                         % info.get("ClusterType"))
    prov = info["Provisioned"]
    itype = prov.get("BrokerNodeGroupInfo", {}).get("InstanceType", "")
    iam = bool(prov.get("ClientAuthentication", {}).get("Sasl", {}).get("Iam", {}).get("Enabled"))
    return {
        "name": info.get("ClusterName"),
        "state": info.get("State"),
        "instance_type": itype,
        # Express broker sizes are named express.<family>.<size> (DOC_QUOTAS, Express broker quota tables).
        "broker_type": "Express" if itype.startswith("express.") else "Standard",
        "brokers": prov.get("NumberOfBrokerNodes"),
        "kafka_version": prov.get("CurrentBrokerSoftwareInfo", {}).get("KafkaVersion"),
        "monitoring": prov.get("EnhancedMonitoring", "DEFAULT"),
        "iam_auth": iam,
    }


def broker_ids(kafka, arn, count, notes):
    """Broker IDs from kafka:ListNodes; falls back to 1..count if that call is not allowed."""
    from botocore.exceptions import ClientError
    ids = []
    try:
        for page in kafka.get_paginator("list_nodes").paginate(ClusterArn=arn):
            for n in page.get("NodeInfoList", []):
                b = n.get("BrokerNodeInfo")
                if b and b.get("BrokerId") is not None:
                    ids.append(str(int(b["BrokerId"])))
    except ClientError as e:
        notes.append("kafka:ListNodes failed (%s); assuming broker IDs 1..%s."
                     % (e.response.get("Error", {}).get("Code", "error"), count))
    if not ids:
        ids = [str(i) for i in range(1, (count or 3) + 1)]
    return sorted(set(ids), key=int)


def fetch(cw, cluster, ids, start, end, specs):
    """specs: list of (key, metric, stat, extra_dims). Returns {(key, broker): [(timestamp, value), ...]}."""
    queries, index = [], {}
    for key, metric, stat, extra in specs:
        for b in ids:
            qid = "q%d" % len(queries)
            dims = [{"Name": "Cluster Name", "Value": cluster}, {"Name": "Broker ID", "Value": b}]
            dims += [{"Name": n, "Value": v} for n, v in extra]
            queries.append({"Id": qid, "ReturnData": True, "MetricStat": {
                "Metric": {"Namespace": "AWS/Kafka", "MetricName": metric, "Dimensions": dims},
                "Period": 60, "Stat": stat}})
            index[qid] = (key, b)
    points = max(1, int(math.ceil((end - start).total_seconds() / 60.0)))
    per_call = max(1, min(MAX_QUERIES_PER_CALL, MAX_POINTS_PER_CALL // points))
    out = {v: [] for v in index.values()}
    for i in range(0, len(queries), per_call):
        token = None
        while True:
            kw = {"MetricDataQueries": queries[i:i + per_call], "StartTime": start, "EndTime": end,
                  "ScanBy": "TimestampAscending"}
            if token:
                kw["NextToken"] = token
            resp = cw.get_metric_data(**kw)
            for r in resp.get("MetricDataResults", []):
                if r["Id"] in index:
                    out[index[r["Id"]]].extend(zip(r.get("Timestamps", []), r.get("Values", [])))
            token = resp.get("NextToken")
            if not token:
                break
    return out


# ---------------------------------------------------------------------------------------------------------
# Analysis helpers
# ---------------------------------------------------------------------------------------------------------
class InputError(Exception):
    pass


def _vals(data, key, b):
    return [v for _, v in data.get((key, b), [])]


def per_broker(data, key, ids, agg):
    """{broker: agg(values)} for brokers that reported data."""
    res = {}
    for b in ids:
        v = _vals(data, key, b)
        if v:
            res[b] = agg(v)
    return res


def idle_fraction(pb):
    """*IdlePercent metrics: AWS/Kafka reports them as a fraction (1.0 = fully idle); accept 0-100 too."""
    if pb and max(pb.values()) > 1.0:
        return {b: v / 100.0 for b, v in pb.items()}
    return pb


def cpu_per_broker(data, ids):
    """Per broker: (mean, peak) of the 1-minute CpuUser + CpuSystem sums."""
    res = {}
    for b in ids:
        user = dict(data.get(("cpu_user", b), []))
        system = dict(data.get(("cpu_sys", b), []))
        total = [user[t] + system[t] for t in user if t in system]
        if total:
            res[b] = (st.mean(total), max(total))
    return res


def worst(pb, lowest=True):
    if not pb:
        return None, None
    b = (min if lowest else max)(pb, key=pb.get)
    return b, pb[b]


def finding(fid, title, status, measured, threshold, source, action):
    return {"id": fid, "check": title, "status": status, "measured": measured, "threshold": threshold,
            "source": source, "action": action}


def pct(x):
    return "%.0f%%" % (x * 100.0)


def mib(x):
    return "%.1f MiB/s" % (x / 1048576.0)


# ---------------------------------------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------------------------------------
def at_least_per_broker(level):
    return level in LEVELS and LEVELS.index(level) >= LEVELS.index("PER_BROKER")


def build_specs(c):
    specs = [("cpu_user", "CpuUser", "Average", ()), ("cpu_sys", "CpuSystem", "Average", ()),
             ("bytes_in", "BytesInPerSec", "Average", ()), ("msgs_in", "MessagesInPerSec", "Average", ()),
             ("partitions", "PartitionCount", "Maximum", ()),
             ("iam_conns", "ClientConnectionCount", "Sum", (("Client Authentication", "IAM"),))]
    if c["broker_type"] == "Standard":
        specs += [("urp", "UnderReplicatedPartitions", "Maximum", ()),
                  ("under_min_isr", "UnderMinIsrPartitionCount", "Maximum", ())]
    if at_least_per_broker(c["monitoring"]):
        specs += [("rh", "RequestHandlerAvgIdlePercent", "Average", ()),
                  ("np", "NetworkProcessorAvgIdlePercent", "Average", ()),
                  ("conn_rate", "ConnectionCreationRate", "Sum", ()),
                  ("iam_too_many", "IAMTooManyConnections", "Sum", ())]
        if c["broker_type"] == "Standard":
            specs += [("conversions", "ProduceMessageConversionsPerSec", "Average", ())]
    return specs


def run_checks(c, ids, data):
    F = []
    express = c["broker_type"] == "Express"
    per_broker_level = at_least_per_broker(c["monitoring"])
    std_only = "Express brokers: this repository's thresholds target Standard brokers"

    # 1. Enhanced monitoring level
    if per_broker_level:
        F.append(finding(1, "Enhanced monitoring level", OK, c["monitoring"], "PER_BROKER or higher",
                         DOC_METRICS, "None needed; request-handler, network-processor and connection-rate "
                         "metrics are available."))
    else:
        F.append(finding(1, "Enhanced monitoring level", WARN, c["monitoring"], "PER_BROKER or higher",
                         DOC_METRICS, "Set enhanced monitoring to PER_BROKER (billable) on this test cluster, "
                         "wait for a window of load, and rerun; checks 2, 3, 7 (rate) and 8 are skipped. "
                         "See README.md#measurement-method."))

    rh = idle_fraction(per_broker(data, "rh", ids, st.mean))
    np_ = idle_fraction(per_broker(data, "np", ids, st.mean))
    cpu = cpu_per_broker(data, ids)
    bytes_in = per_broker(data, "bytes_in", ids, st.mean)
    msgs_in = per_broker(data, "msgs_in", ids, st.mean)

    # 2. RequestHandlerAvgIdlePercent, with BytesInPerSec per broker
    title = "RequestHandlerAvgIdlePercent (mean / worst broker)"
    thr = "worst broker >= %s (heuristic: blog alarm threshold %.1f)" % (pct(RH_IDLE_MIN), RH_IDLE_MIN)
    if not per_broker_level:
        F.append(finding(2, title, SKIP, "-", thr, BLOG_EXPRESS, "Needs PER_BROKER monitoring (check 1)."))
    elif not rh:
        F.append(finding(2, title, SKIP, "no data", thr, BLOG_EXPRESS,
                         "No datapoints in the window; confirm the cluster was ACTIVE and under load."))
    else:
        mean = st.mean(rh.values())
        wb, wv = worst(rh)
        wbytes = bytes_in.get(wb)
        measured = "mean %s, worst broker %s at %s (BytesInPerSec there %s)" % (
            pct(mean), wb, pct(wv), mib(wbytes) if wbytes is not None else "n/a")
        if express:
            F.append(finding(2, title, SKIP, measured, thr, BLOG_EXPRESS, std_only + "; not evaluated."))
        elif wv >= RH_IDLE_MIN:
            F.append(finding(2, title, OK, measured, thr, BLOG_EXPRESS,
                             "Headroom is fine; to find where it runs out, follow %s." % TC[3]))
        else:
            cpu_w = cpu.get(wb, (None, None))[0]
            if cpu_w is not None and cpu_w < CPU_MAX_PCT:
                why = ("request-bound: handler threads are busy while CPU is %.0f%% (< %.0f%%), so the broker "
                       "is serving many small requests, not too many bytes" % (cpu_w, CPU_MAX_PCT))
            else:
                why = "handler threads and CPU are both busy"
            status = ATTENTION if mean < RH_IDLE_MIN else WARN
            if status == WARN:
                why += "; only some brokers are low, so check partition-leader skew too"
            F.append(finding(2, title, status, measured + "; " + why, thr, BLOG_EXPRESS,
                             "Send fewer, fuller requests: compare your client config with "
                             "analysis/check_profile.py and %s (producer and consumer settings), then A/B it "
                             "as in %s." % (LINK_TUNING, TC[5])))

    # 3. NetworkProcessorAvgIdlePercent
    title = "NetworkProcessorAvgIdlePercent (mean / worst broker)"
    thr = "worst broker >= %s (heuristic: blog alarm threshold %.1f)" % (pct(NP_IDLE_MIN), NP_IDLE_MIN)
    if not per_broker_level:
        F.append(finding(3, title, SKIP, "-", thr, BLOG_EXPRESS, "Needs PER_BROKER monitoring (check 1)."))
    elif not np_:
        F.append(finding(3, title, SKIP, "no data", thr, BLOG_EXPRESS, "No datapoints in the window."))
    else:
        mean = st.mean(np_.values())
        wb, wv = worst(np_)
        measured = "mean %s, worst broker %s at %s" % (pct(mean), wb, pct(wv))
        if express:
            F.append(finding(3, title, SKIP, measured, thr, BLOG_EXPRESS, std_only + "; not evaluated."))
        elif wv >= NP_IDLE_MIN:
            F.append(finding(3, title, OK, measured, thr, BLOG_EXPRESS, "None needed."))
        else:
            F.append(finding(3, title, ATTENTION if mean < NP_IDLE_MIN else WARN, measured, thr, BLOG_EXPRESS,
                             "Network threads are busy with connections and requests: reduce connection churn "
                             "and request count (%s#settings-that-apply-to-both, %s)." % (LINK_TUNING, TC[7])))

    # 4. CPU (CpuUser + CpuSystem)
    title = "Broker CPU (CpuUser + CpuSystem, mean / worst broker)"
    thr = "worst broker mean < %.0f%% (AWS best practice)" % CPU_MAX_PCT
    if not cpu:
        F.append(finding(4, title, SKIP, "no data", thr, DOC_BEST_PRACTICES, "No CpuUser/CpuSystem datapoints."))
    else:
        means = {b: v[0] for b, v in cpu.items()}
        wb, wv = worst(means, lowest=False)
        peak_b, peak = max(((b, v[1]) for b, v in cpu.items()), key=lambda x: x[1])
        measured = "mean %.0f%%, worst broker %s at %.0f%%, highest 1-minute %.0f%% (broker %s)" % (
            st.mean(means.values()), wb, wv, peak, peak_b)
        if express:
            F.append(finding(4, title, SKIP, measured, thr, DOC_BEST_PRACTICES,
                             std_only + " (the 60% guidance is on the Standard broker best-practices page)."))
        elif wv >= CPU_MAX_PCT:
            extra = (" Above %.0f%%, AWS advises against partition reassignment as a fix." % CPU_REASSIGN_PCT
                     if wv >= CPU_REASSIGN_PCT else "")
            F.append(finding(4, title, ATTENTION, measured, thr, DOC_BEST_PRACTICES,
                             "Reduce request count first (zstd and batching, %s), then scale per the Monitor "
                             "CPU usage section of the best-practices page.%s" % (LINK_TUNING, extra)))
        elif peak >= CPU_MAX_PCT:
            F.append(finding(4, title, WARN, measured, thr, DOC_BEST_PRACTICES,
                             "Average is under %.0f%% but 1-minute peaks are not; check whether the peaks line "
                             "up with rebalances or reconnect storms (%s)." % (CPU_MAX_PCT, TC[5])))
        else:
            F.append(finding(4, title, OK, measured, thr, DOC_BEST_PRACTICES, "None needed."))

    # 5. Average message size
    title = "Average message size (BytesInPerSec / MessagesInPerSec)"
    thr = ">= %d bytes (heuristic: this tool)" % SMALL_MSG_BYTES
    total_b, total_m = sum(bytes_in.values()), sum(msgs_in.values())
    if total_m <= 0:
        F.append(finding(5, title, SKIP, "no messages in the window", thr, DOC_METRICS,
                         "Run the check while producers are sending."))
    else:
        size = total_b / total_m
        measured = "%.0f bytes per message (on-wire, after producer compression), %.0f msg/s, %s in" % (
            size, total_m, mib(total_b))
        if size < SMALL_MSG_BYTES:
            F.append(finding(5, title, WARN, measured, thr, "heuristic",
                             "Small messages mean many requests per byte: check linger.ms and batch.size "
                             "(%s#producer-settings) and measure as in %s." % (LINK_TUNING, TC[1])))
        else:
            F.append(finding(5, title, OK, measured, thr, "heuristic", "None needed."))

    # 6. PartitionCount per broker vs the documented recommendation
    table = PARTITIONS_EXPRESS if express else PARTITIONS_STANDARD
    src = DOC_QUOTAS if express else DOC_BEST_PRACTICES
    parts = per_broker(data, "partitions", ids, max)
    title = "PartitionCount per broker (incl. replicas, max in window)"
    limits = table.get(c["instance_type"])
    if not parts:
        F.append(finding(6, title, SKIP, "no data", "-", src, "No PartitionCount datapoints."))
    elif limits is None:
        wb, wv = worst(parts, lowest=False)
        F.append(finding(6, title, SKIP, "worst broker %s: %.0f" % (wb, wv), "-", src,
                         "Instance type %s is not in this tool's table; compare with the table on the cited "
                         "page yourself." % c["instance_type"]))
    else:
        rec, mx = limits
        wb, wv = worst(parts, lowest=False)
        thr = "<= %d recommended, %d maximum%s for %s (AWS documentation)" % (
            rec, mx, "" if express else " that supports update operations", c["instance_type"])
        measured = "worst broker %s: %.0f" % (wb, wv)
        if wv > mx:
            F.append(finding(6, title, ATTENTION, measured, thr, src,
                             "Reduce partitions (or replicas) per broker or scale out before updating the "
                             "cluster; see %s for how partition density drives request load." % TC[3]))
        elif wv > rec:
            F.append(finding(6, title, WARN, measured, thr, src,
                             "Above the recommendation: test this density with your clients (%s)." % TC[3]))
        else:
            F.append(finding(6, title, OK, measured, thr, src, "None needed."))

    # 7. IAM connections and connection creation rate
    title = "IAM connections per broker (ClientConnectionCount, Sum, 1-minute period)"
    thr = "< %d per broker (AWS quota)" % IAM_MAX_CONNECTIONS
    conns = per_broker(data, "iam_conns", ids, max)
    if not c["iam_auth"]:
        F.append(finding(7, title, SKIP, "IAM access control is not enabled", thr, DOC_QUOTAS,
                         "Amazon MSK does not enforce connection limits for non-IAM authentication; watch CPU "
                         "and memory instead."))
    else:
        if not conns:
            F.append(finding(7, title, SKIP, "no data", thr, DOC_QUOTAS, "No IAM client connections reported."))
        else:
            wb, wv = worst(conns, lowest=False)
            measured = "peak %.0f on broker %s" % (wv, wb)
            st_ = ATTENTION if wv >= IAM_MAX_CONNECTIONS else (
                WARN if wv >= QUOTA_WARN_FRACTION * IAM_MAX_CONNECTIONS else OK)
            F.append(finding(7, title, st_, measured, thr + "; WARN at %d%% (heuristic)" %
                             (QUOTA_WARN_FRACTION * 100), DOC_QUOTAS,
                             "None needed." if st_ == OK else
                             "Fewer connections per broker: fewer consumer groups or members, shared producers "
                             "(%s#beyond-client-settings, %s)." % (LINK_TUNING, TC[7])))
        title_r = "IAM connection creation rate per broker (ConnectionCreationRate, Sum, 1-minute period)"
        rate_max = IAM_MAX_CONN_RATE_T3 if ".t3." in c["instance_type"] else IAM_MAX_CONN_RATE
        thr_r = "< %.0f per second per broker (AWS quota)" % rate_max
        rates = per_broker(data, "conn_rate", ids, max)
        too_many = per_broker(data, "iam_too_many", ids, max)
        if not per_broker_level:
            F.append(finding(7, title_r, SKIP, "-", thr_r, DOC_QUOTAS, "Needs PER_BROKER monitoring (check 1)."))
        elif not rates:
            F.append(finding(7, title_r, SKIP, "no data", thr_r, DOC_QUOTAS, "No ConnectionCreationRate datapoints."))
        else:
            wb, wv = worst(rates, lowest=False)
            tm = max(too_many.values()) if too_many else 0
            measured = "peak %.1f/s on broker %s; IAMTooManyConnections peak %.0f" % (wv, wb, tm)
            st_ = ATTENTION if (wv >= rate_max or tm > 0) else (
                WARN if wv >= QUOTA_WARN_FRACTION * rate_max else OK)
            F.append(finding(7, title_r, st_, measured, thr_r + "; IAMTooManyConnections = 0", DOC_QUOTAS,
                             "None needed." if st_ == OK else
                             "Stop reconnect storms: keep connections.max.idle.ms high, set reconnect.backoff.ms "
                             "(for example 1000), and avoid restarting many clients at once (%s#settings-that-"
                             "apply-to-both, %s)." % (LINK_TUNING, TC[7])))

    # 8. ProduceMessageConversionsPerSec
    title = "ProduceMessageConversionsPerSec (any broker)"
    thr = "0 (a conversion means the broker rewrites the batch)"
    conv = per_broker(data, "conversions", ids, max)
    if express:
        F.append(finding(8, title, SKIP, "-", thr, DOC_METRICS_EXPRESS, "Not emitted for Express brokers."))
    elif not per_broker_level:
        F.append(finding(8, title, SKIP, "-", thr, DOC_METRICS, "Needs PER_BROKER monitoring (check 1)."))
    elif not conv:
        F.append(finding(8, title, OK, "no datapoints (no conversions reported)", thr, DOC_METRICS, "None needed."))
    else:
        wb, wv = worst(conv, lowest=False)
        if wv > 0:
            F.append(finding(8, title, ATTENTION, "peak %.1f/s on broker %s" % (wv, wb), thr, DOC_METRICS,
                             "Align the topic compression.type with the producers (keep it at 'producer') and "
                             "upgrade old clients (%s#producer-settings)." % LINK_TUNING))
        else:
            F.append(finding(8, title, OK, "0", thr, DOC_METRICS, "None needed."))

    # 9. Under-replicated and under-min-ISR partitions
    title = "UnderReplicatedPartitions / UnderMinIsrPartitionCount (max in window)"
    thr = "0 on every broker"
    if express:
        F.append(finding(9, title, SKIP, "-", thr, DOC_METRICS_EXPRESS, "Not emitted for Express brokers."))
    else:
        urp = per_broker(data, "urp", ids, max)
        umi = per_broker(data, "under_min_isr", ids, max)
        if not urp and not umi:
            F.append(finding(9, title, SKIP, "no data", thr, DOC_METRICS, "No datapoints in the window."))
        else:
            u, m = max(urp.values(), default=0), max(umi.values(), default=0)
            measured = "under-replicated %.0f, under-min-ISR %.0f" % (u, m)
            if u > 0 or m > 0:
                F.append(finding(9, title, ATTENTION, measured, thr, DOC_METRICS,
                                 "Replication is falling behind or a broker was unavailable; do not trust a "
                                 "benchmark from this window (README.md#benchmark-integrity-rules) and check "
                                 "broker load and recent maintenance."))
            else:
                F.append(finding(9, title, OK, measured, thr, DOC_METRICS, "None needed."))

    # Per-broker detail table
    rows = []
    for b in ids:
        rows.append({
            "broker": b,
            "rh_idle": rh.get(b), "np_idle": np_.get(b),
            "cpu_pct": cpu[b][0] if b in cpu else None,
            "bytes_in_MiBps": bytes_in[b] / 1048576.0 if b in bytes_in else None,
            "msgs_in_per_s": msgs_in.get(b),
            "avg_msg_bytes": (bytes_in[b] / msgs_in[b]) if msgs_in.get(b) and b in bytes_in else None,
            "partitions": parts.get(b), "iam_conns_peak": conns.get(b),
        })
    return F, rows


def next_steps(c, findings):
    steps = ["Compare your client configuration with the aws-tuned profile: "
             "`python3 analysis/check_profile.py <your-client.properties>` (see docs/check-profile.md)."]
    ids = {f["id"] for f in findings if f["status"] in (WARN, ATTENTION)}
    if ids & {2, 4, 5}:
        steps.append("Measure the effect on a test cluster: baseline vs aws-tuned at equal MessagesInPerSec "
                     "(%s, %s)." % (TC[1], TC[5]))
    if ids & {3, 7}:
        steps.append("Reproduce the connection pattern: %s." % TC[7])
    if 6 in ids:
        steps.append("Reproduce the partition density: %s." % TC[3])
    if c["broker_type"] == "Express":
        steps.append("Express brokers: this repository's thresholds and measurements target Standard brokers; "
                     "treat the skipped checks as not evaluated.")
    steps.append("All test cases: docs/test-cases/README.md.")
    return steps


# ---------------------------------------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------------------------------------
def _fmt(v, f="%.1f"):
    return "-" if v is None else f % v


def detail_cells(r):
    return [r["broker"], _fmt(r["rh_idle"] * 100 if r["rh_idle"] is not None else None, "%.0f%%"),
            _fmt(r["np_idle"] * 100 if r["np_idle"] is not None else None, "%.0f%%"),
            _fmt(r["cpu_pct"], "%.0f%%"), _fmt(r["bytes_in_MiBps"]), _fmt(r["msgs_in_per_s"], "%.0f"),
            _fmt(r["avg_msg_bytes"], "%.0f"), _fmt(r["partitions"], "%.0f"), _fmt(r["iam_conns_peak"], "%.0f")]


DETAIL_HEAD = ["Broker", "RH idle", "NP idle", "CPU", "BytesIn MiB/s", "Msgs/s", "Avg msg B", "Partitions",
               "IAM conns"]


def header(c, a):
    return [("Cluster", c["name"]), ("State", c["state"]),
            ("Brokers", "%s x %s (%s)" % (c["brokers"], c["instance_type"], c["broker_type"])),
            ("Kafka version", c["kafka_version"]), ("Enhanced monitoring", c["monitoring"]),
            ("IAM access control", "enabled" if c["iam_auth"] else "not enabled"),
            ("Window (UTC)", "%s to %s" % (a.t_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                           a.t_end.strftime("%Y-%m-%dT%H:%M:%SZ")))]


def render_text(c, a, F, rows, steps, notes):
    out = ["Amazon MSK diagnose (read-only)"]
    out += ["  %-20s %s" % (k + ":", v) for k, v in header(c, a)]
    out += ["  Note: " + n for n in notes]
    out.append("")
    for f in F:
        out.append("[%-9s] %d. %s" % (f["status"], f["id"], f["check"]))
        out.append("            measured:  %s" % f["measured"])
        out.append("            threshold: %s" % f["threshold"])
        out.append("            source:    %s" % f["source"])
        out.append("            do:        %s" % f["action"])
    out.append("")
    out.append("Per-broker means over the window")
    table = [DETAIL_HEAD] + [detail_cells(r) for r in rows]
    widths = [max(len(str(r[i])) for r in table) for i in range(len(DETAIL_HEAD))]
    for r in table:
        out.append("  " + "  ".join(str(x).rjust(w) for x, w in zip(r, widths)))
    out.append("")
    out.append("Next steps")
    out += ["  - " + s for s in steps]
    return "\n".join(out)


def _md(s):
    return str(s).replace("|", "\\|")


def render_markdown(c, a, F, rows, steps, notes):
    out = ["## Amazon MSK diagnose: `%s`" % c["name"], ""]
    out += ["- **%s:** %s" % (k, v) for k, v in header(c, a)[1:]]
    out += ["- **Note:** %s" % n for n in notes]
    out += ["", "| Status | # | Check | Measured | Threshold (source) | What to do |",
            "|---|---|---|---|---|---|"]
    for f in F:
        src = "" if f["source"] == "heuristic" else " ([source](%s))" % f["source"]
        out.append("| **%s** | %d | %s | %s | %s%s | %s |" % (
            f["status"], f["id"], _md(f["check"]), _md(f["measured"]), _md(f["threshold"]), src,
            _md(f["action"])))
    out += ["", "Per-broker means over the window:", "",
            "| " + " | ".join(DETAIL_HEAD) + " |", "|" + "---|" * len(DETAIL_HEAD)]
    out += ["| " + " | ".join(detail_cells(r)) + " |" for r in rows]
    out += ["", "**Next steps**", ""] + ["- " + s for s in steps]
    return "\n".join(out)


def render_json(c, a, F, rows, steps, notes):
    return json.dumps({"cluster": {k: v for k, v in c.items()},
                       "window": {"start": a.t_start.isoformat(), "end": a.t_end.isoformat()},
                       "notes": notes, "findings": F, "per_broker": rows, "next_steps": steps}, indent=1)


def scrub(text):
    """Remove ARNs and AWS account IDs from AWS error messages before printing them."""
    text = re.sub(r"arn:aws[a-z-]*:[^\s'\"]+", "<arn>", str(text))
    return re.sub(r"\b\d{12}\b", "<account>", text)


def main(argv):
    a = parse_args(argv)
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:
        print("error: boto3 is required: python3 -m pip install -r analysis/requirements.txt", file=sys.stderr)
        return 2
    notes = []
    try:
        session = boto3.Session(region_name=a.region, profile_name=a.profile)
        kafka = session.client("kafka")
        c = describe(kafka, a.cluster_arn)
        if c["name"] != a.cluster_name or not re.fullmatch(_PATTERNS["cluster"], c["name"] or ""):
            raise InputError("unexpected cluster name in the DescribeClusterV2 response")
        ids = broker_ids(kafka, a.cluster_arn, c["brokers"], notes)
        if c["state"] != "ACTIVE":
            notes.append("cluster state is %s; metrics may be incomplete." % c["state"])
        data = fetch(session.client("cloudwatch"), c["name"], ids, a.t_start, a.t_end, build_specs(c))
    except InputError as e:
        print("error: %s" % e, file=sys.stderr)
        return 2
    except ClientError as e:
        err = e.response.get("Error", {})
        print("error: AWS call failed: %s: %s" % (err.get("Code", "error"), scrub(err.get("Message", ""))[:200]),
              file=sys.stderr)
        return 2
    except BotoCoreError as e:
        print("error: AWS SDK error: %s" % scrub(e)[:200], file=sys.stderr)
        return 2

    F, rows = run_checks(c, ids, data)
    steps = next_steps(c, F)
    render = {"text": render_text, "markdown": render_markdown, "json": render_json}[a.format]
    print(render(c, a, F, rows, steps, notes))
    return 1 if any(f["status"] in (WARN, ATTENTION) for f in F) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
