#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Create/update an Amazon CloudWatch dashboard covering the Amazon MSK broker
# signals used to judge a client-tuning A/B run (and broker right-sizing) under
# sustained load. Can be run locally or on a client host (the host role has
# cloudwatch:PutDashboard).
#
# Covers the blog's broker metric table plus supporting signals, per broker:
#   - Disk pressure        : KafkaDataLogsDiskUsed %, RootDiskUsed %
#   - EBS back pressure     : VolumeQueueLength (I/O requests queued), BurstBalance %
#   - EBS throughput        : VolumeReadBytes / VolumeWriteBytes (bytes/interval)
#   - EBS IOPS              : VolumeReadOps / VolumeWriteOps
#   - Request handler idle  : RequestHandlerAvgIdlePercent   (0 => request threads saturated)
#   - Network proc idle     : NetworkProcessorAvgIdlePercent (0 => network threads saturated)
#   - CPU total             : CpuUser + CpuSystem (metric math)
#   - Memory                : MemoryUsed / MemoryFree
#   - Produce latency       : ProduceTotalTimeMsMean, ProduceLocalTimeMsMean
#   - Fetch latency         : FetchConsumerTotalTimeMsMean
#   - Throughput            : BytesInPerSec / BytesOutPerSec, MessagesInPerSec
#   - Format conversions    : ProduceMessageConversionsPerSec / FetchMessageConversionsPerSec (want 0)
#   - Consumer lag          : SumOffsetLag / MaxOffsetLag / EstimatedMaxTimeLag (auto-discovered groups)
#   - Replication health    : UnderReplicatedPartitions, UnderMinIsrPartitionCount
#   - Connections           : ClientConnectionCount, ConnectionCount, ConnectionCreationRate
#   - Partitions            : PartitionCount (incl. replicas)
#   - Traffic shaping       : BwIn / BwOut / Pps AllowanceExceeded
#
# Horizontal lines on the connection / partition widgets are EXAMPLE thresholds;
# override them for your broker size with CONN_COUNT_THRESHOLD,
# CONN_RATE_THRESHOLD and PARTITION_THRESHOLD.
#
# The Volume*, *AvgIdlePercent, Produce*/Fetch*TimeMsMean, *MessageConversionsPerSec
# and ConnectionCreationRate metrics are PER_BROKER level. The create script sets EnhancedMonitoring=PER_BROKER, so they are
# emitted. The rest are DEFAULT level (free).
#
# Usage: ./15-put-dashboard.sh [cluster-name] [region] [num-brokers] [dashboard-name] [profile]
#   profile is optional; omit it (or pass "") when running on a host so the
#   instance role is used.
set -euo pipefail

CLUSTER_NAME="${1:-load-benchmark}"
REGION="${2:-us-east-1}"
NUM_BROKERS="${3:-3}"
DASHBOARD_NAME="${4:-$CLUSTER_NAME}"
PROFILE="${5:-}"
CONN_COUNT_THRESHOLD="${CONN_COUNT_THRESHOLD:-20000}"
CONN_RATE_THRESHOLD="${CONN_RATE_THRESHOLD:-20}"
PARTITION_THRESHOLD="${PARTITION_THRESHOLD:-4000}"

AWSCW=(aws cloudwatch --region "$REGION")
if [[ -n "$PROFILE" ]]; then
  AWSCW+=(--profile "$PROFILE")
fi

BODY_FILE="$(mktemp)"
trap 'rm -f "$BODY_FILE"' EXIT

# Build the dashboard body with Python so the per-broker metric arrays and the
# SEARCH-based lag expressions are generated cleanly rather than hand-assembled.
python3 - "$CLUSTER_NAME" "$REGION" "$NUM_BROKERS" \
  "$CONN_COUNT_THRESHOLD" "$CONN_RATE_THRESHOLD" "$PARTITION_THRESHOLD" > "$BODY_FILE" <<'PY'
import json, sys

cluster, region, num_brokers = sys.argv[1], sys.argv[2], int(sys.argv[3])
conn_thr, rate_thr, part_thr = (float(v) for v in sys.argv[4:7])
brokers = [str(i) for i in range(1, num_brokers + 1)]
NS = "AWS/Kafka"

def broker_metrics(metric, stat="Average", period=60):
    """One line per broker for a per-broker metric."""
    out = []
    for b in brokers:
        out.append([NS, metric, "Cluster Name", cluster, "Broker ID", b,
                    {"label": f"broker-{b}", "stat": stat, "period": period}])
    return out

def widget(x, y, w, h, title, metrics, stat="Average", period=60, extra_props=None):
    props = {
        "region": region, "title": title, "view": "timeSeries",
        "stacked": False, "stat": stat, "period": period, "metrics": metrics,
    }
    if extra_props:
        props.update(extra_props)
    return {"type": "metric", "x": x, "y": y, "width": w, "height": h, "properties": props}

widgets = []

# Row 1: CPU total + memory. The headline "is the instance keeping up?" signals.
cpu_metrics = []
for b in brokers:
    cpu_metrics.append([NS, "CpuUser", "Cluster Name", cluster, "Broker ID", b,
                        {"id": f"u{b}", "visible": False, "stat": "Average", "period": 60}])
    cpu_metrics.append([NS, "CpuSystem", "Cluster Name", cluster, "Broker ID", b,
                        {"id": f"s{b}", "visible": False, "stat": "Average", "period": 60}])
for b in brokers:
    cpu_metrics.append([{"expression": f"u{b}+s{b}", "label": f"broker-{b} CPU%", "id": f"cpu{b}"}])
widgets.append(widget(0, 0, 12, 6, "CPU total % (user+system, per broker)", cpu_metrics,
                      extra_props={"yAxis": {"left": {"min": 0, "max": 100}}}))

mem_metrics = broker_metrics("MemoryUsed") + broker_metrics("MemoryFree")
widgets.append(widget(12, 0, 12, 6, "Memory used / free (bytes, per broker)", mem_metrics))

# Row 2: the two "am I thread-saturated?" signals. Idle % near 0 => saturated.
widgets.append(widget(0, 6, 12, 6,
                      "RequestHandlerAvgIdlePercent (per broker) -- low = request threads saturated",
                      broker_metrics("RequestHandlerAvgIdlePercent"),
                      extra_props={"yAxis": {"left": {"min": 0, "max": 1}}}))
widgets.append(widget(12, 6, 12, 6,
                      "NetworkProcessorAvgIdlePercent (per broker) -- low = network threads saturated",
                      broker_metrics("NetworkProcessorAvgIdlePercent"),
                      extra_props={"yAxis": {"left": {"min": 0, "max": 1}}}))

# Row 3: produce latency (mean total + local) and cluster throughput context.
lat_metrics = broker_metrics("ProduceTotalTimeMsMean") + broker_metrics("ProduceLocalTimeMsMean")
widgets.append(widget(0, 12, 12, 6, "Produce latency ms (Total + Local mean, per broker)", lat_metrics))

tput_metrics = broker_metrics("BytesInPerSec") + broker_metrics("BytesOutPerSec")
widgets.append(widget(12, 12, 12, 6, "Throughput BytesIn/BytesOut per sec (per broker)", tput_metrics))

# Row 4: EBS back pressure (queue depth + burst balance) and disk used %.
widgets.append(widget(0, 18, 12, 6,
                      "EBS back pressure: VolumeQueueLength (I/O queued, per broker)",
                      broker_metrics("VolumeQueueLength", stat="Average")))
disk_metrics = broker_metrics("KafkaDataLogsDiskUsed") + broker_metrics("RootDiskUsed")
widgets.append(widget(12, 18, 12, 6,
                      "Disk pressure: KafkaDataLogsDiskUsed % + RootDiskUsed % (per broker)",
                      disk_metrics, extra_props={"yAxis": {"left": {"min": 0, "max": 100}}}))

# Row 5: EBS throughput (bytes) and EBS IOPS (ops).
ebs_bytes = broker_metrics("VolumeWriteBytes", stat="Average") + broker_metrics("VolumeReadBytes", stat="Average")
widgets.append(widget(0, 24, 12, 6, "EBS throughput: VolumeWriteBytes / VolumeReadBytes (per broker)", ebs_bytes))
ebs_ops = broker_metrics("VolumeWriteOps", stat="Average") + broker_metrics("VolumeReadOps", stat="Average")
widgets.append(widget(12, 24, 12, 6, "EBS IOPS: VolumeWriteOps / VolumeReadOps (per broker)", ebs_ops))

# Row 6: EBS BurstBalance (only meaningful on gp2/small volumes) + consumer lag.
widgets.append(widget(0, 30, 12, 6,
                      "EBS BurstBalance % (per broker) -- N/A once baseline >= burst",
                      broker_metrics("BurstBalance"),
                      extra_props={"yAxis": {"left": {"min": 0, "max": 100}}}))

# Consumer lag: SEARCH across all groups for this cluster (group/topic names not
# known ahead of time -- each consumer host uses its own group).
lag_metrics = [
    [{"expression": f"SUM(SEARCH('{{{NS},\"Consumer Group\",\"Cluster Name\",Topic}} MetricName=\"SumOffsetLag\" \"Cluster Name\"=\"{cluster}\"', 'Average', 60))",
      "label": "Total SumOffsetLag (all groups)", "id": "e_sumlag"}],
    [{"expression": f"MAX(SEARCH('{{{NS},\"Consumer Group\",\"Cluster Name\",Topic}} MetricName=\"MaxOffsetLag\" \"Cluster Name\"=\"{cluster}\"', 'Maximum', 60))",
      "label": "Max single-partition lag", "id": "e_maxlag"}],
]
widgets.append(widget(12, 30, 12, 6, "Consumer lag (all groups)", lag_metrics))

def hline(value, label):
    return {"horizontal": [{"value": value, "label": label}]}

# Row 7: consumer time lag + replication health.
time_lag = [
    [{"expression": f"MAX(SEARCH('{{{NS},\"Consumer Group\",\"Cluster Name\",Topic}} MetricName=\"EstimatedMaxTimeLag\" \"Cluster Name\"=\"{cluster}\"', 'Maximum', 60))",
      "label": "EstimatedMaxTimeLag (s, worst topic)", "id": "e_timelag"}],
]
widgets.append(widget(0, 36, 12, 6, "Consumer time lag: EstimatedMaxTimeLag seconds (all groups)", time_lag))
repl = broker_metrics("UnderReplicatedPartitions", stat="Maximum") + \
       [[NS, "UnderMinIsrPartitionCount", "Cluster Name", cluster, "Broker ID", b,
         {"label": f"broker-{b} UnderMinIsr", "stat": "Maximum", "period": 60}] for b in brokers]
widgets.append(widget(12, 36, 12, 6, "UnderReplicatedPartitions / UnderMinIsrPartitionCount (per broker) -- want 0",
                      repl, stat="Maximum"))

def fmt(v):
    return f"{v:g}"

# Row 8: connection scaling signals (example thresholds drawn as lines).
widgets.append(widget(0, 42, 12, 6, f"ConnectionCount (per broker) -- example threshold {fmt(conn_thr)}",
                      broker_metrics("ConnectionCount", stat="Maximum"), stat="Maximum",
                      extra_props={"annotations": hline(conn_thr, "example threshold")}))
widgets.append(widget(12, 42, 12, 6, f"ConnectionCreationRate (per broker) -- example threshold {fmt(rate_thr)}/s",
                      broker_metrics("ConnectionCreationRate", stat="Maximum"), stat="Maximum",
                      extra_props={"annotations": hline(rate_thr, "example threshold")}))

# Row 9: partition count + network allowance (traffic shaping) counters.
widgets.append(widget(0, 48, 12, 6, f"PartitionCount (per broker, incl. replicas) -- example threshold {fmt(part_thr)}",
                      broker_metrics("PartitionCount", stat="Maximum"), stat="Maximum",
                      extra_props={"annotations": hline(part_thr, "example threshold")}))
allow = []
for m, short in (("BwInAllowanceExceeded", "bwIn"), ("BwOutAllowanceExceeded", "bwOut"),
                 ("PpsAllowanceExceeded", "pps")):
    allow += [[NS, m, "Cluster Name", cluster, "Broker ID", b,
               {"label": f"broker-{b} {short}", "stat": "Sum", "period": 60}] for b in brokers]
widgets.append(widget(12, 48, 12, 6,
                      "Network allowance exceeded: BwIn / BwOut / Pps (packets shaped, per broker) -- want 0",
                      allow, stat="Sum"))

# Row 10: message rate (records, not bytes -- batching shows up here) + consumer
# fetch latency (fetch.min.bytes / fetch.max.wait.ms show up here).
widgets.append(widget(0, 54, 12, 6, "MessagesInPerSec (per broker)",
                      broker_metrics("MessagesInPerSec")))
widgets.append(widget(12, 54, 12, 6,
                      "FetchConsumerTotalTimeMsMean (per broker) -- includes fetch.max.wait.ms",
                      broker_metrics("FetchConsumerTotalTimeMsMean")))

# Row 11: message format conversions (client/broker format mismatch costs broker
# CPU; want 0) + authenticated client connections. ClientConnectionCount has an
# extra "Client Authentication" dimension, so sum it per broker with SEARCH.
conv = broker_metrics("ProduceMessageConversionsPerSec") + \
       [[NS, "FetchMessageConversionsPerSec", "Cluster Name", cluster, "Broker ID", b,
         {"label": f"broker-{b} fetch", "stat": "Average", "period": 60}] for b in brokers]
widgets.append(widget(0, 60, 12, 6,
                      "ProduceMessageConversionsPerSec / FetchMessageConversionsPerSec (per broker) -- want 0",
                      conv))
client_conn = [
    [{"expression": f"SUM(SEARCH('{{{NS},\"Broker ID\",\"Client Authentication\",\"Cluster Name\"}} "
                    f"MetricName=\"ClientConnectionCount\" \"Cluster Name\"=\"{cluster}\" \"Broker ID\"=\"{b}\"', 'Maximum', 60))",
      "label": f"broker-{b}", "id": f"cc{b}"}]
    for b in brokers
]
widgets.append(widget(12, 60, 12, 6, "ClientConnectionCount (authenticated client connections, per broker)",
                      client_conn, stat="Maximum"))

print(json.dumps({"widgets": widgets}))
PY

echo "==> Putting dashboard '$DASHBOARD_NAME' in $REGION (cluster: $CLUSTER_NAME, $NUM_BROKERS brokers)..."
"${AWSCW[@]}" put-dashboard \
  --dashboard-name "$DASHBOARD_NAME" \
  --dashboard-body "file://$BODY_FILE" \
  --output json

echo
echo "Open it in the Amazon CloudWatch console: Dashboards > ${DASHBOARD_NAME} (region ${REGION})."
echo
echo "NOTE: Volume*, *AvgIdlePercent, Produce*/Fetch*TimeMsMean, *MessageConversionsPerSec"
echo "      and ConnectionCreationRate are PER_BROKER metrics. The create script sets"
echo "      EnhancedMonitoring=PER_BROKER so they are emitted; if you created the cluster"
echo "      elsewhere, set monitoring to PER_BROKER or they will show 'no data'."
