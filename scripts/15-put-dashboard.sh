#!/bin/bash
# Create/update a CloudWatch dashboard covering the broker health signals that
# matter when observing an instance type under sustained load. Can be run
# locally or on a client host (the host role has cloudwatch:PutDashboard).
#
# Covers exactly what the operator asked for, per broker:
#   - Disk pressure        : KafkaDataLogsDiskUsed %, RootDiskUsed %
#   - EBS back pressure     : VolumeQueueLength (I/O requests queued), BurstBalance %
#   - EBS throughput        : VolumeReadBytes / VolumeWriteBytes (bytes/interval)
#   - EBS IOPS              : VolumeReadOps / VolumeWriteOps
#   - Request handler idle  : RequestHandlerAvgIdlePercent   (0 => request threads saturated)
#   - Network proc idle     : NetworkProcessorAvgIdlePercent (0 => network threads saturated)
#   - CPU total             : CpuUser + CpuSystem (metric math)
#   - Memory                : MemoryUsed / MemoryFree
#   - Produce latency       : ProduceTotalTimeMsMean, ProduceLocalTimeMsMean
#   - Throughput            : BytesInPerSec / BytesOutPerSec (context for the above)
#   - Consumer lag          : SumOffsetLag / MaxOffsetLag (auto-discovered groups)
#
# The Volume*, *AvgIdlePercent, and Produce*TimeMsMean metrics are PER_BROKER
# level. The create script sets EnhancedMonitoring=PER_BROKER, so they are
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

AWSCW=(aws cloudwatch --region "$REGION")
if [[ -n "$PROFILE" ]]; then
  AWSCW+=(--profile "$PROFILE")
fi

BODY_FILE="$(mktemp)"
trap 'rm -f "$BODY_FILE"' EXIT

# Build the dashboard body with Python so the per-broker metric arrays and the
# SEARCH-based lag expressions are generated cleanly rather than hand-assembled.
python3 - "$CLUSTER_NAME" "$REGION" "$NUM_BROKERS" > "$BODY_FILE" <<'PY'
import json, sys

cluster, region, num_brokers = sys.argv[1], sys.argv[2], int(sys.argv[3])
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

print(json.dumps({"widgets": widgets}))
PY

echo "==> Putting dashboard '$DASHBOARD_NAME' in $REGION (cluster: $CLUSTER_NAME, $NUM_BROKERS brokers)..."
"${AWSCW[@]}" put-dashboard \
  --dashboard-name "$DASHBOARD_NAME" \
  --dashboard-body "file://$BODY_FILE" \
  --output json

echo
echo "Dashboard URL:"
echo "  https://${REGION}.console.aws.amazon.com/cloudwatch/home?region=${REGION}#dashboards:name=${DASHBOARD_NAME}"
echo
echo "NOTE: Volume*, *AvgIdlePercent, and Produce*TimeMsMean are PER_BROKER metrics."
echo "      The create script sets EnhancedMonitoring=PER_BROKER so they are emitted;"
echo "      if you created the cluster elsewhere, set monitoring to PER_BROKER or they"
echo "      will show 'no data'."
