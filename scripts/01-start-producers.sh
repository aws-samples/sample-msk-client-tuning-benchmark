#!/bin/bash
# Run on the PRODUCER host (via run-on-host.sh). Starts one continuous
# kafka-producer-perf-test.sh per topic, sized so the AGGREGATE ingress hits the
# target MB/s. Runs the processes detached; use 03-stop-load.sh to stop.
#
# Usage:
#   ./01-start-producers.sh <cluster-arn> [topic-prefix] [num-topics] \
#       [target-mb-per-sec] [record-size] [linger-ms] [batch-size]
#
# Defaults: prefix load-bench, 12 topics, 180 MB/s aggregate, 1024-byte records.
# linger-ms / batch-size default to Kafka defaults (0 / 16384) unless passed;
# anything you pass here overrides client-tuning.properties for this run.
set -uo pipefail

CLUSTER_ARN="${1:?Usage: $0 <cluster-arn> [topic-prefix] [num-topics] [target-mb-per-sec] [record-size] [linger-ms] [batch-size]}"
PREFIX="${2:-load-bench}"
NUM_TOPICS="${3:-12}"
TARGET_MBPS="${4:-180}"     # aggregate MB/s across all topics
RECORD_SIZE="${5:-1024}"    # bytes
LINGER_MS="${6:-}"          # empty => Kafka default (0)
BATCH_SIZE="${7:-}"         # empty => Kafka default (16384)

KAFKA_HOME=/opt/kafka
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IAM_PROPS=/opt/kafka/config/client-iam.properties
TUNING_PROPS="$SCRIPT_DIR/client-tuning.properties"
RESULTS_DIR=/opt/load-benchmark/results
mkdir -p "$RESULTS_DIR"

# Merge the mandatory IAM auth props with the tunable overrides into one file
# the perf tool can consume. Tuning props come last so they win on any overlap.
MERGED_PROPS=/opt/kafka/config/producer-effective.properties
cat "$IAM_PROPS" > "$MERGED_PROPS"
if [[ -f "$TUNING_PROPS" ]]; then
  echo "" >> "$MERGED_PROPS"
  grep -Ev '^\s*#' "$TUNING_PROPS" | grep -Ev '^\s*$' >> "$MERGED_PROPS" || true
fi

# shellcheck source=lib-msk-env.sh
source "$SCRIPT_DIR/lib-msk-env.sh"

# Convert the aggregate MB/s target into records/sec per topic.
#   rec/s per topic = target_MB/s * 1048576 / record_size / num_topics
RPS_PER_TOPIC=$(python3 -c "print(round($TARGET_MBPS * 1048576 / $RECORD_SIZE / $NUM_TOPICS))")

echo "Fetching bootstrap brokers for $CLUSTER_ARN..."
BROKERS=$(aws kafka get-bootstrap-brokers --cluster-arn "$CLUSTER_ARN" \
  "${MSK_ENDPOINT_ARGS[@]}" \
  --query 'BootstrapBrokerStringSaslIam' --output text)
echo "$BROKERS" > /opt/kafka/bootstrap-brokers.txt
echo "Bootstrap brokers: $BROKERS"
echo "Target: ${TARGET_MBPS} MB/s aggregate over ${NUM_TOPICS} topics => ${RPS_PER_TOPIC} rec/s/topic (${RECORD_SIZE}B records)"

# Build the extra --producer-props overrides only when explicitly provided, so
# unset knobs stay at the client default / client-tuning.properties value.
PRODUCER_PROPS=(bootstrap.servers="$BROKERS" acks=all)
[[ -n "$LINGER_MS" ]] && PRODUCER_PROPS+=(linger.ms="$LINGER_MS")
[[ -n "$BATCH_SIZE" ]] && PRODUCER_PROPS+=(batch.size="$BATCH_SIZE")

PID_FILE=/opt/load-benchmark/load-pids.txt
: > "$PID_FILE"

# Effectively-unbounded record count so each producer is a continuous feed.
BIG_NUM=999999999999

for i in $(seq 1 "$NUM_TOPICS"); do
  TOPIC="${PREFIX}-${i}"
  echo "=== Starting producer for '$TOPIC' (~${RPS_PER_TOPIC} rec/s) ==="
  nohup "$KAFKA_HOME/bin/kafka-producer-perf-test.sh" \
    --topic "$TOPIC" \
    --num-records "$BIG_NUM" \
    --record-size "$RECORD_SIZE" \
    --throughput "$RPS_PER_TOPIC" \
    --producer-props "${PRODUCER_PROPS[@]}" \
    --producer.config "$MERGED_PROPS" \
    > "$RESULTS_DIR/producer-${TOPIC}.log" 2>&1 &
  PRODUCER_PID=$!
  echo "producer $TOPIC $PRODUCER_PID" >> "$PID_FILE"
  echo "  producer PID: $PRODUCER_PID (log: $RESULTS_DIR/producer-${TOPIC}.log)"
done

echo
TOTAL_RPS=$((RPS_PER_TOPIC * NUM_TOPICS))
echo "Started $NUM_TOPICS producers (~${TOTAL_RPS} rec/s aggregate, ~$(( TOTAL_RPS * RECORD_SIZE / 1024 / 1024 )) MB/s)."
echo "PIDs in $PID_FILE. Effective client config: $MERGED_PROPS"
echo "Stop with 03-stop-load.sh; check with 04-load-status.sh."
