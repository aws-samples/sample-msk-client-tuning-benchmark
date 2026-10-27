#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Run on a CONSUMER host (via run-on-host.sh). Starts one SlowConsumer.java per
# topic, all under a SINGLE consumer group for this host -- same fan-out model as
# 02-start-consumers.sh, but each consumer sleeps between polls to mimic a
# micro-batch engine (for example Spark Structured Streaming) that spends most of
# its time processing between poll() calls.
#
# Unlike 02, this script APPENDS to the PID file instead of truncating it, so you
# can add slow consumers to a host that is already running producers/consumers
# without losing the ability to stop them.
#
# Usage:
#   ./06-start-slow-consumers.sh <cluster-arn> [topic-prefix] [num-topics] \
#       [group-name] [sleep-ms]
#
# Defaults: prefix load-bench, 12 topics, group "group-<hostname>", sleep 3000 ms.
# All Kafka client knobs (max.poll.records, max.poll.interval.ms, enable.auto.commit,
# fetch.*, partition.assignment.strategy, ...) come from client-tuning.properties
# layered over the IAM auth file -- this script does NOT override them.
set -uo pipefail

CLUSTER_ARN="${1:?Usage: $0 <cluster-arn> [topic-prefix] [num-topics] [group-name] [sleep-ms]}"
PREFIX="${2:-load-bench}"
NUM_TOPICS="${3:-12}"
GROUP_NAME="${4:-group-$(hostname)}"
SLEEP_MS="${5:-3000}"        # simulated micro-batch processing time per poll

KAFKA_HOME=/opt/kafka
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IAM_PROPS=/opt/kafka/config/client-iam.properties
TUNING_PROPS="$SCRIPT_DIR/client-tuning.properties"
JAVA_SRC="$SCRIPT_DIR/SlowConsumer.java"
RESULTS_DIR=/opt/load-benchmark/results
PID_FILE=/opt/load-benchmark/load-pids.txt
# The Kafka client jars AND aws-msk-iam-auth live here on the benchmark hosts.
CLASSPATH_GLOB="$KAFKA_HOME/libs/*"
REPORT_INTERVAL_MS=60000

mkdir -p "$RESULTS_DIR"
mkdir -p "$(dirname "$PID_FILE")"

if ! command -v java >/dev/null 2>&1; then
  echo "ERROR: 'java' not found on PATH -- SlowConsumer.java needs a JDK 17+ (single-file source mode)." >&2
  exit 1
fi
if [[ ! -f "$JAVA_SRC" ]]; then
  echo "ERROR: $JAVA_SRC not found (did run-on-host.sh sync scripts/ ?)." >&2
  exit 1
fi

# Merge mandatory IAM auth with the tunable overrides, exactly like 02. Tuning
# props come last so they win on any overlap. No per-run consumer overrides here
# on purpose: the point of the experiment is that only the sleep changes.
MERGED_PROPS=/opt/kafka/config/consumer-effective.properties
cat "$IAM_PROPS" > "$MERGED_PROPS"
if [[ -f "$TUNING_PROPS" ]]; then
  echo "" >> "$MERGED_PROPS"
  grep -Ev '^\s*#' "$TUNING_PROPS" | grep -Ev '^\s*$' >> "$MERGED_PROPS" || true
fi

# shellcheck source=lib-msk-env.sh
source "$SCRIPT_DIR/lib-msk-env.sh"

echo "Fetching bootstrap brokers for $CLUSTER_ARN..."
BROKERS=$(aws kafka get-bootstrap-brokers --cluster-arn "$CLUSTER_ARN" \
  "${MSK_ENDPOINT_ARGS[@]}" \
  --query 'BootstrapBrokerStringSaslIam' --output text)
echo "$BROKERS" > /opt/kafka/bootstrap-brokers.txt
echo "Bootstrap brokers: $BROKERS"
echo "Consumer group for this host: $GROUP_NAME"

# Expected ceiling per consumer: at most max.poll.records per loop iteration, and
# each iteration costs at least sleep-ms. Read the value that actually took effect.
MAX_POLL_RECORDS=$(grep -E '^\s*max\.poll\.records\s*=' "$MERGED_PROPS" | tail -n 1 | cut -d= -f2 | tr -d '[:space:]')
MAX_POLL_RECORDS="${MAX_POLL_RECORDS:-500}"
EST_RPS=$(( MAX_POLL_RECORDS * 1000 / (SLEEP_MS > 0 ? SLEEP_MS : 1) ))

echo "Simulated micro-batch processing time: ${SLEEP_MS} ms per poll (effective --sleep-ms)"
echo "WARNING: expect unbounded lag -- ceiling is ~${EST_RPS} rec/s per consumer" \
     "(max.poll.records=${MAX_POLL_RECORDS} / ${SLEEP_MS}ms) vs ~15360 rec/s/topic produced," \
     "so this group falls behind and will start reading cold data from disk."

for i in $(seq 1 "$NUM_TOPICS"); do
  TOPIC="${PREFIX}-${i}"
  LOG="$RESULTS_DIR/slowconsumer-${GROUP_NAME}-${TOPIC}.log"
  echo "=== Starting slow consumer for '$TOPIC' (group $GROUP_NAME, sleep ${SLEEP_MS}ms) ==="
  nohup java -cp "$CLASSPATH_GLOB" "$JAVA_SRC" \
    --bootstrap "$BROKERS" \
    --topic "$TOPIC" \
    --group "$GROUP_NAME" \
    --config "$MERGED_PROPS" \
    --sleep-ms "$SLEEP_MS" \
    --report-interval-ms "$REPORT_INTERVAL_MS" \
    > "$LOG" 2>&1 &
  SLOW_PID=$!
  # Format must stay "<role> <topic> <pid>": 03-stop-load.sh / 04-load-status.sh
  # parse it with `while read -r role topic pid`.
  echo "slowconsumer $TOPIC $SLOW_PID" >> "$PID_FILE"
  echo "  slow consumer PID: $SLOW_PID (log: $LOG)"
done

echo
echo "Started $NUM_TOPICS slow consumers under group '$GROUP_NAME' (sleep ${SLEEP_MS} ms)."
echo "PIDs appended to $PID_FILE. Effective client config: $MERGED_PROPS"
echo "Logs are CSV: time, topic, group, records, bytes, records_per_sec, MB_per_sec, polls, empty_polls, max_poll_gap_ms, sleep_ms"
echo "Watch max_poll_gap_ms against max.poll.interval.ms to see eviction risk."
echo "Stop with 03-stop-load.sh; check with 04-load-status.sh."
