#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Run on the PRODUCER host (via run-on-host.sh). Starts ONE MultiProducer JVM that
# runs N independent KafkaProducer instances sharing a fixed TOTAL record rate --
# the "many microservices" pattern, where hundreds of services each produce a small
# share of the traffic.
# Same total load as 01-start-producers.sh, split across many more clients.
#
# APPENDS to the PID file; 03-stop-load.sh stops it with everything else.
#
# Usage:
#   ./08-start-multi-producers.sh <cluster-arn> [topic-prefix] [num-topics] \
#       [target-mb-per-sec] [record-size] [num-producers] [client-prefix]
# Defaults: load-bench, 12 topics, 119 MB/s, 625-byte records, 120 producers, mp.
# Client tuning (linger.ms, batch.size, compression.type, buffer.memory, acks...)
# comes from client-tuning.properties layered over the IAM auth file.
set -uo pipefail

CLUSTER_ARN="${1:?Usage: $0 <cluster-arn> [topic-prefix] [num-topics] [target-mb-per-sec] [record-size] [num-producers] [client-prefix]}"
PREFIX="${2:-load-bench}"
NUM_TOPICS="${3:-12}"
TARGET_MBPS="${4:-119}"
RECORD_SIZE="${5:-625}"
NUM_PRODUCERS="${6:-120}"
CLIENT_PREFIX="${7:-mp}"

KAFKA_HOME=/opt/kafka
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IAM_PROPS=/opt/kafka/config/client-iam.properties
TUNING_PROPS="$SCRIPT_DIR/client-tuning.properties"
JAVA_SRC="$SCRIPT_DIR/MultiProducer.java"
RESULTS_DIR=/opt/load-benchmark/results
PID_FILE=/opt/load-benchmark/load-pids.txt
mkdir -p "$RESULTS_DIR" "$(dirname "$PID_FILE")"

command -v java >/dev/null 2>&1 || { echo "ERROR: 'java' not found on PATH (need JDK 17+)." >&2; exit 1; }
[[ -f "$JAVA_SRC" ]] || { echo "ERROR: $JAVA_SRC not found (did run-on-host.sh sync scripts/ ?)." >&2; exit 1; }

# shellcheck source=lib-msk-env.sh
source "$SCRIPT_DIR/lib-msk-env.sh"
# Refuse a tuning profile that sets authentication keys (see lib-msk-env.sh).
msk_check_tuning_props "$TUNING_PROPS"

MERGED_PROPS=/opt/kafka/config/multiproducer-effective.properties
cat "$IAM_PROPS" > "$MERGED_PROPS"
if [[ -f "$TUNING_PROPS" ]]; then
  echo "" >> "$MERGED_PROPS"
  grep -Ev '^\s*#' "$TUNING_PROPS" | grep -Ev '^\s*$' >> "$MERGED_PROPS" || true
fi

BROKERS=$(aws kafka get-bootstrap-brokers --cluster-arn "$CLUSTER_ARN" \
  "${MSK_ENDPOINT_ARGS[@]}" --query 'BootstrapBrokerStringSaslIam' --output text)
if [[ -z "$BROKERS" || "$BROKERS" == "None" ]]; then
  echo "ERROR: could not get SASL/IAM bootstrap brokers for $CLUSTER_ARN" >&2; exit 1
fi
echo "$BROKERS" > /opt/kafka/bootstrap-brokers.txt

TOPICS=$(seq -s, -f "${PREFIX}-%g" 1 "$NUM_TOPICS")
TOTAL_RPS=$(python3 -c "print(round($TARGET_MBPS * 1048576 / $RECORD_SIZE))")
# buffer.memory is per producer; size heap for N x buffer.memory (default 32 MB) + slack.
BUF=$(grep -E '^\s*buffer\.memory\s*=' "$MERGED_PROPS" | tail -n1 | cut -d= -f2 | tr -d '[:space:]')
BUF="${BUF:-33554432}"
HEAP_MB=$(python3 -c "print(max(2048, int($NUM_PRODUCERS * $BUF / 1048576 * 1.3) + 1024))")

LOG="$RESULTS_DIR/multiproducer-${CLIENT_PREFIX}.log"
echo "Starting $NUM_PRODUCERS producers (${CLIENT_PREFIX}-0..$((NUM_PRODUCERS-1))) on $NUM_TOPICS topics:" \
     "total ${TOTAL_RPS} rec/s (${TARGET_MBPS} MB/s, ${RECORD_SIZE} B), heap ${HEAP_MB}M"
nohup java -Xmx${HEAP_MB}m -cp "$KAFKA_HOME/libs/*" "$JAVA_SRC" \
  --bootstrap "$BROKERS" --topics "$TOPICS" --producers "$NUM_PRODUCERS" \
  --total-records-per-sec "$TOTAL_RPS" --record-size "$RECORD_SIZE" \
  --config "$MERGED_PROPS" --client-prefix "$CLIENT_PREFIX" --report-interval-ms 60000 \
  > "$LOG" 2>&1 &
PID=$!
echo "multiproducer ${CLIENT_PREFIX} $PID" >> "$PID_FILE"
echo "PID $PID (log: $LOG). Stop with 03-stop-load.sh."
