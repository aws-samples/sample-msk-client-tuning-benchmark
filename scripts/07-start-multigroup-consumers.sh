#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Run on a CONSUMER host (via run-on-host.sh). Starts ONE MultiGroupConsumer JVM
# that runs N independent consumer groups (one KafkaConsumer thread per group),
# all subscribed to the same topic list -- the "broadcast" / per-instance group
# pattern common in microservice fleets, where every service instance uses its own
# group id (for example <service>-broadcast-<N>) and so reads every message of the
# same topic. Group count drives coordinator, heartbeat and offset-commit load on
# the brokers independently of bytes.
#
# APPENDS to the PID file (does not truncate), so it can be added to a host that
# is already running consumers; 03-stop-load.sh stops it with the rest.
#
# Usage:
#   ./07-start-multigroup-consumers.sh <cluster-arn> <topics-csv> <group-prefix> \
#       <num-groups> [start-index] [stagger-ms]
#
# Example (100 per-instance groups on one broadcast topic):
#   ./07-start-multigroup-consumers.sh $ARN broadcast-1 bench-bcast-host0 100 0 200
#
# All Kafka client knobs come from client-tuning.properties layered over the IAM
# auth file -- this script does NOT override them.
set -uo pipefail

CLUSTER_ARN="${1:?Usage: $0 <cluster-arn> <topics-csv> <group-prefix> <num-groups> [start-index] [stagger-ms]}"
TOPICS="${2:?topics-csv required}"
GROUP_PREFIX="${3:?group-prefix required}"
NUM_GROUPS="${4:?num-groups required}"
START_INDEX="${5:-0}"
STAGGER_MS="${6:-200}"

KAFKA_HOME=/opt/kafka
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IAM_PROPS=/opt/kafka/config/client-iam.properties
TUNING_PROPS="$SCRIPT_DIR/client-tuning.properties"
JAVA_SRC="$SCRIPT_DIR/MultiGroupConsumer.java"
RESULTS_DIR=/opt/load-benchmark/results
PID_FILE=/opt/load-benchmark/load-pids.txt
CLASSPATH_GLOB="$KAFKA_HOME/libs/*"
mkdir -p "$RESULTS_DIR" "$(dirname "$PID_FILE")"

command -v java >/dev/null 2>&1 || { echo "ERROR: 'java' not found on PATH (need JDK 17+)." >&2; exit 1; }
[[ -f "$JAVA_SRC" ]] || { echo "ERROR: $JAVA_SRC not found (did run-on-host.sh sync scripts/ ?)." >&2; exit 1; }

# Separate merged file so a running 02/06 consumer's effective config is not overwritten.
MERGED_PROPS=/opt/kafka/config/multigroup-effective.properties
cat "$IAM_PROPS" > "$MERGED_PROPS"
if [[ -f "$TUNING_PROPS" ]]; then
  echo "" >> "$MERGED_PROPS"
  grep -Ev '^\s*#' "$TUNING_PROPS" | grep -Ev '^\s*$' >> "$MERGED_PROPS" || true
fi

# shellcheck source=lib-msk-env.sh
source "$SCRIPT_DIR/lib-msk-env.sh"
BROKERS=$(aws kafka get-bootstrap-brokers --cluster-arn "$CLUSTER_ARN" \
  "${MSK_ENDPOINT_ARGS[@]}" --query 'BootstrapBrokerStringSaslIam' --output text)
if [[ -z "$BROKERS" || "$BROKERS" == "None" ]]; then
  echo "ERROR: could not get SASL/IAM bootstrap brokers for $CLUSTER_ARN" >&2; exit 1
fi
echo "$BROKERS" > /opt/kafka/bootstrap-brokers.txt

LOG="$RESULTS_DIR/multigroup-${GROUP_PREFIX}.log"
# Heap scales with group count (~4 MB per consumer incl. fetch buffers), min 512M.
HEAP_MB=$(( NUM_GROUPS * 4 < 512 ? 512 : NUM_GROUPS * 4 ))
echo "Starting $NUM_GROUPS groups ${GROUP_PREFIX}-${START_INDEX}..${GROUP_PREFIX}-$((START_INDEX + NUM_GROUPS - 1))" \
     "on topics [$TOPICS], stagger ${STAGGER_MS} ms, heap ${HEAP_MB}M"
nohup java -Xmx${HEAP_MB}m -cp "$CLASSPATH_GLOB" "$JAVA_SRC" \
  --bootstrap "$BROKERS" --topics "$TOPICS" --group-prefix "$GROUP_PREFIX" \
  --groups "$NUM_GROUPS" --start-index "$START_INDEX" --config "$MERGED_PROPS" \
  --stagger-ms "$STAGGER_MS" --report-interval-ms 60000 > "$LOG" 2>&1 &
PID=$!
# Format "<role> <topic> <pid>" so 03-stop-load.sh / 04-load-status.sh can parse it.
echo "multigroup ${GROUP_PREFIX} $PID" >> "$PID_FILE"
echo "PID $PID (log: $LOG). CSV: time, prefix, groups_configured, groups_running, groups_failed, records, bytes, records_per_sec, MB_per_sec, polls, empty_polls"
echo "Stop with 03-stop-load.sh."
