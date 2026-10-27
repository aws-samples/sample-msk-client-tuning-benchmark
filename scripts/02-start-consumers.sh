#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Run on a CONSUMER host (via run-on-host.sh). Starts one continuous
# kafka-consumer-perf-test.sh per topic, all under a SINGLE consumer group for
# this host. Each consumer host should use a DISTINCT group name so the hosts
# fan out (every group receives the full stream => egress = N groups x ingress).
#
# Usage:
#   ./02-start-consumers.sh <cluster-arn> [topic-prefix] [num-topics] \
#       [group-name] [fetch-min-bytes] [fetch-max-wait-ms]
#
# Defaults: prefix load-bench, 12 topics, group "group-<hostname>". fetch knobs
# default to client-tuning.properties unless passed; anything passed overrides
# client-tuning.properties for this run.
set -uo pipefail

CLUSTER_ARN="${1:?Usage: $0 <cluster-arn> [topic-prefix] [num-topics] [group-name] [fetch-min-bytes] [fetch-max-wait-ms]}"
PREFIX="${2:-load-bench}"
NUM_TOPICS="${3:-12}"
GROUP_NAME="${4:-group-$(hostname)}"
FETCH_MIN_BYTES="${5:-}"     # empty => client-tuning.properties
FETCH_MAX_WAIT_MS="${6:-}"   # empty => client-tuning.properties

KAFKA_HOME=/opt/kafka
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IAM_PROPS=/opt/kafka/config/client-iam.properties
TUNING_PROPS="$SCRIPT_DIR/client-tuning.properties"
RESULTS_DIR=/opt/load-benchmark/results
mkdir -p "$RESULTS_DIR"

# shellcheck source=lib-msk-env.sh
source "$SCRIPT_DIR/lib-msk-env.sh"
# Refuse a tuning profile that sets authentication keys (see lib-msk-env.sh).
msk_check_tuning_props "$TUNING_PROPS"

# Merge mandatory IAM auth with tunable overrides, then append any per-run fetch
# knobs (these win, since they are appended last).
MERGED_PROPS=/opt/kafka/config/consumer-effective.properties
cat "$IAM_PROPS" > "$MERGED_PROPS"
if [[ -f "$TUNING_PROPS" ]]; then
  echo "" >> "$MERGED_PROPS"
  grep -Ev '^\s*#' "$TUNING_PROPS" | grep -Ev '^\s*$' >> "$MERGED_PROPS" || true
fi
[[ -n "$FETCH_MIN_BYTES" ]] && echo "fetch.min.bytes=$FETCH_MIN_BYTES" >> "$MERGED_PROPS"
[[ -n "$FETCH_MAX_WAIT_MS" ]] && echo "fetch.max.wait.ms=$FETCH_MAX_WAIT_MS" >> "$MERGED_PROPS"


echo "Fetching bootstrap brokers for $CLUSTER_ARN..."
BROKERS=$(aws kafka get-bootstrap-brokers --cluster-arn "$CLUSTER_ARN" \
  "${MSK_ENDPOINT_ARGS[@]}" \
  --query 'BootstrapBrokerStringSaslIam' --output text)
echo "$BROKERS" > /opt/kafka/bootstrap-brokers.txt
echo "Bootstrap brokers: $BROKERS"
echo "Consumer group for this host: $GROUP_NAME"

PID_FILE=/opt/load-benchmark/load-pids.txt
: > "$PID_FILE"

# Effectively-unbounded message count / long timeout so each consumer reads
# continuously rather than stopping after a fixed batch.
BIG_NUM=999999999999

for i in $(seq 1 "$NUM_TOPICS"); do
  TOPIC="${PREFIX}-${i}"
  echo "=== Starting consumer for '$TOPIC' (group $GROUP_NAME) ==="
  nohup "$KAFKA_HOME/bin/kafka-consumer-perf-test.sh" \
    --bootstrap-server "$BROKERS" \
    --topic "$TOPIC" \
    --group "$GROUP_NAME" \
    --consumer.config "$MERGED_PROPS" \
    --messages "$BIG_NUM" \
    --timeout 604800000 \
    --show-detailed-stats \
    --reporting-interval 60000 \
    > "$RESULTS_DIR/consumer-${GROUP_NAME}-${TOPIC}.log" 2>&1 &
  CONSUMER_PID=$!
  echo "consumer $TOPIC $CONSUMER_PID" >> "$PID_FILE"
  echo "  consumer PID: $CONSUMER_PID (log: $RESULTS_DIR/consumer-${GROUP_NAME}-${TOPIC}.log)"
done

echo
echo "Started $NUM_TOPICS consumers under group '$GROUP_NAME'."
echo "PIDs in $PID_FILE. Effective client config: $MERGED_PROPS"
echo "To fan out, run this on another consumer host with a DIFFERENT group name."
echo "Stop with 03-stop-load.sh; check with 04-load-status.sh."
