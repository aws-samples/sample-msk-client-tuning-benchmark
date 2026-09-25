#!/bin/bash
# Run on a client host (via run-on-host.sh). Creates the benchmark topics on the
# Standard cluster with RF=3, min.insync.replicas=2, and a disk-conscious
# retention (default 1h). Idempotent (--if-not-exists).
#
# Usage: ./00-setup-topics.sh <cluster-arn> [topic-prefix] [num-topics] [partitions] [retention-ms]
set -uo pipefail

CLUSTER_ARN="${1:?Usage: $0 <cluster-arn> [topic-prefix] [num-topics] [partitions] [retention-ms]}"
PREFIX="${2:-load-bench}"
NUM_TOPICS="${3:-12}"
PARTITIONS="${4:-24}"
RETENTION_MS="${5:-3600000}"   # 1 hour -- keep disk usage bounded under sustained load
REPLICATION_FACTOR=3
MIN_INSYNC_REPLICAS=2

KAFKA_HOME=/opt/kafka
PROPS=/opt/kafka/config/client-iam.properties

# shellcheck source=lib-msk-env.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib-msk-env.sh"

echo "Fetching bootstrap brokers for cluster..."
BROKERS=$(aws kafka get-bootstrap-brokers --cluster-arn "$CLUSTER_ARN" \
  "${MSK_ENDPOINT_ARGS[@]}" \
  --query 'BootstrapBrokerStringSaslIam' --output text)

if [[ -z "$BROKERS" || "$BROKERS" == "None" ]]; then
  echo "ERROR: could not retrieve SASL/IAM bootstrap brokers. Is the cluster ACTIVE?" >&2
  exit 1
fi

echo "$BROKERS" > /opt/kafka/bootstrap-brokers.txt
echo "Bootstrap brokers: $BROKERS"

for i in $(seq 1 "$NUM_TOPICS"); do
  TOPIC="${PREFIX}-${i}"
  echo "=== Creating topic '$TOPIC' (partitions=$PARTITIONS, RF=$REPLICATION_FACTOR, min.insync.replicas=$MIN_INSYNC_REPLICAS, retention.ms=$RETENTION_MS) ==="
  "$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server "$BROKERS" \
    --command-config "$PROPS" \
    --create --if-not-exists \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR" \
    --config retention.ms="$RETENTION_MS" \
    --config min.insync.replicas="$MIN_INSYNC_REPLICAS"
done

echo
echo "Describing each topic individually (a regex --describe would touch internal"
echo "topics outside the scoped IAM policy and fail with TopicAuthorizationException):"
for i in $(seq 1 "$NUM_TOPICS"); do
  TOPIC="${PREFIX}-${i}"
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BROKERS" --command-config "$PROPS" \
    --describe --topic "$TOPIC"
done

echo
echo "Topic names (for use by other scripts):"
for i in $(seq 1 "$NUM_TOPICS"); do echo "${PREFIX}-${i}"; done | tee /opt/kafka/topic-list.txt

echo "Done. Brokers saved to /opt/kafka/bootstrap-brokers.txt, topic list saved to /opt/kafka/topic-list.txt"
