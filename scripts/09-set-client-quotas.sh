#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Run on a client host (via run-on-host.sh). Sets, describes or deletes a Kafka
# client quota for the DEFAULT (user, client-id) entity -- i.e. every client that
# has no more specific quota -- by wrapping kafka-configs.sh. Used for the
# "Protecting the cluster with Kafka quotas" guardrail (docs/quotas.md): cap per-client produce/fetch bandwidth and request
# handler time so a misbehaving client cannot saturate the brokers.
#
# DRY-RUN BY DEFAULT: prints the kafka-configs.sh command it would run. Pass
# --apply to execute it. (Resolving the bootstrap brokers is a read-only
# `aws kafka get-bootstrap-brokers` call and happens in both modes.)
#
# Usage:
#   ./09-set-client-quotas.sh <cluster-arn> set \
#       [--producer-byte-rate BYTES_PER_SEC] [--consumer-byte-rate BYTES_PER_SEC] \
#       [--request-percentage PERCENT] [--user NAME] [--client-id ID] [--apply]
#   ./09-set-client-quotas.sh <cluster-arn> describe [--user NAME] [--client-id ID] [--apply]
#   ./09-set-client-quotas.sh <cluster-arn> delete   [--user NAME] [--client-id ID] [--apply]
#
# Quota keys (Kafka semantics, enforced PER BROKER, per client group):
#   producer_byte_rate   bytes/sec a client may produce to one broker
#   consumer_byte_rate   bytes/sec a client may fetch from one broker
#   request_percentage   % of one request-handler/network thread a client may use
#                        (100 = one full thread; can exceed 100 across threads)
# `set` requires at least one of them. `delete` removes all three.
#
# --user / --client-id default to the default entity (<default>). With IAM
# authentication the Kafka user principal is the IAM role/user ARN, so a user
# name must be given in that form.
#
# Example (10 MiB/s produce, 20 MiB/s fetch, 50% request time for every client):
#   ./09-set-client-quotas.sh $ARN set --producer-byte-rate 10485760 \
#       --consumer-byte-rate 20971520 --request-percentage 50 --apply
#
# IAM: altering quotas needs kafka-cluster:AlterClusterDynamicConfiguration and
# describing them needs kafka-cluster:DescribeClusterDynamicConfiguration on the
# cluster resource, in addition to kafka-cluster:Connect.
set -uo pipefail

usage() { sed -n '/^# Usage:/,/^# Quota keys/p' "$0" | sed '$d; s/^# \{0,1\}//' >&2; exit 2; }

CLUSTER_ARN="${1:-}"
ACTION="${2:-}"
[[ -n "$CLUSTER_ARN" && -n "$ACTION" ]] || usage
shift 2

PRODUCER_BYTE_RATE=""
CONSUMER_BYTE_RATE=""
REQUEST_PERCENTAGE=""
USER_ENTITY=""
CLIENT_ENTITY=""
APPLY=false

is_number() { [[ "$1" =~ ^[0-9]+([.][0-9]+)?$ ]]; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --producer-byte-rate) PRODUCER_BYTE_RATE="${2:-}"; shift 2 ;;
    --consumer-byte-rate) CONSUMER_BYTE_RATE="${2:-}"; shift 2 ;;
    --request-percentage) REQUEST_PERCENTAGE="${2:-}"; shift 2 ;;
    --user)               USER_ENTITY="${2:-}"; shift 2 ;;
    --client-id)          CLIENT_ENTITY="${2:-}"; shift 2 ;;
    --apply)              APPLY=true; shift ;;
    -h|--help)            usage ;;
    *) echo "ERROR: unknown argument '$1'" >&2; usage ;;
  esac
done

for pair in "producer-byte-rate:$PRODUCER_BYTE_RATE" "consumer-byte-rate:$CONSUMER_BYTE_RATE" \
            "request-percentage:$REQUEST_PERCENTAGE"; do
  name="${pair%%:*}"; val="${pair#*:}"
  if [[ -n "$val" ]] && ! is_number "$val"; then
    echo "ERROR: --$name must be a non-negative number, got '$val'" >&2; exit 2
  fi
done

KAFKA_HOME=/opt/kafka
IAM_PROPS=/opt/kafka/config/client-iam.properties
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Entity selection: default (user, client-id) unless a specific one is given.
ENTITY_ARGS=(--entity-type users)
if [[ -n "$USER_ENTITY" ]]; then ENTITY_ARGS+=(--entity-name "$USER_ENTITY"); else ENTITY_ARGS+=(--entity-default); fi
ENTITY_ARGS+=(--entity-type clients)
if [[ -n "$CLIENT_ENTITY" ]]; then ENTITY_ARGS+=(--entity-name "$CLIENT_ENTITY"); else ENTITY_ARGS+=(--entity-default); fi

case "$ACTION" in
  set)
    CONFIGS=()
    [[ -n "$PRODUCER_BYTE_RATE" ]] && CONFIGS+=("producer_byte_rate=$PRODUCER_BYTE_RATE")
    [[ -n "$CONSUMER_BYTE_RATE" ]] && CONFIGS+=("consumer_byte_rate=$CONSUMER_BYTE_RATE")
    [[ -n "$REQUEST_PERCENTAGE" ]] && CONFIGS+=("request_percentage=$REQUEST_PERCENTAGE")
    if [[ ${#CONFIGS[@]} -eq 0 ]]; then
      echo "ERROR: 'set' needs at least one of --producer-byte-rate, --consumer-byte-rate, --request-percentage" >&2
      exit 2
    fi
    JOINED=$(printf '%s,' "${CONFIGS[@]}"); JOINED=${JOINED%,}
    ACTION_ARGS=(--alter --add-config "$JOINED")
    ;;
  describe)
    ACTION_ARGS=(--describe)
    ;;
  delete)
    ACTION_ARGS=(--alter --delete-config producer_byte_rate,consumer_byte_rate,request_percentage)
    ;;
  *)
    echo "ERROR: action must be set, describe or delete (got '$ACTION')" >&2; usage ;;
esac

# shellcheck source=lib-msk-env.sh
source "$SCRIPT_DIR/lib-msk-env.sh"

BROKERS=$(aws kafka get-bootstrap-brokers --cluster-arn "$CLUSTER_ARN" \
  ${MSK_ENDPOINT_ARGS[@]+"${MSK_ENDPOINT_ARGS[@]}"} --query 'BootstrapBrokerStringSaslIam' --output text)
if [[ -z "$BROKERS" || "$BROKERS" == "None" ]]; then
  echo "ERROR: could not get SASL/IAM bootstrap brokers for $CLUSTER_ARN" >&2; exit 1
fi

CMD=("$KAFKA_HOME/bin/kafka-configs.sh" --bootstrap-server "$BROKERS" --command-config "$IAM_PROPS"
     "${ACTION_ARGS[@]}" "${ENTITY_ARGS[@]}")

echo "Quota entity: user=${USER_ENTITY:-<default>} client-id=${CLIENT_ENTITY:-<default>}"
echo "Command:"
printf ' %q' "${CMD[@]}"; echo

if [[ "$APPLY" != true ]]; then
  echo
  echo "DRY RUN -- nothing executed. Re-run with --apply to execute."
  exit 0
fi

"${CMD[@]}"
RC=$?
if [[ $RC -ne 0 ]]; then
  echo "ERROR: kafka-configs.sh exited with $RC" >&2; exit "$RC"
fi

if [[ "$ACTION" != describe ]]; then
  echo
  echo "Quota now in effect:"
  "$KAFKA_HOME/bin/kafka-configs.sh" --bootstrap-server "$BROKERS" --command-config "$IAM_PROPS" \
    --describe "${ENTITY_ARGS[@]}"
fi
echo
echo "Watch ProduceThrottleTime / FetchThrottleTime / RequestThrottleTime (PER_BROKER) in Amazon CloudWatch."
