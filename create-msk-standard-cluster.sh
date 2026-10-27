#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Creates an Amazon MSK Standard cluster for client-tuning benchmarks, then
# creates its topics via the producer host. Run locally (from your workstation).
#
# Defaults: kafka.m7g.8xlarge x3, Kafka 3.8.x (ZooKeeper), 2000 GB EBS per broker
# with 625 MB/s provisioned throughput, IAM auth + TLS in transit (client-broker
# and in-cluster), PER_BROKER enhanced monitoring, a custom configuration
# (RF=3, min.insync.replicas=2, unclean.leader.election.enable=false), and
# 12 topics x 24 partitions.
#
# Usage:
#   ./create-msk-standard-cluster.sh [cluster-name] [instance-type] [num-brokers] \
#       [topic-prefix] [num-topics] [partitions] [ebs-gb]
#
# The cluster-name MUST match the `clusterName` the CDK client hosts were
# deployed with (default: load-benchmark), because their IAM is scoped to it.
set -euo pipefail

CLUSTER_NAME="${1:-load-benchmark}"
INSTANCE_TYPE="${2:-kafka.m7g.8xlarge}"
NUM_BROKERS="${3:-3}"
TOPIC_PREFIX="${4:-load-bench}"
NUM_TOPICS="${5:-12}"
PARTITIONS="${6:-24}"
EBS_GB="${7:-2000}"
# Replication settings (override via env). Set these to match production so
# the benchmark replicates the same bytes per produced message.
RF="${RF:-3}"
MIN_ISR="${MIN_ISR:-2}"
CONFIG_NAME="${CONFIG_NAME:-${CLUSTER_NAME}-config}"

KAFKA_VERSION=3.8.x
# Region: BENCH_REGION, else AWS_REGION / AWS_DEFAULT_REGION, else us-east-1.
# Must be the Region the CDK stacks were deployed to.
REGION="${BENCH_REGION:-${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}}"
PROFILE="${AWS_PROFILE:-default}"
NETWORK_STACK=LoadBenchmarkNetworkStack
PRODUCER_STACK=LoadBenchmarkProducerStack
AWSCLI=(aws --profile "$PROFILE" --region "$REGION")
AWSKAFKA=(aws kafka --profile "$PROFILE" --region "$REGION")

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=============================================================="
echo " Creating an Amazon MSK Standard cluster for benchmarking"
echo "   name:     $CLUSTER_NAME"
echo "   type:     $INSTANCE_TYPE x$NUM_BROKERS"
echo "   region:   $REGION"
echo "   version:  $KAFKA_VERSION"
echo "   storage:  ${EBS_GB} GB EBS per broker"
echo "   topics:   ${NUM_TOPICS} x ${PARTITIONS} partitions"
echo "=============================================================="
echo

echo "==> Looking up VPC subnets/security group from $NETWORK_STACK..."
VPC_OUTPUTS=$("${AWSCLI[@]}" cloudformation describe-stacks --stack-name "$NETWORK_STACK" \
  --query 'Stacks[0].Outputs' --output json)
SUBNET_IDS=$(echo "$VPC_OUTPUTS" | python3 -c "
import json, sys
outputs = json.load(sys.stdin)
print([o['OutputValue'] for o in outputs if o['OutputKey'] == 'PrivateSubnetIds'][0])
")
MSK_SG=$(echo "$VPC_OUTPUTS" | python3 -c "
import json, sys
outputs = json.load(sys.stdin)
print([o['OutputValue'] for o in outputs if o['OutputKey'] == 'MskSecurityGroupId'][0])
")
echo "    Subnets: $SUBNET_IDS"
echo "    MSK SG:  $MSK_SG"

echo "==> Checking for existing configuration named '$CONFIG_NAME'..."
EXISTING_CONFIG=$("${AWSKAFKA[@]}" list-configurations \
  --query "Configurations[?Name=='$CONFIG_NAME'].Arn | [0]" --output text 2>/dev/null || echo "None")

if [[ -n "$EXISTING_CONFIG" && "$EXISTING_CONFIG" != "None" ]]; then
  CONFIG_ARN="$EXISTING_CONFIG"
  CONFIG_REVISION=$("${AWSKAFKA[@]}" describe-configuration --arn "$CONFIG_ARN" \
    --query 'LatestRevision.Revision' --output text)
  echo "    Reusing existing configuration: $CONFIG_ARN (revision $CONFIG_REVISION)"
else
  echo "    Creating new cluster configuration..."
  # Written to a file and passed with fileb:// so the CLI uploads the raw bytes
  # verbatim with real newlines. Passing these as one escaped string would
  # store a single invalid property line and the cluster would not apply it.
  PROPS_FILE="$SCRIPT_DIR/.cluster-config.properties.tmp"
  cat > "$PROPS_FILE" <<EOF
default.replication.factor=${RF}
min.insync.replicas=${MIN_ISR}
unclean.leader.election.enable=false
EOF
  CONFIG_RESULT=$("${AWSKAFKA[@]}" create-configuration \
    --name "$CONFIG_NAME" \
    --kafka-versions "$KAFKA_VERSION" \
    --server-properties "fileb://$PROPS_FILE")
  rm -f "$PROPS_FILE"
  CONFIG_ARN=$(echo "$CONFIG_RESULT" | python3 -c "import json,sys; print(json.load(sys.stdin)['Arn'])")
  CONFIG_REVISION=$(echo "$CONFIG_RESULT" | python3 -c "import json,sys; print(json.load(sys.stdin)['LatestRevision']['Revision'])")
  echo "    Created: $CONFIG_ARN (revision $CONFIG_REVISION)"
fi

echo "==> Checking for existing cluster named '$CLUSTER_NAME'..."
EXISTING_CLUSTER=$("${AWSKAFKA[@]}" list-clusters-v2 \
  --query "ClusterInfoList[?ClusterName=='$CLUSTER_NAME'].ClusterArn | [0]" --output text 2>/dev/null || echo "None")

if [[ -n "$EXISTING_CLUSTER" && "$EXISTING_CLUSTER" != "None" ]]; then
  CLUSTER_ARN="$EXISTING_CLUSTER"
  echo "    Reusing existing cluster: $CLUSTER_ARN"
else
  echo "    Creating new Standard cluster..."
  PROVISIONED_FILE="$SCRIPT_DIR/.provisioned-cluster.json.tmp"
  # Standard brokers use provisioned EBS storage, so StorageInfo/EbsStorageInfo is
  # required. StorageMode is left unset so it defaults to LOCAL. EBS provisioned
  # throughput is enabled at 625 MB/s per broker so the volumes can sustain the
  # target ingress without EBS becoming the bottleneck (see the dashboard's EBS
  # widgets); MSK exposes that via update-storage for later changes.
  python3 -c "
import json
subnets = '$SUBNET_IDS'.split(',')
provisioned = {
    'KafkaVersion': '$KAFKA_VERSION',
    'NumberOfBrokerNodes': $NUM_BROKERS,
    'BrokerNodeGroupInfo': {
        'InstanceType': '$INSTANCE_TYPE',
        'ClientSubnets': subnets,
        'SecurityGroups': ['$MSK_SG'],
        'StorageInfo': {'EbsStorageInfo': {
            'VolumeSize': $EBS_GB,
            'ProvisionedThroughput': {'Enabled': True, 'VolumeThroughput': 625},
        }},
    },
    'ClientAuthentication': {'Sasl': {'Iam': {'Enabled': True}}},
    'EncryptionInfo': {'EncryptionInTransit': {'ClientBroker': 'TLS', 'InCluster': True}},
    'ConfigurationInfo': {'Arn': '$CONFIG_ARN', 'Revision': $CONFIG_REVISION},
    # PER_BROKER (or higher) is required: RequestHandlerAvgIdlePercent,
    # NetworkProcessorAvgIdlePercent, Volume*, and Produce/Fetch *TimeMsMean
    # are only emitted at PER_BROKER level and above.
    'EnhancedMonitoring': 'PER_BROKER',
}
with open('$PROVISIONED_FILE', 'w') as f:
    json.dump(provisioned, f, indent=2)
"
  cat "$PROVISIONED_FILE"
  CLUSTER_RESULT=$("${AWSKAFKA[@]}" create-cluster-v2 \
    --cluster-name "$CLUSTER_NAME" \
    --provisioned "file://$PROVISIONED_FILE")
  rm -f "$PROVISIONED_FILE"
  CLUSTER_ARN=$(echo "$CLUSTER_RESULT" | python3 -c "import json,sys; print(json.load(sys.stdin)['ClusterArn'])")
  echo "    Created: $CLUSTER_ARN (state: CREATING -- takes ~20-30 min)"
fi

echo
echo "==> Waiting for cluster to become ACTIVE (~20-30 min)..."
while true; do
  STATE=$("${AWSKAFKA[@]}" describe-cluster-v2 --cluster-arn "$CLUSTER_ARN" \
    --query 'ClusterInfo.State' --output text)
  echo "    State: $STATE"
  [[ "$STATE" == "ACTIVE" ]] && break
  if [[ "$STATE" == "FAILED" ]]; then
    echo "ERROR: cluster entered FAILED state." >&2
    exit 1
  fi
  sleep 30
done

echo
echo "==> Creating $NUM_TOPICS topics via the producer host..."
STATUS=0
"$SCRIPT_DIR/orchestration/run-on-host.sh" \
  "$PRODUCER_STACK" "$REGION" "$PROFILE" \
  00-setup-topics.sh "$CLUSTER_ARN" "$TOPIC_PREFIX" "$NUM_TOPICS" "$PARTITIONS" 3600000 "$RF" "$MIN_ISR" || STATUS=$?

echo
echo "=============================== Summary ==============================="
echo "Cluster ARN: $CLUSTER_ARN"
echo "Config ARN:  $CONFIG_ARN (revision $CONFIG_REVISION)"
echo "Topics:      ${TOPIC_PREFIX}-1 .. ${TOPIC_PREFIX}-${NUM_TOPICS} (RF=${RF}, min.insync.replicas=${MIN_ISR})"
echo
echo "Publish the CloudWatch dashboard:"
echo "  ./scripts/15-put-dashboard.sh $CLUSTER_NAME $REGION $NUM_BROKERS"
echo
echo "Start ~180 MB/s of producers on the producer host:"
echo "  ./orchestration/run-on-host.sh $PRODUCER_STACK $REGION $PROFILE \\"
echo "    01-start-producers.sh --background $CLUSTER_ARN $TOPIC_PREFIX $NUM_TOPICS 180"
echo
echo "Start a consumer group on each consumer host (one per host, scale the fleet):"
echo "  ./orchestration/run-on-host.sh LoadBenchmarkConsumerStack-0 $REGION $PROFILE \\"
echo "    02-start-consumers.sh --background $CLUSTER_ARN $TOPIC_PREFIX $NUM_TOPICS group-0"
echo
echo "CLEANUP when done (billable: ${INSTANCE_TYPE} x${NUM_BROKERS} + ${EBS_GB}GB x${NUM_BROKERS} EBS):"
echo "  aws kafka delete-cluster --cluster-arn $CLUSTER_ARN --profile $PROFILE --region $REGION"
echo "  # configuration can only be deleted once the cluster is fully gone:"
echo "  aws kafka delete-configuration --arn $CONFIG_ARN --profile $PROFILE --region $REGION"
echo "======================================================================"

if [[ "$STATUS" -ne 0 ]]; then
  echo "WARNING: topic creation returned non-zero ($STATUS). Review output above." >&2
  exit 1
fi
