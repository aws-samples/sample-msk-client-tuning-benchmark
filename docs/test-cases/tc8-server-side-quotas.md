# Test case 8: Server-side quotas as a guardrail

**Purpose.** Run the production-shaped workload of test case 5 with the `baseline` profile and step a default
client quota on `request_percentage` down, to see how a server-side quota protects broker request-handler headroom
from clients that are not tuned, and what it costs those clients.

**What to expect.** `request_percentage` caps the share of broker request-handler and network-thread time that
each client (each user and client-id pair) may use on each broker. When a client exceeds it, the broker delays
its responses (throttling). As you lower the quota, broker headroom is protected, while the throttled clients see
higher latency, lower throughput, or growing lag. The quota is a guardrail, not a substitute for client tuning.

**Harness.** Amazon EC2 producer host to set the quota (`scripts/09-set-client-quotas.sh`), Amazon EKS for the
load (`k8s/examples/workload-high-density.yaml`, `baseline` profile).

**Prerequisites.** [Before you start](README.md#before-you-start) (EC2 quick start steps 1-3, EKS quick start
steps 1-4), the [common shell setup](README.md#common-shell-setup) including `PRODUCER_ID`, and the topics of
[test case 5](tc5-production-shaped-head-to-head.md#0-topics). The quota is set on the **default** entity, so it
applies to every client of the cluster that has no more specific quota, including the EKS pods. Test cluster only.

## 0. Allow the producer host to manage quotas (test account only)

The shipped host role does not grant `kafka-cluster:AlterClusterDynamicConfiguration` (set and delete) or
`kafka-cluster:DescribeClusterDynamicConfiguration` (describe). Add them as an inline policy on the producer host
role for the duration of this test, scoped to the benchmark cluster:

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
PROFILE_ARN=$(aws ec2 describe-instances --instance-ids "$PRODUCER_ID" \
  --query 'Reservations[0].Instances[0].IamInstanceProfile.Arn' --output text)
HOST_ROLE=$(aws iam get-instance-profile --instance-profile-name "${PROFILE_ARN##*/}" \
  --query 'InstanceProfile.Roles[0].RoleName' --output text)
aws iam put-role-policy --role-name "$HOST_ROLE" --policy-name tc8-client-quotas --policy-document "{
  \"Version\": \"2012-10-17\",
  \"Statement\": [{
    \"Effect\": \"Allow\",
    \"Action\": [\"kafka-cluster:AlterClusterDynamicConfiguration\", \"kafka-cluster:DescribeClusterDynamicConfiguration\"],
    \"Resource\": \"arn:aws:kafka:$AWS_REGION:$ACCOUNT_ID:cluster/load-benchmark/*\"
  }]
}"
```

IAM changes can take a short time to apply. `09-set-client-quotas.sh` is a dry run unless you pass `--apply`:

```bash
run_p 09-set-client-quotas.sh "$CLUSTER_ARN" describe --apply      # works once the policy is active; no quota yet
```

## 1. Reference run without a quota

```bash
SPEC=k8s/examples/workload-high-density.yaml
python3 k8s/gen-workload.py $SPEC --image "$IMG" --tuning baseline > /tmp/tc8-baseline.yaml
kubectl apply -f /tmp/tc8-baseline.yaml
kubectl -n msk-bench get pods -l msk-bench/workload=high-density | awk 'NR>1 && ($3!="Running" || $4!="0")'

sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc8-no-quota "$START" "$END"
```

## 2. Step the quota down

Choose the steps from a high value (little or no throttling) down to a value where clients are clearly
throttled. The values below are **examples**. A `request_percentage` of 100 is one full thread.

```bash
for RP in 200 100 50 25; do
  run_p 09-set-client-quotas.sh "$CLUSTER_ARN" set --request-percentage "$RP"            # dry run: prints the command
  run_p 09-set-client-quotas.sh "$CLUSTER_ARN" set --request-percentage "$RP" --apply
  run_p 09-set-client-quotas.sh "$CLUSTER_ARN" describe --apply
  sleep 600                                                                            # let throttling settle
  START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1200; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  python3 analysis/measure_cluster.py load-benchmark "tc8-rp-$RP" "$START" "$END"
  python3 analysis/compare.py tc8-no-quota "tc8-rp-$RP"
done
```

Use one window length for every step (20 minutes above). In this test a SUSPECT result on the message rate is the
observation: it means the quota is holding the clients below their offered rate. Record at which step that
starts.

## 3. Read the throttle metrics

`ProduceThrottleTime`, `FetchThrottleTime`, and `RequestThrottleTime` are `PER_BROKER` metrics. They are on the
benchmark dashboard (the last row, from `scripts/15-put-dashboard.sh`), `analysis/measure_cluster.py` records them
per window (`produce_throttle_ms`, `fetch_throttle_ms`, `request_throttle_ms`), and `analysis/compare.py` shows
them side by side. To read one directly for a window with the AWS CLI (repeat for each broker ID and metric):

```bash
aws cloudwatch get-metric-statistics --namespace AWS/Kafka --metric-name RequestThrottleTime \
  --dimensions Name="Cluster Name",Value=load-benchmark Name="Broker ID",Value=1 \
  --start-time "$START" --end-time "$END" --period 60 --statistics Average --output table
```

On the client side, the producer pods report `avg_ack_latency_ms` and `records_per_sec`
(`kubectl -n msk-bench logs -l role=producer --prefix --tail=2`), and consumer lag shows on the dashboard.

## Dashboard

- `RequestHandlerAvgIdlePercent` and `NetworkProcessorAvgIdlePercent`: the headroom the quota protects.
- `MessagesInPerSec` and `Throughput BytesIn/BytesOut`: drop once the quota throttles the producers.
- `Produce latency ms`, `FetchConsumerTotalTimeMsMean`, `Consumer lag (all groups)`.
- `CPU total %`.

**Key metrics.** `RequestHandlerAvgIdlePercent`, `RequestThrottleTime` / `ProduceThrottleTime` /
`FetchThrottleTime`, `MessagesInPerSec`, client ack latency and lag, per quota step.

## Watch out

- Quotas are enforced per broker and per client (user and client-id pair). With IAM access control the user
  principal is the IAM role ARN, so the EKS pods and the EC2 hosts are different users; the default entity covers
  both.
- Every pod in the cluster is throttled, including anything else you run at the same time. Run nothing else on
  the cluster during this test.
- Throttled consumers fall behind; lag keeps growing after you remove the quota until they catch up. Allow time
  before the next test case.
- Always delete the quota and the extra IAM permission at the end (below).

## Stop and clean up

```bash
run_p 09-set-client-quotas.sh "$CLUSTER_ARN" delete --apply
run_p 09-set-client-quotas.sh "$CLUSTER_ARN" describe --apply      # no quota left
kubectl -n msk-bench delete deploy -l msk-bench/workload=high-density
aws iam delete-role-policy --role-name "$HOST_ROLE" --policy-name tc8-client-quotas
```

**Limitations.** The shipped client roles don't grant the quota permissions, so step 0 adds them by hand. `09-set-client-quotas.sh` manages only the
default (user, client-id) entity unless you pass `--user` and `--client-id`.
