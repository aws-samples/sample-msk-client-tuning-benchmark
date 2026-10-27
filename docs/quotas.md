# Protect the cluster with Kafka quotas

**Purpose.** Use server-side Kafka quotas as a guardrail, as the blog describes in "Protecting the cluster with
Kafka quotas". Client tuning reduces how hard well-behaved clients work the brokers; quotas make sure that a single
misbehaving or newly deployed client cannot overwhelm a broker. This is not one of the three tests: it is a safety
net you apply during and after the client-tuning rollout. An optional measurement procedure at the end shows the
effect of a quota on the benchmark cluster.

**Blog section:** "Protecting the cluster with Kafka quotas" (and step 5 of "A staged rollout plan").

> **WARNING: test clusters only.** The commands below set a quota on the default client entity, which applies to
> every client of the cluster that has no more specific quota. Never run them against a production cluster
> without a change plan.

## The three quota types

Apache Kafka supports three kinds of quota, all enforced on the broker:

| Quota | Config keys | What it protects |
|---|---|---|
| Network bandwidth | `producer_byte_rate`, `consumer_byte_rate` | Bytes per second a client may produce to or fetch from one broker. Protects network and disk throughput from a runaway client. |
| Request rate | `request_percentage` | Share of broker request-handler (I/O) and network-thread time a client may use, where 100 is one full thread. Because broker saturation at high density is driven by request count, this is usually the most effective quota for protecting `RequestHandlerAvgIdlePercent`. |
| Controller mutation | `controller_mutation_rate` | Rate of partition creation and deletion. Useful where many clients or tools create and drop topics dynamically. |

Quotas apply to an **entity**: a `user` (the authenticated principal; with AWS Identity and Access Management
(IAM) access control this is the IAM role ARN), a `client-id`, or the `(user, client-id)` pair. A default for the
`(user, client-id)` pair, applied to every broker, is a good starting point because it bounds each client
independently instead of sharing one budget across all of them. Quotas are enforced per broker.

## How enforcement behaves

Quotas throttle rather than reject. When a client exceeds a byte-rate quota, the broker delays its responses so
that the client's effective rate converges on the limit. When it exceeds a request-rate quota, the broker holds
the request in a throttle queue. The client sees higher latency, not errors, so a correctly sized quota is
invisible to healthy clients and only affects a client that is over its share. Tighter settings trade more
latency for more protection; reserve them for latency-tolerant producers.

## Apply quotas with this repository

[`scripts/09-set-client-quotas.sh`](../scripts/09-set-client-quotas.sh) wraps `kafka-configs.sh` and runs on a
client host through `run_p` (see the [shared setup](test-cases/README.md#shared-setup)). It manages the default
`(user, client-id)` entity unless you pass `--user` and `--client-id`, and it is a **dry run** unless you pass
`--apply`: without it, it only prints the `kafka-configs.sh` command.

```bash
# Dry run: print the command for every client (10 MiB/s produce, 20 MiB/s fetch, 50% of one request thread).
run_p 09-set-client-quotas.sh "$CLUSTER_ARN" set --producer-byte-rate 10485760 \
  --consumer-byte-rate 20971520 --request-percentage 50
# Apply it, then show what is in effect.
run_p 09-set-client-quotas.sh "$CLUSTER_ARN" set --producer-byte-rate 10485760 \
  --consumer-byte-rate 20971520 --request-percentage 50 --apply
run_p 09-set-client-quotas.sh "$CLUSTER_ARN" describe --apply
# Remove all three keys from the entity.
run_p 09-set-client-quotas.sh "$CLUSTER_ARN" delete --apply
```

The values above are examples. `set` needs at least one of the three options; `delete` removes
`producer_byte_rate`, `consumer_byte_rate`, and `request_percentage` together. With IAM access control a
`--user` value must be the IAM role or user ARN.

The script does not manage `controller_mutation_rate`. To set it, run `kafka-configs.sh` directly on the producer
host with `kafka_cli` (same entity flags, `--add-config controller_mutation_rate=<partitions per second>`).

### Grant the quota permissions (test account only)

Setting and deleting quotas needs `kafka-cluster:AlterClusterDynamicConfiguration`, and describing them needs
`kafka-cluster:DescribeClusterDynamicConfiguration`. The shipped producer host role grants neither. Add them as an
inline policy scoped to the benchmark cluster for as long as you work with quotas:

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
PROFILE_ARN=$(aws ec2 describe-instances --instance-ids "$PRODUCER_ID" \
  --query 'Reservations[0].Instances[0].IamInstanceProfile.Arn' --output text)
HOST_ROLE=$(aws iam get-instance-profile --instance-profile-name "${PROFILE_ARN##*/}" \
  --query 'InstanceProfile.Roles[0].RoleName' --output text)
aws iam put-role-policy --role-name "$HOST_ROLE" --policy-name msk-bench-client-quotas --policy-document "{
  \"Version\": \"2012-10-17\",
  \"Statement\": [{
    \"Effect\": \"Allow\",
    \"Action\": [\"kafka-cluster:AlterClusterDynamicConfiguration\", \"kafka-cluster:DescribeClusterDynamicConfiguration\"],
    \"Resource\": \"arn:aws:kafka:$AWS_REGION:$ACCOUNT_ID:cluster/load-benchmark/*\"
  }]
}"
run_p 09-set-client-quotas.sh "$CLUSTER_ARN" describe --apply      # works once the policy is active; no quota yet
```

IAM changes can take a short time to apply. Remove the policy when you are done (see [Clean up](#clean-up)).

## Choose the values

- **Start loose, then tighten gradually.** Begin with a generous quota and step it down while you watch consumer
  lag and client latency. Lowering the quota too far in one move can stall consumers and let lag build; lower it
  in stages.
- **Size byte-rate quotas from observed per-client throughput.** Set the cap comfortably above a healthy client's
  normal rate so that it engages only during anomalies.
- **Use `request_percentage` as the primary guardrail** on request-count-bound clusters; byte-rate quotas are the
  backstop against a runaway producer or consumer.
- **Reserve the tightest settings** for clients that tolerate added latency; keep latency-sensitive producers on
  looser limits (with a more specific `--user` / `--client-id` quota).
- **Treat quotas as a safety net, not the primary fix.** They protect the cluster during and after the
  client-tuning rollout; they do not replace the tuning.

## Amazon CloudWatch metrics to watch

| Metric | What it shows |
|---|---|
| `RequestThrottleQueueSize` / `RequestThrottleTime` | Request-rate quota activity. A non-zero, steadily growing queue means `request_percentage` is actively throttling. |
| `ProduceThrottleQueueSize` / `ProduceThrottleTime`, `FetchThrottleQueueSize` / `FetchThrottleTime` | Byte-rate quota activity on the produce and fetch paths. Stay at zero unless a client exceeds its byte-rate cap. |
| Client-side `produce-throttle-time-avg`, `fetch-throttle-time-avg`, `request-latency-avg` | The latency the quota adds from the client's point of view. Keep them within your produce and consume SLOs. |
| `RequestHandlerAvgIdlePercent`, `NetworkProcessorAvgIdlePercent` | The headroom the quota protects. |

In this repository:

- The dashboard from `scripts/15-put-dashboard.sh` has a row with `ProduceThrottleTime`, `FetchThrottleTime`, and
  `RequestThrottleTime` per broker. `analysis/measure_cluster.py` records them per window (`produce_throttle_ms`,
  `fetch_throttle_ms`, `request_throttle_ms`) and `analysis/compare.py` shows them side by side.
- The `*ThrottleQueueSize` metrics are not on the dashboard and not recorded by `measure_cluster.py`. Read them
  with the AWS CLI (repeat for each broker ID):

  ```bash
  aws cloudwatch get-metric-statistics --namespace AWS/Kafka --metric-name RequestThrottleQueueSize \
    --dimensions Name="Cluster Name",Value=load-benchmark Name="Broker ID",Value=1 \
    --start-time "$START" --end-time "$END" --period 60 --statistics Maximum --output table
  ```

- The benchmark clients do not log the Kafka client throttle metrics. The producer pods report
  `avg_ack_latency_ms` and `records_per_sec` (`kubectl -n msk-bench logs -l role=producer --prefix --tail=2`),
  which show the added latency and the reduced rate.
- Some throttle metrics are emitted only at finer Amazon MSK monitoring levels. The benchmark cluster uses
  `PER_BROKER`; confirm that the metrics you rely on are emitted before you read a zero as "no throttling".

## Measure the effect of a quota (optional)

This procedure runs the production-shaped workload from [Test 1](test-cases/test-1-head-to-head.md) with the
`baseline` profile and steps a default `request_percentage` quota down, to see how the quota protects
request-handler headroom from clients that are not tuned, and what it costs those clients.

**Harness.** The Amazon Elastic Compute Cloud (Amazon EC2) producer host sets the quota; Amazon Elastic Kubernetes
Service (Amazon EKS) runs the load ([`k8s/examples/workload-high-density.yaml`](../k8s/examples/workload-high-density.yaml)).

**Prerequisites.** The root README [Prerequisites](../README.md#prerequisites), steps 1-3 of the
[Amazon EC2 quick start](../README.md#quick-start-amazon-ec2-harness), steps 1-4 of the
[Amazon EKS quick start](../README.md#quick-start-amazon-eks-harness), the
[shared setup](test-cases/README.md#shared-setup) including `PRODUCER_ID`, and the quota permissions
[above](#grant-the-quota-permissions-test-account-only). Run nothing else on the cluster: the default quota
throttles every client, including the Amazon EKS pods and the Amazon EC2 hosts (different IAM users, both covered
by the default entity).

### 1. Topics

The spec needs `load-bench-1..12` with 100 partitions and `bcast-1` with 20. Stop all load and reset the topics
([Topics and reset](test-cases/README.md#topics-and-reset)) if they exist with another partition count:

```bash
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --delete --topic 'load-bench-.*'"
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --list"                  # wait until no load-bench-* is listed
run_p 00-setup-topics.sh "$CLUSTER_ARN" load-bench 12 100
run_p 00-setup-topics.sh "$CLUSTER_ARN" bcast 1 20
```

### 2. Reference run without a quota

```bash
SPEC=k8s/examples/workload-high-density.yaml
python3 k8s/gen-workload.py $SPEC --image "$IMG" --tuning baseline > /tmp/quota-baseline.yaml
kubectl apply -f /tmp/quota-baseline.yaml
kubectl -n msk-bench get pods -l msk-bench/workload=high-density | awk 'NR>1 && ($3!="Running" || $4!="0")'

sleep 900                                                            # warm-up
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark quota-none "$START" "$END"
```

### 3. Step the quota down

Choose steps from a high value (little or no throttling) down to a value where clients are clearly throttled. The
values below are examples; 100 is one full thread.

```bash
for RP in 200 100 50 25; do
  run_p 09-set-client-quotas.sh "$CLUSTER_ARN" set --request-percentage "$RP"            # dry run: prints the command
  run_p 09-set-client-quotas.sh "$CLUSTER_ARN" set --request-percentage "$RP" --apply
  run_p 09-set-client-quotas.sh "$CLUSTER_ARN" describe --apply
  sleep 600                                                                            # let throttling settle
  START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1200; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  python3 analysis/measure_cluster.py load-benchmark "quota-rp-$RP" "$START" "$END"
  python3 analysis/compare.py quota-none "quota-rp-$RP"
done
```

Use one window length for every step (20 minutes above). Here a SUSPECT result from `compare.py` on the message
rate is the observation, not an error: it means the quota holds the clients below their offered rate. Record the
step at which it starts.

### What good looks like

At the looser steps, `RequestHandlerAvgIdlePercent` rises while `RequestThrottleTime` becomes non-zero and the
clients see only a small increase in produce latency and no send errors. At the tighter steps, the throttle
metrics and client latency grow, `MessagesInPerSec` drops below the offered rate, and consumer lag builds. The
right production value is above the point where healthy clients start to notice.

### Watch out

- Quotas are enforced per broker and per client (user and client-id pair).
- Throttled consumers fall behind, and lag keeps growing after you remove the quota until they catch up. Allow
  time before the next test.
- Always delete the quota and the extra IAM permission at the end.

**Key metrics.** `RequestHandlerAvgIdlePercent`, `RequestThrottleTime` / `RequestThrottleQueueSize`,
`ProduceThrottleTime`, `FetchThrottleTime`, `MessagesInPerSec`, client ack latency, and consumer lag, per quota
step.

## Clean up

```bash
run_p 09-set-client-quotas.sh "$CLUSTER_ARN" delete --apply
run_p 09-set-client-quotas.sh "$CLUSTER_ARN" describe --apply      # no quota left
kubectl -n msk-bench delete deploy -l msk-bench/workload=high-density
aws iam delete-role-policy --role-name "$HOST_ROLE" --policy-name msk-bench-client-quotas
```
