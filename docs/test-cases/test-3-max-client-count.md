# Test 3: Stress-test connections at your maximum client count

**Purpose.** Reproduce the blog test "Stress-test connections at your maximum client count": run the largest
client count you expect in production, with one consumer instance per partition across several consumer groups,
and compare the `baseline` and `aws-tuned` client profiles. The test shows how close the client fleet gets to the
Amazon Managed Streaming for Apache Kafka (Amazon MSK) connection limits for AWS Identity and Access Management
(IAM) access control, and how much tuning still helps when each consumer owns very few partitions.

**Blog section:** "Stress-test connections at your maximum client count" (in "How to test: a step-by-step
approach").

**Harness.** Amazon Elastic Kubernetes Service (Amazon EKS). This is the only harness for this test: the Amazon
Elastic Compute Cloud (Amazon EC2) multi-group consumer does not run several members per group
(`consumers.members_per_group` is Amazon EKS only).

**Prerequisites.**

- The root README [Prerequisites](../../README.md#prerequisites), steps 1-3 of the
  [Amazon EC2 quick start](../../README.md#quick-start-amazon-ec2-harness) (the producer host is used for topic
  and group commands), and steps 1-4 of the
  [Amazon EKS quick start](../../README.md#quick-start-amazon-eks-harness).
- The [shared setup](README.md#shared-setup) in your shell, including `IMG`, `run_p`, `kafka_cli`, `KB`, and
  `PRODUCER_ID`.
- `baseline` is your production client configuration (see the shared setup), so that the baseline run shows the
  connection and request pattern your production fleet produces.

**Workload.** [`k8s/examples/workload-one-consumer-per-partition.yaml`](../../k8s/examples/workload-one-consumer-per-partition.yaml):
4 topics x 60 partitions, 4 consumer groups (one per topic) with 60 members each (`members_per_group: 20`
KafkaConsumer instances per pod x `pods_per_group: 3` replicas, so 12 consumer pods and 240 consumers), and 20
producer clients. `members_per_group` x `pods_per_group` must equal the partition count; the generator warns when
a group has more members than `estimate.partitions_per_topic`. To get closer to your production maximum, raise
`pods_per_group`, `total_groups`, and the partition count together in a copy of the spec (and recreate the topics).

## Steps

### 1. Look up the IAM connection limits for your broker size

Amazon MSK allows up to 3,000 IAM connections per broker and limits how many new IAM connections a broker
accepts per second (100 per second on M7g instances). The limits depend on the broker instance size; look up
yours in [Amazon MSK quotas](https://docs.aws.amazon.com/msk/latest/developerguide/limits.html). Each client
opens several connections (bootstrap, one per broker it talks to, and the group coordinator), so the connection
count grows faster than the client count.

Draw the limits on the Amazon CloudWatch dashboard (the default lines are examples only):

```bash
CONN_COUNT_THRESHOLD=<max IAM connections per broker> CONN_RATE_THRESHOLD=<max new IAM connections per second per broker> \
  ./scripts/15-put-dashboard.sh load-benchmark "$AWS_REGION" "$NUM_BROKERS"
```

### 2. Create the topics and remove stale consumer groups

The spec needs `load-bench-1..4` with 60 partitions. Stop all load, then reset the `load-bench` topics as described
in [Topics and reset](README.md#topics-and-reset):

```bash
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --delete --topic 'load-bench-.*'"
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --list"                  # wait until no load-bench-* is listed
run_p 00-setup-topics.sh "$CLUSTER_ARN" load-bench 4 60
```

Dead groups and stale offsets from earlier runs inflate `SumOffsetLag` and can make a healthy run look broken.
List the groups and delete every group this test does not use (stop its consumers first):

```bash
kafka_cli "/opt/kafka/bin/kafka-consumer-groups.sh $KB --list"
kafka_cli "/opt/kafka/bin/kafka-consumer-groups.sh $KB --delete --group <group>"
```

### 3. Generate the manifests

```bash
SPEC=k8s/examples/workload-one-consumer-per-partition.yaml
python3 k8s/gen-workload.py $SPEC --image "$IMG" --summary-only     # 12 consumer pods, 240 KafkaConsumer clients
python3 k8s/gen-workload.py $SPEC --image "$IMG" --tuning baseline  > /tmp/test3-baseline.yaml
python3 k8s/gen-workload.py $SPEC --image "$IMG" --tuning aws-tuned > /tmp/test3-aws-tuned.yaml
```

Each consumer group is its own Deployment, labeled `group=bench-cg-0` to `group=bench-cg-3`; the producers are
labeled `role=producer`.

### 4. Baseline run: ramp up group by group

Start the producers, then add the consumer groups one at a time instead of all at once, so the connection
creation rate stays below the per-broker limit. Watch `ConnectionCreationRate` on the dashboard while each group
joins, and wait until it settles before you add the next one.

```bash
kubectl apply -f /tmp/test3-baseline.yaml -l role=producer
for g in 0 1 2 3; do
  kubectl apply -f /tmp/test3-baseline.yaml -l group=bench-cg-$g
  sleep 120                                  # let the group's members connect and the rebalance finish
done
kubectl -n msk-bench get pods -l msk-bench/workload=one-consumer-per-partition | awk 'NR>1 && ($3!="Running" || $4!="0")'
```

The `awk` filter prints only pods that are not running or that restarted; it should print nothing.

Check that every member owns exactly one partition (repeat for `bench-cg-1` to `bench-cg-3`). In the `--members`
output, the `#PARTITIONS` column should be 1 for all 60 members of the group:

```bash
kafka_cli "/opt/kafka/bin/kafka-consumer-groups.sh $KB --describe --group bench-cg-0 --members"
```

Measure the window as described under [Measure](#measure), with the label `test3-baseline`.

### 5. Tuned run

Stop the baseline consumers and producers, wait for the connection count to drop, and ramp the tuned profile up
the same way. Applying the tuned manifest on top of the running baseline would restart all 12 consumer pods in a
rolling update and create a burst of new connections.

```bash
kubectl -n msk-bench delete deploy -l msk-bench/workload=one-consumer-per-partition
kubectl apply -f /tmp/test3-aws-tuned.yaml -l role=producer
for g in 0 1 2 3; do
  kubectl apply -f /tmp/test3-aws-tuned.yaml -l group=bench-cg-$g
  sleep 120
done
kubectl -n msk-bench get pods -l msk-bench/workload=one-consumer-per-partition | awk 'NR>1 && ($3!="Running" || $4!="0")'
kafka_cli "/opt/kafka/bin/kafka-consumer-groups.sh $KB --describe --group bench-cg-0 --members"
```

The tuned run keeps the same group names, so the groups resume from the offsets the baseline committed. Measure
with the label `test3-aws-tuned` and compare the two runs (see [Measure](#measure)).

## Measure

Follow the measuring procedure in the [test case index](README.md): warm up for about 15 minutes after the last
group joined (all members connected, no rebalance in progress, lag flat, no pod restarts), then record one steady
window of the same length for both runs (30 minutes below).

```bash
sleep 900                                                            # warm-up
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark test3-baseline "$START" "$END"     # test3-aws-tuned for step 5
python3 analysis/compare.py test3-baseline test3-aws-tuned                            # after both runs
```

For each window `measure_cluster.py` records `iam_conns_mean_max_broker` (the busiest broker's
`ClientConnectionCount` with `Client Authentication` = `IAM`, Sum statistic) and the consumer lag per consumer
group and topic (`sum_offset_lag_max`, `time_lag_max_s`, `time_lag_p50_s`, from an Amazon CloudWatch Metrics
Insights query over the 100 largest group and topic pairs; `lag_pairs_reported` says how many it found).

**Read lag per consumer group and topic.** The dashboard's lag widgets sum a `SEARCH` over every group and topic.
With thousands of series that search can hit Amazon CloudWatch metric limits and quietly show zero lag. Query one
group and topic directly instead (group `bench-cg-N` reads topic `load-bench-N+1`):

```bash
for METRIC in SumOffsetLag EstimatedMaxTimeLag; do
  aws cloudwatch get-metric-statistics --namespace AWS/Kafka --metric-name "$METRIC" \
    --dimensions Name="Cluster Name",Value=load-benchmark Name="Consumer Group",Value=bench-cg-0 Name=Topic,Value=load-bench-1 \
    --start-time "$START" --end-time "$END" --period 60 --statistics Maximum --output table
done
```

**Connection creation rate.** `measure_cluster.py` does not record `ConnectionCreationRate`. Read it on the
dashboard, or for one broker (repeat for each broker ID), including the ramp-up period:

```bash
aws cloudwatch get-metric-statistics --namespace AWS/Kafka --metric-name ConnectionCreationRate \
  --dimensions Name="Cluster Name",Value=load-benchmark Name="Broker ID",Value=1 \
  --start-time "$START" --end-time "$END" --period 60 --statistics Maximum Sum --output table
```

## What good looks like

- The tuned profile still raises `RequestHandlerAvgIdlePercent`, but by less than in the head-to-head test:
  consumers that own a single partition leave little fetch batching to improve.
- Connection count and connection creation rate climb toward the IAM limits as the groups join, and stay below
  80% of those limits on every broker, during ramp-up as well as in the steady window.
- `EstimatedMaxTimeLag` rises with the tuned fetch settings (to tens of seconds) but stays flat over the window
  instead of growing. `SumOffsetLag` is flat and bounded for every group and topic.
- Throughput (`MessagesInPerSec`) is the same in both runs, so `compare.py` reports OK.

## Watch out

- **IAM connection limits.** Each consumer opens several connections. Start groups gradually and watch
  `ClientConnectionCount` and `ConnectionCreationRate`. `stagger_ms: 200` in the spec spaces the members inside a
  pod, not the pods. If connections are refused or authentication fails, clients retry, pods may restart, and the
  reconnect traffic adds to the rate. Treat any window with pod restarts as invalid.
- **Lag dashboards that show zero.** A dashboard covering thousands of group and topic series can hit Amazon
  CloudWatch metric limits and show zero lag. Query lag per consumer group and topic (see [Measure](#measure)).
- **Stale offsets.** Dead groups and stale partition offsets from earlier runs inflate `SumOffsetLag`. Delete them
  before the run (step 2).
- **Bounded time lag.** The tuned fetch settings raise `EstimatedMaxTimeLag`; confirm it stays flat, not growing.
- **Assignment.** A group with more members than partitions leaves members idle; with fewer, some members own two
  partitions. Verify the assignment with `--members` after every change.
- **Scaling.** To probe higher client counts, raise `pods_per_group` and the partition count together in a copy of
  the spec. Do not use `kubectl scale`, which adds members without new partitions.
- **Client pod size.** Each consumer pod runs 20 KafkaConsumer instances, so the spec gives it 1 CPU and 2 GiB.
  Check `kubectl top pods -n msk-bench -l role=consumer`; a CPU-saturated client sends requests in bursts.

## Key metrics

`ClientConnectionCount` (`Client Authentication` = `IAM`, Sum), `ConnectionCreationRate` (Sum), `SumOffsetLag` and
`EstimatedMaxTimeLag` per consumer group and topic, and `RequestHandlerAvgIdlePercent`, at equal
`MessagesInPerSec`. On the dashboard: `ClientConnectionCount`, `ConnectionCount`, `ConnectionCreationRate` against
the thresholds from step 1, `RequestHandlerAvgIdlePercent`, `NetworkProcessorAvgIdlePercent`,
`FetchConsumerTotalTimeMsMean`, and `MessagesInPerSec`.

## Teardown

```bash
kubectl -n msk-bench delete deploy -l msk-bench/workload=one-consumer-per-partition
```

Reset the `load-bench` topics ([Topics and reset](README.md#topics-and-reset)) before running a test that needs
another partition count, and delete the `bench-cg-*` groups if the next test does not reuse them.
