# Test case 7: One consumer per partition and IAM connection limits

**Purpose.** Run consumer groups with as many members as their topic has partitions, so every member owns one
partition, and compare the profiles. This maximizes consumer connections and fetch requests per broker, and it
shows how close the client fleet gets to the Amazon MSK quotas for connections with IAM access control.

**What to expect.** Each member holds its own connections and sends its own fetch, heartbeat, and commit requests,
so the request rate and connection count scale with the member count rather than with bytes. Fetch settings
(`fetch.min.bytes`, `fetch.max.wait.ms`) decide how many of those fetches return little or no data. Connection
counts and the connection creation rate during startup and rebalances are what approach the IAM connection
quotas.

**Harness.** Amazon EKS.

**Prerequisites.** [Before you start](README.md#before-you-start) (EC2 quick start steps 1-3, EKS quick start
steps 1-4) and the [common shell setup](README.md#common-shell-setup) including `kafka_cli` and `KB`.

Workload: [`k8s/examples/workload-one-consumer-per-partition.yaml`](../../k8s/examples/workload-one-consumer-per-partition.yaml):
4 topics x 60 partitions, 1 group per topic with 60 members (`members_per_group: 20` KafkaConsumer instances per
pod x `pods_per_group: 3` replicas), 20 producer clients. `members_per_group` x `pods_per_group` must equal the
partition count; the generator warns when a group has more members than `estimate.partitions_per_topic`.

## 0. Topics and dashboard thresholds

The spec needs `load-bench-1..4` with 60 partitions. Stop all load, reset the `load-bench` topics if they exist
with another partition count, and create them:

```bash
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --delete --topic 'load-bench-.*'"
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --list"                  # no load-bench-* left
run_p 00-setup-topics.sh "$CLUSTER_ARN" load-bench 4 60
```

Look up the IAM access control connection quotas for your broker instance size in
[Amazon MSK quotas](https://docs.aws.amazon.com/msk/latest/developerguide/limits.html) and draw them on the
dashboard (the default lines are examples):

```bash
CONN_COUNT_THRESHOLD=<max TCP connections per broker> CONN_RATE_THRESHOLD=<max new connections per second per broker> \
  ./scripts/15-put-dashboard.sh load-benchmark "$AWS_REGION" 3
```

## 1. Generate the manifests

```bash
SPEC=k8s/examples/workload-one-consumer-per-partition.yaml
python3 k8s/gen-workload.py $SPEC --image "$IMG" --summary-only     # 12 consumer pods, 240 KafkaConsumer clients
python3 k8s/gen-workload.py $SPEC --image "$IMG" --tuning baseline  > /tmp/tc7-baseline.yaml
python3 k8s/gen-workload.py $SPEC --image "$IMG" --tuning aws-tuned > /tmp/tc7-aws-tuned.yaml
```

## 2. Baseline run

```bash
kubectl apply -f /tmp/tc7-baseline.yaml
kubectl -n msk-bench get pods -l msk-bench/workload=one-consumer-per-partition | awk 'NR>1 && ($3!="Running" || $4!="0")'

# Every member should own exactly one partition (repeat for bench-cg-1..3).
kafka_cli "/opt/kafka/bin/kafka-consumer-groups.sh $KB --describe --group bench-cg-0 --members"

sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc7-baseline "$START" "$END"
```

In the `--members` output, the `#PARTITIONS` column should be 1 for all 60 members of each group.

## 3. Tuned run

```bash
kubectl apply -f /tmp/tc7-aws-tuned.yaml
kubectl -n msk-bench get pods -l msk-bench/workload=one-consumer-per-partition | awk 'NR>1 && ($3!="Running" || $4!="0")'
kafka_cli "/opt/kafka/bin/kafka-consumer-groups.sh $KB --describe --group bench-cg-0 --members"

sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc7-aws-tuned "$START" "$END"
python3 analysis/compare.py tc7-baseline tc7-aws-tuned
```

`measure_cluster.py` also reports the maximum IAM connections per broker for each window.

## Dashboard

- `ConnectionCount` and `ClientConnectionCount` (per broker) against `CONN_COUNT_THRESHOLD`.
- `ConnectionCreationRate` against `CONN_RATE_THRESHOLD`, especially while the pods start and during the profile
  switch.
- `RequestHandlerAvgIdlePercent`, `NetworkProcessorAvgIdlePercent`, `FetchConsumerTotalTimeMsMean`.
- `MessagesInPerSec`, `Consumer lag (all groups)`.

**Key metrics.** Connections per broker, connection creation rate, `RequestHandlerAvgIdlePercent`, and fetch
latency, at equal `MessagesInPerSec`.

## Watch out

- Startup and the profile switch are the risky moments for the connection creation rate: 12 pods start 20
  members each (`stagger_ms: 200` spaces the members inside a pod, not the pods). If connections are refused or
  authentication fails, the clients retry, pods may restart, and the reconnect traffic adds to the rate. Treat
  any window with pod restarts as invalid.
- A group with more members than partitions leaves members idle; with fewer, some members own two partitions.
  Verify the assignment with `--members` after every change.
- Each consumer pod runs 20 KafkaConsumer instances, so the spec gives it 1 CPU and 2Gi; check
  `kubectl top pods -n msk-bench -l role=consumer`.
- To probe the connection quotas, raise `pods_per_group` and the partition count together in a copy of the spec
  (and recreate the topics); do not use `kubectl scale`, which adds members without new partitions.

## Stop and clean up

```bash
kubectl -n msk-bench delete deploy -l msk-bench/workload=one-consumer-per-partition
```

Reset the `load-bench` topics (README, [Topics per test case](README.md#topics-per-test-case)) before running a
test case that needs another partition count.
