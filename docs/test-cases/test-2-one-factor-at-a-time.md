# Test 2: Find what drives the load, one factor at a time

**Purpose.** Reproduce the blog section "Find what drives the load, one factor at a time": start from a
low-density version of the cluster, add the production shape back one factor per run, and measure after each,
so every change in broker saturation is attributed to exactly one cause.

**Blog section:** "Find what drives the load, one factor at a time" (and the ranking in "What drives the request
count").

> **WARNING: test clusters only.** This runbook creates and deletes topics, changes a topic's configuration, and
> drives sustained load. Never run it against a production cluster.

## Harness

- **Core factors (steps 1-4): Amazon Elastic Compute Cloud (Amazon EC2), recommended.** The host scripts give
  direct control over partition count, producer instance count, consumer group count, and topic codec, one
  factor at a time. One producer host and consumer host 0.
- **Optional factors:** slow micro-batch consumers on Amazon EC2; consumer group count, worker threads, and Kafka
  Streams `at_least_once` (ALOS) versus `exactly_once_v2` (EOS) on Amazon Elastic Kubernetes Service (Amazon EKS).
  Run the Amazon EKS factors if production runs its clients on Amazon EKS.

## Prerequisites

- The [root README quick start](../../README.md#quick-start-amazon-ec2-harness) steps 1-3 (AWS CDK stacks, the
  Amazon Managed Streaming for Apache Kafka (Amazon MSK) cluster `load-benchmark`, the Amazon CloudWatch
  dashboard). For the Amazon EKS factors, also the
  [Amazon EKS quick start](../../README.md#quick-start-amazon-eks-harness) steps 1-4.
- The [shared setup](README.md#shared-setup): `run_p`, `run_c`, `use_profile`, `kafka_cli`, `KB`, and, for
  Amazon EKS, `IMG` and `KSE_IMG`.
- `scripts/profiles/baseline.properties` holds your production client settings.
- One consumer host is enough; add hosts (`npx cdk deploy --all -c consumerCount=<n>`) if a single host becomes
  CPU- or network-bound.

Topics used by this test (see [Topics and reset](README.md#topics-and-reset)):

| Part | Topics |
|---|---|
| Core factors, slow consumers | `factor-1..12` (your production partition count), `factor-bcast-1` (20 partitions) |
| Amazon EKS factors | `eks-factor-1..4` (24 partitions), `eks-factor-streams-1` (6 partitions) |

## Steps: core factors

The order follows the blog: partitions and replication factor first, then producer instances, then consumer
groups, then the codec mismatch. Each step keeps the previous steps' factors and adds one.

### 0. Choose the production shape and create the topics

Set these from your production cluster. The values below are **examples** to replace, not recommendations.

```bash
PARTS=100      # partitions per topic, so that 12 x PARTS x RF / NUM_BROKERS matches production partitions per broker
RF=3           # replication factor
MIN_ISR=2      # min.insync.replicas
REC=625        # average record size in bytes (use realistic payload sizes)
MBPS=100       # aggregate produce rate in MB/s (1 MB = 1048576 bytes), kept constant across all steps
NPROD=120      # producer instances in production
NGRP=3         # additional consumer groups per topic in production (besides the one 02 starts); 0 for none
NBCAST=100     # broadcast (fan-out) groups reading the broadcast topic
```

```bash
run_p 00-setup-topics.sh "$CLUSTER_ARN" factor 12 "$PARTS" 3600000 "$RF" "$MIN_ISR"
run_p 00-setup-topics.sh "$CLUSTER_ARN" factor-bcast 1 20 3600000 "$RF" "$MIN_ISR"
FACTOR_TOPICS=$(echo factor-{1..12} | tr ' ' ,)    # factor-1,...,factor-12 (for 07)
```

`00-setup-topics.sh` takes `<cluster-arn> [prefix] [num-topics] [partitions] [retention-ms] [rf] [min-isr]` and
creates `<prefix>-1..<num-topics>`. It never changes the partition count of a topic that already exists.

### How each step runs

Every step is measured twice, first with `PROF=baseline`, then with `PROF=aws-tuned`:

```bash
PROF=baseline                                    # second pass: PROF=aws-tuned
STEP=s1                                          # s1, s2, s3 or s4
run_p 03-stop-load.sh; run_c 03-stop-load.sh     # stop the previous run
use_profile "$PROF"
# ... start the step's load (the commands in the step) ...
run_p 04-load-status.sh; run_c 04-load-status.sh
```

Then measure the window as described in [Measure](#measure). Group names include the step and the profile, so
no run resumes from another run's committed offsets.

On each host, start the script that truncates the PID file first (`01` on the producer host, `02` on the consumer
host), then the scripts that append to it (`06`, `07`, `08`), so `03-stop-load.sh` stops everything. From step 2
on, the producer host runs only `08`, which appends; `03-stop-load.sh` checks every PID before it stops it, so the
older lines are harmless.

### Step 1: production partition count and replication factor, with realistic message sizes

```bash
run_p 01-start-producers.sh --background "$CLUSTER_ARN" factor 12 "$MBPS" "$REC"
run_c 02-start-consumers.sh --background "$CLUSTER_ARN" factor 12 "factor-$STEP-$PROF-cg"
```

```bash
python3 analysis/compare.py factor-s1-baseline factor-s1-aws-tuned
```

To see the cost of density itself, compare `factor-s1-baseline` with the low-density sanity check of
[Test 1](test-1-head-to-head.md) run with the same profile.

### Step 2: production-like producer instance counts

Step 1 with the same total rate and record size, sent by `NPROD` producer instances (`MultiProducer`) instead of
one `kafka-producer-perf-test` per topic.

```bash
run_p 08-start-multi-producers.sh "$CLUSTER_ARN" factor 12 "$MBPS" "$REC" "$NPROD" mp
run_c 02-start-consumers.sh --background "$CLUSTER_ARN" factor 12 "factor-$STEP-$PROF-cg"
```

`08` takes `<cluster-arn> [prefix] [num-topics] [target-mb-per-sec] [record-size] [num-producers] [client-prefix]`.

```bash
python3 analysis/compare.py factor-s2-baseline factor-s2-aws-tuned
python3 analysis/compare.py factor-s1-baseline factor-s2-baseline     # cost of many producer instances
```

### Step 3: production-like consumer group counts, including broadcast groups

Step 2 plus `NGRP` more groups on the main topics and a low-volume broadcast topic that `NBCAST` groups each read
in full (one group per service instance). A single producer feeds the broadcast topic at 1 MB/s (an example rate).

```bash
run_p 08-start-multi-producers.sh "$CLUSTER_ARN" factor 12 "$MBPS" "$REC" "$NPROD" mp
run_p 08-start-multi-producers.sh "$CLUSTER_ARN" factor-bcast 1 1 "$REC" 1 bcast-feed
run_c 02-start-consumers.sh --background "$CLUSTER_ARN" factor 12 "factor-$STEP-$PROF-cg"
run_c 07-start-multigroup-consumers.sh "$CLUSTER_ARN" "$FACTOR_TOPICS" "factor-$STEP-$PROF-grp" "$NGRP" 0 200   # skip if NGRP=0
run_c 07-start-multigroup-consumers.sh "$CLUSTER_ARN" factor-bcast-1 "factor-$STEP-$PROF-bcast" "$NBCAST" 0 200
```

`07` takes `<cluster-arn> <topics-csv> <group-prefix> <num-groups> [start-index] [stagger-ms]` and runs groups
`<group-prefix>-0..<num-groups - 1>` in one JVM, one `KafkaConsumer` per group, each subscribed to every topic in
the list. Every extra group on the main topics reads the full `MBPS`, so egress grows by `NGRP x MBPS`.

```bash
python3 analysis/compare.py factor-s3-baseline factor-s3-aws-tuned
python3 analysis/compare.py factor-s2-baseline factor-s3-baseline     # cost of the extra and broadcast groups
```

### Step 4: a topic whose codec differs from its producers

Step 3 plus one topic whose `compression.type` differs from what its producers send. The shipped `baseline`
producers send uncompressed batches (`compression.type=none`; yours may differ) and `aws-tuned` producers send
`zstd`, so `gzip` (an example) differs from both and the brokers recompress every batch on `factor-1`. See "Align
the topic codec with the producers" in [docs/client-tuning.md](../client-tuning.md). Pick a codec that differs from
your production producers' codec.

```bash
kafka_cli "/opt/kafka/bin/kafka-configs.sh $KB --alter --entity-type topics --entity-name factor-1 --add-config compression.type=gzip"
kafka_cli "/opt/kafka/bin/kafka-configs.sh $KB --describe --entity-type topics --entity-name factor-1"
```

Then run the step 3 load commands again for each profile with `STEP=s4`.

```bash
python3 analysis/compare.py factor-s4-baseline factor-s4-aws-tuned
python3 analysis/compare.py factor-s3-aws-tuned factor-s4-aws-tuned   # cost of the codec mismatch
```

Remove the override when you finish (see [Teardown](#teardown)).

## Optional factors

### A. Slow, micro-batch consumers (Amazon EC2)

Consumers that behave like a micro-batch engine (for example Spark Structured Streaming) poll, spend most of
their time processing, and poll again. `SlowConsumer.java` sleeps between polls, so each consumer moves at most
about `max.poll.records / (sleep-ms / 1000)` records per second (`06` prints this ceiling at start). The slow group
falls behind by design; once its read position leaves the broker page cache its fetches read from disk.

Run it on top of the step 1 load, once per profile:

```bash
PROF=baseline                                    # second pass: PROF=aws-tuned
run_p 03-stop-load.sh; run_c 03-stop-load.sh
use_profile "$PROF"
run_p 01-start-producers.sh --background "$CLUSTER_ARN" factor 12 "$MBPS" "$REC"
run_c 02-start-consumers.sh --background "$CLUSTER_ARN" factor 12 "factor-slow-$PROF-cg"
run_c 06-start-slow-consumers.sh "$CLUSTER_ARN" factor 12 "factor-slow-$PROF-slow" 3000
run_p 04-load-status.sh; run_c 04-load-status.sh
# ... measure the window with label factor-slow-$PROF ...
run_c 05-collect-diagnostics.sh "factor-slow-$PROF-slow"
```

`06` takes `<cluster-arn> [prefix] [num-topics] [group-name] [sleep-ms]` and starts one `SlowConsumer` per topic,
all in one group. `3000` ms is the script default; set it to your engine's typical batch duration.

```bash
python3 analysis/compare.py factor-slow-baseline factor-slow-aws-tuned
python3 analysis/compare.py factor-s1-baseline factor-slow-baseline   # cost of adding slow consumers
```

The slow consumers' logs (`slowconsumer-<group>-<topic>.log`) are CSV with `records_per_sec`, `polls`,
`empty_polls`, `max_poll_gap_ms`, and `sleep_ms`.

### B. Amazon EKS: consumer group count, worker threads, Kafka Streams ALOS vs EOS

Workload: [`k8s/examples/workload-eks-factors.yaml`](../../k8s/examples/workload-eks-factors.yaml), a base shape
of 4 topics, 8 producer clients, 4 consumer groups, no worker threads, plus a 1 MB/s feed into the Kafka Streams
input topic (topics `eks-factor-1..4` and `eks-factor-streams-1`). Each variant changes one line of the spec. All runs use the `baseline` profile; repeat with
`--tuning aws-tuned` if you also want the tuned cost of each factor.

```bash
run_p 00-setup-topics.sh "$CLUSTER_ARN" eks-factor 4 24            # eks-factor-1..4
run_p 00-setup-topics.sh "$CLUSTER_ARN" eks-factor-streams 1 6     # eks-factor-streams-1 (Kafka Streams input)

SPEC=k8s/examples/workload-eks-factors.yaml
sed 's/^  total_groups: 4 /  total_groups: 16 /'   $SPEC > /tmp/eks-factor-spec-groups.yaml
sed 's/^  worker_threads: 0 /  worker_threads: 8 /' $SPEC > /tmp/eks-factor-spec-workers.yaml

for v in base groups workers; do
  python3 k8s/gen-workload.py "/tmp/eks-factor-spec-$v.yaml" --image "$IMG" --summary-only
  python3 k8s/gen-workload.py "/tmp/eks-factor-spec-$v.yaml" --image "$IMG" --tuning baseline > "/tmp/eks-factor-$v.yaml"
done

apply_eks_factor() {
  kubectl apply -f "/tmp/eks-factor-$1.yaml" --prune -l msk-bench/workload=eks-factors \
    --prune-allowlist=apps/v1/Deployment
}
```

The variant values (16 groups, 8 worker threads) are examples; use the values from your production fleet. The
groups variant's summary shows egress at about 4x ingress. `--prune` removes Deployments of this workload that
the applied manifest no longer contains (for example the extra groups of the previous run).

1. **Base.** `apply_eks_factor base`, check `kubectl -n msk-bench get pods -l msk-bench/workload=eks-factors`
   (Running, `RESTARTS` 0), then measure with label `eks-factor-base`.
2. **Consumer groups.** `apply_eks_factor groups`, measure `eks-factor-groups`, then
   `python3 analysis/compare.py eks-factor-base eks-factor-groups`.
3. **Worker threads.** `apply_eks_factor workers` (also removes the extra groups), measure `eks-factor-workers`,
   then `python3 analysis/compare.py eks-factor-base eks-factor-workers`. `consumers.worker_threads` becomes
   `MultiGroupConsumer --worker-threads`: each group's poll thread splits every polled batch by partition across
   that many processing threads and waits for them before the next poll. Worker threads do not add group members.
4. **Kafka Streams ALOS vs EOS.** Apply the base workload, then add Kafka Streams emulator apps that read
   `eks-factor-streams-1` (see [kstreams-emulator/README.md](../../kstreams-emulator/README.md); build and push the
   image per [eks/ecr.md](../../eks/ecr.md)). `gen-kstreams.sh <alos|eos> <num-apps> [input-topic] [tuning-profile]`
   does not set the internal topics' replication factor (emulator default 2), so set it with
   `kubectl set env --local`. 4 apps is an example.

   ```bash
   apply_eks_factor base

   IMAGE="$KSE_IMG" k8s/kstreams/gen-kstreams.sh alos 4 eks-factor-streams-1 baseline > /tmp/eks-factor-kse-alos-raw.yaml
   kubectl set env --local -f /tmp/eks-factor-kse-alos-raw.yaml REPLICATION_FACTOR=3 -o yaml > /tmp/eks-factor-kse-alos.yaml
   kubectl apply -f /tmp/eks-factor-kse-alos.yaml
   kubectl -n msk-bench logs -l role=kstreams --prefix --tail=3            # CSV lines, state RUNNING
   # ... measure the window with label eks-factor-kse-alos ...
   kubectl -n msk-bench delete deploy -l role=kstreams,guarantee=alos

   IMAGE="$KSE_IMG" k8s/kstreams/gen-kstreams.sh eos 4 eks-factor-streams-1 baseline > /tmp/eks-factor-kse-eos-raw.yaml
   kubectl set env --local -f /tmp/eks-factor-kse-eos-raw.yaml REPLICATION_FACTOR=3 -o yaml > /tmp/eks-factor-kse-eos.yaml
   kubectl apply -f /tmp/eks-factor-kse-eos.yaml
   # ... measure the window with label eks-factor-kse-eos ...
   kubectl -n msk-bench delete deploy -l role=kstreams,guarantee=eos

   python3 analysis/compare.py eks-factor-kse-alos eks-factor-kse-eos
   python3 analysis/compare.py eks-factor-base eks-factor-kse-alos
   ```

   Optional variants with the same `kubectl set env --local` step: `COMMIT_INTERVAL_MS=<ms>` separates the cost
   of transactions from the cost of the commit frequency (defaults: 30000 ms for ALOS, 100 ms for EOS), and
   `NUM_STREAM_THREADS=<n>` changes the stream threads per app.

## Measure

For every run (see the root README [Measurement method](../../README.md#measurement-method)):

```bash
sleep 900                                                    # warm-up: 15 minutes
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)   # 30-minute window
python3 analysis/measure_cluster.py load-benchmark <label> "$START" "$END"
```

- **Warm-up.** About 15 minutes: all clients connected, no rebalance in progress, lag flat on the regular groups,
  no client errors (`04-load-status.sh`; on Amazon EKS, `RESTARTS` 0 and no `ERROR` lines in the pod logs).
- **Window.** The same length (15-30 minutes) for every run of this test.
- **Labels.** Core factors: `factor-<step>-<profile>`, for example `factor-s1-baseline` (with `STEP` and `PROF`
  set, `"factor-$STEP-$PROF"`). Slow consumers: `factor-slow-<profile>`. Amazon EKS: `eks-factor-base`,
  `eks-factor-groups`, `eks-factor-workers`, `eks-factor-kse-alos`, `eks-factor-kse-eos`.
- **Compare** with `python3 analysis/compare.py <label-a> <label-b>`. It prints the mean and the worst broker
  and exits 1 (SUSPECT) when message rates are more than 5% apart, partitions are under-replicated, or
  `RequestHandlerAvgIdlePercent` is missing.

## What good looks like

- Throughput stays roughly flat from step to step while saturation changes a lot.
- The factors that cause the largest drops in request-handler idle are where tuning effort belongs; typically
  partition density first and producer fan-out second, then consumer group count, broadcast groups, and the codec
  mismatch. Doubling consumer groups does not double the load.
- If saturation tracks partition and client counts rather than bytes, the cluster is request-bound and the fix
  belongs in the clients.
- `ProduceMessageConversionsPerSec` is 0 in every step except step 4.
- The optional Amazon EKS factors (Kafka Streams ALOS vs EOS, worker threads) move request-handler idle by a
  small amount compared with density and producer fan-out.

## Watch out

- **Change one factor per run** and keep `MBPS` and `REC` identical in every step, so the comparisons are at equal
  `MessagesInPerSec`. The broadcast feeder adds a small rate from step 3 on; `compare.py` flags pairs more than 5%
  apart.
- **Don't scale client counts down.** Load is not linear in client count; dividing production client counts by
  the broker ratio makes the test cluster look too comfortable. Reproduce partition and client counts per broker.
- **Use realistic payloads.** Random bytes compress very differently from real JSON or Avro.
- **Reset between runs.** Stale groups and offsets inflate `SumOffsetLag`; the group names here are unique per
  step and profile, and [Topics and reset](README.md#topics-and-reset) shows how to delete old groups.
- **Producer host memory.** `08` sizes its heap as `num-producers x buffer.memory` (64 MB with `aws-tuned`) plus
  slack. A large `NPROD` can exceed the host's memory; check `run_p 05-collect-diagnostics.sh` and the
  multiproducer log for `ERROR:` lines or `send_errors`.
- **Group start-up.** `07` starts groups 200 ms apart, so `NBCAST` groups need `NBCAST x 0.2` seconds plus
  rebalances to settle. Start the window only after `groups_running` in each multigroup log equals its group
  count.
- **Consumer host limits.** Extra groups multiply egress; a single consumer host may become network-bound. Check
  `run_c 05-collect-diagnostics.sh` (network allowance counters) before trusting a window.
- **Step 4 is an anti-pattern on purpose.** Remove the `compression.type` override afterwards.
- **Slow consumers.** The slow group never catches up, so "lag flat" applies only to the regular group. Use the
  same `sleep-ms`, window length, and time after load start for both profiles. Watch `max_poll_gap_ms` against
  `max.poll.interval.ms` (300000 in both shipped profiles); a consumer that exceeds it triggers rebalances, so
  discard that window. With 1-hour retention, a slow group more than an hour behind loses data instead of reading
  it from disk.
- **Amazon EKS.** Do not `kubectl scale` a `cg-*` Deployment: a second replica joins the same group instead of
  adding a group. Consumer pods have a 250m CPU limit; with worker threads, check `kubectl top pods -n msk-bench`
  for pods at their limit. Kafka Streams writes the input again to its repartition and changelog topics, so the
  Streams windows can be flagged SUSPECT on rate; judge ALOS and EOS against each other at the same input rate and
  app count. Wait for state `RUNNING` in the pod logs before the window.
- **Discard** any run with pod restarts, repeated rebalances, saturated client hosts, or producer send errors.

## Key metrics

`RequestHandlerAvgIdlePercent` (mean and worst broker), `PartitionCount`, `MessagesInPerSec` against
`BytesInPerSec`, `ProduceMessageConversionsPerSec`, `ProduceTotalTimeMsMean`, and `FetchConsumerTotalTimeMsMean`.

On the Amazon CloudWatch dashboard also watch `NetworkProcessorAvgIdlePercent`, `CPU total %`, `ConnectionCount`
and `ClientConnectionCount` (steps 2 and 3, Amazon EKS groups and Streams), `Consumer lag` and `Consumer time lag`
plus `VolumeReadBytes`, `VolumeReadOps`, and `VolumeQueueLength` (slow consumers), and, for Kafka Streams, the
emulator's `commit_rate` and `producer_request_rate` columns in the pod logs. Set `PARTITION_THRESHOLD` when you
publish the dashboard to your broker size's recommended maximum.

## Teardown

```bash
run_p 03-stop-load.sh
run_c 03-stop-load.sh
use_profile aws-tuned
kafka_cli "/opt/kafka/bin/kafka-configs.sh $KB --alter --entity-type topics --entity-name factor-1 --delete-config compression.type"
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --delete --topic 'factor-.*'"          # factor-1..12, factor-bcast-1

# Amazon EKS factors
kubectl -n msk-bench delete deploy -l role=kstreams
kubectl -n msk-bench delete deploy -l msk-bench/workload=eks-factors
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --delete --topic 'eks-factor-.*'"      # eks-factor-1..4, eks-factor-streams-1
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --delete --topic 'kse-.*'"             # Kafka Streams internal topics
```

The groups (`factor-*`, `bench-cg-*`, `kse-*`) stay on the cluster until `offsets.retention.minutes` expires them;
delete them earlier with `kafka-consumer-groups.sh --delete` as shown in [Topics and reset](README.md#topics-and-reset).

**Limitations.** No script changes a topic's `compression.type` or deletes topics; this runbook uses the standard
Kafka CLI through `kafka_cli`. `07` does not expose worker threads or members per group on Amazon EC2 (Amazon EKS
only). `gen-kstreams.sh` does not expose `REPLICATION_FACTOR`, `NUM_STREAM_THREADS`, or `COMMIT_INTERVAL_MS`; this
runbook sets them with `kubectl set env --local`. The emulator Deployments are not labeled with
`msk-bench/workload`, so they are selected with `role=kstreams`. Every `gen-kstreams.sh` call creates new
`application.id`s and therefore new internal topics.
