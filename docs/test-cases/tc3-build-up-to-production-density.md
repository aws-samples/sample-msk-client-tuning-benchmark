# Test case 3: Build up to production density

**Purpose.** Start from a simple workload and add one production characteristic per step, measuring both
client profiles at every step, so you can attribute broker load to each factor: partition count and replication
factor with a realistic message size, broadcast consumer groups, a topic whose codec differs from its producers,
and a production-like number of producer instances.

**What to expect.** Each factor adds broker work that byte throughput alone does not show: more partitions and
replicas with small records mean more produce and replica-fetch requests; broadcast groups add fetch, heartbeat,
and commit traffic per group; a codec mismatch makes the brokers recompress every batch; many small producers
send many small requests. Comparing the profiles at each step shows how much of each cost the client settings
can remove.

**Harness.** Amazon EC2 (one producer host, consumer host 0).

**Prerequisites.** [Before you start](README.md#before-you-start) (EC2 quick start steps 1-3), the
[common shell setup](README.md#common-shell-setup) including `kafka_cli` and `KB`. One consumer host is enough;
add hosts (`npx cdk deploy --all -c consumerCount=<n>`) if a single host becomes CPU- or network-bound.

## 0. Choose the production shape

Set these from your production cluster. The values below are **examples** to replace, not recommendations.

```bash
PARTS=100      # partitions per topic, so that 12 x PARTS x RF / NUM_BROKERS matches production partitions per broker
RF=3           # replication factor
MIN_ISR=2      # min.insync.replicas
REC=625        # average record size in bytes
MBPS=100       # aggregate produce rate in MB/s (1 MB = 1048576 bytes), kept constant across all steps
NBCAST=100     # broadcast groups on the broadcast topic
NPROD=120      # producer instances in production
```

Create the topics for this test case (a separate prefix, so other test cases are not affected):

```bash
run_p 00-setup-topics.sh "$CLUSTER_ARN" tc3 12 "$PARTS" 3600000 "$RF" "$MIN_ISR"
run_p 00-setup-topics.sh "$CLUSTER_ARN" tc3-bcast 1 20 3600000 "$RF" "$MIN_ISR"
```

`00-setup-topics.sh` takes `<cluster-arn> [prefix] [num-topics] [partitions] [retention-ms] [rf] [min-isr]` and
creates `<prefix>-1..<num-topics>`.

## How each step runs

Every step is measured twice, first with `PROF=baseline`, then with `PROF=aws-tuned`. For each profile:

```bash
PROF=baseline                                    # second pass: PROF=aws-tuned
STEP=s1                                          # s1, s2, s3 or s4
run_p 03-stop-load.sh; run_c 03-stop-load.sh     # stop the previous run
use_profile "$PROF"
# ... start the step's load (the commands in the step) ...
run_p 04-load-status.sh; run_c 04-load-status.sh
sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark "tc3-$STEP-$PROF" "$START" "$END"
```

On each host, start the script that truncates the PID file first (`01` on the producer host, `02` on the
consumer host), then the scripts that append to it (`06`, `07`, `08`), so `03-stop-load.sh` stops everything.

## Step 1: partitions and replication factor with a realistic message size

```bash
# Load for step 1 (STEP=s1)
run_p 01-start-producers.sh --background "$CLUSTER_ARN" tc3 12 "$MBPS" "$REC"
run_c 02-start-consumers.sh --background "$CLUSTER_ARN" tc3 12 "tc3-$STEP-$PROF-cg"
```

```bash
python3 analysis/compare.py tc3-s1-baseline tc3-s1-aws-tuned
```

## Step 2: add broadcast groups

Step 1 plus a low-volume broadcast topic that `NBCAST` groups each read in full, one group per service instance.
`08` feeds the broadcast topic with a single producer at 1 MB/s (an example rate).

```bash
# Load for step 2 (STEP=s2)
run_p 01-start-producers.sh --background "$CLUSTER_ARN" tc3 12 "$MBPS" "$REC"
run_p 08-start-multi-producers.sh "$CLUSTER_ARN" tc3-bcast 1 1 "$REC" 1 bcast-feed
run_c 02-start-consumers.sh --background "$CLUSTER_ARN" tc3 12 "tc3-$STEP-$PROF-cg"
run_c 07-start-multigroup-consumers.sh "$CLUSTER_ARN" tc3-bcast-1 bench-bcast-host0 "$NBCAST" 0 200
```

`07` takes `<cluster-arn> <topics-csv> <group-prefix> <num-groups> [start-index] [stagger-ms]` and runs groups
`bench-bcast-host0-0..NBCAST-1` in one JVM. `08` takes
`<cluster-arn> [prefix] [num-topics] [target-mb-per-sec] [record-size] [num-producers] [client-prefix]`.

```bash
python3 analysis/compare.py tc3-s2-baseline tc3-s2-aws-tuned
python3 analysis/compare.py tc3-s1-baseline tc3-s2-baseline     # cost of the broadcast groups
```

## Step 3: a topic whose codec differs from its producers

Step 2 plus one topic whose `compression.type` differs from what its producers send. `baseline` producers send
uncompressed batches and `aws-tuned` producers send `zstd`, so `gzip` (an example) differs from both and the
brokers recompress every batch on `tc3-1`. See "Align the topic codec with the producers" in
[docs/client-tuning.md](../client-tuning.md).

The repository has no script that changes a topic configuration, so set it with `kafka-configs.sh` through
`kafka_cli`:

```bash
kafka_cli "/opt/kafka/bin/kafka-configs.sh $KB --alter --entity-type topics --entity-name tc3-1 --add-config compression.type=gzip"
kafka_cli "/opt/kafka/bin/kafka-configs.sh $KB --describe --entity-type topics --entity-name tc3-1"
```

Then run the step 2 load commands again for each profile with `STEP=s3`.

```bash
python3 analysis/compare.py tc3-s3-baseline tc3-s3-aws-tuned
python3 analysis/compare.py tc3-s2-aws-tuned tc3-s3-aws-tuned   # cost of the codec mismatch
```

## Step 4: production-like producer instance counts

Step 3 with the same total rate and record size, sent by `NPROD` producer instances (`MultiProducer`) instead of
one `kafka-producer-perf-test` per topic. Keep the `tc3-1` codec override from step 3.

```bash
# Load for step 4 (STEP=s4)
run_p 08-start-multi-producers.sh "$CLUSTER_ARN" tc3 12 "$MBPS" "$REC" "$NPROD" mp
run_p 08-start-multi-producers.sh "$CLUSTER_ARN" tc3-bcast 1 1 "$REC" 1 bcast-feed
run_c 02-start-consumers.sh --background "$CLUSTER_ARN" tc3 12 "tc3-$STEP-$PROF-cg"
run_c 07-start-multigroup-consumers.sh "$CLUSTER_ARN" tc3-bcast-1 bench-bcast-host0 "$NBCAST" 0 200
```

```bash
python3 analysis/compare.py tc3-s4-baseline tc3-s4-aws-tuned
python3 analysis/compare.py tc3-s3-baseline tc3-s4-baseline     # cost of many producer instances
```

## Dashboard

- `RequestHandlerAvgIdlePercent` and `NetworkProcessorAvgIdlePercent` (worst broker) at every step.
- `PartitionCount` after step 0 (set `PARTITION_THRESHOLD` when you publish the dashboard to your broker size's
  recommended maximum).
- `CPU total %` and `ProduceMessageConversionsPerSec / FetchMessageConversionsPerSec` in step 3.
- `ConnectionCount` and `ClientConnectionCount` in steps 2 and 4.
- `MessagesInPerSec` constant across steps; `Produce latency ms`, `FetchConsumerTotalTimeMsMean`.

**Key metrics.** `RequestHandlerAvgIdlePercent` per step and per profile, CPU, conversions, connection counts.

## Watch out

- Keep `MBPS` and `REC` identical in every step, so the comparisons between steps are at equal
  `MessagesInPerSec`. The broadcast feeder adds a small rate from step 2 on; `compare.py` flags pairs more than 5%
  apart.
- `08` sizes its heap as `num-producers x buffer.memory` (64 MB with `aws-tuned`) plus slack. A large `NPROD`
  can exceed the producer host's memory; check `run_p 05-collect-diagnostics.sh` and the multiproducer log for
  `ERROR:` lines or `send_errors`.
- `07` starts the `NBCAST` broadcast groups 200 ms apart, so the fleet needs `NBCAST x 0.2` seconds plus
  rebalances to settle. Start the window only after `groups_running` in the multigroup log equals `NBCAST`.
- A single consumer host may become network-bound at high egress; check `run_c 05-collect-diagnostics.sh`
  (network allowance counters) before trusting a window.
- Step 3 is an anti-pattern on purpose. Remove the override when you finish (below).

## Stop and clean up

```bash
run_p 03-stop-load.sh
run_c 03-stop-load.sh
use_profile aws-tuned
kafka_cli "/opt/kafka/bin/kafka-configs.sh $KB --alter --entity-type topics --entity-name tc3-1 --delete-config compression.type"
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --delete --topic 'tc3.*'"     # tc3-1..12 and tc3-bcast-1
```

The groups `tc3-*-cg` and `bench-bcast-host0-*` stay on the cluster until `offsets.retention.minutes` expires
them.

**Limitations.** The repository has no script to change a topic's `compression.type` or to delete topics; this
runbook uses the standard Kafka CLI through `kafka_cli`. `07` does not expose worker threads or members per
group on Amazon EC2.
