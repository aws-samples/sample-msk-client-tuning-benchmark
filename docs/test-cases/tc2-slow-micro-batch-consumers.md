# Test case 2: Slow, micro-batch consumers

**Purpose.** Add consumers that behave like a micro-batch engine (for example Spark Structured Streaming): they
poll, spend most of their time processing, and poll again. Measure how the two client profiles behave on the
brokers when part of the consumer fleet is slow and falls behind.

**What to expect.** `SlowConsumer.java` sleeps between polls, so each consumer moves at most about
`max.poll.records / (sleep-ms / 1000)` records per second (`06-start-slow-consumers.sh` prints this ceiling at
start). The slow group falls behind by design, its lag grows, and once its read position leaves the broker page
cache its fetches read from disk. The profiles differ in `max.poll.records` and in the fetch settings, so they
change how much each poll moves and how many fetch requests the brokers serve.

**Harness.** Amazon EC2.

**Prerequisites.** [Before you start](README.md#before-you-start) (EC2 quick start steps 1-3) and the
[common shell setup](README.md#common-shell-setup). Topics: `load-bench-1..12` with 24 partitions, as created by
`create-msk-standard-cluster.sh` (if you reset them for another test case, [recreate](README.md#topics-per-test-case)
them with `00-setup-topics.sh "$CLUSTER_ARN" load-bench 12 24`).

The slow consumers run on the same consumer host as the regular consumers. Start `02` first: it truncates the
host's PID file, while `06` appends to it, so `03-stop-load.sh` stops both.

## 1. Baseline run

```bash
use_profile baseline
run_p 01-start-producers.sh --background "$CLUSTER_ARN" load-bench 12 100
run_c 02-start-consumers.sh --background "$CLUSTER_ARN" load-bench 12 tc2-baseline-cg
run_c 06-start-slow-consumers.sh "$CLUSTER_ARN" load-bench 12 tc2-baseline-slow 3000
run_p 04-load-status.sh
run_c 04-load-status.sh

sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)

run_c 05-collect-diagnostics.sh tc2-baseline-slow
run_p 03-stop-load.sh
run_c 03-stop-load.sh
python3 analysis/measure_cluster.py load-benchmark tc2-baseline "$START" "$END"
```

`06` takes `<cluster-arn> [prefix] [num-topics] [group-name] [sleep-ms]` and starts one `SlowConsumer` per
topic, all in one group. `3000` ms is the script default; set it to your engine's typical batch duration.

## 2. Tuned run

Use new group names so that neither group resumes from the previous run's committed offsets (the slow group
would otherwise start with the backlog it left behind).

```bash
use_profile aws-tuned
run_p 01-start-producers.sh --background "$CLUSTER_ARN" load-bench 12 100
run_c 02-start-consumers.sh --background "$CLUSTER_ARN" load-bench 12 tc2-aws-tuned-cg
run_c 06-start-slow-consumers.sh "$CLUSTER_ARN" load-bench 12 tc2-aws-tuned-slow 3000
run_p 04-load-status.sh
run_c 04-load-status.sh

sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)

run_c 05-collect-diagnostics.sh tc2-aws-tuned-slow
run_p 03-stop-load.sh
run_c 03-stop-load.sh
python3 analysis/measure_cluster.py load-benchmark tc2-aws-tuned "$START" "$END"
```

## 3. Compare

```bash
python3 analysis/compare.py tc2-baseline tc2-aws-tuned
# Optional: the cost of adding slow consumers, against the test case 1 run with the same profile.
python3 analysis/compare.py tc1-ec2-baseline tc2-baseline
```

The slow consumers' logs (`results/.../slowconsumer-<group>-<topic>.log` after a pull) are CSV with
`records_per_sec`, `polls`, `empty_polls`, `max_poll_gap_ms`, and `sleep_ms`.

## Dashboard

- `RequestHandlerAvgIdlePercent`, `CPU total %`, `FetchConsumerTotalTimeMsMean`.
- `Consumer lag (all groups)` and `Consumer time lag`: the slow group's lag climbs; the regular group's stays
  flat.
- `EBS throughput` (`VolumeReadBytes`), `EBS IOPS` (`VolumeReadOps`), and `EBS back pressure: VolumeQueueLength`:
  disk reads appear once the slow group reads data that is no longer in the page cache.
- `Throughput BytesIn/BytesOut`.

**Key metrics.** `RequestHandlerAvgIdlePercent`, `FetchConsumerTotalTimeMsMean`, volume read activity, and the
slow group's lag, at equal `MessagesInPerSec`.

## Watch out

- The slow group never catches up, so "lag flat" applies only to the regular group. Both runs must use the same
  `sleep-ms` and window length, and start their slow group from an empty group (a new group name).
- Watch `max_poll_gap_ms` in the slow consumer logs against `max.poll.interval.ms` (300000 in both shipped
  profiles). A consumer that exceeds it leaves the group and triggers rebalances; discard such a window.
- The disk-read effect depends on how long the run has been going, so compare windows that start at the same
  time after load start.
- Retention is 1 hour (`00-setup-topics.sh` default). A slow group that falls more than an hour behind loses
  data instead of reading it from disk.

## Stop and clean up

```bash
run_p 03-stop-load.sh
run_c 03-stop-load.sh
use_profile aws-tuned
```

The groups `tc2-*` stay on the cluster until `offsets.retention.minutes` expires them.
