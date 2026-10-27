# Test 1: Validate the test cluster, then compare baseline and tuned clients head to head

**Purpose.** Prove that the test cluster behaves like production ("Validate the test cluster against production"),
then run the baseline and tuned client profiles against it at the same message rate and again at peak
("Compare baseline and tuned client profiles head to head").

**Blog section:** "Validate the test cluster against production" and "Compare baseline and tuned client profiles
head to head" in the AWS blog post "Right-sizing Amazon MSK by tuning your Kafka clients".

**Harness.** Amazon Elastic Kubernetes Service (Amazon EKS) is recommended, and required for the
production-shaped runs (steps 3-6) and the peak variant (step 7): the blog's guidance is to drive the test from
where production runs its clients. The optional sanity check (step 2) runs on Amazon EC2 or Amazon EKS.

> **WARNING: test clusters only.** This runbook deletes and recreates topics and drives sustained load. Run the
> load only against the Amazon Managed Streaming for Apache Kafka (Amazon MSK) test cluster. The only command
> that touches production is the read-only `analysis/diagnose.py` in step 1.

## Prerequisites

- The root README [Prerequisites](../../README.md#prerequisites), steps 1-3 of the
  [Amazon EC2 quick start](../../README.md#quick-start-amazon-ec2-harness) (test cluster `load-benchmark` with
  `PER_BROKER` enhanced monitoring, Amazon CloudWatch dashboard published), and steps 1-4 of the
  [Amazon EKS quick start](../../README.md#quick-start-amazon-eks-harness).
- The [shared setup](README.md#shared-setup): `run_p`, `run_c`, `use_profile`, `kafka_cli`, `KB`, `IMG`,
  `CLUSTER_ARN`.
- `python3 -m pip install -r analysis/requirements.txt`.
- Node capacity for the production-shaped specs: about 37.25 vCPU and 74.5 GiB of requests (see the spec
  header); `eks/cluster.yaml` provides 14 c5n.2xlarge nodes by default.
- Read-only credentials for the production cluster (`kafka:DescribeClusterV2`, `kafka:ListNodes`,
  `cloudwatch:GetMetricData`) for step 1.

Run every command from the repository root.

## Steps

### 1. Capture the production baseline

Run [`analysis/diagnose.py`](../diagnose.md) read-only against the production cluster you want to fix, over a
representative window that includes a peak period. Use the production account's read-only profile.
`diagnose.py` needs `PER_BROKER` (or higher) monitoring on that cluster to report `RequestHandlerAvgIdlePercent`.

```bash
PROD_ARN=<production cluster ARN>
python3 analysis/diagnose.py --cluster-arn "$PROD_ARN" --profile <read-only-profile> \
    --start <peak-window-start, e.g. 2026-01-01T10:00:00Z> --end <peak-window-end> \
    --format markdown > /tmp/test1-prod-baseline.md
```

From the report, write down `RequestHandlerAvgIdlePercent` (mean and worst broker),
`NetworkProcessorAvgIdlePercent`, CPU (`CpuUser` + `CpuSystem`), `MessagesInPerSec`, `BytesInPerSec` /
`BytesOutPerSec`, average message size, `PartitionCount` per broker, and IAM `ClientConnectionCount`. The
markdown output contains no ARNs or account IDs. Also record the client inventory: producer instance count,
consumer group count (including broadcast groups), and current client settings.

Put the production client settings into the `baseline` profile so that `baseline` really is production
(see [Bring your own client config](../../README.md#bring-your-own-client-config); on Amazon EKS regenerate the
`client-tuning-baseline` ConfigMap as in [k8s/README.md](../../k8s/README.md#6-client-tuning-profiles-baseline-vs-aws-tuned)).
Optionally diff them against the tuned profile:

```bash
python3 analysis/check_profile.py scripts/profiles/baseline.properties
```

### 2. Optional: sanity check on a low-density cluster

A quick end-to-end check of the harness and the measurement method before the production-shaped runs: the same
small workload with `baseline` and then `aws-tuned`, few partitions, few clients. At low density the brokers have
headroom with either profile, so expect differences in request-related metrics, not a capacity limit. Run one
harness.

**Amazon EC2.** Topics `load-bench-1..12` with 24 partitions, created by `create-msk-standard-cluster.sh`
(`00-setup-topics.sh` is idempotent and prints a describe of every topic).

```bash
run_p 00-setup-topics.sh "$CLUSTER_ARN" load-bench 12 24

for p in baseline aws-tuned; do
  use_profile $p
  run_p 01-start-producers.sh --background "$CLUSTER_ARN" load-bench 12 100
  run_c 02-start-consumers.sh --background "$CLUSTER_ARN" load-bench 12 test1-sanity-$p-cg
  run_p 04-load-status.sh; run_c 04-load-status.sh      # processes running, no errors in the log tails
  sleep 900                                            # warm-up
  START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  run_c 05-collect-diagnostics.sh test1-sanity-$p-cg  # client-side check: CPU, GC, network allowance, lag
  run_p 03-stop-load.sh; run_c 03-stop-load.sh
  sleep 180                                            # let the last 1-minute data points land
  python3 analysis/measure_cluster.py load-benchmark test1-sanity-ec2-$p "$START" "$END"
done
python3 analysis/compare.py test1-sanity-ec2-baseline test1-sanity-ec2-aws-tuned
use_profile aws-tuned                                  # restore the shipped active profile
```

`01` takes `<cluster-arn> [prefix] [num-topics] [target-mb-per-sec] [record-size]`; `02` takes
`<cluster-arn> [prefix] [num-topics] [group-name]`. Keep every argument identical between the two runs except
the group name. The effective client configuration is on the hosts in
`/opt/kafka/config/producer-effective.properties` and `consumer-effective.properties`.

**Amazon EKS.** [`k8s/examples/workload-low-density.yaml`](../../k8s/examples/workload-low-density.yaml): 2
topics, 4 producer clients (2 pods), 1 consumer group per topic (2 pods). The spec assumes `load-bench-1..2`
with 12 partitions; the 24-partition topics from `create-msk-standard-cluster.sh` also work (still low density),
or [reset](README.md#topics-and-reset) them and run `run_p 00-setup-topics.sh "$CLUSTER_ARN" load-bench 2 12`.

```bash
SPEC=k8s/examples/workload-low-density.yaml
python3 k8s/gen-workload.py $SPEC --image "$IMG" --summary-only
for p in baseline aws-tuned; do
  python3 k8s/gen-workload.py $SPEC --image "$IMG" --tuning $p > /tmp/test1-sanity-$p.yaml
done

for p in baseline aws-tuned; do
  kubectl apply -f /tmp/test1-sanity-$p.yaml           # switching profile recreates every Deployment
  kubectl -n msk-bench get pods -l msk-bench/workload=low-density   # all Running, RESTARTS 0
  sleep 900
  kubectl -n msk-bench logs -l msk-bench/workload=low-density --prefix --tail=-1 | grep ERROR
  START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  sleep 180
  python3 analysis/measure_cluster.py load-benchmark test1-sanity-eks-$p "$START" "$END"
done
python3 analysis/compare.py test1-sanity-eks-baseline test1-sanity-eks-aws-tuned
kubectl -n msk-bench delete deploy -l msk-bench/workload=low-density
```

### 3. Build the production-shaped workload

The example specs model a high-density shape. Copy and edit them so that partitions, replication factor, record
size, producer instances, consumer groups and broadcast groups match production **per broker**, not divided by
the broker ratio (the comments in each spec document the keys):

- [`k8s/examples/workload-high-density.yaml`](../../k8s/examples/workload-high-density.yaml): 12 topics x 100
  partitions plus `bcast-1` (20 partitions), 121 producer clients, 100 consumer groups (24 on the main topics,
  76 broadcast groups on `bcast-1`), at the average rate.
- [`k8s/examples/workload-high-density-peak.yaml`](../../k8s/examples/workload-high-density-peak.yaml): the same
  shape at 2x the producer rate. 2x is an example; set `total_mb_per_sec` and `estimate.topic_mb_per_sec` in a
  copy to your production peak-to-average ratio.

Stop all load, then [reset](README.md#topics-and-reset) the `load-bench` topics to the spec's partition count
and create the broadcast topic:

```bash
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --delete --topic 'load-bench-.*'"
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --list"        # wait until no load-bench-* topic is listed
run_p 00-setup-topics.sh "$CLUSTER_ARN" load-bench 12 100
run_p 00-setup-topics.sh "$CLUSTER_ARN" bcast 1 20
```

Generate the manifests and check the egress estimate in the summary (about 2.3x ingress in `low-volume-topics`
mode) against your node network bandwidth:

```bash
SPEC=k8s/examples/workload-high-density.yaml
PEAK=k8s/examples/workload-high-density-peak.yaml
python3 k8s/gen-workload.py $SPEC --image "$IMG" --summary-only
python3 k8s/gen-workload.py $PEAK --image "$IMG" --summary-only
python3 k8s/gen-workload.py $SPEC --image "$IMG" --tuning baseline  > /tmp/test1-baseline.yaml
python3 k8s/gen-workload.py $SPEC --image "$IMG" --tuning aws-tuned > /tmp/test1-aws-tuned.yaml
python3 k8s/gen-workload.py $PEAK --image "$IMG" --tuning aws-tuned > /tmp/test1-aws-tuned-peak.yaml
```

### 4. Baseline at production rate, and the validation gate

```bash
kubectl apply -f /tmp/test1-baseline.yaml
kubectl -n msk-bench get pods -l msk-bench/workload=high-density | awk 'NR>1 && ($3!="Running" || $4!="0")'
sleep 900
kubectl -n msk-bench logs -l msk-bench/workload=high-density --prefix --tail=-1 --max-log-requests=120 | grep ERROR
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 180
python3 analysis/measure_cluster.py load-benchmark test1-baseline-avg "$START" "$END"

# The same checks as step 1, on the test cluster and the same window, for a like-for-like comparison.
python3 analysis/diagnose.py --cluster-arn "$CLUSTER_ARN" --start "$START" --end "$END" \
    --format markdown > /tmp/test1-test-baseline.md
```

The `awk` filter prints only pods that are not `Running` or have restarted; it should print nothing.

**Validation gate.** Compare `/tmp/test1-test-baseline.md` with `/tmp/test1-prod-baseline.md`. The baseline's
`RequestHandlerAvgIdlePercent` (mean and worst broker) should land close to the production baseline, at a
comparable per-broker `MessagesInPerSec`, average message size and `PartitionCount`. If it doesn't, the test
cluster is missing something, usually partition density, client count, or message size, and nothing you measure
on it yet will translate to production. Fix the shape before you go on; [Test 2](test-2-one-factor-at-a-time.md)
adds the production shape back one factor at a time and shows which factor closes the gap.

### 5. Tuned at the same rate

```bash
kubectl apply -f /tmp/test1-aws-tuned.yaml        # recreates every Deployment with client-tuning-aws-tuned
kubectl -n msk-bench get pods -l msk-bench/workload=high-density | awk 'NR>1 && ($3!="Running" || $4!="0")'
sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 180
python3 analysis/measure_cluster.py load-benchmark test1-aws-tuned-avg "$START" "$END"
python3 analysis/compare.py test1-baseline-avg test1-aws-tuned-avg
```

### 6. Tuned at peak

The peak spec uses the same Deployment names with a different workload label, so remove the average-rate
workload first.

```bash
kubectl -n msk-bench delete deploy -l msk-bench/workload=high-density
kubectl apply -f /tmp/test1-aws-tuned-peak.yaml
kubectl -n msk-bench get pods -l msk-bench/workload=high-density-peak | awk 'NR>1 && ($3!="Running" || $4!="0")'
sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 180
python3 analysis/measure_cluster.py load-benchmark test1-aws-tuned-peak "$START" "$END"

# The rates differ by design, so compare.py reports SUSPECT (exit 1); read the table for headroom at peak,
# not as an A/B result.
python3 analysis/compare.py test1-aws-tuned-avg test1-aws-tuned-peak
```

Confirm the producers reach the peak rate: the `mp-*` pod logs report `records_per_sec` and `send_errors` per
interval (`kubectl -n msk-bench logs -l role=producer --prefix --tail=2`), and they are not CPU-limited
(`kubectl top pods -n msk-bench -l role=producer`).

### 7. Optional: peak variant with many worker threads

If production consumers hand each polled batch to a pool of worker threads, repeat the head-to-head at peak
with two consumer shapes, each with `baseline` and `aws-tuned`. Worker threads let a group return to `poll()`
sooner, which raises its fetch and commit rates; fewer pods mean fewer groups, connections and fetch sessions.
Both shapes use the peak producers of `workload-high-density-peak.yaml` (121 producer clients) and the topics
from step 3.

| Shape | Spec | Consumer groups / pods | `worker_threads` |
|---|---|---|---|
| A: many groups | [`workload-peak-worker-threads.yaml`](../../k8s/examples/workload-peak-worker-threads.yaml) | 100 (24 main + 76 broadcast) | 16 |
| B: fewer pods | [`workload-peak-worker-threads-fewer-pods.yaml`](../../k8s/examples/workload-peak-worker-threads-fewer-pods.yaml) | 36 (24 main + 12 broadcast) | 16 |

The worker thread count, the consolidated group count and the 2x rate are examples; set them in copies of the
specs. `consumers.worker_threads` becomes `WORKER_THREADS` (`MultiGroupConsumer --worker-threads`).

```bash
kubectl -n msk-bench delete deploy -l msk-bench/workload=high-density-peak
A=k8s/examples/workload-peak-worker-threads.yaml
B=k8s/examples/workload-peak-worker-threads-fewer-pods.yaml
for p in baseline aws-tuned; do
  python3 k8s/gen-workload.py $A --image "$IMG" --tuning $p > /tmp/test1-peak-a-$p.yaml
  python3 k8s/gen-workload.py $B --image "$IMG" --tuning $p > /tmp/test1-peak-b-$p.yaml
done

for s in a b; do
  [ $s = a ] && W=peak-worker-threads || W=peak-worker-threads-fewer-pods
  for p in baseline aws-tuned; do
    kubectl apply -f /tmp/test1-peak-$s-$p.yaml
    kubectl -n msk-bench get pods -l msk-bench/workload=$W | awk 'NR>1 && ($3!="Running" || $4!="0")'
    sleep 900
    START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    sleep 180
    python3 analysis/measure_cluster.py load-benchmark test1-peak-$s-$p "$START" "$END"
  done
  python3 analysis/compare.py test1-peak-$s-baseline test1-peak-$s-aws-tuned
  kubectl -n msk-bench delete deploy -l msk-bench/workload=$W
done

# Same producers in both shapes, so MessagesInPerSec should match; egress differs (fewer broadcast groups in B).
python3 analysis/compare.py test1-peak-a-baseline  test1-peak-b-baseline
python3 analysis/compare.py test1-peak-a-aws-tuned test1-peak-b-aws-tuned
```

## Measure

- **Warm-up:** about 15 minutes after every apply or profile switch: all pods `Running` with 0 restarts (EC2:
  `04-load-status.sh` clean), no rebalance in progress, consumer lag flat, no `ERROR` lines.
- **Window:** 30 minutes (`sleep 1800`) in every run above; use the same length for every run you compare.
  Run `measure_cluster.py` a few minutes after `END` (the `sleep 180`).
- **Labels:** `test1-sanity-ec2-{baseline,aws-tuned}`, `test1-sanity-eks-{baseline,aws-tuned}`,
  `test1-baseline-avg`, `test1-aws-tuned-avg`, `test1-aws-tuned-peak`, `test1-peak-{a,b}-{baseline,aws-tuned}`.
  `measure_cluster.py` appends to `analysis/snapshots.jsonl`; a reused label makes `compare.py` take the newest.
- **Compare:** `analysis/compare.py <baseline> <candidate>` prints the mean and worst broker per metric and
  exits 0 (OK) or 1 (SUSPECT: message rates more than 5% apart, under-replicated partitions, or no
  `RequestHandlerAvgIdlePercent`). Add `--format markdown` for a shareable table.
- Log the profile, spec (or script arguments), cluster size and window of every run. See
  [Measurement method](../../README.md#measurement-method) and
  [Benchmark integrity rules](../../README.md#benchmark-integrity-rules).

## What good looks like

- The baseline lands close to the production baseline from step 1; only then do the tuned results mean
  something for production.
- At the same `MessagesInPerSec`, the tuned profile raises `RequestHandlerAvgIdlePercent` by a meaningful margin
  on the mean **and** on the worst broker.
- `CpuUser` + `CpuSystem` and on-wire bytes (`BytesInPerSec` / `BytesOutPerSec`) fall at the same message rate.
- `ProduceTotalTimeMsMean` and `FetchConsumerTotalTimeMsMean` stay roughly unchanged (see the fetch note below).
- At peak, request-handler idle holds steady instead of degrading.
- If latency rises materially, or the gain disappears at peak, revisit the tuned settings before going further.

## Watch out

- **Validate first.** A baseline that doesn't reproduce production's request-handler idle means the cluster
  shape is wrong (usually partition density, client count, or message size). Don't tune against it.
- **Compare at equal `MessagesInPerSec`, never at equal bytes:** zstd lowers `BytesInPerSec`. `compare.py`
  enforces a 5% gate.
- **Profile switches recreate every Deployment** (113 in the high-density spec) and rebalance every group. Wait
  for all pods to be `Running` and lag to flatten before a window.
- **Client bottlenecks look like broker results.** A CPU-bound client host or pod sends bursty requests or lowers
  the offered load. Check `05-collect-diagnostics.sh` (EC2) or `kubectl top pods -n msk-bench` (EKS). In the
  peak variant, consumer pods keep the 250m CPU limit; with 16 worker threads raise `resources.consumer` in the
  spec copy if they hit it, and keep it identical across compared runs.
- **Consumer lag must be flat in the window.** If it grows with one profile, the consumers can't keep up; record
  that instead of comparing broker metrics.
- **`fetch.max.wait.ms=1000`** in `aws-tuned` raises `FetchConsumerTotalTimeMsMean` on low-volume partitions;
  that is the broker holding the fetch by design, not a slower broker.
- **Don't `kubectl scale`** the Deployments: on `cg-*` it adds group members, on `mp-*` it multiplies the rate.
  Change the spec and regenerate.
- **Stale groups.** Runs reuse group ids `bench-cg-*` and `bench-bcast-*` and resume from previous offsets; keep
  the warm-up long enough to catch up. Old groups inflate `SumOffsetLag`; delete them as in the
  [shared setup](README.md#shared-setup) (`kafka-consumer-groups.sh --delete`).
- **Discard, don't explain,** any run with pod restarts, repeated rebalances, saturated client hosts, or producer
  send errors.
- Worker threads exist only on the Amazon EKS harness; `07-start-multigroup-consumers.sh` on Amazon EC2 doesn't
  pass `--worker-threads`.

## Key metrics

From the blog's Watch lines, on the dashboard published by `scripts/15-put-dashboard.sh`:

- Validation: `RequestHandlerAvgIdlePercent`, `PartitionCount`, `MessagesInPerSec` against `BytesInPerSec`,
  `ProduceMessageConversionsPerSec` (target 0), `ProduceTotalTimeMsMean`, `FetchConsumerTotalTimeMsMean`.
- Head to head: `RequestHandlerAvgIdlePercent` (mean and worst broker), `NetworkProcessorAvgIdlePercent`,
  `CpuUser` + `CpuSystem`, `MessagesInPerSec`, `BytesInPerSec` / `BytesOutPerSec`, `ProduceTotalTimeMsMean`,
  `FetchConsumerTotalTimeMsMean`, `VolumeQueueLength`.
- Health in every window: `UnderReplicatedPartitions` / `UnderMinIsrPartitionCount` at 0, consumer lag flat;
  `ClientConnectionCount` and `ConnectionCreationRate` (lower in peak shape B).

## Teardown

```bash
# Amazon EKS
for w in low-density high-density high-density-peak peak-worker-threads peak-worker-threads-fewer-pods; do
  kubectl -n msk-bench delete deploy -l msk-bench/workload=$w
done

# Amazon EC2 (sanity check, if still running)
run_p 03-stop-load.sh
run_c 03-stop-load.sh
use_profile aws-tuned          # restore the shipped active profile
```

Keep the `load-bench` (100 partitions) and `bcast-1` topics if you run [Test 2](test-2-one-factor-at-a-time.md)
or the [quota measurement](../quotas.md) next. The consumer groups (`test1-*-cg`, `bench-cg-*`) and their offsets
stay on the cluster until `offsets.retention.minutes` expires them, or delete them as in the
[shared setup](README.md#shared-setup).
