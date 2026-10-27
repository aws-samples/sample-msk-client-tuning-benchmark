# Test case 1: Baseline vs tuned config on a low-density cluster

**Purpose.** Run the same small workload with the `baseline` profile (your production client settings) and the
`aws-tuned` profile, on a cluster with few partitions and few clients. This validates the harness and the
measurement method end to end, and shows what client tuning changes when the brokers are not under pressure.

**What to expect.** With `aws-tuned`, producers send fewer, larger, compressed batches and consumers send fewer,
fuller fetches for the same message rate, so the brokers handle fewer requests per message. At low density the
brokers have headroom with either profile, so expect the differences to be visible in request-related metrics
rather than as a capacity limit.

**Harness.** Amazon EC2 (section A) or Amazon EKS (section B). Run one or both.

**Prerequisites.** [Before you start](README.md#before-you-start) (EC2 quick start steps 1-3; for section B
also EKS quick start steps 1-4) and the [common shell setup](README.md#common-shell-setup).

## A. Amazon EC2 harness

Topics: `load-bench-1..12` with 24 partitions, created by `create-msk-standard-cluster.sh`. Confirm them (the
script is idempotent and prints a describe of every topic):

```bash
run_p 00-setup-topics.sh "$CLUSTER_ARN" load-bench 12 24
```

### A1. Baseline run

```bash
use_profile baseline
run_p 01-start-producers.sh --background "$CLUSTER_ARN" load-bench 12 100
run_c 02-start-consumers.sh --background "$CLUSTER_ARN" load-bench 12 tc1-baseline-cg

# Check both hosts: processes running, no errors in the log tails.
run_p 04-load-status.sh
run_c 04-load-status.sh

sleep 900                                     # warm-up
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)

run_c 05-collect-diagnostics.sh tc1-baseline-cg   # client-side check: CPU, GC, network allowance, lag
run_p 03-stop-load.sh
run_c 03-stop-load.sh
python3 analysis/measure_cluster.py load-benchmark tc1-ec2-baseline "$START" "$END"
```

`01` takes `<cluster-arn> [prefix] [num-topics] [target-mb-per-sec] [record-size]` (default record size 1024
bytes); `02` takes `<cluster-arn> [prefix] [num-topics] [group-name]`. Keep every argument identical between the
two runs except the group name.

### A2. Tuned run

```bash
use_profile aws-tuned
run_p 01-start-producers.sh --background "$CLUSTER_ARN" load-bench 12 100
run_c 02-start-consumers.sh --background "$CLUSTER_ARN" load-bench 12 tc1-aws-tuned-cg
run_p 04-load-status.sh
run_c 04-load-status.sh

sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)

run_c 05-collect-diagnostics.sh tc1-aws-tuned-cg
run_p 03-stop-load.sh
run_c 03-stop-load.sh
python3 analysis/measure_cluster.py load-benchmark tc1-ec2-aws-tuned "$START" "$END"
```

### A3. Compare

```bash
python3 analysis/compare.py tc1-ec2-baseline tc1-ec2-aws-tuned
```

The effective client configuration of each run is on the hosts in
`/opt/kafka/config/producer-effective.properties` and `consumer-effective.properties`.

## B. Amazon EKS harness

Workload: [`k8s/examples/workload-low-density.yaml`](../../k8s/examples/workload-low-density.yaml): 2 topics,
4 producer clients (2 pods), 1 consumer group per topic (2 pods).

Topics: `load-bench-1..2`. The spec assumes 12 partitions. If the cluster still has the 24-partition topics from
`create-msk-standard-cluster.sh`, you can use them (still low density) or [reset](README.md#topics-per-test-case)
them and create `load-bench-1..2` with 12 partitions:

```bash
run_p 00-setup-topics.sh "$CLUSTER_ARN" load-bench 2 12
```

### B1. Baseline run

```bash
SPEC=k8s/examples/workload-low-density.yaml
python3 k8s/gen-workload.py $SPEC --image "$IMG" --summary-only
python3 k8s/gen-workload.py $SPEC --image "$IMG" --tuning baseline  > /tmp/tc1-baseline.yaml
python3 k8s/gen-workload.py $SPEC --image "$IMG" --tuning aws-tuned > /tmp/tc1-aws-tuned.yaml

kubectl apply -f /tmp/tc1-baseline.yaml
kubectl -n msk-bench get pods -l msk-bench/workload=low-density        # all Running, RESTARTS 0
kubectl -n msk-bench logs -l msk-bench/workload=low-density --prefix --tail=-1 | grep ERROR

sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc1-eks-baseline "$START" "$END"
```

### B2. Tuned run

Applying the other profile changes the mounted ConfigMap, so every Deployment is recreated with `aws-tuned`.

```bash
kubectl apply -f /tmp/tc1-aws-tuned.yaml
kubectl -n msk-bench get pods -l msk-bench/workload=low-density
sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc1-eks-aws-tuned "$START" "$END"
python3 analysis/compare.py tc1-eks-baseline tc1-eks-aws-tuned
```

## Dashboard

- `RequestHandlerAvgIdlePercent` and `NetworkProcessorAvgIdlePercent`: headroom with each profile.
- `CPU total %`, `Produce latency ms`, `FetchConsumerTotalTimeMsMean` (includes `fetch.max.wait.ms`, so it
  rises with `aws-tuned` by design).
- `MessagesInPerSec` equal across the two runs; `Throughput BytesIn/BytesOut` lower with `aws-tuned` because of
  compression.
- `ProduceMessageConversionsPerSec` at 0.

**Key metrics.** `RequestHandlerAvgIdlePercent` (mean and worst broker), CPU, produce and fetch latency, at equal
`MessagesInPerSec`.

## Watch out

- Compare at equal `MessagesInPerSec`, never at equal `BytesInPerSec`: `zstd` compression lowers bytes.
- Use the same window length for both runs, and skip the ramp-up and the profile switch.
- A client host or pod that is CPU-bound makes the brokers look better or worse than they are; check
  `05-collect-diagnostics.sh` (EC2) or `kubectl top pods` (EKS).
- `fetch.max.wait.ms=1000` in `aws-tuned` raises `FetchConsumerTotalTimeMsMean` on low-volume partitions; that is
  the broker holding the fetch, not a slower broker.

## Stop and clean up

```bash
# EC2 (if not already stopped)
run_p 03-stop-load.sh
run_c 03-stop-load.sh
use_profile aws-tuned          # restore the shipped active profile

# EKS
kubectl -n msk-bench delete deploy -l msk-bench/workload=low-density
```

The consumer groups (`tc1-*-cg`, `bench-cg-*`) and their offsets stay on the cluster until
`offsets.retention.minutes` expires them.
