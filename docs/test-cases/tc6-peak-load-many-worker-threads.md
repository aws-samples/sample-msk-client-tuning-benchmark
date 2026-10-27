# Test case 6: Peak load with many worker threads

**Purpose.** At peak producer rate, compare two consumer deployment shapes that both use many worker threads
per consumer group: (A) many consumer groups, one pod each, and (B) the same topics consumed by fewer consumer
pods. Run each shape with `baseline` and with `aws-tuned`.

**What to expect.** Worker threads let a group finish processing a batch sooner and return to `poll()` more
often, which raises its fetch and commit rates. Many groups multiply that effect; fewer pods reduce the number of
groups, connections, and fetch sessions. The client profile changes how much data each of those requests carries.

**Harness.** Amazon EKS.

**Prerequisites.** [Before you start](README.md#before-you-start) (EC2 quick start steps 1-3, EKS quick start
steps 1-4), the [common shell setup](README.md#common-shell-setup), and the topics of
[test case 5](tc5-production-shaped-head-to-head.md#0-topics) (`load-bench-1..12` with 100 partitions, `bcast-1`
with 20).

Workloads (both at the peak rate of
[`workload-high-density-peak.yaml`](../../k8s/examples/workload-high-density-peak.yaml), 121 producer clients):

| Shape | Spec | Consumer groups / pods | `worker_threads` |
|---|---|---|---|
| A: many groups | [`workload-peak-worker-threads.yaml`](../../k8s/examples/workload-peak-worker-threads.yaml) | 100 (24 main + 76 broadcast) | 16 |
| B: fewer pods | [`workload-peak-worker-threads-fewer-pods.yaml`](../../k8s/examples/workload-peak-worker-threads-fewer-pods.yaml) | 36 (24 main + 12 broadcast) | 16 |

The worker thread count, the consolidated group count, and the 2x peak rate are examples; set them in copies of
the specs to match the shapes you are evaluating. `consumers.worker_threads` becomes `WORKER_THREADS`
(`MultiGroupConsumer --worker-threads`).

## 1. Generate the manifests

```bash
A=k8s/examples/workload-peak-worker-threads.yaml
B=k8s/examples/workload-peak-worker-threads-fewer-pods.yaml
python3 k8s/gen-workload.py $A --image "$IMG" --summary-only
python3 k8s/gen-workload.py $B --image "$IMG" --summary-only
for p in baseline aws-tuned; do
  python3 k8s/gen-workload.py $A --image "$IMG" --tuning $p > /tmp/tc6-a-$p.yaml
  python3 k8s/gen-workload.py $B --image "$IMG" --tuning $p > /tmp/tc6-b-$p.yaml
done
```

Remove any other workload on the same topics first (for example
`kubectl -n msk-bench delete deploy -l msk-bench/workload=high-density-peak`).

## 2. Shape A: many groups

```bash
kubectl apply -f /tmp/tc6-a-baseline.yaml
kubectl -n msk-bench get pods -l msk-bench/workload=peak-worker-threads | awk 'NR>1 && ($3!="Running" || $4!="0")'
sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc6-a-baseline "$START" "$END"

kubectl apply -f /tmp/tc6-a-aws-tuned.yaml
kubectl -n msk-bench get pods -l msk-bench/workload=peak-worker-threads | awk 'NR>1 && ($3!="Running" || $4!="0")'
sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc6-a-aws-tuned "$START" "$END"

python3 analysis/compare.py tc6-a-baseline tc6-a-aws-tuned
kubectl -n msk-bench delete deploy -l msk-bench/workload=peak-worker-threads
```

## 3. Shape B: fewer consumer pods

```bash
kubectl apply -f /tmp/tc6-b-baseline.yaml
kubectl -n msk-bench get pods -l msk-bench/workload=peak-worker-threads-fewer-pods | awk 'NR>1 && ($3!="Running" || $4!="0")'
sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc6-b-baseline "$START" "$END"

kubectl apply -f /tmp/tc6-b-aws-tuned.yaml
kubectl -n msk-bench get pods -l msk-bench/workload=peak-worker-threads-fewer-pods | awk 'NR>1 && ($3!="Running" || $4!="0")'
sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc6-b-aws-tuned "$START" "$END"

python3 analysis/compare.py tc6-b-baseline tc6-b-aws-tuned
```

## 4. Compare the shapes

The producers are identical in both shapes, so `MessagesInPerSec` should match and the gate should pass; egress
differs because shape B has fewer broadcast groups.

```bash
python3 analysis/compare.py tc6-a-baseline  tc6-b-baseline
python3 analysis/compare.py tc6-a-aws-tuned tc6-b-aws-tuned
```

## Dashboard

- `RequestHandlerAvgIdlePercent` and `NetworkProcessorAvgIdlePercent` (worst broker) for all four windows.
- `FetchConsumerTotalTimeMsMean`, `Produce latency ms`, `CPU total %`.
- `ConnectionCount` and `ClientConnectionCount`: lower in shape B.
- `Throughput BytesIn/BytesOut`, `MessagesInPerSec`, `Consumer lag (all groups)`.

**Key metrics.** `RequestHandlerAvgIdlePercent` for each shape and profile at equal `MessagesInPerSec`, connection
counts, consumer lag.

## Watch out

- Consumer pods keep the 250m CPU limit of the high-density spec. With 16 worker threads, check
  `kubectl top pods -n msk-bench -l role=consumer`; a pod at its CPU limit processes more slowly, polls less often,
  and lags, which is a client bottleneck, not a broker result. Raise `resources.consumer` in the spec copy if
  needed, and keep it identical across the runs you compare.
- At peak, confirm the producer pods reach their target rate (`records_per_sec` and `send_errors` in the `mp-*`
  pod logs).
- Consumer lag must be flat in the window. If it grows with one profile, the consumers can't keep up; record
  that instead of comparing broker metrics.
- Both shapes reuse the group ids `bench-cg-*` and `bench-bcast-*`, so a group resumes from the previous run's
  offsets. Keep the warm-up long enough for that catch-up to finish.

## Stop and clean up

```bash
kubectl -n msk-bench delete deploy -l msk-bench/workload=peak-worker-threads
kubectl -n msk-bench delete deploy -l msk-bench/workload=peak-worker-threads-fewer-pods
```

**Limitations.** Worker threads exist only on the Amazon EKS harness; `07-start-multigroup-consumers.sh` on
Amazon EC2 does not pass `--worker-threads`.
