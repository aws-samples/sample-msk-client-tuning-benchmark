# Test case 5: Production-shaped head-to-head

**Purpose.** Run a workload shaped like production (many partitions, many producer instances, many consumer
groups including broadcast groups) first with the `baseline` profile and then with `aws-tuned` at the average
production rate, and finally with `aws-tuned` at the peak rate. This is the comparison that tells you whether the
tuned clients let the same cluster, or a smaller one, carry the production load.

**What to expect.** At production density the brokers are bound by request count, so the head-to-head at equal
`MessagesInPerSec` shows the request-handler headroom that the tuned profile frees. The peak run shows whether the
tuned clients keep enough headroom when the rate rises.

**Harness.** Amazon EKS.

**Prerequisites.** [Before you start](README.md#before-you-start) (EC2 quick start steps 1-3, EKS quick start
steps 1-4) and the [common shell setup](README.md#common-shell-setup) including `kafka_cli` and `KB`. Node
capacity: about 37.25 vCPU and 74.5 GiB of requests (see the spec header); `eks/cluster.yaml` provides 14
c5n.2xlarge nodes by default.

Workloads:
- [`k8s/examples/workload-high-density.yaml`](../../k8s/examples/workload-high-density.yaml): 12 topics x 100
  partitions plus `bcast-1`, 121 producer clients, 100 consumer groups (24 on the main topics, 76 broadcast groups
  on `bcast-1`), at the average rate.
- [`k8s/examples/workload-high-density-peak.yaml`](../../k8s/examples/workload-high-density-peak.yaml): the same
  shape at 2x the producer rate. 2x is an example; set `total_mb_per_sec` and `estimate.topic_mb_per_sec` in a copy
  of the spec to your production peak-to-average ratio.

Copy and edit the specs if your production shape differs (partitions, record size, groups, producer instances);
the comments in each spec document the keys.

## 0. Topics

The spec needs `load-bench-1..12` with 100 partitions. If they exist with another partition count (for example
the 24 partitions from `create-msk-standard-cluster.sh`), stop all load and reset them first:

```bash
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --delete --topic 'load-bench-.*'"
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --list"                  # no load-bench-* left
run_p 00-setup-topics.sh "$CLUSTER_ARN" load-bench 12 100
run_p 00-setup-topics.sh "$CLUSTER_ARN" bcast 1 20
```

## 1. Generate the manifests

```bash
SPEC=k8s/examples/workload-high-density.yaml
PEAK=k8s/examples/workload-high-density-peak.yaml
python3 k8s/gen-workload.py $SPEC --image "$IMG" --summary-only
python3 k8s/gen-workload.py $PEAK --image "$IMG" --summary-only
python3 k8s/gen-workload.py $SPEC --image "$IMG" --tuning baseline  > /tmp/tc5-baseline.yaml
python3 k8s/gen-workload.py $SPEC --image "$IMG" --tuning aws-tuned > /tmp/tc5-aws-tuned.yaml
python3 k8s/gen-workload.py $PEAK --image "$IMG" --tuning aws-tuned > /tmp/tc5-aws-tuned-peak.yaml
```

Check the egress estimate in the summary (about 2.3x ingress in `low-volume-topics` mode) against your node
network bandwidth.

## 2. Baseline at average rate

```bash
kubectl apply -f /tmp/tc5-baseline.yaml
kubectl -n msk-bench get pods -l msk-bench/workload=high-density | awk 'NR>1 && ($3!="Running" || $4!="0")'
kubectl -n msk-bench logs -l msk-bench/workload=high-density --prefix --tail=-1 --max-log-requests=120 | grep ERROR

sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc5-baseline-avg "$START" "$END"
```

The `awk` filter prints only pods that are not `Running` or have restarted; it should print nothing.

## 3. Tuned at average rate

```bash
kubectl apply -f /tmp/tc5-aws-tuned.yaml        # recreates every Deployment with client-tuning-aws-tuned
kubectl -n msk-bench get pods -l msk-bench/workload=high-density | awk 'NR>1 && ($3!="Running" || $4!="0")'

sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc5-aws-tuned-avg "$START" "$END"
python3 analysis/compare.py tc5-baseline-avg tc5-aws-tuned-avg
```

## 4. Tuned at peak rate

The peak spec has the same Deployment names with a different workload label, so remove the average-rate
workload first.

```bash
kubectl -n msk-bench delete deploy -l msk-bench/workload=high-density
kubectl apply -f /tmp/tc5-aws-tuned-peak.yaml
kubectl -n msk-bench get pods -l msk-bench/workload=high-density-peak | awk 'NR>1 && ($3!="Running" || $4!="0")'

sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc5-aws-tuned-peak "$START" "$END"

# Side by side with the average run. The rates differ by design, so compare.py reports SUSPECT (exit 1);
# read the table for headroom at peak, not as an A/B result.
python3 analysis/compare.py tc5-aws-tuned-avg tc5-aws-tuned-peak
```

Confirm the producers reach the peak rate: the `mp-*` pod logs report `records_per_sec` and `send_errors` per
interval (`kubectl -n msk-bench logs -l role=producer --prefix --tail=2`).

## Dashboard

- `RequestHandlerAvgIdlePercent` and `NetworkProcessorAvgIdlePercent`: the worst broker in each window.
- `CPU total %`, `Produce latency ms`, `FetchConsumerTotalTimeMsMean`.
- `MessagesInPerSec` equal in the two average-rate windows; `Throughput BytesIn/BytesOut`.
- `ConnectionCount`, `ClientConnectionCount`, `ConnectionCreationRate`, `PartitionCount`.
- `Consumer lag (all groups)` flat; `UnderReplicatedPartitions` at 0.

**Key metrics.** `RequestHandlerAvgIdlePercent` (mean and worst broker) baseline vs tuned at equal
`MessagesInPerSec`, and its value at peak; produce latency; CPU.

## Watch out

- The profile switch recreates all 113 Deployments; 100 consumer groups rebalance. Wait for lag to flatten and
  for all pods to be `Running` before you start a window.
- Compare baseline and tuned only at equal `MessagesInPerSec`; `compare.py` enforces the 5% gate.
- At peak, check that the producer pods are not CPU-limited (`kubectl top pods -n msk-bench -l role=producer`)
  and that they report the target rate; a producer that cannot keep up lowers the offered load and flatters the
  brokers.
- Don't use `kubectl scale` on the Deployments: on `cg-*` it adds members to a group, and on `mp-*` it multiplies
  the rate. Change the spec and regenerate.
- Earlier runs leave consumer groups behind. They inflate `SumOffsetLag` until their offsets expire.

## Stop and clean up

```bash
kubectl -n msk-bench delete deploy -l msk-bench/workload=high-density
kubectl -n msk-bench delete deploy -l msk-bench/workload=high-density-peak
```

Keep the `load-bench` (100 partitions) and `bcast-1` topics if you run test cases 6 or 8 next.
