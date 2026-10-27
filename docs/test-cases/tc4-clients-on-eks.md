# Test case 4: Clients on Amazon EKS

**Purpose.** Run the clients as containerized microservices on Amazon EKS and change one client-side factor at a
time: the number of consumer groups, Kafka Streams with `at_least_once` (ALOS) versus `exactly_once_v2` (EOS),
the number of producer instances, and the number of consumer worker threads. Each step shows what that factor
costs the brokers with your production client settings.

**What to expect.** More consumer groups add fetch, heartbeat, and commit requests per group. More producer
instances for the same total rate mean smaller batches and more produce requests and connections. EOS adds
transactional requests and, with its default 100 ms commit interval, commits and flushes far more often than ALOS
(30 s default). Worker threads change how quickly each group returns to `poll()`, and therefore its fetch and
commit rates.

**Harness.** Amazon EKS.

**Prerequisites.** [Before you start](README.md#before-you-start) (EC2 quick start steps 1-3, EKS quick start
steps 1-4) and the [common shell setup](README.md#common-shell-setup). For the Kafka Streams step, build the
emulator jar and push the `kstreams-emulator` image as described in [eks/ecr.md](../../eks/ecr.md), and set
`KSE_IMG`.

Workload: [`k8s/examples/workload-eks-factors.yaml`](../../k8s/examples/workload-eks-factors.yaml), a base shape
(4 topics, 8 producer clients, 4 consumer groups, no worker threads, plus a 1 MB/s feed into the Kafka Streams
input topic). Each step derives a variant by changing one line with `sed`. All steps use the `baseline` profile;
repeat the whole test case with `aws-tuned` if you also want the tuned cost of each factor.

## 0. Topics and manifests

```bash
run_p 00-setup-topics.sh "$CLUSTER_ARN" tc4 4 24            # tc4-1..4
run_p 00-setup-topics.sh "$CLUSTER_ARN" tc4-streams 1 6     # tc4-streams-1 (Kafka Streams input)

SPEC=k8s/examples/workload-eks-factors.yaml
sed 's/^  total_groups: 4 /  total_groups: 16 /'             $SPEC > /tmp/tc4-spec-groups.yaml
sed 's/^    producers_per_pod: 2 /    producers_per_pod: 10 /' $SPEC > /tmp/tc4-spec-producers.yaml
sed 's/^  worker_threads: 0 /  worker_threads: 8 /'           $SPEC > /tmp/tc4-spec-workers.yaml

for v in base groups producers workers; do
  src=/tmp/tc4-spec-$v.yaml; [ "$v" = base ] && src=$SPEC
  python3 k8s/gen-workload.py "$src" --image "$IMG" --summary-only
  python3 k8s/gen-workload.py "$src" --image "$IMG" --tuning baseline > /tmp/tc4-$v.yaml
done
```

The variant values (16 groups, 10 producer clients per pod, 8 worker threads) are examples; use the values from
your production fleet. Check each summary: the groups variant raises egress to about 4x ingress, and the
producers variant keeps the same total rate across 5x more clients.

Every step below applies one manifest with `--prune`, which also removes Deployments of this workload that the
manifest no longer contains (for example the extra groups of the previous step):

```bash
apply_tc4() {
  kubectl apply -f "/tmp/tc4-$1.yaml" --prune -l msk-bench/workload=eks-factors \
    --prune-allowlist=apps/v1/Deployment
}
```

## 1. Base

```bash
apply_tc4 base
kubectl -n msk-bench get pods -l msk-bench/workload=eks-factors          # Running, RESTARTS 0
sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc4-base "$START" "$END"
```

## 2. Consumer groups

```bash
apply_tc4 groups
sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc4-groups "$START" "$END"
python3 analysis/compare.py tc4-base tc4-groups
```

## 3. Producer count

```bash
apply_tc4 producers       # also removes the extra groups of step 2
sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc4-producers "$START" "$END"
python3 analysis/compare.py tc4-base tc4-producers
```

## 4. Worker threads

`consumers.worker_threads` becomes the `WORKER_THREADS` environment variable, which maps to
`MultiGroupConsumer --worker-threads`: each group's poll thread splits every polled batch by partition across
that many processing threads and waits for them before the next poll.

```bash
apply_tc4 workers
sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc4-workers "$START" "$END"
python3 analysis/compare.py tc4-base tc4-workers
```

## 5. Kafka Streams: ALOS vs EOS

Run the base workload, then add Kafka Streams emulator apps that read `tc4-streams-1`. Each app is one
Deployment (one pod, one stream thread) with its own `application.id`; it repartitions the input, keeps a count in
RocksDB, and writes a changelog (see [kstreams-emulator/README.md](../../kstreams-emulator/README.md)).
`gen-kstreams.sh <alos|eos> <num-apps> [input-topic] [tuning-profile]` does not set the replication factor of
the internal topics (emulator default 2), so set it to the cluster's value with `kubectl set env --local` before
you apply. 4 apps is an example.

```bash
apply_tc4 base

IMAGE="$KSE_IMG" k8s/kstreams/gen-kstreams.sh alos 4 tc4-streams-1 baseline > /tmp/tc4-kse-alos-raw.yaml
kubectl set env --local -f /tmp/tc4-kse-alos-raw.yaml REPLICATION_FACTOR=3 -o yaml > /tmp/tc4-kse-alos.yaml
kubectl apply -f /tmp/tc4-kse-alos.yaml
kubectl -n msk-bench get pods -l role=kstreams
kubectl -n msk-bench logs -l role=kstreams --prefix --tail=3            # CSV lines, state RUNNING

sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc4-kse-alos "$START" "$END"
kubectl -n msk-bench delete deploy -l role=kstreams,guarantee=alos

IMAGE="$KSE_IMG" k8s/kstreams/gen-kstreams.sh eos 4 tc4-streams-1 baseline > /tmp/tc4-kse-eos-raw.yaml
kubectl set env --local -f /tmp/tc4-kse-eos-raw.yaml REPLICATION_FACTOR=3 -o yaml > /tmp/tc4-kse-eos.yaml
kubectl apply -f /tmp/tc4-kse-eos.yaml
kubectl -n msk-bench get pods -l role=kstreams

sleep 900
START=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1800; END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 analysis/measure_cluster.py load-benchmark tc4-kse-eos "$START" "$END"
kubectl -n msk-bench delete deploy -l role=kstreams,guarantee=eos

python3 analysis/compare.py tc4-kse-alos tc4-kse-eos
python3 analysis/compare.py tc4-base tc4-kse-alos
```

Optional variants with the same `kubectl set env --local` step: `COMMIT_INTERVAL_MS=<ms>` separates the cost of
transactions from the cost of the commit frequency, and `NUM_STREAM_THREADS=<n>` changes the stream (worker)
threads per app. The kstreams-emulator README lists a suggested matrix.

## Dashboard

- `RequestHandlerAvgIdlePercent` and `NetworkProcessorAvgIdlePercent` (worst broker) for every step.
- `ConnectionCount`, `ClientConnectionCount`, and `ConnectionCreationRate`: steps 2, 3, and 5 add clients.
- `Throughput BytesIn/BytesOut`: step 2 multiplies egress; step 5 adds repartition and changelog traffic.
- `Produce latency ms`, `FetchConsumerTotalTimeMsMean`, `CPU total %`.
- `MessagesInPerSec`: constant in steps 1-4; higher in step 5 (internal topics).

**Key metrics.** `RequestHandlerAvgIdlePercent` per step, connection counts, produce latency, and, for Streams,
the emulator's own `commit_rate` and `producer_request_rate` columns in the pod logs.

## Watch out

- Change one factor per step. Do not use `kubectl scale` on a `cg-*` Deployment: a second replica joins the same
  group instead of adding a group.
- `compare.py` gates on equal `MessagesInPerSec`. Steps 1-4 keep it constant. Kafka Streams writes the input
  again to its repartition and changelog topics, and EOS flushes its cache every 100 ms, so the Streams windows
  can be flagged SUSPECT on rate. Report that, and judge the ALOS and EOS windows against each other at the same
  input rate and app count.
- Every `gen-kstreams.sh` call creates new `application.id`s (with a timestamp), so each run creates new internal
  topics. They are not deleted automatically (below).
- Give the Streams apps time to create their internal topics and finish the first rebalance before you start
  the window; check the pod logs for state `RUNNING`.
- Consumer pods have a 250m CPU limit; with worker threads, check `kubectl top pods -n msk-bench` for pods at
  their limit before trusting a window.

## Stop and clean up

```bash
kubectl -n msk-bench delete deploy -l role=kstreams
kubectl -n msk-bench delete deploy -l msk-bench/workload=eks-factors
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --delete --topic 'tc4.*'"     # tc4-1..4 and tc4-streams-1
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --delete --topic 'kse-.*'"    # Kafka Streams internal topics
```

**Limitations.** `gen-kstreams.sh` does not expose `REPLICATION_FACTOR`, `NUM_STREAM_THREADS`, or
`COMMIT_INTERVAL_MS`; this runbook sets them with `kubectl set env --local`. The emulator Deployments are not
labeled with `msk-bench/workload`, so they are selected with `role=kstreams`. No script deletes the internal
topics or the consumer groups; the groups (`bench-cg-*`, `kse-*`) expire after `offsets.retention.minutes`.
