# How to test: the blog's method, step by step

These pages reproduce the testing method of the AWS blog post "Right-sizing Amazon MSK by tuning your Kafka
clients" with this repository. The blog works through the steps in order; each one builds on the previous one.
The method contains **three tests**, plus a guide for protecting the cluster with Kafka quotas.

> **WARNING: test clusters only.** The tests create and delete topics, change topic and quota configuration, and
> drive sustained load. Never run them against a production cluster. Only `analysis/diagnose.py` (read-only) is
> meant to be pointed at production.

| Blog section | What you do here | Page |
|---|---|---|
| Capture a production baseline | Record what "busy" looks like on the cluster you want to fix: `python3 analysis/diagnose.py --cluster-arn <production-arn> --hours 24` (read-only), plus your client inventory and settings: `python3 analysis/check_profile.py <prod-client.properties>` | [../diagnose.md](../diagnose.md), [../check-profile.md](../check-profile.md) |
| Build a test cluster that mirrors production's shape | Create the test cluster with the same instance type and Kafka version (`KAFKA_VERSION`), copy your production client settings into `scripts/profiles/baseline.properties`, and pick the harness that matches where production clients run | [Quick start: EC2](../../README.md#quick-start-amazon-ec2-harness), [Quick start: EKS](../../README.md#quick-start-amazon-eks-harness), [Bring your own client config](../../README.md#bring-your-own-client-config) |
| Measure the same way every time | Warm up, measure a steady window, record it with `measure_cluster.py`, reset between runs | [Measuring a run](#measuring-a-run-used-by-every-test), [Benchmark integrity rules](../../README.md#benchmark-integrity-rules) |
| Validate the test cluster against production, then compare baseline and tuned head to head | **Test 1** | [test-1-head-to-head.md](test-1-head-to-head.md) |
| Find what drives the load, one factor at a time | **Test 2** | [test-2-one-factor-at-a-time.md](test-2-one-factor-at-a-time.md) |
| Stress-test connections at your maximum client count | **Test 3** | [test-3-max-client-count.md](test-3-max-client-count.md) |
| Decide whether the results are ready for production | Check the five criteria below | [Ready for production?](#ready-for-production) |
| Protecting the cluster with Kafka quotas | Guardrail, not a test | [../quotas.md](../quotas.md) |

If you only have time for one test, run **Test 1**: it answers "would the tuned profile help my workload?"

## Ready for production?

Carry the tuned profile into a production canary only when all of these hold on the test cluster (from the blog):

| Criterion | How to check it with this repository |
|---|---|
| Request-handler idle improved on the **worst broker**, not only the mean | `analysis/compare.py <baseline> <tuned>`: `RequestHandlerAvgIdlePercent - worst broker` row |
| CPU stays well under 60% at peak | `compare.py` `CpuUser`/`CpuSystem` rows for the peak window, or `analysis/diagnose.py` on the test cluster during the peak run |
| Produce and fetch latency within your SLOs | `compare.py` `ProduceTotalTimeMsMean` / `FetchConsumerTotalTimeMsMean` rows, plus client-side latency (`analysis/latency_report.py` on Amazon EKS) |
| Consumer lag flat and bounded | Dashboard lag widgets per consumer group and topic; `EstimatedMaxTimeLag` in the snapshot |
| Connection count and connection rate below 80% of the IAM limits | `analysis/diagnose.py` checks 7 (IAM connections) on the test cluster at your maximum client count (Test 3) |

Discard any run where client-side health misbehaved (pod restarts, repeated rebalances, saturated client hosts,
producer send errors) rather than trying to explain it. `compare.py` marks a pair **SUSPECT** when the message
rates differ by more than 5% or partitions were under-replicated.

## Before you start

All runbooks assume that you finished these steps of the [root README](../../README.md):

- **Every test:** [Prerequisites](../../README.md#prerequisites), and steps 1-3 of the
  [Amazon EC2 quick start](../../README.md#quick-start-amazon-ec2-harness): the AWS CDK stacks are deployed,
  the Amazon Managed Streaming for Apache Kafka (Amazon MSK) cluster `load-benchmark` exists (with
  `PER_BROKER` enhanced monitoring, which `create-msk-standard-cluster.sh` turns on), and the Amazon CloudWatch
  dashboard is published. Install the analysis dependencies too:
  `python3 -m pip install -r analysis/requirements.txt`.
- **Amazon EKS tests (Test 1 and Test 3, and Test 2's optional EKS factors):** steps 1-4 of the
  [Amazon EKS quick start](../../README.md#quick-start-amazon-eks-harness): the Amazon Elastic Kubernetes
  Service (Amazon EKS) cluster exists, the client image is in Amazon ECR, the bootstrap ConfigMap is filled in,
  and `k8s/base` is applied.
- Replace `scripts/profiles/baseline.properties` with your production client settings (and, for Amazon EKS,
  regenerate the `client-tuning-baseline` ConfigMap as shown in [k8s/README.md](../../k8s/README.md#6-client-tuning-profiles-baseline-vs-aws-tuned))
  so that `baseline` really is your production configuration.

Run every command from the repository root.

## Shared setup

Paste this block into the shell you run the runbooks from. The runbooks use these variables and functions.

```bash
export AWS_REGION=us-east-1 AWS_PROFILE=default NUM_BROKERS=3
CLUSTER_ARN=<cluster ARN printed by create-msk-standard-cluster.sh>

# Amazon EKS tests only (from the EKS quick start):
ECR=<account>.dkr.ecr.<region>.amazonaws.com; TAG=<image tag you pushed>
IMG=$ECR/msk-bench-client:$TAG
KSE_IMG=$ECR/kstreams-emulator:$TAG   # Test 2 Kafka Streams factor only; build and push it per eks/ecr.md

# Run a host-side script (scripts/<name>) on the producer host or on consumer host 0.
run_p() { ./orchestration/run-on-host.sh LoadBenchmarkProducerStack  "$AWS_REGION" "$AWS_PROFILE" "$@"; }
run_c() { ./orchestration/run-on-host.sh LoadBenchmarkConsumerStack-0 "$AWS_REGION" "$AWS_PROFILE" "$@"; }

# Select the client profile for the Amazon EC2 harness (takes effect on the next start script).
use_profile() { cp "scripts/profiles/$1.properties" scripts/client-tuning.properties; }
```

### Running a Kafka CLI command on the producer host

`run-on-host.sh` only runs scripts that exist in `scripts/`. Some steps need a plain Kafka CLI command
(`kafka-topics.sh --delete`, `kafka-configs.sh --alter`, `kafka-consumer-groups.sh --describe`) for which the
repository has no script. The helper below sends one shell command to the producer host with AWS Systems
Manager Run Command (the same mechanism `run-on-host.sh` uses) and prints its output. It is not part of the
repository's tooling; it uses only the AWS CLI.

```bash
PRODUCER_ID=$(aws cloudformation describe-stacks --stack-name LoadBenchmarkProducerStack \
  --query "Stacks[0].Outputs[?OutputKey=='HostInstanceId'].OutputValue" --output text)

kafka_cli() {
  local id
  id=$(aws ssm send-command --instance-ids "$PRODUCER_ID" --document-name AWS-RunShellScript \
    --parameters "$(python3 -c 'import json,sys; print(json.dumps({"commands": [sys.argv[1]]}))' "$1")" \
    --query Command.CommandId --output text) || return 1
  aws ssm wait command-executed --command-id "$id" --instance-id "$PRODUCER_ID" || true
  aws ssm get-command-invocation --command-id "$id" --instance-id "$PRODUCER_ID" \
    --query '[Status,StandardOutputContent,StandardErrorContent]' --output text
}

# Connection arguments, expanded ON THE HOST (keep the single quotes). The bootstrap file is written by
# every scripts/0x start or setup script, for example 00-setup-topics.sh.
KB='--bootstrap-server $(cat /opt/kafka/bootstrap-brokers.txt) --command-config /opt/kafka/config/client-iam.properties'

# Example: describe one topic.
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --describe --topic load-bench-1"
```

The host role can create, alter, describe, and delete topics, and describe and delete consumer groups
(`kafka-cluster:DeleteGroup`, scoped to the benchmark cluster). It cannot change cluster-level configuration such
as quotas ([../quotas.md](../quotas.md) adds that permission for the duration of a quota test). To remove a stale group from an
earlier run (stop its consumers first):

```bash
kafka_cli "/opt/kafka/bin/kafka-consumer-groups.sh $KB --delete --group <group>"
```

## Topics and reset

`scripts/00-setup-topics.sh` uses `--if-not-exists`: it never changes the partition count of a topic that
already exists, and it prints a `--describe` of every topic at the end so you can check what you got.
`create-msk-standard-cluster.sh` already creates `load-bench-1..12` with 24 partitions.

| Test | Topics | Partitions | Create with |
|---|---|---|---|
| Test 1 (sanity check, low density) | `load-bench-1..12` (EC2) or `load-bench-1..2` (EKS) | 24 (EC2), 12 in the spec (EKS) | created by `create-msk-standard-cluster.sh`; on EKS `00-setup-topics.sh "$CLUSTER_ARN" load-bench 2 12` on a fresh cluster |
| Test 1 (production-shaped, average and peak) | `load-bench-1..12`, `bcast-1` | 100, 20 | reset `load-bench-*` (below), then `00-setup-topics.sh` |
| Test 2 (EC2 factors) | `factor-1..12`, `factor-bcast-1` (+ the codec-mismatch topic) | your production value, 20 | see the runbook |
| Test 2 (optional EKS factors) | `eks-factor-1..4`, `eks-factor-streams-1` | 24, 6 | see the runbook |
| Test 3 | `load-bench-1..4` | 60 | reset `load-bench-*` (below), then `00-setup-topics.sh` |

To **reset** the `load-bench` topics to a new partition count, stop every producer and consumer first (EC2 hosts
and EKS pods), then delete the topics and create them again. Deletion is asynchronous; if `00-setup-topics.sh`
reports that a topic is marked for deletion, wait a minute and run it again.

```bash
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --delete --topic 'load-bench-.*'"
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --list"        # wait until no load-bench-* topic is listed
run_p 00-setup-topics.sh "$CLUSTER_ARN" load-bench <num-topics> <partitions>
```

## Measuring a run (used by every test)

1. **Warm up.** Start the load and wait about 15 minutes: all clients connected, no rebalance in progress,
   consumer lag flat, and no client-side errors (EC2: `run_p 04-load-status.sh`, `run_c 04-load-status.sh`;
   EKS: `RESTARTS` is 0 in `kubectl -n msk-bench get pods` and no `ERROR` lines in the pod logs).
2. **Record a steady window of 15-30 minutes.** Use the same window length for every run of a test.

   ```bash
   START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
   sleep 1800                                   # 30-minute window
   END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
   ```

3. **Measure it** a few minutes after `END` (so the last 1-minute data points are in Amazon CloudWatch), with a
   label that is unique per run. `measure_cluster.py` appends one JSON line to `analysis/snapshots.jsonl`; if you
   reuse a label, `compare.py` uses the newest record with that label.

   ```bash
   python3 analysis/measure_cluster.py load-benchmark <label> "$START" "$END"
   ```

4. **Compare two runs** at equal `MessagesInPerSec`. `compare.py` prints the mean and the worst broker for each
   metric and exits 0 (OK) or 1 (SUSPECT: message rates more than 5% apart, under-replicated partitions, or no
   `RequestHandlerAvgIdlePercent`).

   ```bash
   python3 analysis/compare.py <baseline-label> <candidate-label>
   ```

Log the profile, spec or script arguments, cluster size, and window of every run. See
[Measurement method](../../README.md#measurement-method) and
[Benchmark integrity rules](../../README.md#benchmark-integrity-rules) in the root README.

## The Amazon CloudWatch dashboard

`scripts/15-put-dashboard.sh load-benchmark "$AWS_REGION" 3` publishes the dashboard `load-benchmark`
(Amazon CloudWatch console, **Dashboards**). Every runbook names the widgets to watch. The ones used most:

- `RequestHandlerAvgIdlePercent` and `NetworkProcessorAvgIdlePercent` (per broker): request-handler and
  network-thread headroom. The worst broker matters most.
- `CPU total %`, `Produce latency ms`, `FetchConsumerTotalTimeMsMean`.
- `MessagesInPerSec` and `Throughput BytesIn/BytesOut`: confirm that the offered load is what you expect.
- `ConnectionCount`, `ConnectionCreationRate`, `ClientConnectionCount`, `PartitionCount`. The horizontal lines
  are example thresholds; set `CONN_COUNT_THRESHOLD`, `CONN_RATE_THRESHOLD`, and `PARTITION_THRESHOLD` when you
  run `15-put-dashboard.sh` to match your broker size.
- `UnderReplicatedPartitions / UnderMinIsrPartitionCount` (want 0) and consumer lag.

## Limitations found in the repository

- No script deletes consumer groups; use `kafka_cli` with `kafka-consumer-groups.sh --delete` (above). The
  shipped roles grant `kafka-cluster:DeleteGroup` on the benchmark cluster's groups for this purpose.
- No script changes topic configuration or deletes topics; the tests use `kafka_cli` (above).
- The Amazon EC2 multi-group consumer (`07-start-multigroup-consumers.sh`) does not expose worker threads or
  members per group; those are Amazon EKS only (`consumers.worker_threads`, `consumers.members_per_group`).
- `k8s/kstreams/gen-kstreams.sh` does not set `REPLICATION_FACTOR`, `NUM_STREAM_THREADS`, or
  `COMMIT_INTERVAL_MS`; Test 2's optional Kafka Streams factor sets them with `kubectl set env --local`.
