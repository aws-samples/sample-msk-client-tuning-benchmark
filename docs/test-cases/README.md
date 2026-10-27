# Test case runbooks

These runbooks reproduce the eight test cases of the AWS blog post "Right-sizing Amazon MSK by tuning your
Kafka clients" with this repository. Each runbook lists its purpose, the harness it uses, the commands to run
in order, how to measure and compare the runs, what to watch on the Amazon CloudWatch dashboard, and how to
stop the test.

> **WARNING: test clusters only.** The runbooks create and delete topics, change topic and quota
> configuration, and drive sustained load. Never run them against a production cluster.

| # | Test case | Harness | Runbook | Workload |
|---|---|---|---|---|
| 1 | Baseline vs tuned config on a low-density cluster | Amazon EC2 or Amazon EKS | [tc1-baseline-vs-tuned-low-density.md](tc1-baseline-vs-tuned-low-density.md) | `01`/`02` scripts, or `k8s/examples/workload-low-density.yaml` |
| 2 | Slow, micro-batch consumers | Amazon EC2 | [tc2-slow-micro-batch-consumers.md](tc2-slow-micro-batch-consumers.md) | `01`/`02` plus `06-start-slow-consumers.sh` |
| 3 | Build up to production density | Amazon EC2 | [tc3-build-up-to-production-density.md](tc3-build-up-to-production-density.md) | `00`, `01`, `02`, `07`, `08` scripts, one factor per step |
| 4 | Clients on Amazon EKS | Amazon EKS | [tc4-clients-on-eks.md](tc4-clients-on-eks.md) | `k8s/examples/workload-eks-factors.yaml`, `k8s/kstreams/gen-kstreams.sh` |
| 5 | Production-shaped head-to-head | Amazon EKS | [tc5-production-shaped-head-to-head.md](tc5-production-shaped-head-to-head.md) | `k8s/examples/workload-high-density.yaml`, `workload-high-density-peak.yaml` |
| 6 | Peak load with many worker threads | Amazon EKS | [tc6-peak-load-many-worker-threads.md](tc6-peak-load-many-worker-threads.md) | `k8s/examples/workload-peak-worker-threads.yaml`, `workload-peak-worker-threads-fewer-pods.yaml` |
| 7 | One consumer per partition and IAM connection limits | Amazon EKS | [tc7-one-consumer-per-partition-iam-connections.md](tc7-one-consumer-per-partition-iam-connections.md) | `k8s/examples/workload-one-consumer-per-partition.yaml` |
| 8 | Server-side quotas as a guardrail | Amazon EC2 (quota) + Amazon EKS (load) | [tc8-server-side-quotas.md](tc8-server-side-quotas.md) | `09-set-client-quotas.sh` + `workload-high-density.yaml` |

## Before you start

All runbooks assume that you finished these steps of the [root README](../../README.md):

- **Every test case:** [Prerequisites](../../README.md#prerequisites), and steps 1-3 of the
  [Amazon EC2 quick start](../../README.md#quick-start-amazon-ec2-harness): the AWS CDK stacks are deployed,
  the Amazon Managed Streaming for Apache Kafka (Amazon MSK) cluster `load-benchmark` exists (with
  `PER_BROKER` enhanced monitoring, which `create-msk-standard-cluster.sh` turns on), and the Amazon CloudWatch
  dashboard is published. Install the analysis dependencies too:
  `python3 -m pip install -r analysis/requirements.txt`.
- **Amazon EKS test cases (1 on EKS, 4, 5, 6, 7, 8):** steps 1-4 of the
  [Amazon EKS quick start](../../README.md#quick-start-amazon-eks-harness): the Amazon Elastic Kubernetes
  Service (Amazon EKS) cluster exists, the client image is in Amazon ECR, the bootstrap ConfigMap is filled in,
  and `k8s/base` is applied.
- Replace `scripts/profiles/baseline.properties` with your production client settings (and, for Amazon EKS,
  regenerate the `client-tuning-baseline` ConfigMap as shown in [k8s/README.md](../../k8s/README.md#6-client-tuning-profiles-baseline-vs-aws-tuned))
  so that `baseline` really is your production configuration.

Run every command from the repository root.

## Common shell setup

Paste this block into the shell you run the runbooks from. The runbooks use these variables and functions.

```bash
export AWS_REGION=us-east-1 AWS_PROFILE=default NUM_BROKERS=3
CLUSTER_ARN=<cluster ARN printed by create-msk-standard-cluster.sh>

# Amazon EKS test cases only (from the EKS quick start):
ECR=<account>.dkr.ecr.<region>.amazonaws.com; TAG=<image tag you pushed>
IMG=$ECR/msk-bench-client:$TAG
KSE_IMG=$ECR/kstreams-emulator:$TAG   # test case 4 only; build and push it per eks/ecr.md

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
as quotas (test case 8 adds that permission for the duration of the test). To remove a stale group from an
earlier run (stop its consumers first):

```bash
kafka_cli "/opt/kafka/bin/kafka-consumer-groups.sh $KB --delete --group <group>"
```

## Topics per test case

`scripts/00-setup-topics.sh` uses `--if-not-exists`: it never changes the partition count of a topic that
already exists, and it prints a `--describe` of every topic at the end so you can check what you got.
`create-msk-standard-cluster.sh` already creates `load-bench-1..12` with 24 partitions.

| Test case | Topics | Partitions | Create with |
|---|---|---|---|
| 1 (EC2), 2 | `load-bench-1..12` | 24 | created by `create-msk-standard-cluster.sh` |
| 1 (EKS) | `load-bench-1..2` | 12 in the spec (24 if you keep the cluster's topics; still low density) | `00-setup-topics.sh "$CLUSTER_ARN" load-bench 2 12` on a fresh cluster |
| 3 | `tc3-1..12`, `tc3-bcast-1` | your production value, 20 | see the runbook |
| 4 | `tc4-1..4`, `tc4-streams-1` | 24, 6 | see the runbook |
| 5, 6, 8 | `load-bench-1..12`, `bcast-1` | 100, 20 | reset `load-bench-*` (below), then `00-setup-topics.sh` |
| 7 | `load-bench-1..4` | 60 | reset `load-bench-*` (below), then `00-setup-topics.sh` |

To **reset** the `load-bench` topics to a new partition count, stop every producer and consumer first (EC2 hosts
and EKS pods), then delete the topics and create them again. Deletion is asynchronous; if `00-setup-topics.sh`
reports that a topic is marked for deletion, wait a minute and run it again.

```bash
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --delete --topic 'load-bench-.*'"
kafka_cli "/opt/kafka/bin/kafka-topics.sh $KB --list"        # wait until no load-bench-* topic is listed
run_p 00-setup-topics.sh "$CLUSTER_ARN" load-bench <num-topics> <partitions>
```

## Measuring a run (used by every runbook)

1. **Warm up.** Start the load and wait about 15 minutes: all clients connected, no rebalance in progress,
   consumer lag flat, and no client-side errors (EC2: `run_p 04-load-status.sh`, `run_c 04-load-status.sh`;
   EKS: `RESTARTS` is 0 in `kubectl -n msk-bench get pods` and no `ERROR` lines in the pod logs).
2. **Record a steady window of 15-30 minutes.** Use the same window length for every run of a test case.

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
- No script changes topic configuration or deletes topics; the runbooks use `kafka_cli` (above).
- The Amazon EC2 multi-group consumer (`07-start-multigroup-consumers.sh`) does not expose worker threads or
  members per group; those are Amazon EKS only (`consumers.worker_threads`, `consumers.members_per_group`).
- `k8s/kstreams/gen-kstreams.sh` does not set `REPLICATION_FACTOR`, `NUM_STREAM_THREADS`, or
  `COMMIT_INTERVAL_MS`; test case 4 sets them with `kubectl set env --local`.
- Throttle metrics (`ProduceThrottleTime`, `FetchThrottleTime`, `RequestThrottleTime`) are not on the dashboard
  and not collected by `measure_cluster.py`; test case 8 reads them with the AWS CLI.
