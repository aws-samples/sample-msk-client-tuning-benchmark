# Project: MSK Cluster Load Benchmark

This workspace is a harness for putting a **sustained, high-throughput workload** on an Amazon
MSK cluster to observe how a broker instance type behaves under load. Read this before
proposing changes or running anything.

## What it does

- Stands up a **Standard MSK cluster** (`kafka.m7g.4xlarge` × 3 by default) in a private VPC.
- Runs a **producer host** driving a target ingress (default ~180 MB/s) and a
  **horizontally-scalable consumer fleet** (one consumer group per host, scale to fan out).
- Publishes a **CloudWatch dashboard** for disk/EBS pressure, EBS throughput & IOPS,
  request-handler and network-processor idle, CPU, memory, produce latency, throughput, and lag.

The goal is **observation, not pass/fail**. Reproduce a realistic workload and watch the
instance type hold up.

## Architecture (know this before changing anything)

- **CDK provisions only the VPC + client EC2 hosts.** The MSK cluster is created **out-of-band**
  by `create-msk-standard-cluster.sh`, NOT by CloudFormation. Do not try to add MSK to CDK.
- Client hosts are **SSM-managed** (no SSH, no inbound rules). Everything runs via
  `orchestration/run-on-host.sh`, which stages `scripts/` to S3 and executes them on a host
  with `aws ssm send-command` (`AWS-RunShellScript`), then pulls `results/` back.
- Host IAM is scoped by **cluster name** (default `load-benchmark`), because the cluster does
  not exist at CDK deploy time. The name in `infra/bin/infra.ts` (`clusterName` context) MUST
  match `CLUSTER_NAME` in the create script.
- On each host: scripts live at `/opt/load-benchmark/scripts/`, results at
  `/opt/load-benchmark/results/`, Kafka at `/opt/kafka`, IAM auth at
  `/opt/kafka/config/client-iam.properties`.

## Conventions

- Region/account/sizing/counts are **CDK context** (`-c consumerCount=3`, `-c region=...`), not
  hardcoded. Prefer context over editing files.
- Producer and consumer roles live on **separate hosts** on purpose: at ~180 MB/s each, sharing
  one host makes the client the bottleneck and invalidates the broker measurement. Do not merge
  them.
- Kafka client config defaults to **Kafka defaults**. Tweak via `scripts/client-tuning.properties`
  (persistent) or trailing CLI args on the start scripts (per-run). Both re-sync on the next
  `run-on-host.sh` call.
- Shell scripts use `set -euo pipefail` (orchestration) or `set -uo pipefail` (host-side load
  scripts, which must survive individual process exits). Match the existing style.
- The CloudWatch dashboard depends on **PER_BROKER** enhanced monitoring, which the create
  script sets. Volume*/*AvgIdlePercent/Produce*TimeMsMean are PER_BROKER metrics.

## Safety

- The MSK cluster and its EBS are **billable** (3 × m7g.4xlarge + 1000 GB × 3 by default).
  Always surface cleanup steps. Never delete the cluster or destroy stacks without explicit
  confirmation.
- Treat the target account as production unless told otherwise; prefer read/describe over
  mutate/delete.

## Where things are

- `create-msk-standard-cluster.sh` — create the cluster + topics (run locally).
- `infra/` — CDK (network-stack, client-host-stack, bin/infra.ts).
- `orchestration/` — laptop-side drivers (run-on-host, pull-results, tail-command).
- `scripts/` — host-side scripts + client-tuning.properties + dashboard.
- `results/` — logs pulled back from hosts.
- Root `README.md` has the full runbook and the 180 MB/s instance-sizing analysis.

To walk a new operator through a run end to end, use the `msk-load-benchmark` skill.
