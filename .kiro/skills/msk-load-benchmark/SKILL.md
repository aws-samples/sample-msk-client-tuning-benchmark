---
name: msk-load-benchmark
description: >-
  Step-by-step guided runbook for the MSK cluster load benchmark in this
  workspace. Use when the user wants to spin up the benchmark stack, create the
  MSK cluster, start producers, add/scale consumers, publish the CloudWatch
  dashboard, tweak Kafka client config, check load status, pull results, or tear
  everything down. Drives the CDK app, create-msk-standard-cluster.sh, and the
  orchestration/ + scripts/ helpers. Trigger on phrases like "run the load
  benchmark", "spin up the MSK benchmark", "start the producers", "add a
  consumer", "180 MB/s test", "put up the dashboard", or "tear down the cluster".
---

# MSK Load Benchmark — guided runbook

You are guiding an operator (possibly new to this project) through a load-benchmark run. Go
**one step at a time**, confirm the result of each step before moving on, and never run a
billable or destructive action without stating the cost/impact and getting an explicit yes.

Read `.kiro/steering/project.md` and the root `README.md` first if you have not — they carry the
architecture and the instance-sizing rationale. Do not re-derive them.

## Ground rules

- All host interaction goes through `orchestration/run-on-host.sh <stack> <region> <profile>
  <script> [args...]`. Never SSH. Never edit files on the host directly.
- Stacks: `LoadBenchmarkNetworkStack`, `LoadBenchmarkProducerStack`,
  `LoadBenchmarkConsumerStack-0`, `-1`, … (one per consumer host).
- The `<cluster-name>` used by CDK (`clusterName` context, default `load-benchmark`) MUST match
  the create script's `CLUSTER_NAME`. If the operator changes one, change both.
- Confirm `AWS_PROFILE` / `AWS_REGION` (or `--profile`) with the operator up front. Verify creds
  with `aws sts get-caller-identity` before long steps — an expired session silently breaks the
  SSM/CloudWatch calls.
- Producers and consumers run on separate hosts by design. Do not combine them.

## Step 0 — Preflight

1. Confirm target account/region/profile with the operator.
2. `aws sts get-caller-identity --profile <profile> --region <region>` — confirm it's the
   intended account (treat as production; prefer read/describe).
3. Confirm Node 18+ and Python 3 are available for the CDK app.

## Step 1 — Deploy the VPC + client hosts

Ask how many consumer groups they want (default 1) and whether the default sizing
(`c5n.2xlarge` producer + consumers, `kafka.m7g.4xlarge` × 3 brokers, ~180 MB/s) is right. For
other throughput targets, consult the sizing table in the root README and set
`-c producerInstanceType=... -c consumerInstanceType=...`.

```bash
cd infra
npm install
AWS_PROFILE=<profile> npx cdk deploy --all --require-approval never \
  -c region=<region> [-c consumerCount=<N>] [-c account=<acct>]
cd ..
```

Confirm all stacks reached `CREATE_COMPLETE`/`UPDATE_COMPLETE`. This is billable (EC2 + NAT).

## Step 2 — Create the MSK cluster + topics (~20-30 min, billable)

State the cost before running: 3 × `kafka.m7g.4xlarge` + EBS. Get explicit confirmation.

```bash
./create-msk-standard-cluster.sh   # add args to override name/type/brokers/topics/partitions/ebs
```

It waits for `ACTIVE`, then creates topics via the producer host. **Capture the printed
`Cluster ARN`** — every later step needs it. Save it: `export CLUSTER_ARN=<arn>`.

## Step 3 — Publish the CloudWatch dashboard

```bash
./scripts/15-put-dashboard.sh <cluster-name> <region> <num-brokers>
```

Give the operator the printed dashboard URL. Note it needs PER_BROKER monitoring (the create
script sets it).

## Step 4 — Start the producers (~180 MB/s)

```bash
./orchestration/run-on-host.sh LoadBenchmarkProducerStack <region> <profile> \
  01-start-producers.sh --background "$CLUSTER_ARN" load-bench 12 180
```

`--background` returns immediately. Then verify:

```bash
./orchestration/run-on-host.sh LoadBenchmarkProducerStack <region> <profile> 04-load-status.sh
```

Expect all producers `RUNNING` and producer logs showing records/sec climbing toward the target.
If they exit immediately, check the `.out` log for auth/broker errors (usually a
cluster-name/IAM-scope mismatch or the cluster not `ACTIVE`).

## Step 5 — Start / scale consumers

Run one consumer group per consumer host, each with a **distinct group name** so they fan out
(each group re-reads the full stream, multiplying egress):

```bash
./orchestration/run-on-host.sh LoadBenchmarkConsumerStack-0 <region> <profile> \
  02-start-consumers.sh --background "$CLUSTER_ARN" load-bench 12 group-0
# host 1:
./orchestration/run-on-host.sh LoadBenchmarkConsumerStack-1 <region> <profile> \
  02-start-consumers.sh --background "$CLUSTER_ARN" load-bench 12 group-1
```

To add MORE consumer hosts than were deployed: re-run Step 1 with a higher `-c consumerCount`,
then start consumers on the new `LoadBenchmarkConsumerStack-<i>`. Remind the operator each extra
group adds ~180 MB/s of cluster egress.

## Step 6 — Observe

Point the operator at the dashboard. The key signals when judging the instance type:

- `RequestHandlerAvgIdlePercent` / `NetworkProcessorAvgIdlePercent` approaching 0 → broker
  threads saturating.
- `VolumeQueueLength` rising, `ProduceTotalTimeMsMean` climbing → EBS/back-pressure bound.
- `CpuUser+CpuSystem` near 100% → CPU bound.
- Consumer lag growing → consumers not keeping up (add a group or larger consumer hosts).

Pull full logs any time:

```bash
./orchestration/pull-results.sh LoadBenchmarkProducerStack <region> <profile>
./orchestration/pull-results.sh LoadBenchmarkConsumerStack-0 <region> <profile>
```

## Step 7 — Tweak Kafka config and re-observe

To change client behavior: edit `scripts/client-tuning.properties` (persistent) or pass trailing
args to the start scripts (per-run). Then stop and restart load on the relevant host(s):

```bash
./orchestration/run-on-host.sh <stack> <region> <profile> 03-stop-load.sh
./orchestration/run-on-host.sh <stack> <region> <profile> 01-start-producers.sh --background "$CLUSTER_ARN" load-bench 12 180
```

`run-on-host.sh` re-syncs `scripts/` (including the tuning file) on every call, so no redeploy.
To change the throughput target, change the last arg to `01-start-producers.sh` (aggregate MB/s).

## Step 8 — Stop / tear down (confirm first)

```bash
# Stop load on every host (producer + each consumer):
./orchestration/run-on-host.sh LoadBenchmarkProducerStack <region> <profile> 03-stop-load.sh
./orchestration/run-on-host.sh LoadBenchmarkConsumerStack-0 <region> <profile> 03-stop-load.sh
```

Tearing down the billable cluster + infra is **destructive** — state it and confirm:

```bash
aws kafka delete-cluster --cluster-arn "$CLUSTER_ARN" --profile <profile> --region <region>
# after the cluster is fully gone:
aws kafka delete-configuration --arn <config-arn> --profile <profile> --region <region>
cd infra && AWS_PROFILE=<profile> npx cdk destroy --all
```

## Troubleshooting quick hits

- **Producers/consumers exit at once**: cluster not `ACTIVE`, or `clusterName` ≠ create script
  `CLUSTER_NAME` (IAM scope miss). Check the `.out` log.
- **Dashboard "no data" on Volume/idle/latency widgets**: cluster isn't on PER_BROKER monitoring.
- **`run-on-host.sh` can't resolve outputs**: wrong stack name, or stack not deployed.
- **Throughput plateaus below target**: producer host network-bound — check the producer host's
  CPU/network, or step up to a larger `producerInstanceType`. `--throughput` is a ceiling, not a
  guarantee.
