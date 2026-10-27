<!-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. -->
<!-- SPDX-License-Identifier: MIT-0 -->
# Diagnose an existing cluster (`analysis/diagnose.py`)

`diagnose.py` is a read-only health check for an Amazon MSK provisioned cluster you already run. Point it at
your non-production or performance-test cluster while a representative load is running, and in about a minute
it tells you whether the brokers look **request-bound** (the pattern this repository's client tuning
addresses), and which test to run next.

It reads the cluster description and the `AWS/Kafka` per-broker metrics in Amazon CloudWatch. It doesn't
connect to Kafka and doesn't change anything.

## Run it

```bash
python3 -m pip install -r analysis/requirements.txt     # boto3, once
python3 analysis/diagnose.py --cluster-arn "$CLUSTER_ARN"                     # last 3 hours, text
python3 analysis/diagnose.py --cluster-arn "$CLUSTER_ARN" --hours 6 --format markdown > diagnose.md
python3 analysis/diagnose.py --cluster-arn "$CLUSTER_ARN" \
    --start 2026-01-01T10:00:00Z --end 2026-01-01T10:30:00Z --format json
```

| Option | Meaning |
|---|---|
| `--cluster-arn` | Required. The cluster to check. Only the cluster name appears in the output. |
| `--hours N` | Window ending now, 1-336 hours (default 3). |
| `--start`, `--end` | Explicit ISO-8601 UTC window instead of `--hours`. Must start within the last 15 days, because the checks use 1-minute data ([CloudWatch retention](https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_GetMetricData.html)). |
| `--region`, `--profile` | Default: the Region in the ARN, and the SDK's normal credential resolution. |
| `--format text\|markdown\|json` | `markdown` and `json` contain no ARNs or account IDs, so they're safe to paste into a doc or a GitHub Discussion. |

Exit codes: `0` = every check is OK or skipped, `1` = at least one WARN or ATTENTION, `2` = usage error or the
AWS calls failed (credentials, permissions, cluster not found).

Permissions needed (read-only): `kafka:DescribeClusterV2`, `kafka:ListNodes` (optional; without it the tool
assumes broker IDs `1..N`), and `cloudwatch:GetMetricData`.

## What it checks

Each finding prints a status, the measured value, the threshold and its source, and one "what to do" line.
Thresholds marked **heuristic** come from an AWS blog post or were chosen for this tool; they are not
documented AWS limits.

| # | Check | Threshold | Source |
|---|---|---|---|
| 1 | Enhanced monitoring level | `PER_BROKER` or higher (needed for checks 2, 3, 7 rate, 8) | [Standard broker metrics](https://docs.aws.amazon.com/msk/latest/developerguide/metrics-details.html) |
| 2 | `RequestHandlerAvgIdlePercent`, mean and worst broker, next to `BytesInPerSec` and CPU | worst broker >= 40% (0.4). Low idle with CPU under 60% is reported as **request-bound** | **Heuristic**: alarm threshold in the AWS Big Data Blog post [Simplifying Kafka operations with Amazon MSK Express brokers](https://aws.amazon.com/blogs/big-data/simplifying-kafka-operations-with-amazon-msk-express-brokers/) |
| 3 | `NetworkProcessorAvgIdlePercent` | worst broker >= 50% (0.5) | **Heuristic**: same blog post |
| 4 | Broker CPU = `CpuUser + CpuSystem` | mean per broker < 60% (WARN if 1-minute peaks cross it) | [Best practices for Standard brokers, Monitor CPU usage](https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html) |
| 5 | Average message size = `BytesInPerSec / MessagesInPerSec` (on-wire, after producer compression) | >= 1024 bytes | **Heuristic** (this tool) |
| 6 | `PartitionCount` per broker (including replicas) | Recommended and maximum per instance type, from the documented tables | Standard: [best practices](https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html); Express: [quotas](https://docs.aws.amazon.com/msk/latest/developerguide/limits.html) |
| 7 | IAM `ClientConnectionCount` and `ConnectionCreationRate` per broker (Sum, 1-minute period), `IAMTooManyConnections` | < 3000 connections; < 100 new connections/s (4/s on t3); WARN at 80% (**heuristic**) | [Amazon MSK quota](https://docs.aws.amazon.com/msk/latest/developerguide/limits.html) |
| 8 | `ProduceMessageConversionsPerSec` | 0 | [Standard broker metrics](https://docs.aws.amazon.com/msk/latest/developerguide/metrics-details.html) |
| 9 | `UnderReplicatedPartitions`, `UnderMinIsrPartitionCount` | 0 | [Standard broker metrics](https://docs.aws.amazon.com/msk/latest/developerguide/metrics-details.html) |

`*IdlePercent` metrics are reported by Amazon MSK as fractions (1.0 = fully idle); the tool prints them as
percentages. A per-broker table at the end shows the mean of every metric per broker, so you can spot a single
hot broker (partition-leader skew) as opposed to a cluster-wide problem.

**Express brokers.** This repository's thresholds and measurements target Standard brokers. On Express brokers
the tool still runs checks 1, 5, 6 (with the Express partition table) and 7, prints the measured values for
2-4 as SKIP, and skips 8 and 9 because those metrics are not emitted for Express brokers
([Express broker metrics](https://docs.aws.amazon.com/msk/latest/developerguide/metrics-details-express.html)).

## Example (unhealthy test cluster, abbreviated)

```text
[ATTENTION] 2. RequestHandlerAvgIdlePercent (mean / worst broker)
            measured:  mean 27%, worst broker 2 at 22% (BytesInPerSec there 5.7 MiB/s); request-bound: handler
                       threads are busy while CPU is 58% (< 60%), so the broker is serving many small requests
            threshold: worst broker >= 40% (heuristic: blog alarm threshold 0.4)
            do:        Send fewer, fuller requests: compare your client config with analysis/check_profile.py
                       and docs/client-tuning.md, then A/B it as in docs/test-cases/test-1-head-to-head.md.
[WARN     ] 5. Average message size (BytesInPerSec / MessagesInPerSec)
            measured:  200 bytes per message (on-wire, after producer compression), 90000 msg/s, 17.2 MiB/s in
[ATTENTION] 7. IAM connection creation rate per broker (ConnectionCreationRate, Sum, 1-minute period)
            measured:  peak 140.0/s on broker 1; IAMTooManyConnections peak 12
            threshold: < 100 per second per broker (AWS quota); IAMTooManyConnections = 0
```

## What to do next

1. Compare your client configuration with the `aws-tuned` profile:
   `python3 analysis/check_profile.py <your-client.properties>` ([check-profile.md](check-profile.md)).
2. Reproduce the finding on a test cluster with the matching runbook in [test-cases/](test-cases/README.md):
   request-handler, CPU or message-size findings with
   [Test 1: validate and compare head to head](test-cases/test-1-head-to-head.md), partition density with
   [Test 2: one factor at a time](test-cases/test-2-one-factor-at-a-time.md), connection findings with
   [Test 3: maximum client count](test-cases/test-3-max-client-count.md).
3. Measure baseline against `aws-tuned` at equal `MessagesInPerSec` with `analysis/measure_cluster.py` and
   `analysis/compare.py` ([README](../README.md#measurement-method)).

The tool reports what the broker metrics show over one window. It can't see client-side problems (CPU-saturated
client hosts, rebalances, send errors); check those as described in
[Benchmark integrity rules](../README.md#benchmark-integrity-rules).
