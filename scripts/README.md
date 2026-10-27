# scripts — host-side scripts

These run **on the client hosts** (Amazon EC2 instances), not your laptop. They are staged
to each host (`/opt/load-benchmark/scripts/`) via Amazon S3 and executed over
AWS Systems Manager (SSM) by `../orchestration/run-on-host.sh`. You rarely invoke them
directly; you invoke `run-on-host.sh <stack> <region> <profile> <script> [args...]` and it
runs them for you. Every run re-syncs `scripts/` (including `profiles/`) to the host.

All scripts authenticate to Amazon MSK with AWS IAM (`/opt/kafka/config/client-iam.properties`,
using the host's instance role) and resolve the bootstrap brokers with
`aws kafka get-bootstrap-brokers` via `lib-msk-env.sh`.

| Script / file | Runs on | Purpose |
|---|---|---|
| `lib-msk-env.sh` | (sourced) | Selects the Amazon MSK control-plane endpoint (public regional endpoint by default; `MSK_ENDPOINT_URL` overrides). |
| `00-setup-topics.sh` | any host | Create the benchmark topics (RF=3, min.insync=2, bounded retention). |
| `01-start-producers.sh` | producer host | Start one `kafka-producer-perf-test` per topic to hit an aggregate MB/s target. |
| `02-start-consumers.sh` | consumer host | Start one consumer group (per host) across all topics. |
| `03-stop-load.sh` | any host | Stop this host's producers/consumers. |
| `04-load-status.sh` | any host | Show this host's process status + log tails. |
| `05-collect-diagnostics.sh` | any host | Read-only client-side snapshot (CPU, memory, GC, network allowance, consumer lag). |
| `06-start-slow-consumers.sh` | consumer host | `SlowConsumer.java` per topic: sleeps between polls to mimic a micro-batch engine. |
| `07-start-multigroup-consumers.sh` | consumer host | `MultiGroupConsumer.java`: many consumer groups (one per "service instance") in one JVM. |
| `08-start-multi-producers.sh` | producer host | `MultiProducer.java`: many small producers ("many microservices") sharing a fixed total rate. |
| `09-set-client-quotas.sh` | any host | Set / describe / delete a default (user, client-id) Kafka client quota. Dry-run unless `--apply`. |
| `15-put-dashboard.sh` | laptop or host | Create/update the Amazon CloudWatch dashboard for the broker metrics. |
| `client-tuning.properties` | (synced) | The **active** Kafka client profile, layered over IAM auth. Ships as a copy of `profiles/aws-tuned.properties`. |
| `profiles/aws-tuned.properties` | (synced) | The blog's recommended client profile. |
| `profiles/baseline.properties` | (synced) | Template for **your production** client settings (ships with Apache Kafka 3.9 client defaults). |

## Argument reference

```
00-setup-topics.sh    <cluster-arn> [prefix=load-bench] [num-topics=12] [partitions=24] [retention-ms=3600000] [rf=3] [min-isr=2]
01-start-producers.sh <cluster-arn> [prefix=load-bench] [num-topics=12] [target-mb-per-sec=180] [record-size=1024] [linger-ms] [batch-size]
02-start-consumers.sh <cluster-arn> [prefix=load-bench] [num-topics=12] [group-name=group-<hostname>] [fetch-min-bytes] [fetch-max-wait-ms]
04-load-status.sh     [lines=5]
05-collect-diagnostics.sh [group-name=group-0]
06-start-slow-consumers.sh <cluster-arn> [prefix=load-bench] [num-topics=12] [group-name] [sleep-ms=3000]
07-start-multigroup-consumers.sh <cluster-arn> <topics-csv> <group-prefix> <num-groups> [start-index=0] [stagger-ms=200]
08-start-multi-producers.sh <cluster-arn> [prefix=load-bench] [num-topics=12] [target-mb-per-sec=119] [record-size=625] [num-producers=120] [client-prefix=mp]
09-set-client-quotas.sh <cluster-arn> set|describe|delete [--producer-byte-rate N] [--consumer-byte-rate N] [--request-percentage N] [--user NAME] [--client-id ID] [--apply]
15-put-dashboard.sh   [cluster-name=load-benchmark] [region=us-east-1] [num-brokers=3] [dashboard-name=<cluster-name>] [profile]
```

For `07`, use group prefixes of the form `bench-bcast-<host>` for broadcast-style groups
(for example `bench-bcast-host0`), so they are easy to find in consumer-lag metrics.

## Throughput ↔ records/sec

Producers take an **aggregate MB/s target** and split it across topics:

```
rec/s per topic = target_MB/s * 1048576 / record_size / num_topics
```

At the defaults (180 MB/s, 1024-byte records, 12 topics) that is ~15,360 rec/s per topic.

## Client profiles (A/B testing your config)

A profile is one `.properties` file holding producer **and** consumer keys. Any profile
name `<p>` maps to `scripts/profiles/<p>.properties` (and, on the Amazon EKS path, to the
ConfigMap `client-tuning-<p>`; see `../k8s/README.md`).

| Profile | What it is |
|---|---|
| `aws-tuned` | The blog's recommended settings: `linger.ms=25`, `batch.size=131072`, `compression.type=zstd`, `fetch.min.bytes=128`, `fetch.max.wait.ms=1000`, `max.poll.records=1000`, `CooperativeStickyAssignor`, OS-tuned socket buffers. |
| `baseline` | A **template**. Replace its values with your production client settings so the baseline reproduces production. As shipped it contains the Apache Kafka 3.9 client defaults, written out explicitly. |
| `aws-tuned-snappy`, `aws-tuned-lz4` | Identical to `aws-tuned` except `compression.type`, for a codec A/B (for example `../orchestration/run-ab.sh "$CLUSTER_ARN" aws-tuned aws-tuned-snappy`). |
| `<your-name>` | Bring your own: add `scripts/profiles/<your-name>.properties`. Check it first with `python3 analysis/check_profile.py scripts/profiles/<your-name>.properties`. |

On the EC2/SSM path the start scripts read only `scripts/client-tuning.properties`. To
switch profile, copy one over it, then restart the load (the next `run-on-host.sh` call
re-syncs the file):

```bash
cp scripts/profiles/baseline.properties  scripts/client-tuning.properties   # run A: production
cp scripts/profiles/aws-tuned.properties scripts/client-tuning.properties   # run B: AWS-tuned
```

Keep everything else (load, record size, topic/partition count, consumer count, broker
size) identical between the two runs so only the client properties change. Do not put
authentication keys in a profile; they come from the IAM auth file. The start scripts
(`01`, `02`, `06`, `07`, `08`) refuse to start with an `ERROR:` if `client-tuning.properties`
sets `security.protocol`, `sasl.mechanism`, `sasl.jaas.config` or
`sasl.client.callback.handler.class` (the same rule the Amazon EKS image entrypoint applies).

### One-command A/B (`../orchestration/run-ab.sh`)

`orchestration/run-ab.sh <cluster-arn> <profile-a> <profile-b> [--mbps 100] [--topics 12] [--warmup-min 15]
[--measure-min 30] [--brokers 3] [--dry-run]` runs both profiles back to back with identical load from your
workstation. For each profile it copies the profile over `client-tuning.properties`, starts `01` and `02`, waits
out the warm-up, measures a fixed window with `analysis/measure_cluster.py`, and stops the load. It then writes
`analysis/compare.py --format markdown` to `results/ab-<timestamp>.md`, and restores the original
`client-tuning.properties` at the end and on Ctrl-C. Both runs use the consumer group `bench-cg-0` on the first
consumer host; load stops between runs and the warm-up absorbs any catch-up. Run with `--dry-run` first to print
every command. Exit code 0 = OK, 1 = SUSPECT or a failed step, 2 = bad input.

### Override points

Both are re-applied on the next `run-on-host.sh` call:

1. **`client-tuning.properties`** (the active profile) — persistent, applies to producers and consumers.
2. **Trailing CLI args** on `01`/`02` — per-run, override the file for that run.

The effective merged config is written on the host to
`/opt/kafka/config/{producer,consumer,multigroup,multiproducer}-effective.properties` so you
can confirm what actually took effect.

## Client quotas (`09-set-client-quotas.sh`)

Wraps `kafka-configs.sh` to manage a quota on the **default (user, client-id)** entity, which
applies to every client without a more specific quota. Quotas are enforced per broker:

| Flag | Kafka key | Meaning |
|---|---|---|
| `--producer-byte-rate` | `producer_byte_rate` | bytes/sec a client may produce to one broker |
| `--consumer-byte-rate` | `consumer_byte_rate` | bytes/sec a client may fetch from one broker |
| `--request-percentage` | `request_percentage` | % of one broker I/O/network thread a client may use |

```bash
# Dry run (default): prints the kafka-configs.sh command only
./09-set-client-quotas.sh $ARN set --producer-byte-rate 10485760 --consumer-byte-rate 20971520 --request-percentage 50
# Execute, then inspect and remove
./09-set-client-quotas.sh $ARN set --producer-byte-rate 10485760 --consumer-byte-rate 20971520 --request-percentage 50 --apply
./09-set-client-quotas.sh $ARN describe --apply
./09-set-client-quotas.sh $ARN delete --apply
```

`--user` / `--client-id` target a specific entity instead of the default; with AWS IAM
authentication the Kafka user principal is the IAM role or user ARN. The host role needs
`kafka-cluster:AlterClusterDynamicConfiguration` (set/delete) and
`kafka-cluster:DescribeClusterDynamicConfiguration` (describe). While a quota is active,
watch `ProduceThrottleTime`, `FetchThrottleTime` and `RequestThrottleTime` (PER_BROKER). How to
size, apply and tighten quotas as a guardrail: [../docs/quotas.md](../docs/quotas.md).

## Dashboard (`15-put-dashboard.sh`)

Creates one Amazon CloudWatch dashboard per cluster with per-broker widgets for every
metric in the blog's broker metric table:

`RequestHandlerAvgIdlePercent`, `NetworkProcessorAvgIdlePercent`, `CpuUser + CpuSystem`,
`MessagesInPerSec`, `BytesInPerSec` / `BytesOutPerSec`, `ProduceTotalTimeMsMean`,
`FetchConsumerTotalTimeMsMean`, `ProduceMessageConversionsPerSec`, `VolumeQueueLength`,
`PartitionCount`, `ClientConnectionCount`, `ConnectionCreationRate`,
`UnderReplicatedPartitions`, `UnderMinIsrPartitionCount`

plus supporting signals (memory, disk used, EBS throughput/IOPS/burst balance, produce local
time, fetch message conversions, consumer lag, `ConnectionCount`, network allowance exceeded).
Several of these are PER_BROKER metrics, so the cluster needs enhanced monitoring at
`PER_BROKER` or higher. The horizontal lines on the connection and partition widgets are
example thresholds; set `CONN_COUNT_THRESHOLD`, `CONN_RATE_THRESHOLD` and
`PARTITION_THRESHOLD` for your broker size.

## Log formats (in `../results/` after a pull)

- **Producer** (`producer-<topic>.log`): human-readable summary per interval, e.g.
  `20000 records sent, 4000.0 records/sec (3.91 MB/sec), 10.0 ms avg latency, 60.0 ms max latency.`
- **Consumer** (`consumer-<group>-<topic>.log`, `--show-detailed-stats`): CSV, header then rows:
  `time, threadId, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec`
- **SlowConsumer / MultiGroupConsumer / MultiProducer**: one CSV line per report interval;
  the columns are printed by the start script and documented at the top of each `.java` file.
