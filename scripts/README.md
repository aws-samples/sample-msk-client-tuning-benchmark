# scripts — host-side scripts

These run **on the client hosts**, not your laptop. They are staged to each host
(`/opt/load-benchmark/scripts/`) via S3 and executed over SSM by
`../orchestration/run-on-host.sh`. You rarely invoke them directly; you invoke
`run-on-host.sh <stack> <region> <profile> <script> [args...]` and it runs them for you.

| Script | Runs on | Purpose |
|---|---|---|
| `lib-msk-env.sh` | (sourced) | Selects the MSK control-plane endpoint (`prod` by default). |
| `00-setup-topics.sh` | any host | Create the benchmark topics (RF=3, min.insync=2). |
| `01-start-producers.sh` | producer host | Start one producer per topic to hit an aggregate MB/s target. |
| `02-start-consumers.sh` | consumer host | Start one consumer group (per host) across all topics. |
| `03-stop-load.sh` | any host | Stop this host's producers/consumers. |
| `04-load-status.sh` | any host | Show this host's process status + log tails. |
| `client-tuning.properties` | (synced) | Editable Kafka client config, layered over IAM auth. |
| `15-put-dashboard.sh` | laptop or host | Create/update the CloudWatch dashboard. |

## Argument reference

```
00-setup-topics.sh   <cluster-arn> [prefix=load-bench] [num-topics=12] [partitions=24] [retention-ms=3600000]
01-start-producers.sh <cluster-arn> [prefix=load-bench] [num-topics=12] [target-mb-per-sec=180] [record-size=1024] [linger-ms] [batch-size]
02-start-consumers.sh <cluster-arn> [prefix=load-bench] [num-topics=12] [group-name=group-<hostname>] [fetch-min-bytes] [fetch-max-wait-ms]
04-load-status.sh    [lines=5]
15-put-dashboard.sh  [cluster-name=load-benchmark] [region=us-east-1] [num-brokers=3] [dashboard-name=<cluster-name>] [profile]
```

## Throughput ↔ records/sec

Producers take an **aggregate MB/s target** and split it across topics:

```
rec/s per topic = target_MB/s * 1048576 / record_size / num_topics
```

At the defaults (180 MB/s, 1024-byte records, 12 topics) that is ~15,360 rec/s per topic.

## Tweaking Kafka config

Everything defaults to Kafka defaults. Two override points, both re-applied on the next
`run-on-host.sh` call (which re-syncs `scripts/`):

1. **`client-tuning.properties`** — persistent, applies to producers and consumers.
2. **Trailing CLI args** on the start scripts — per-run, override the file for that run.

Effective merged config is written on the host to
`/opt/kafka/config/{producer,consumer}-effective.properties` so you can confirm what actually
took effect.

## Log formats (in `../results/` after a pull)

- **Producer** (`producer-<topic>.log`): human-readable summary per interval, e.g.
  `20033 records sent, 4003.4 records/sec (3.91 MB/sec), 10.6 ms avg latency, 59.0 ms max latency.`
- **Consumer** (`consumer-<group>-<topic>.log`, `--show-detailed-stats`): CSV, header then rows:
  `time, threadId, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec`
