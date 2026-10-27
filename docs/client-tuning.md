# Client tuning guide

This guide explains each setting in the `aws-tuned` profile (`scripts/profiles/aws-tuned.properties`): what it
changes on the Amazon MSK brokers, and what it costs on the client. The profile is deliberately conservative.
Treat it as a starting point, measure it against your own `baseline` with the method in the
[README](../README.md#measurement-method), and keep any setting only if it wins on your workload.

At high partition and client density, brokers spend their request-handler threads on **requests**, not bytes.
Every setting below works by sending fewer, fuller requests for the same message rate.

## Producer settings

| Setting | `baseline` (Kafka 3.9 default) | `aws-tuned` | Broker-side effect | Client-side cost |
|---|---|---|---|---|
| `linger.ms` | 0 | 25 | The producer waits up to 25 ms to fill a batch, so each produce request carries more records and the broker handles fewer requests. | Adds up to 25 ms of produce latency when traffic is light. Under heavy traffic, batches fill sooner and the wait is shorter. |
| `batch.size` | 16384 (16 KB) | 131072 (128 KB) | Raises the ceiling on records per partition per request, so `linger.ms` can actually fill larger batches. | More memory per in-flight partition batch. |
| `buffer.memory` | 33554432 (32 MB) | 67108864 (64 MB) | None directly; it keeps larger batches from blocking `send()`. | 64 MB of heap per producer instance. Size the JVM heap and the pod memory limit for it. |
| `compression.type` | none | zstd | Fewer bytes on the wire, in replication, and on disk (the blog measured about 40% fewer on-wire bytes at equal message rate). | Client CPU to compress and decompress. Measure client CPU, not only broker CPU. |
| `acks`, `enable.idempotence`, `max.in.flight.requests.per.connection` | all, true, 5 | all, true, 5 | Unchanged between profiles, so durability is identical and the comparison is fair. | Idempotence needs the `kafka-cluster:WriteDataIdempotently` permission with IAM access control (the shipped role grants it). |
| `send.buffer.bytes` | 131072 | -1 | Lets the operating system size the socket send buffer. | None on typical Linux hosts. |

**Align the topic codec with the producers.** If a topic's `compression.type` differs from what its producers
send, the brokers recompress every batch. That shows up as `ProduceMessageConversionsPerSec` above 0 and as a
steady CPU cost on every broker. Keep the topic at `compression.type=producer` (the default), or set it to the
same codec the producers use.

## Consumer settings

| Setting | `baseline` (Kafka 3.9 default) | `aws-tuned` | Broker-side effect | Client-side cost |
|---|---|---|---|---|
| `fetch.min.bytes` | 1 | 128 | The broker holds a fetch until at least 128 bytes are available instead of answering with near-empty responses, so idle and low-traffic partitions generate far fewer fetch requests. | More end-to-end latency on quiet partitions. |
| `fetch.max.wait.ms` | 500 | 1000 | Caps how long the broker holds a fetch that hasn't reached `fetch.min.bytes`. Together with `fetch.min.bytes`, it cuts the fetch request rate. | `EstimatedMaxTimeLag` can rise to tens of seconds on low-traffic topics. It should stay bounded and flat, not grow. That's acceptable for most asynchronous consumers, but not for latency-critical ones. |
| `max.poll.records` | 500 | 1000 | Fewer, larger polls for the same throughput. | Each `poll()` returns more records, so processing between polls takes longer. Keep it well under `max.poll.interval.ms` (300000 in both profiles), or the consumer leaves the group. |
| `partition.assignment.strategy` | RangeAssignor, CooperativeStickyAssignor | CooperativeStickyAssignor | Incremental rebalances: only the partitions that move are revoked, instead of every partition of every member. That means fewer stop-the-world rebalances and less fetch and commit churn when pods scale or restart. | **Migrating a live group from eager (Range) to cooperative needs two rolling restarts:** first deploy with both assignors listed (`CooperativeStickyAssignor` first), then remove the eager one. Switching directly can fail the rebalance. |
| `auto.commit.interval.ms` | 5000 | 5000 | Unchanged. Committing more often adds offset-commit requests. Amazon MSK consumer-lag metrics rely on the group's committed offsets (they are absent until the group has committed one), so the commit interval adds to the lag you see in CloudWatch. See [Monitor consumer lags](https://docs.aws.amazon.com/msk/latest/developerguide/consumer-lag.html). | - |
| `receive.buffer.bytes` | 65536 | -1 | Lets the operating system size the socket receive buffer, which helps larger fetch responses. | None on typical Linux hosts. |

## Settings that apply to both

| Setting | Value | Why |
|---|---|---|
| `connections.max.idle.ms` | 540000 (both profiles) | Keeps idle connections open for 9 minutes, so clients don't repeatedly reconnect. With IAM access control, every new connection is authenticated, and connection creation is rate-limited per broker. |

## Beyond client settings

- **Ship the settings in a shared client library or SDK** so every service gets them and they don't drift.
- **Watch connection counts.** Many consumer groups, or one consumer per partition, multiply connections per
  broker. Track `ClientConnectionCount` and `ConnectionCreationRate` against the IAM connection quotas in
  [Amazon MSK quota](https://docs.aws.amazon.com/msk/latest/developerguide/limits.html).
- **Use server-side quotas as a safety net, not as the fix.** `scripts/09-set-client-quotas.sh` sets a default
  `(user, client-id)` quota. Start loose and tighten gradually while watching consumer lag and client latency.

## References

- [Amazon MSK best practices for Standard brokers](https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html)
- [Amazon MSK quota](https://docs.aws.amazon.com/msk/latest/developerguide/limits.html)
- [Apache Kafka producer configs](https://kafka.apache.org/documentation/#producerconfigs) and
  [consumer configs](https://kafka.apache.org/documentation/#consumerconfigs)
