<!--
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
-->
# Check your client config against `aws-tuned`

`analysis/check_profile.py` reads your Kafka client `.properties` file and compares it, key by key, with the
`aws-tuned` profile (`scripts/profiles/aws-tuned.properties`). It runs entirely offline: no AWS calls, no
network, standard library only.

```bash
python3 analysis/check_profile.py my-producer.properties
python3 analysis/check_profile.py my-consumer.properties --role consumer --format markdown > check.md
python3 analysis/check_profile.py my-client.properties --against scripts/profiles/my-candidate.properties
```

| Option | Default | Meaning |
|---|---|---|
| `--against FILE` | `scripts/profiles/aws-tuned.properties` | Profile to compare with. |
| `--role producer\|consumer\|both\|auto` | `auto` | Which keys to check. `auto` infers the role from the keys in your file (for example `key.serializer` or `group.id`). |
| `--defaults FILE` | `scripts/profiles/baseline.properties` | Source of the Apache Kafka 3.9 client defaults shown for keys you don't set. The shipped `baseline` holds those defaults; if you already replaced its values with your production settings, point this at an unedited copy (`git show HEAD:scripts/profiles/baseline.properties > /tmp/kafka-defaults.properties`). |
| `--format text\|markdown\|json` | `text` | Markdown is ready to paste into a doc or a GitHub Discussion. |

Exit codes: `0` = nothing needs attention, `1` = at least one ATTENTION finding, `2` = usage or input error.

## Reading the report

Each finding shows a status, your value (`default <v>` when you don't set the key), the `aws-tuned` value, the
threshold and where it comes from, and one "what to do" line linking to [client-tuning.md](client-tuning.md).

| Status | Meaning |
|---|---|
| OK | Same as `aws-tuned`, or at least as request-efficient. |
| WARN | Differs from `aws-tuned`. Worth measuring, not urgent. |
| ATTENTION | Likely to cost broker request-handler capacity, changes durability, or stops the client from starting. |
| INFO | A key you set that `aws-tuned` doesn't. Keep it identical in both profiles of an A/B. |

Thresholds labeled **heuristic** come from the blog and this repo's measurements, not from AWS documentation.

## What it flags specially

| Check | Status | Why |
|---|---|---|
| `compression.type` = `none` | ATTENTION | Every byte goes on the wire, into replication, and onto disk. |
| `compression.type` = `snappy`, `lz4`, `gzip` | WARN | Consider `zstd`, and measure it with the codec A/B (`orchestration/run-ab.sh <cluster-arn> aws-tuned aws-tuned-snappy`). |
| `linger.ms` = 0 | ATTENTION (heuristic) | Batches leave as soon as the sender is free: many small produce requests. |
| `batch.size` < 32768 | ATTENTION (heuristic) | Caps records per request, so `linger.ms` can't build larger requests. |
| Eager assignor only (`Range`, `RoundRobin`, `Sticky`) | ATTENTION | Every rebalance revokes every partition. Migrating needs two rolling restarts: first list `CooperativeStickyAssignor` first plus the eager one, then remove the eager one. |
| Eager and cooperative assignors listed | WARN | The group keeps rebalancing eagerly until the eager assignor is removed. |
| `fetch.min.bytes` = 1 | ATTENTION (heuristic) | Near-empty fetch responses: idle partitions drive many fetch requests. |
| `auto.commit.interval.ms` < 5000 (auto-commit on) | ATTENTION (heuristic) | Extra OffsetCommit requests on the group coordinators. |
| `acks` or `enable.idempotence` weaker than `aws-tuned` | ATTENTION / WARN | Durability is a business decision, not a tuning knob. The tool never recommends weakening it; use the same value in both A/B profiles. |
| `enable.idempotence=true` with `acks` other than `all` or `max.in.flight.requests.per.connection` > 5 | ATTENTION | The Kafka 3.x producer refuses to start; if idempotence isn't set explicitly, it is silently turned off instead. |
| `heartbeat.interval.ms` > `session.timeout.ms` / 3 | WARN | Members can be evicted after a single missed heartbeat. |
| `connections.max.idle.ms` lower than `aws-tuned` | WARN | More reconnects. With IAM access control, Amazon MSK limits the connection creation rate per broker ([Amazon MSK quota](https://docs.aws.amazon.com/msk/latest/developerguide/limits.html)). |
| `security.protocol`, `sasl.mechanism`, `sasl.jaas.config`, `sasl.client.callback.handler.class` | ATTENTION | Both harnesses refuse a tuning profile that sets them; the harness adds IAM authentication itself. Other `sasl.*` and `ssl.*` keys are flagged too. |
| A key set more than once | WARN | The last value wins. |

## What it never prints

- Values of `sasl.jaas.config` and of any key whose name contains `password`, `secret`, `token`, or
  `credential`: shown as `<redacted>`.
- ARNs, 12-digit account IDs, and Amazon MSK broker host names in any value: masked in every format.
- The full path of your file: only the file name.

Even so, review the output before you share it, and never paste authentication settings publicly.

## Next steps

1. Fix ATTENTION findings one key group at a time (producer batching, compression, consumer fetch, assignor).
2. Put the candidate settings in `scripts/profiles/<p>.properties` and measure them against your `baseline` at
   equal `MessagesInPerSec` ([measurement method](../README.md#measurement-method), `analysis/compare.py`).
3. Pick the matching test case in the [README](../README.md#blog-test-cases-and-how-to-run-them): test case 1
   for batching, fetch and compression, test case 4 for rebalances and consumer groups, test case 7 for
   connection counts.
