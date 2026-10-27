# analysis: measuring the broker side the same way every time

These tools read Amazon MSK broker metrics from Amazon CloudWatch (namespace `AWS/Kafka`) for a
time window you choose, and summarize consumer end-to-end latency from the consumer logs. They
take the cluster name, Region, broker count, and window as arguments or environment variables;
nothing is tied to a specific account or cluster. They need AWS credentials that allow
`cloudwatch:GetMetricStatistics` and `cloudwatch:GetMetricData`, and `PER_BROKER` (or higher)
enhanced monitoring on the cluster, which `../create-msk-standard-cluster.sh` enables.

| Tool | What it does |
|---|---|
| `diagnose.py --cluster-arn <arn> [--hours 3 \| --start <t> --end <t>] [--format text\|markdown\|json]` | **Read-only health check of an existing cluster.** Enhanced monitoring level, `RequestHandlerAvgIdlePercent` / `NetworkProcessorAvgIdlePercent` (worst broker, request-bound hint), CPU against the 60% guidance, average message size, partitions per broker against the documented table, IAM connections and connection rate against the quotas, produce message conversions, and under-replicated partitions. Each finding is OK / WARN / ATTENTION with its source and a next step. Needs only `kafka:DescribeClusterV2`, `kafka:ListNodes`, and `cloudwatch:GetMetricData`. See [../docs/diagnose.md](../docs/diagnose.md). |
| `check_profile.py <your-client.properties> [--role producer\|consumer\|both] [--format ...]` | **Offline check of a client config** against `aws-tuned`, key by key, with a reason per setting; flags codecs other than zstd, `linger.ms=0`, eager assignors, and auth keys; redacts secrets. See [../docs/check-profile.md](../docs/check-profile.md). |
| `measure_cluster.py <cluster> <label> <start> <end> [broker_ids]` | The main measurement. For one window: per-broker means of 1-minute averages for `RequestHandlerAvgIdlePercent`, `NetworkProcessorAvgIdlePercent`, `CpuUser`, `CpuSystem`, `ProduceTotalTimeMsMean`, `FetchConsumerTotalTimeMsMean`, `VolumeQueueLength`, reported as the mean across brokers plus the min and max broker; summed `BytesInPerSec`, `BytesOutPerSec`, `MessagesInPerSec`; max partitions and IAM connections per broker; consumer lag via CloudWatch Metrics Insights. Rejects a window whose end is not after its start. Appends one JSON line, tagged `"source": "measure_cluster"`, to `snapshots.jsonl` (the file `compare.py` reads). |
| `snap.py <label> <start> <end>` | Lighter snapshot at 5-minute resolution, including EBS write ops/bytes, connection counts, and lag for named consumer groups and topics (`--cluster`, `--num-brokers`, `--topic-prefix`, `--num-topics`, `--groups`). Appends to `snap-snapshots.jsonl` by default (`--out`), a separate file, because its records use different keys and are not comparable with `measure_cluster.py` records. |
| `measure.sh <label> <start> [end]` | Console readout: per-broker table of idle % and CPU, peak IAM connections, a `snap.py` summary, and the most-lagging consumer groups. Uses `CLUSTER_NAME` and `NUM_BROKERS`. |
| `latency_report.py <label> <log_glob>...` | Aggregates the `E2E_LATENCY` lines that the consumers print into a markdown table of p50/p90/p99/p99.9/max per group type (broadcast `bench-bcast-*` vs. other groups) and stage. |

`measure_cluster.py` and `snap.py` call Amazon CloudWatch through the AWS SDK for Python (boto3). Install it
once, preferably in a virtual environment:

```bash
python3 -m venv .venv && . .venv/bin/activate
python3 -m pip install -r analysis/requirements.txt
```

All times are ISO-8601 UTC, for example `2026-01-01T10:00:00Z`. Region and profile follow the AWS SDK's
normal resolution (`AWS_REGION`, `AWS_PROFILE`), or pass `--region` / `--profile` to the Python tools. Both
tools validate their arguments (cluster name, times, Region, profile, topic and group names) before calling
AWS. `measure.sh` uses the AWS CLI. `compare.py` and `latency_report.py` use only the Python standard library.

## Method: measure the same way every time

A client-tuning A/B is only meaningful if both profiles are measured identically.

1. **Use a steady-state window.** Start the load, wait for warm-up to finish (connections
   established, consumer groups stable, no rebalances in progress, lag flat), then measure a
   fixed-length window, for example 30 minutes. Use the same window length for every run and
   exclude ramp-up, rebalances, and shutdown.
2. **Report the mean of per-broker averages AND the worst broker.** Take each broker's average
   over the window, then report the mean across brokers and the worst single broker (lowest
   `RequestHandlerAvgIdlePercent` / `NetworkProcessorAvgIdlePercent`, highest CPU and latency).
   The mean shows overall headroom; the worst broker shows whether partition or leader skew,
   not the client configuration, is what saturates first.
3. **Compare at equal `MessagesInPerSec`.** Broker cost is driven largely by request count,
   which depends on how many records each request carries. Compare profiles only when the
   cluster-wide `MessagesInPerSec` (and `BytesInPerSec`) are the same within a few percent;
   otherwise you are comparing different workloads, not different client settings.

Example A/B on the same cluster, one window per profile:

```bash
export AWS_REGION=us-east-1 NUM_BROKERS=3
./measure_cluster.py load-benchmark baseline  2026-01-01T10:00:00Z 2026-01-01T10:30:00Z
./measure_cluster.py load-benchmark aws-tuned 2026-01-01T12:00:00Z 2026-01-01T12:30:00Z
```

Then compare the two windows:

```bash
./compare.py baseline aws-tuned            # reads snapshots.jsonl next to this script
./compare.py baseline aws-tuned --format markdown > ../results/ab.md   # shareable table, no ARNs or account IDs
```

`compare.py` prints both windows side by side (mean across brokers and the worst broker; idle metrics as a
change in percentage points, other metrics as a percentage) and runs a quality gate. It reports **OK** (exit 0)
only if `MessagesInPerSec` matches within 5% (`--max-rate-diff-pct`), `UnderReplicatedPartitions` was 0 in both
windows, and `RequestHandlerAvgIdlePercent` is present. Otherwise it reports **SUSPECT** (exit 1) with the
reasons. It only considers `measure_cluster.py` records (`"source": "measure_cluster"`, or, in files written
before that tag existed, records that have an `rh_mean` key), and exits 2 if the file is missing or unreadable,
a line is not valid JSON, or a label has no matching record.

## Start here if you already run Amazon MSK

Run `diagnose.py` against your performance-test cluster while it carries its usual load, then `check_profile.py` on
your client config, then the matching runbook in [../docs/test-cases/](../docs/test-cases/README.md). Exit codes
for the analysis tools are consistent: 0 = nothing needs attention, 1 = findings to look at (or SUSPECT for
`compare.py`), 2 = usage or input error.
