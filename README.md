# MSK Cluster Load Benchmark

A repeatable harness for putting a **sustained, high-throughput workload** on an Amazon MSK
cluster so you can observe how a given broker instance type behaves under load. It stands up:

- A **Standard MSK cluster** (`kafka.m7g.4xlarge` × 3 brokers by default) in a private VPC.
- A **producer client host** that drives a target ingress (default **~180 MB/s**).
- A **horizontally-scalable consumer fleet** — spin up as many consumer hosts as you want,
  each running its own consumer group so you can multiply egress / fan-out at will.
- A **CloudWatch dashboard** covering the broker health signals that matter under load
  (disk / EBS pressure, EBS throughput & IOPS, request-handler and network-processor idle,
  CPU, memory, produce latency, throughput, and consumer lag).

Everything is driven from your laptop over **SSM** (no SSH, no bastion inbound rules). Client
hosts live in private subnets, reach MSK over IAM-authenticated TLS, and are managed entirely
through `aws ssm send-command`.

The goal is **observation, not a pass/fail test**: reproduce a cluster with a realistic
workload attached and watch how the instance type holds up. It is deliberately easy to change
the throughput target, the Kafka client configuration, and the number of consumers so you can
sweep the workload while watching the dashboard.

> This project was extracted from and reuses the artifacts of the sibling
> `express-inplace-upgrade-testing` project (bastion pattern, SSM orchestration, Kafka
> perf-test invocation). It is a clean, general-purpose benchmarking harness rather than an
> upgrade-specific one.

---

## Why the client fleet is split (the 180 MB/s sizing decision)

The user's key question: *can one instance produce 180 MB/s AND consume it simultaneously, or
do we need multiple?*

**Answer: split the roles across hosts, and make the consumers a fleet.** Here is the reasoning.

| Traffic leg | Rate at 180 MB/s | Per consumer group |
|---|---|---|
| Producer → cluster (egress from producer host) | ~180 MB/s ≈ **1.44 Gbps** | — |
| Cluster → consumer (egress from cluster, ingress to consumer host) | ~180 MB/s ≈ **1.44 Gbps** | **× N groups** |

Two things make a single shared host a poor choice:

1. **Network.** 180 MB/s produce + 180 MB/s consume on one host is ~360 MB/s ≈ **2.9 Gbps of
   sustained client traffic**, before TLS framing overhead. A `c5.2xlarge` has a ~2.5 Gbps
   *baseline* (bursts to 10 Gbps but throttles back to baseline under sustained load). The
   sibling project measured a `c5.2xlarge` becoming the bottleneck at ~70 MiB/s produce + ~70
   MiB/s consume — well under this target. A shared host would make the *client* the
   bottleneck and the broker measurements meaningless.
2. **Fan-out.** "Connect it with various consumers … spin off as many as we want" means
   consumer egress is `N × 180 MB/s`. That cannot live on one host at all.

**Chosen topology (defaults):**

- **Producer host:** 1 × `c5n.2xlarge` — network-optimized, ~10 Gbps guaranteed at this size
  (not burst-limited), 8 vCPU for TLS + IAM signing. Comfortably drives 180 MB/s from a single
  host. (One `c5n.4xlarge` is available as an override if you push a single producer well past
  180 MB/s.)
- **Consumer fleet:** `N` × `c5n.2xlarge`, **default N = 1** (one consumer group reading the
  full 180 MB/s). Scale `consumerCount` up to add more independent consumer groups and multiply
  egress. Each host runs one consumer group across all topics.

`c5n` is chosen over `c5` specifically because its network performance is **guaranteed at the
instance-size level** rather than credit/burst-based, which is what you want for a steady
multi-hour soak.

### Rules of thumb for other throughput targets

| Target produce | Producer host | Consumer host per group |
|---|---|---|
| ≤ 125 MB/s (~1 Gbps) | `c5n.xlarge` | `c5n.xlarge` |
| ~180 MB/s (~1.44 Gbps) — **default** | `c5n.2xlarge` | `c5n.2xlarge` |
| ~375 MB/s (~3 Gbps) | `c5n.4xlarge` | `c5n.4xlarge` |
| ≥ 600 MB/s (~5 Gbps) | `c5n.9xlarge` or 2× `c5n.4xlarge` | scale the fleet horizontally |

Override the instance types and counts in `infra/bin/infra.ts` (or via CDK context) — see
[infra/README.md](infra/README.md).

---

## Layout

```
msk-cluster-load-benchmark/
├── README.md                        <- you are here
├── create-msk-standard-cluster.sh   <- create the Standard m7g.4xlarge x3 cluster (run locally)
├── infra/                           <- CDK: VPC + producer host + consumer fleet
│   ├── bin/infra.ts
│   └── lib/{network-stack,client-host-stack}.ts
├── orchestration/                   <- laptop-side drivers (SSM RunShellScript, S3 staging)
│   ├── run-on-host.sh
│   ├── pull-results.sh
│   └── tail-command.sh
├── scripts/                         <- host-side scripts, staged to each client host via S3
│   ├── lib-msk-env.sh
│   ├── 00-setup-topics.sh
│   ├── 01-start-producers.sh        <- runs on the producer host
│   ├── 02-start-consumers.sh        <- runs on each consumer host (scalable)
│   ├── 03-stop-load.sh
│   ├── 04-load-status.sh
│   ├── client-tuning.properties     <- DEFAULT Kafka client config; edit to tweak
│   └── 15-put-dashboard.sh          <- CloudWatch dashboard for the requested metrics
├── results/                         <- perf-test logs pulled back from the hosts
└── .kiro/
    ├── steering/                    <- always-on project context for the agent
    └── skills/                      <- step-by-step handoff runbook (agent-guided)
```

---

## Quick start

The fastest path is to open this folder in Kiro and say **"run the load benchmark setup"** —
the `msk-load-benchmark` skill walks you through every step. If you'd rather do it by hand:

```bash
# 0. Prereqs: AWS creds for the target account (aws sts get-caller-identity), Node 18+, Python 3.
export AWS_PROFILE=main
export AWS_REGION=us-east-1

# 1. Deploy the VPC + client hosts (1 producer + N consumers).
cd infra
npm install
AWS_PROFILE=main npx cdk deploy --all --require-approval never
cd ..

# 2. Create the Standard cluster (kafka.m7g.4xlarge x3) and its topics. ~20-30 min.
./create-msk-standard-cluster.sh

# 3. Publish the CloudWatch dashboard.
./scripts/15-put-dashboard.sh load-benchmark us-east-1 3

# 4. Start ~180 MB/s of producers on the producer host.
CLUSTER_ARN=<arn from step 2>
./orchestration/run-on-host.sh LoadBenchmarkProducerStack us-east-1 main \
  01-start-producers.sh --background "$CLUSTER_ARN" load-bench 12 180

# 5. Start a consumer group on each consumer host (repeat per host / scale the fleet).
./orchestration/run-on-host.sh LoadBenchmarkConsumerStack-0 us-east-1 main \
  02-start-consumers.sh --background "$CLUSTER_ARN" load-bench 12 group-0

# 6. Watch the dashboard, then stop when done.
./orchestration/run-on-host.sh LoadBenchmarkProducerStack us-east-1 main 03-stop-load.sh
./orchestration/run-on-host.sh LoadBenchmarkConsumerStack-0 us-east-1 main 03-stop-load.sh
```

See [scripts/README.md](scripts/README.md) for every host-side script and its arguments, and
"Tweaking the Kafka configuration" below.

---

## Tweaking the Kafka configuration

All producer/consumer Kafka settings start at **Kafka defaults**. Two override points:

1. **`scripts/client-tuning.properties`** — a properties file synced to every host and passed
   to the perf tools via `--producer.config` / `--consumer.config`. It ships with only the IAM
   auth settings (the mandatory bits) and commented-out examples for the common tunables
   (`linger.ms`, `batch.size`, `compression.type`, `fetch.min.bytes`, …). Uncomment/add lines,
   then re-run the start script — the next `run-on-host.sh` invocation re-syncs it.
2. **Per-run flags** — `01-start-producers.sh` and `02-start-consumers.sh` take optional
   trailing args for the hot knobs (record size, linger, batch size, fetch bytes) without
   editing the properties file. Anything not passed falls back to the client default.

Because the config is re-synced on every `run-on-host.sh` call, the tweak loop is: edit the
file (or change a flag) → stop load → start load → observe. No redeploy needed.

---

## Cleanup

The client hosts and VPC are CDK-managed; the MSK cluster is **not** (it's created out-of-band
by the shell script, same pattern as the sibling project). Tear down in this order:

```bash
# 1. Stop load on every host (producer + all consumers).
# 2. Delete the MSK cluster + its configuration (billable; ~3 x m7g.4xlarge + EBS).
aws kafka delete-cluster --cluster-arn "$CLUSTER_ARN" --profile main --region us-east-1
# (configuration can only be deleted once the cluster is fully gone)
# 3. Destroy the CDK stacks.
cd infra && AWS_PROFILE=main npx cdk destroy --all
```
