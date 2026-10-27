# Amazon MSK client-tuning benchmark

This repository accompanies the AWS blog post "Right-sizing Amazon MSK by tuning your Kafka clients"
(<!-- TODO: blog URL -->). At high partition and client density, Amazon Managed Streaming for Apache Kafka
(Amazon MSK) brokers usually saturate because of the **number of requests** they must serve, not the bytes per
second they move. The broker metric that shows this first is `RequestHandlerAvgIdlePercent`. This repository
lets you reproduce that effect on a small test cluster: you run your **production Kafka client configuration**
(the `baseline` profile) and an **AWS-tuned profile** (`aws-tuned`) against the same workload shape, from
Amazon EC2 or Amazon Elastic Kubernetes Service (Amazon EKS), and compare the broker metrics in
Amazon CloudWatch.

> **Disclaimer:** This is sample code, for non-production usage. Work with your security and legal teams to
> meet your organizational requirements before deployment.

> **Broker type:** the harness creates an Amazon MSK provisioned cluster with **Standard brokers**, and the
> blog's measurements were taken on Standard brokers. Some signals used here (for example `VolumeQueueLength`
> for Amazon EBS pressure) apply only to Standard brokers. Results on Express brokers may differ.

> **WARNING: test clusters only.** The scripts and manifests in this repository create and delete topics and
> consumer groups, and drive sustained load. Never point them at a production cluster.

## Start here: pick the question you have

You don't need to run the tests in order. Start from the question you have today.

| Your question | Use | Effort |
|---|---|---|
| Is my existing cluster request-bound right now? | [`analysis/diagnose.py`](docs/diagnose.md): read-only health check of a cluster you already run (non-prod or performance) | ~5 minutes, no load, nothing deployed |
| How far is my client config from the tuned profile? | [`analysis/check_profile.py`](docs/check-profile.md): offline diff of your `.properties` file against `aws-tuned`, with a reason for every setting | ~1 minute, no AWS access |
| Would the tuned profile help my workload? | [Test case 1](docs/test-cases/tc1-baseline-vs-tuned-low-density.md), then [test case 5](docs/test-cases/tc5-production-shaped-head-to-head.md) with your production client settings in `baseline` | One test cluster, a few hours |
| Snappy, LZ4, or zstd for us? | Codec A/B: `orchestration/run-ab.sh "$CLUSTER_ARN" aws-tuned aws-tuned-snappy` (profiles differ only in `compression.type`) | One A/B run |
| Do slow consumers, Kafka Streams exactly-once, or worker threads change broker load? | Test cases [2](docs/test-cases/tc2-slow-micro-batch-consumers.md), [4](docs/test-cases/tc4-clients-on-eks.md), [6](docs/test-cases/tc6-peak-load-many-worker-threads.md) | Per question |
| Will we hit IAM connection limits? | [Test case 7](docs/test-cases/tc7-one-consumer-per-partition-iam-connections.md) | Amazon EKS harness |
| How do we protect the cluster from one misbehaving client? | [Test case 8](docs/test-cases/tc8-server-side-quotas.md): server-side quotas | Add-on to any run |

All eight test cases, their prerequisites, and the shared setup are in [docs/test-cases/](docs/test-cases/README.md).
To compare two profiles end to end on the EC2 harness with one command, use `orchestration/run-ab.sh` (`--dry-run`
prints every step first); it writes a Markdown report you can share.

## Contents

- [Start here: pick the question you have](#start-here-pick-the-question-you-have)
- [What drives broker saturation](#what-drives-broker-saturation)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick start: Amazon EC2 harness](#quick-start-amazon-ec2-harness)
- [Quick start: Amazon EKS harness](#quick-start-amazon-eks-harness)
- [Client profiles](#client-profiles)
- [Bring your own client config](#bring-your-own-client-config)
- [Measurement method](#measurement-method)
- [Benchmark integrity rules](#benchmark-integrity-rules)
- [Blog test cases and how to run them](#blog-test-cases-and-how-to-run-them)
- [Cost](#cost)
- [Cleanup](#cleanup)
- [Version notes](#version-notes)
- [Security](#security)

## What drives broker saturation

Each produce, fetch, heartbeat, commit, and metadata request occupies a broker request-handler thread, whatever
its size. When `RequestHandlerAvgIdlePercent` drops toward zero, latency rises and the cluster looks
undersized even though `BytesInPerSec` is well below the instance's limit. In rough order of impact, the
drivers that the blog post discusses are:

1. **Partition density, replication factor, and small messages.** More partitions and replicas with small,
   unbatched records mean more produce and replica-fetch requests per byte.
2. **Number of producer instances.** Each producer sends its own requests, so many small producers cost more
   than a few batched ones that carry the same total bytes.
3. **Number of consumer groups.** Every group fetches, heartbeats, and commits on its own.
4. **Broadcast groups.** Patterns where each application instance has its own group on a shared topic multiply
   the group count.
5. **Produce message conversions.** Clients that use an older message format force the broker to convert
   records.
6. **Kafka Streams processing guarantee and worker threads.** Exactly-once (EOS) adds transactional requests
   compared with at-least-once (ALOS). The number of stream or worker threads also changes fetch and commit
   rates.

Client settings such as `linger.ms`, `batch.size`, `compression.type`, `fetch.min.bytes`, and
`fetch.max.wait.ms` change how many requests carry the same data, which is why client tuning can let a
workload run on fewer or smaller brokers.

## Architecture

The repository contains two load harnesses that share the same Java clients and client profiles, plus a
Kafka Streams emulator.

You use one harness at a time: the Amazon EC2 harness or the Amazon EKS harness. Both drive the same Amazon MSK
test cluster in a VPC created by the AWS CDK app, and both read the results from Amazon CloudWatch.

### Amazon EC2 harness

![Amazon EC2 harness: the operator workstation syncs scripts to Amazon S3 and runs them on the producer and consumer hosts through AWS Systems Manager; the hosts reach three Amazon MSK Standard brokers across three Availability Zones over IAM and TLS on port 9098; broker metrics flow to Amazon CloudWatch](docs/architecture-ec2.png)

1. `orchestration/run-on-host.sh` syncs `scripts/` (including every profile in `scripts/profiles/`) to the Amazon S3
   staging bucket of each host stack; the hosts pull the scripts and push run results back.
2. It then runs the script on the host through AWS Systems Manager Run Command (no SSH, no inbound rules).
3. The producer and consumer hosts reach the brokers only over SASL/IAM on TLS, port 9098.
4. Amazon MSK publishes `PER_BROKER` metrics to Amazon CloudWatch; `scripts/15-put-dashboard.sh` builds the dashboard.
5. `analysis/diagnose.py` and `analysis/measure_cluster.py` read Amazon CloudWatch; `analysis/compare.py` compares two
   recorded windows offline.

### Amazon EKS harness

![Amazon EKS harness: the operator workstation applies manifests with kubectl to an Amazon EKS cluster whose managed node group spans three Availability Zones; pods pull images from Amazon ECR and reach three Amazon MSK Standard brokers over IAM and TLS on port 9098; broker metrics flow to Amazon CloudWatch](docs/architecture-eks.png)

1. `kubectl apply -f k8s/base` creates the namespace, ServiceAccount, and `client-tuning-<profile>` ConfigMaps;
   `k8s/gen-workload.py` and `k8s/kstreams/gen-kstreams.sh` render the Deployments.
2. Pods pull the client and Kafka Streams emulator images from Amazon ECR, and authenticate with Amazon EKS Pod Identity.
3. Producer, consumer, and Kafka Streams pods reach the brokers only over SASL/IAM on TLS, port 9098.
4. Amazon MSK publishes `PER_BROKER` metrics to Amazon CloudWatch; `scripts/15-put-dashboard.sh` builds the dashboard.
5. `analysis/diagnose.py` and `analysis/measure_cluster.py` read Amazon CloudWatch; `analysis/compare.py` compares two
   recorded windows offline.

The EKS harness reuses the VPC from the AWS CDK app, and the topics are created once when you create the
Amazon MSK cluster (EC2 quick start steps 1-2). The editable sources are
[docs/architecture-ec2.drawio](docs/architecture-ec2.drawio) and [docs/architecture-eks.drawio](docs/architecture-eks.drawio)
(open them with [draw.io](https://www.drawio.com/)).

```
.
├── create-msk-standard-cluster.sh   Creates the Amazon MSK Standard test cluster (IAM auth, TLS, PER_BROKER monitoring)
├── infra/                           AWS CDK app: VPC, producer host, consumer host fleet, S3 staging bucket
├── orchestration/                   Laptop-side drivers: run a script on a host through AWS Systems Manager
├── scripts/                         Host-side scripts and Java load tools (EC2 harness)
│   ├── profiles/                    Client profiles: baseline.properties, aws-tuned.properties, your own
│   └── client-tuning.properties     ACTIVE profile for the EC2 harness (ships as a copy of aws-tuned)
├── eks/                             eksctl cluster definition and Amazon ECR notes (EKS harness)
├── k8s/                             Container image, base manifests, IAM policies, workload generator
│   ├── base/                        Namespace, ServiceAccount, bootstrap ConfigMap, client-tuning-<profile> ConfigMaps
│   ├── examples/                    Example workload specs for k8s/gen-workload.py
│   └── kstreams/                    Generator for Kafka Streams emulator Deployments
├── kstreams-emulator/               Stateful Kafka Streams app (repartition + RocksDB changelog), ALOS or EOS
├── analysis/                        Amazon CloudWatch metric collection and latency report helpers
└── results/                         Local run output (git-ignored)
```

**Amazon EC2 harness (`infra/`, `scripts/`, `orchestration/`).** The AWS CDK app deploys a VPC, one producer
host, and N consumer hosts in private subnets. Hosts have no inbound rules and no SSH keys. From your laptop,
`orchestration/run-on-host.sh` syncs `scripts/` to an Amazon S3 staging bucket and runs the requested script on
a host through AWS Systems Manager Run Command. The hosts connect to Amazon MSK with AWS Identity and Access
Management (IAM) authentication over TLS on port 9098.

**Amazon EKS harness (`eks/`, `k8s/`).** An eksctl-managed cluster runs the same Java producers and consumers as
pods. `k8s/gen-workload.py` turns a workload spec (topics, producer count, consumer groups, broadcast groups)
into Deployments. Pods authenticate with EKS Pod Identity (or IRSA), and the client profile is mounted from the
`client-tuning-<profile>` ConfigMap.

**Kafka Streams emulator (`kstreams-emulator/`).** A small stateful Kafka Streams application that you deploy
with `k8s/kstreams/gen-kstreams.sh` to measure what ALOS and EOS cost the brokers.

## Prerequisites

- An AWS account dedicated to testing, and credentials that can create VPC, Amazon EC2, Amazon MSK,
  Amazon S3, IAM, and (for the EKS path) Amazon EKS and Amazon ECR resources.
- AWS CLI v2, Python 3.9 or later, and Bash. The analysis tools also need boto3
  (`python3 -m pip install -r analysis/requirements.txt`).
- Node.js 20 or later and npm (the AWS CDK CLI is installed from `infra/package.json`). Both harnesses use the
  AWS CDK app for the VPC; the EC2 harness also uses it for the client hosts.
- EKS path: `eksctl` (a version that supports the Amazon EKS version in `eks/cluster.yaml`), `kubectl`, and
  Docker with `buildx`.
- Kafka Streams emulator: JDK 17 (the Maven Wrapper downloads Maven).
- Service quotas that allow the broker and client instance types you choose.

## Quick start: Amazon EC2 harness

Architecture: [Amazon EC2 harness](#amazon-ec2-harness).

The commands below use `us-east-1` and the `default` AWS CLI profile. Replace them with your own values.

```bash
export AWS_REGION=us-east-1 AWS_PROFILE=default

# 1. Deploy the VPC, producer host and consumer hosts (AWS CDK). Bootstrap once per account and Region.
cd infra && npm ci && npx cdk bootstrap && npx cdk deploy --all && cd ..

# 2. Create the Amazon MSK Standard test cluster and its topics (20-30 minutes).
./create-msk-standard-cluster.sh            # see the script header for arguments
CLUSTER_ARN=<cluster ARN printed by the script>

# 3. Publish the Amazon CloudWatch dashboard.
./scripts/15-put-dashboard.sh load-benchmark "$AWS_REGION" 3

# 4. Choose the client profile for this run (see "Bring your own client config"). This overwrites the tracked
#    active file, so `git status` shows it as modified; that's expected.
cp scripts/profiles/baseline.properties scripts/client-tuning.properties

# 5. Start producers on the producer host and a consumer group on each consumer host.
./orchestration/run-on-host.sh LoadBenchmarkProducerStack "$AWS_REGION" "$AWS_PROFILE" \
  01-start-producers.sh --background "$CLUSTER_ARN" load-bench 12 100
./orchestration/run-on-host.sh LoadBenchmarkConsumerStack-0 "$AWS_REGION" "$AWS_PROFILE" \
  02-start-consumers.sh --background "$CLUSTER_ARN" load-bench 12 bench-cg-0

# 6. Let the load reach steady state, record the window, then stop load.
./orchestration/run-on-host.sh LoadBenchmarkProducerStack "$AWS_REGION" "$AWS_PROFILE" 03-stop-load.sh
./orchestration/run-on-host.sh LoadBenchmarkConsumerStack-0 "$AWS_REGION" "$AWS_PROFILE" 03-stop-load.sh

# 7. Repeat steps 4-6 with scripts/profiles/aws-tuned.properties and compare.
```

Every `run-on-host.sh` call re-syncs `scripts/`, so switching profiles needs no redeploy. See
[scripts/README.md](scripts/README.md) for every script and its arguments, and [infra/README.md](infra/README.md)
for AWS CDK context options (instance types, consumer host count, VPC CIDR).

## Quick start: Amazon EKS harness

Architecture: [Amazon EKS harness](#amazon-eks-harness).

The EKS harness reuses the VPC and the Amazon MSK test cluster from the EC2 harness, and it runs the same Java
clients as pods. `eks/cluster.yaml` creates the EKS Pod Identity role, its association, and the
`msk-bench/msk-bench-client` ServiceAccount, so you don't create an IAM role yourself on this path.

```bash
# 0. Run steps 1 and 2 of the EC2 quick start first. Step 1 creates the VPC (LoadBenchmarkNetworkStack); step 2
#    creates the MSK cluster and its topics (the topics are created from the producer host). The pods don't
#    create topics.

# 1. Fill in eks/cluster.yaml. Replace every REPLACE_ME value with the LoadBenchmarkNetworkStack outputs
#    (VpcId, PrivateSubnetIds, ClientSecurityGroupId), and replace the account ID 111122223333 and the Region
#    us-east-1 (region, availability zones, and the pod policy ARNs) with yours. Lower desiredCapacity if you
#    only run small workloads. Then create the cluster.
aws cloudformation describe-stacks --stack-name LoadBenchmarkNetworkStack --query 'Stacks[0].Outputs'
grep -nE '111122223333|REPLACE_ME|us-east-1' eks/cluster.yaml | grep -v '^ *[0-9]*: *#'
eksctl create cluster -f eks/cluster.yaml

# 2. Build the client image for the node architecture (c5n is x86_64) and push it to Amazon ECR
#    (create the repository first; see eks/ecr.md).
ECR=<account>.dkr.ecr.<region>.amazonaws.com; TAG=$(git rev-parse --short HEAD)
docker buildx build --platform linux/amd64 -f k8s/image/Dockerfile -t $ECR/msk-bench-client:$TAG --push .

# 3. Put the IAM bootstrap string of the MSK cluster into k8s/base/20-configmap-msk-bootstrap.yaml.
aws kafka get-bootstrap-brokers --cluster-arn "$CLUSTER_ARN" --query BootstrapBrokerStringSaslIam --output text

# 4. Apply the namespace, ServiceAccount and ConfigMaps.
kubectl apply -f k8s/base

# 5. Generate and apply a workload with the baseline profile, then with aws-tuned. Always pass your image.
IMG=$ECR/msk-bench-client:$TAG
python3 k8s/gen-workload.py k8s/examples/workload-low-density.yaml --image "$IMG" --summary-only
python3 k8s/gen-workload.py k8s/examples/workload-low-density.yaml --image "$IMG" --tuning baseline  > /tmp/baseline.yaml
python3 k8s/gen-workload.py k8s/examples/workload-low-density.yaml --image "$IMG" --tuning aws-tuned > /tmp/aws-tuned.yaml
kubectl apply -f /tmp/baseline.yaml
# ... measure the steady-state window ...
kubectl apply -f /tmp/aws-tuned.yaml     # rolls every Deployment onto the new profile
```

If you bring your own Amazon EKS cluster instead of `eks/cluster.yaml`, create the pod role from `k8s/iam/`
(EKS Pod Identity or IRSA) as described in [k8s/README.md](k8s/README.md).

See [eks/README.md](eks/README.md) and [k8s/README.md](k8s/README.md) for details, including fan-out modes,
scaling, and the container environment contract.

## Client profiles

Two main profiles ship with the repository, plus two codec variants of `aws-tuned` (`aws-tuned-snappy`,
`aws-tuned-lz4`) that differ only in `compression.type`, for a one-variable codec A/B.
`scripts/profiles/<profile>.properties` is the source of truth; the `k8s/base/3*-configmap-client-tuning-*.yaml`
ConfigMaps carry the same values for the EKS harness.

| Setting | `baseline` (Apache Kafka 3.9 client defaults; replace with your production values) | `aws-tuned` |
|---|---|---|
| `linger.ms` | 0 | 25 |
| `batch.size` | 16384 (16 KB) | 131072 (128 KB) |
| `buffer.memory` | 33554432 (32 MB) | 67108864 (64 MB) |
| `compression.type` | none | zstd |
| `fetch.min.bytes` | 1 | 128 |
| `fetch.max.wait.ms` | 500 | 1000 |
| `max.poll.records` | 500 | 1000 |
| `partition.assignment.strategy` | RangeAssignor, CooperativeStickyAssignor | CooperativeStickyAssignor |
| `auto.commit.interval.ms` | 5000 | 5000 |

Both profiles also set `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`,
`max.partition.fetch.bytes`, session and heartbeat timeouts, socket buffer sizes, and
`connections.max.idle.ms` explicitly, so every run records its full effective configuration. `baseline` is a
**template**: replace its values with your production client settings so that it reproduces production.

[docs/client-tuning.md](docs/client-tuning.md) explains what each `aws-tuned` setting changes on the brokers,
what it costs on the client, and how to migrate a live consumer group to `CooperativeStickyAssignor`.

## Bring your own client config

A profile is a Kafka client properties file. Any profile name `<p>` maps to:

| Harness | Where the profile lives | How you select it |
|---|---|---|
| Source of truth | `scripts/profiles/<p>.properties` | - |
| Amazon EC2 | `scripts/client-tuning.properties` (the active file) | `cp scripts/profiles/<p>.properties scripts/client-tuning.properties`, then restart load |
| Amazon EKS | ConfigMap `client-tuning-<p>` in namespace `msk-bench` | `python3 k8s/gen-workload.py <spec> --tuning <p>` |

To add your own profile:

1. Copy `scripts/profiles/baseline.properties` to `scripts/profiles/<p>.properties` and set your values.
   Do not set `security.protocol`, `sasl.mechanism`, `sasl.jaas.config`, or
   `sasl.client.callback.handler.class`; the harness adds the IAM authentication properties itself, and both
   harnesses refuse to start a client if the profile overrides them.
2. For the EKS harness, create the matching ConfigMap:

   ```bash
   kubectl -n msk-bench create configmap client-tuning-<p> \
     --from-file=client-tuning.properties=scripts/profiles/<p>.properties \
     --dry-run=client -o yaml | kubectl apply -f -
   ```

3. Run the same workload with `baseline`, `aws-tuned`, and `<p>`, and compare them as described below.

## Measurement method

1. **Use a small cluster of the same broker instance type as production.** Request saturation is per broker,
   so a three-broker cluster of the production instance type shows the same per-broker behavior.
2. **Shape the data like production.** Match partitions per broker, replication factor, record size, number of
   producer instances, consumer groups, and broadcast groups (`scripts/00-setup-topics.sh` and the workload
   spec control these).
3. **Run clients where production runs them.** Use the EC2 harness for VM-based clients and the EKS harness for
   containerized microservices, so connection counts and per-pod parallelism match.
4. **Use two client profiles:** `baseline` (a copy of production) and `aws-tuned`. Change one thing at a time.
5. **Turn on enhanced monitoring at `PER_BROKER` or higher.** `RequestHandlerAvgIdlePercent`,
   `NetworkProcessorAvgIdlePercent`, and the produce latency metrics are `PER_BROKER` metrics.
   `create-msk-standard-cluster.sh` enables it.
6. **Measure steady-state windows only.** Skip ramp-up and rebalances; use windows of at least 15-30 minutes.
7. **Report the mean and the worst broker.** One hot broker saturates first. `analysis/measure_cluster.py`
   prints per-broker means of 1-minute averages and a cluster summary for a window.
8. **Compare at equal `MessagesInPerSec`.** A profile only wins if it serves the same message rate with more
   request-handler headroom.
9. **Log every run:** profile, workload spec, cluster size, window start and end, and the results.
   `analysis/measure_cluster.py` appends one JSON line per window to `analysis/snapshots.jsonl`.
10. **Compare two windows with the quality gate.** `analysis/compare.py` prints baseline versus candidate
    (mean and worst broker; idle changes in percentage points) and marks the comparison **SUSPECT** if the
    message rates differ by more than 5% or any partition was under-replicated. Fix a SUSPECT run; don't
    explain it.

```bash
python3 analysis/measure_cluster.py load-benchmark baseline  2026-01-01T10:00:00Z 2026-01-01T10:30:00Z
python3 analysis/measure_cluster.py load-benchmark aws-tuned 2026-01-01T12:00:00Z 2026-01-01T12:30:00Z
python3 analysis/compare.py baseline aws-tuned        # exit 0 = OK, exit 1 = SUSPECT
```

End-to-end latency is optional and EKS only: set `consumers.latency_report_sec` (for example `30`) in the workload
spec, collect the consumer logs for the window, and summarize them:

```bash
kubectl -n msk-bench logs -l role=consumer --prefix --tail=-1 --since=30m > /tmp/run-aws-tuned.log
python3 analysis/latency_report.py "aws-tuned" '/tmp/run-aws-tuned*.log'
```

## Benchmark integrity rules

A run you can't trust is worse than no run. The harness and the analysis tools enforce some of these rules;
the rest are on you.

- **Compare at equal message rate.** Compression changes on-wire bytes, so compare runs at equal
  `MessagesInPerSec`, never at equal `BytesInPerSec`. `compare.py` rejects pairs more than 5% apart.
- **Change one thing at a time.** Keep the workload spec, cluster, broker configuration, and client placement
  identical between the two profiles.
- **Warm up, then measure a steady window.** Skip the first 10-15 minutes and any rebalance, and use windows of
  similar length (15-30 minutes) for every run.
- **Discard runs with client-side trouble.** Pod restarts (`kubectl get pods` `RESTARTS`), repeated rebalances,
  producer send errors, or a CPU-saturated client host make the brokers look worse than production. Check
  `scripts/04-load-status.sh` and `scripts/05-collect-diagnostics.sh` on EC2, or pod restarts and logs on EKS.
- **Don't use `kubectl scale` on consumer Deployments.** A second replica joins the same group and splits its
  partitions instead of adding a group. Change the workload spec and regenerate (see `k8s/README.md`).
- **Start from a clean cluster.** Delete consumer groups and stale offsets from earlier runs, because they
  inflate `SumOffsetLag` long after their clients are gone.
- **Keep `PER_BROKER` monitoring on**, and confirm the metrics you rely on (including throttle metrics) are
  being emitted before you start the window.
- **Keep topics on `message.timestamp.type=CreateTime`** (the Kafka default) if you measure end-to-end latency;
  `LogAppendTime` hides the producer-side batching delay.
- **Use ASCII consumer group names without colons**, or Amazon MSK doesn't emit consumer-lag metrics for them
  ([Monitor consumer lags](https://docs.aws.amazon.com/msk/latest/developerguide/consumer-lag.html)).
- **Report the worst broker as well as the mean**, and log the window, profile, workload spec, and cluster
  size for every run.

## Blog test cases and how to run them

Each test case has a step-by-step runbook (linked below) with prerequisites, exact commands, what to expect,
what to watch out for, and teardown. The summary column shows the main tools it uses.

| # | Test case (blog) | What to run |
|---|---|---|
| 1 | [Baseline vs tuned at low partition density](docs/test-cases/tc1-baseline-vs-tuned-low-density.md) | EC2: `scripts/00-setup-topics.sh` with few partitions, then `scripts/01-start-producers.sh` and `scripts/02-start-consumers.sh`, once per profile. EKS: `k8s/examples/workload-low-density.yaml`. |
| 2 | [Slow micro-batch consumers](docs/test-cases/tc2-slow-micro-batch-consumers.md) | EC2: `scripts/06-start-slow-consumers.sh` (each consumer sleeps between polls), added to a host that runs the 01/02 load. |
| 3 | [Build up to production density](docs/test-cases/tc3-build-up-to-production-density.md) | EC2: raise partitions and RF with `scripts/00-setup-topics.sh`, then add many producer instances with `scripts/08-start-multi-producers.sh` and many consumer and broadcast groups with `scripts/07-start-multigroup-consumers.sh`. |
| 4 | [Clients on Amazon EKS: groups, Streams, producers, worker threads](docs/test-cases/tc4-clients-on-eks.md) | EKS: `k8s/gen-workload.py` with a spec from `k8s/examples/` (vary `consumers.total_groups`, the producer sets, and `consumers.worker_threads`); Kafka Streams ALOS vs EOS with `k8s/kstreams/gen-kstreams.sh <alos\|eos> <num-apps>` and `kstreams-emulator/`. |
| 5 | [Production-shaped head-to-head](docs/test-cases/tc5-production-shaped-head-to-head.md) | EKS: `k8s/examples/workload-high-density.yaml` (or your own production-shaped spec), generated with `--tuning baseline`, then with `--tuning aws-tuned`, compared at equal `MessagesInPerSec`. |
| 6 | [Peak load with many worker threads](docs/test-cases/tc6-peak-load-many-worker-threads.md) | EKS: the test case 5 spec at peak producer rate with `consumers.worker_threads` raised (`WORKER_THREADS`, which maps to `MultiGroupConsumer --worker-threads`). |
| 7 | [One consumer per partition and IAM connection limits](docs/test-cases/tc7-one-consumer-per-partition-iam-connections.md) | EKS: `k8s/examples/workload-one-consumer-per-partition.yaml` (`members_per_group` x `pods_per_group` = partitions); watch `ConnectionCount` and `ConnectionCreationRate` on the dashboard against the Amazon MSK IAM connection quotas. |
| 8 | [Server-side quotas as a guardrail](docs/test-cases/tc8-server-side-quotas.md) | EC2: `scripts/09-set-client-quotas.sh <cluster-arn> set --request-percentage <n> ...` (dry run by default; add `--apply`), then rerun test case 5. Setting quotas needs `kafka-cluster:AlterClusterDynamicConfiguration` and `kafka-cluster:DescribeClusterDynamicConfiguration`, which the shipped client role does not grant; add them only in your test account. |

Supporting tools: `scripts/04-load-status.sh` and `scripts/05-collect-diagnostics.sh` (client-side
bottlenecks), `scripts/15-put-dashboard.sh` (broker dashboard), and `analysis/` (window metrics and latency).

## Cost

Amazon MSK brokers and storage, Amazon EC2 client hosts, the Amazon EKS control plane and nodes, NAT gateway
data processing, cross-AZ data transfer, Amazon ECR storage, and Amazon CloudWatch dashboards and enhanced
monitoring are **billable**. Benchmark-sized clusters can cost tens of US dollars per hour or more. Run tests in
bounded windows and tear everything down when you finish. Use the
[AWS Pricing Calculator](https://calculator.aws/) to estimate your configuration.

## Cleanup

Tear down in this order so that nothing is orphaned:

```bash
# 1. Stop load on every EC2 host, and delete the EKS workloads.
./orchestration/run-on-host.sh LoadBenchmarkProducerStack "$AWS_REGION" "$AWS_PROFILE" 03-stop-load.sh
./orchestration/run-on-host.sh LoadBenchmarkConsumerStack-0 "$AWS_REGION" "$AWS_PROFILE" 03-stop-load.sh
kubectl delete namespace msk-bench --wait

# 2. Delete the Amazon EKS cluster (if you created one).
eksctl delete cluster -f eks/cluster.yaml --wait

# 3. Delete the Amazon MSK cluster, wait until it is gone, then delete its configuration
#    (create-msk-standard-cluster.sh prints both ARNs).
aws kafka delete-cluster --cluster-arn "$CLUSTER_ARN"
while aws kafka describe-cluster-v2 --cluster-arn "$CLUSTER_ARN" >/dev/null 2>&1; do sleep 30; done
aws kafka delete-configuration --arn "$CONFIG_ARN"

# 4. Destroy the AWS CDK stacks (VPC, hosts, S3 staging bucket).
cd infra && npx cdk destroy --all

# 5. Delete the Amazon ECR repository and the pod IAM role if you created them (see eks/ecr.md and k8s/README.md).
```

## Version notes

Validated in October 2026 with:

- Amazon MSK provisioned cluster, Standard brokers, Apache Kafka 3.8.x (`create-msk-standard-cluster.sh`,
  `KAFKA_VERSION`), `kafka.m7g.8xlarge` x 3 by default.
- Apache Kafka 3.9.2 clients and aws-msk-iam-auth 2.2.0 (EC2 user data and `k8s/image/Dockerfile`).
- Amazon EKS 1.35 with eksctl (`eks/cluster.yaml`), and AWS CDK v2 (`infra/package.json`).
- Eclipse Temurin 17 base images from Amazon ECR Public.

Kafka clients are compatible with older and newer broker versions, so you can run the 3.9.2 clients against
other Amazon MSK versions; set `KAFKA_VERSION` to match production when you create the test cluster.

## Security

See [SECURITY.md](SECURITY.md) for the AWS services used, the permissions the sample needs, the accepted
security trade-offs, and production hardening recommendations. See
[CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for how to report a security issue.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
