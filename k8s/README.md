# Running the benchmark clients on Amazon EKS

This directory runs the benchmark's Java clients (`scripts/MultiProducer.java`,
`scripts/MultiGroupConsumer.java`) as pods on Amazon Elastic Kubernetes Service (Amazon EKS)
instead of processes on Amazon EC2 hosts. Each workload is described in a small spec file
and turned into Kubernetes Deployments by `gen-workload.py`. The model is a set of
microservices: **one consumer group reads one topic**, and every group is its own
Deployment. Producers are Deployments that each run several `KafkaProducer` clients.

You run the same workload twice against an Amazon Managed Streaming for Apache Kafka
(Amazon MSK) cluster, once with the `baseline` client profile (your production settings)
and once with `aws-tuned`, and compare the broker metrics in Amazon CloudWatch. The Kafka
Streams emulator is built separately in `kstreams-emulator/`.

```
k8s/
  image/            Dockerfile, entrypoint.sh, Dockerfile.dockerignore (container image)
  base/             namespace, ServiceAccount, ConfigMaps (bootstrap, client-tuning-baseline,
                    client-tuning-aws-tuned)
  iam/              AWS IAM permissions policy and trust policies (Pod Identity or IRSA)
  gen-workload.py   spec -> Deployments (Python 3 standard library only)
  examples/         example workload specs
    workload-low-density.yaml                 2 topics, 4 producer clients, 2 groups
    workload-high-density.yaml                12 topics + broadcast topic, 121 producer
                                              clients, 100 groups
    workload-one-consumer-per-partition.yaml  4 topics x 60 partitions, 60 members per group
  kstreams/         gen-kstreams.sh: Kafka Streams emulator Deployments
```

Every command below is for you to run after you fill in the placeholders
(`111122223333` = your AWS account ID, `us-east-1` = your Region, `REPLACE_ME_*`).

## 0. Prerequisites

* An Amazon EKS cluster whose nodes (or pods) can reach the Amazon MSK brokers on
  **TCP 9098** (SASL_SSL with IAM access control). That means the same VPC or a peered one,
  plus a rule on the MSK cluster's security group that allows the node security group.
  The supported path is `eks/cluster.yaml` (see `eks/README.md`): it uses the
  `LoadBenchmarkNetworkStack` VPC and client security group and sets up pod IAM for you.
* The topics must already exist. The pods do not create topics, and Amazon MSK's default is
  `auto.create.topics.enable=false`. Each example spec lists the `scripts/00-setup-topics.sh`
  commands that create its topics.
* Enough node capacity for the spec's requests (requests equal limits by default). For
  example, `workload-high-density.yaml` needs consumers 100 x 250m / 512Mi = **25 vCPU /
  50 GiB** and producers 12 x 1 CPU / 2Gi + 1 x 250m / 512Mi = **12.25 vCPU / 24.5 GiB**,
  plus DaemonSets. Also check node network bandwidth against the egress estimate that
  `gen-workload.py` prints (section 4).
* The `eks/cluster.yaml` node group is c5n (x86_64), so the image is built for
  `linux/amd64`. For AWS Graviton nodes, build `linux/arm64` instead (section 1).

## 1. Build and push the image (Amazon ECR)

The build context is the **repo root**, because the Java sources live in `scripts/`.
`k8s/image/Dockerfile.dockerignore` keeps the context small.

```bash
cd <repo-root>
AWS_ACCOUNT_ID=111122223333 REGION=us-east-1 REPO=msk-bench-client TAG=$(git rev-parse --short HEAD)
ECR=$AWS_ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

# Optional local build (fails fast on a Kafka or IAM auth jar checksum mismatch)
docker build -f k8s/image/Dockerfile -t $REPO:local .

# One-time: create the repository
aws ecr create-repository --repository-name $REPO --region $REGION \
  --image-scanning-configuration scanOnPush=true

aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ECR

# Build and push for the c5n (x86_64) nodes of eks/cluster.yaml.
# For Graviton nodes use --platform linux/arm64 (or linux/amd64,linux/arm64 for multi-arch).
docker buildx build --platform linux/amd64 -f k8s/image/Dockerfile -t $ECR/msk-bench-client:$TAG --push .
```

Put `$ECR/msk-bench-client:$TAG` in `image:` in the spec, or pass `--image` to the
generator. While the image still holds the example account `111122223333`, the
generator prints a `WARNING:` on stderr.

Both downloads are verified against checksums pinned in the Dockerfile, and the build
fails on a mismatch or an empty value: the Kafka tgz against `KAFKA_SHA512` and the
aws-msk-iam-auth jar against `MSK_IAM_AUTH_SHA256` (required by default). If you change
`KAFKA_VERSION` or `MSK_IAM_AUTH_VERSION`, update the matching hash too, for example
`--build-arg MSK_IAM_AUTH_VERSION=<v> --build-arg MSK_IAM_AUTH_SHA256=<sha256>`.

Image contents: `eclipse-temurin:17-jdk` (single-file source mode needs the compiler),
`/opt/kafka` from `kafka_2.13-3.9.2.tgz`, `/opt/kafka/libs/aws-msk-iam-auth-2.2.0-all.jar`,
`/opt/msk-bench/{MultiProducer,MultiGroupConsumer}.java` and `/opt/msk-bench/entrypoint.sh`.
It runs as uid/gid 10001 and works with a read-only root filesystem (it only writes to
`/tmp`, which is an emptyDir).

## 2. AWS IAM for the client pods

**Cluster from `eks/cluster.yaml`: nothing to do.** eksctl already created the EKS Pod
Identity role `msk-bench-eks-client-pod-role`, its association, and the ServiceAccount
`msk-bench/msk-bench-client`. `base/10-serviceaccount.yaml` ships without an
`eks.amazonaws.com/role-arn` annotation, so applying it does not change that setup.

**Bring-your-own EKS cluster only.** The permissions policy is
`iam/msk-bench-client-policy.json` (Amazon MSK data-plane `kafka-cluster:*` actions on one
cluster's `cluster/`, `topic/`, `group/` and `transactional-id/` resources).
`iam/README.md` lists the values to replace. aws-msk-iam-auth uses the AWS SDK default
credential chain, so either mechanism below works. Use only one.

*Amazon EKS Pod Identity (preferred)*. Needs the `eks-pod-identity-agent` add-on.

```bash
ROLE_NAME=msk-bench-client
aws iam create-role --role-name $ROLE_NAME \
  --assume-role-policy-document file://k8s/iam/trust-policy-pod-identity.json
aws iam put-role-policy --role-name $ROLE_NAME --policy-name msk-bench-data-plane \
  --policy-document file://k8s/iam/msk-bench-client-policy.json
aws eks create-pod-identity-association --cluster-name <eks-cluster> --region us-east-1 \
  --namespace msk-bench --service-account msk-bench-client \
  --role-arn arn:aws:iam::111122223333:role/$ROLE_NAME
```
The ServiceAccount needs no annotation for Pod Identity.

*IAM roles for service accounts (IRSA)*. Replace `REPLACE_ME_OIDC_ID` in
`iam/trust-policy-irsa.json` with the ID from
`aws eks describe-cluster --name <eks> --query cluster.identity.oidc.issuer`, create the
role and policy as above with that trust policy, and add the annotation
`eks.amazonaws.com/role-arn: arn:aws:iam::111122223333:role/<role-name>` to
`base/10-serviceaccount.yaml` yourself.

## 3. Fill the bootstrap placeholder

```bash
BOOT=$(aws kafka get-bootstrap-brokers --cluster-arn <cluster-arn> --region us-east-1 \
        --query BootstrapBrokerStringSaslIam --output text)
```
Put `$BOOT` in `base/20-configmap-msk-bootstrap.yaml` (key `BOOTSTRAP_SERVERS`), or
override it in the cluster after applying `base/`:
```bash
kubectl -n msk-bench create configmap msk-bootstrap --from-literal=BOOTSTRAP_SERVERS="$BOOT" \
  --dry-run=client -o yaml | kubectl apply -f -
```
Pods exit with `ERROR: [entrypoint] BOOTSTRAP_SERVERS still holds the placeholder` until
the placeholder is replaced.

## 4. Generate and apply

```bash
SPEC=k8s/examples/workload-high-density.yaml
python3 k8s/gen-workload.py $SPEC --summary-only     # arithmetic only
python3 k8s/gen-workload.py $SPEC > /tmp/out.yaml    # manifests (tuning from the spec, default aws-tuned)

kubectl apply -f k8s/base            # namespace, ServiceAccount, ConfigMaps (00- sorts first)
kubectl apply -f /tmp/out.yaml       # Deployments

kubectl -n msk-bench get deploy -l app=msk-bench-client -L role,group,topic
kubectl -n msk-bench logs -l role=consumer --prefix --tail=3 --max-log-requests=100
kubectl -n msk-bench logs -l app=msk-bench-client --prefix --tail=-1 --max-log-requests=120 | grep ERROR
```
Each pod writes one CSV line per `report_interval_ms` to stdout. The columns are the same
as on the EC2 hosts (see the headers of the two Java files). Startup takes longer than on
EC2 because the tool's source is compiled in the JVM, and a 250m CPU limit slows that
down.

### Example specs

| Spec | Shape | Pods |
|---|---|---|
| `examples/workload-low-density.yaml` | 2 topics x 12 partitions, 4 producer clients, 1 group per topic | 2 producer + 2 consumer |
| `examples/workload-high-density.yaml` | 12 topics x 100 partitions + `bcast-1`, 121 producer clients, 100 groups (24 on the main topics, 76 on `bcast-1`) | 13 producer + 100 consumer |
| `examples/workload-one-consumer-per-partition.yaml` | 4 topics x 60 partitions, 1 group per topic with 60 members (20 per pod x 3 pods) | 4 producer + 12 consumer |

Copy one and edit it for your own shape. The comments in each spec document the keys.
Main keys:

* `tuning` (default `aws-tuned`) and optional `tuning_profiles` (an allowlist; when set,
  only those profiles are accepted).
* `consumers.total_groups`, `group_prefix` (default `bench-cg`), `fanout_mode`
  (`round-robin`, `low-volume-topics` or `explicit`) and the matching block.
* `consumers.members_per_group` (KafkaConsumer members per pod, default 1),
  `consumers.pods_per_group` (Deployment replicas, default 1), `worker_threads`,
  `stagger_ms`, `latency_report_sec`.
* `producers[]`: `layout` (`per-topic` or `shared`), `producers_per_pod`, `replicas`,
  `total_mb_per_sec` or `total_records_per_sec`, `record_size`.
* `estimate.topic_mb_per_sec` (for the egress arithmetic), `estimate.partitions_per_topic`
  (warns when a group has more members than partitions), `estimate.warn_above_multiple`
  (default 2.5) and optional `estimate.reference_egress_multiple` (an egress/ingress ratio
  you want to reproduce, for example your production cluster's; printed next to each
  mode when set).

### Fan-out modes: check egress before you apply

Every group reads its whole topic, so egress = sum(groups on topic x topic MB/s). The
generator prints the numbers for every mode configured in the spec, and prints a `WARNING`
when the selected mode is above `warn_above_multiple`. For
`examples/workload-high-density.yaml` (ingress = 12 x 10 + 0.5 = 120.5 MB/s):

| `consumers.fanout_mode` | groups per topic | egress | x ingress |
|---|---|---|---|
| `low-volume-topics`, `heavy_groups: 24` (spec default) | 2 per load-bench topic; bcast-1: 76 | 24 x 10 + 76 x 0.5 = 278 MB/s | **2.31x** |
| `round-robin` | load-bench-1..9: 8; load-bench-10..12: 7; bcast-1: 7 | 9 x 8 x 10 + 3 x 7 x 10 + 7 x 0.5 = 933.5 MB/s | **7.75x** |

To switch modes without editing the spec, add `--fanout-mode round-robin`.

## 5. Scaling

* **More or fewer consumer groups.** Change `consumers.total_groups`, or the mode's
  parameters, then regenerate and apply.
* **More members per group.** Set `consumers.members_per_group` and/or
  `consumers.pods_per_group` in the spec and regenerate. Do not use `kubectl scale` on a
  `cg-*` Deployment unless you mean to add members: a second replica joins the *same* group
  and splits the topic's partitions, so you get more members but no new group and no extra
  egress.
* **More or fewer producers.** Change `producers_per_pod`, `replicas` or the set's
  `total_mb_per_sec` / `total_records_per_sec`, then regenerate. The rate per pod is fixed
  when the manifests are generated, so `kubectl scale` on an `mp-*` Deployment multiplies
  the rate instead of splitting it.
* **Removing Deployments that are no longer in the spec.** Either prune by label:
  ```bash
  kubectl apply -f /tmp/out.yaml --prune -l msk-bench/workload=high-density \
    --prune-allowlist=apps/v1/Deployment
  ```
  or delete everything (section 7) and apply again.
* **Pausing load without deleting.**
  `kubectl -n msk-bench scale deploy -l msk-bench/workload=high-density --replicas=0`. To
  resume, apply `/tmp/out.yaml` again.

## 6. Client tuning profiles: baseline vs aws-tuned

Pods mount ConfigMap `client-tuning-<profile>` and merge its `client-tuning.properties`
into both the producer and the consumer configuration. Two profiles ship in `k8s/base/`:

| Profile | ConfigMap | Source of truth |
|---|---|---|
| `baseline` | `client-tuning-baseline` (`base/30-configmap-client-tuning-baseline.yaml`) | `scripts/profiles/baseline.properties`: a template with the Apache Kafka 3.9 client defaults written out. Replace the values with your production client settings so the baseline reproduces production. |
| `aws-tuned` | `client-tuning-aws-tuned` (`base/31-configmap-client-tuning-aws-tuned.yaml`) | `scripts/profiles/aws-tuned.properties`: the recommended profile. |

An A/B run is the same spec generated twice:

```bash
python3 k8s/gen-workload.py $SPEC --tuning baseline > /tmp/out.yaml && kubectl apply -f /tmp/out.yaml
# ... let the load run, record the CloudWatch window ...
python3 k8s/gen-workload.py $SPEC --tuning aws-tuned > /tmp/out.yaml && kubectl apply -f /tmp/out.yaml
```
Changing the profile changes the mounted ConfigMap and the `msk-bench/tuning` pod
annotation, so every Deployment rolls out again (strategy `Recreate`, so a single-member
group never has two members during the switch). If you edit a ConfigMap's *contents* in
place, pods do not pick it up until
`kubectl -n msk-bench rollout restart deploy -l app=msk-bench-client`. Every pod logs its
effective properties at startup, prefixed with `[entrypoint]`.

If you edit `scripts/profiles/baseline.properties`, regenerate its ConfigMap with the
command in the header of `base/30-configmap-client-tuning-baseline.yaml`, or apply it
directly:
```bash
kubectl -n msk-bench create configmap client-tuning-baseline \
  --from-file=client-tuning.properties=scripts/profiles/baseline.properties \
  --dry-run=client -o yaml | kubectl apply -f -
```

### Bring your own profile

Any profile name works (lowercase letters, digits and `-`). Profile `<p>` is the file
`scripts/profiles/<p>.properties` and the ConfigMap `client-tuning-<p>`:

```bash
cp scripts/profiles/baseline.properties scripts/profiles/myprod.properties   # then edit
kubectl -n msk-bench create configmap client-tuning-myprod \
  --from-file=client-tuning.properties=scripts/profiles/myprod.properties
python3 k8s/gen-workload.py $SPEC --tuning myprod > /tmp/out.yaml
kubectl apply -f /tmp/out.yaml
```
Whenever `k8s/base/` has no ConfigMap for the profile, the generator prints a `NOTE` with
the `kubectl create configmap` command, whether or not `scripts/profiles/<p>.properties`
exists; pods stay in `ContainerCreating` until the ConfigMap exists. It reads
`buffer.memory` from `scripts/profiles/<p>.properties` (if present) for its producer-heap
check. An empty profile name (`--tuning ""` or `tuning: ""`) is an error. If the spec sets
`tuning_profiles`, add your profile to that list.

For a one-off override on top of any profile, set `extra_props:` in the spec (newline
separated `k=v`, appended last). The MSK IAM auth keys are fixed by the image: pods refuse
to start with `ERROR: [entrypoint] ...` if the profile or `extra_props` sets
`security.protocol`, `sasl.mechanism`, `sasl.jaas.config` or
`sasl.client.callback.handler.class` (the EC2 host scripts apply the same rule).

## 7. Tear down

```bash
kubectl delete -f /tmp/out.yaml                 # all Deployments of this workload
# or: kubectl -n msk-bench delete deploy -l msk-bench/workload=<spec name>
kubectl delete -f k8s/base                      # ConfigMaps, ServiceAccount, then the namespace
# Bring-your-own cluster with Pod Identity only (eks/cluster.yaml cleans up its own association):
aws eks delete-pod-identity-association --cluster-name <eks> --association-id <id>
```
The consumer groups (`bench-cg-*`, `bench-bcast-*`) and their committed offsets stay on
the MSK cluster until `offsets.retention.minutes` (7 days by default) expires them. You
can delete them earlier with `kafka-consumer-groups.sh --delete` from a client host (the shipped roles grant
`kafka-cluster:DeleteGroup` on the benchmark cluster; see `docs/test-cases/README.md`). The
topics are not touched.

## Kafka Streams workloads

* `kstreams/gen-kstreams.sh <alos|eos> <num-apps> [input-topic] [tuning-profile]` prints one
  Deployment per Kafka Streams emulator app (default input `bcast-1`, profile `aws-tuned`).
  Set `IMAGE` (or `AWS_ACCOUNT_ID` / `AWS_REGION`) for the emulator image URI (built per
  `eks/ecr.md`); it prints a `WARNING:` on stderr while the image holds `111122223333`.

## Environment contract of the image (entrypoint.sh)

| Var | Role | Meaning |
|---|---|---|
| `ROLE` | both | `producer` or `consumer` |
| `BOOTSTRAP_SERVERS` | both | SASL/IAM bootstrap string (from ConfigMap `msk-bootstrap`) |
| `TOPICS` | both | comma-separated topics |
| `TUNING_FILE` | both | default `/etc/msk-bench/tuning/client-tuning.properties` |
| `EXTRA_PROPS`, `REPORT_INTERVAL_MS`, `STAGGER_MS`, `JAVA_OPTS` | both | optional |
| `NUM_PRODUCERS`, `TOTAL_RECORDS_PER_SEC` (this pod), `RECORD_SIZE`, `TOPIC_MODE`, `CLIENT_PREFIX`, `PAYLOAD_POOL` | producer | MultiProducer args |
| `GROUP_PREFIX`, `NUM_GROUPS` (default 1), `START_INDEX` | consumer | group id = `<GROUP_PREFIX>-<START_INDEX>` |
| `MEMBERS_PER_GROUP`, `WORKER_THREADS`, `LATENCY_REPORT_SEC` | consumer | optional: members per group in this pod, processing threads per group, latency report interval |

Any missing or invalid value prints `ERROR: [entrypoint] ...` and exits 1. The Java tools
also print `ERROR: ...` and exit non-zero when every client in the pod has failed, so
Kubernetes restarts the pod and the failure shows up in `RESTARTS`.

## Per-message end-to-end latency (opt-in)

Consumer pods can report produce-to-consume latency in milliseconds per consumer group.
It is off by default. To turn it on, set `consumers.latency_report_sec` in the spec (the
report interval in seconds, for example `30`; `0` or unset = off), then regenerate and
apply. The generator passes it to the consumer pods as `LATENCY_REPORT_SEC`. Each interval,
`MultiGroupConsumer` prints one line per group and stage on stdout:

    E2E_LATENCY group=<g> stage=poll|processed count=<n> p50=<ms> p90=<ms> p99=<ms> p999=<ms> max=<ms> window_s=<s>

- latency = consumer wall clock - record CreateTime (stamped by the producer client at `send()`),
  so it includes linger/batching, the produce request, acks=all replication, fetch and poll.
- `stage=poll`: when `poll()` returns; `stage=processed`: after worker processing.
- Topics must keep `message.timestamp.type=CreateTime` (Kafka default). LogAppendTime hides the
  producer-side batching delay.
- Producer and consumer pods may be on different nodes; accuracy depends on node clock sync
  (Amazon EKS nodes use chrony with the Amazon Time Sync Service, typically sub-millisecond).
  Negative values from skew are clamped to 0.

Collect and summarize one run (repeat per run label, for example `baseline` and
`aws-tuned`). First save the consumer logs to `/tmp/run-<label>.log`; `latency_report.py`
reads files, not the cluster. Set `--since` to cover the run:

    kubectl -n msk-bench logs -l role=consumer --prefix --tail=-1 --since=20m --max-log-requests=120 > /tmp/run-baseline.log
    python3 analysis/latency_report.py "baseline" '/tmp/run-baseline*.log'          # both stages
    python3 analysis/latency_report.py "baseline" '/tmp/run-baseline*.log' --stage poll

The report combines windows with count-weighted means of each window's percentiles (approximation;
max is exact). For exact tails use one long window.
