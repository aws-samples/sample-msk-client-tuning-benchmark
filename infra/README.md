# infra — CDK app (VPC + producer host + consumer fleet)

CDK provisions **only networking and the client EC2 hosts**. The MSK cluster is created
out-of-band by `../create-msk-standard-cluster.sh` (the same pattern as the sibling project),
so nothing here talks to the MSK control plane.

## Stacks

| Stack | What it is |
|---|---|
| `LoadBenchmarkNetworkStack` | VPC (3 AZs, public + private-egress subnets), client SG, MSK broker SG with client ingress on 9092/9094/9096/9098. |
| `LoadBenchmarkProducerStack` | 1 producer host (default `c5n.2xlarge`). |
| `LoadBenchmarkConsumerStack-0 .. -(N-1)` | `consumerCount` consumer hosts, one stack each so they're independently create/destroy-able. |

Every client host: AL2023, SSM-managed (no SSH), Kafka 3.9.0 + `aws-msk-iam-auth` 2.2.0
installed via user data, an S3 bucket for staging scripts / pulling results, 100 GiB GP3 root
volume, and kafka data-plane IAM scoped to the cluster **name**.

## Deploy

```bash
npm install
AWS_PROFILE=main npx cdk deploy --all --require-approval never
```

## Configuration (CDK context — no file edits needed)

| Context key | Default | Meaning |
|---|---|---|
| `account` | `CDK_DEFAULT_ACCOUNT` | Target AWS account. |
| `region` | `us-east-1` | Target region (also fixes the 3 AZs `<region>a/b/c`). |
| `clusterName` | `load-benchmark` | Cluster name IAM is scoped to. **Must match the create script.** |
| `consumerCount` | `1` | Number of consumer hosts / groups. Raise to fan out egress. |
| `producerInstanceType` | `c5n.2xlarge` | Producer host size. |
| `consumerInstanceType` | `c5n.2xlarge` | Each consumer host size. |
| `cidr` | `10.60.0.0/16` | VPC CIDR. |

Example — 3 consumer groups on larger hosts, different account:

```bash
npx cdk deploy --all --require-approval never \
  -c account=123456789012 -c region=us-west-2 \
  -c consumerCount=3 -c consumerInstanceType=c5n.4xlarge
```

## Scaling the consumer fleet after the fact

Re-deploy with a higher `consumerCount`; existing consumer stacks are untouched and new
`LoadBenchmarkConsumerStack-N` stacks are added. To remove one, lower the count and
`cdk destroy LoadBenchmarkConsumerStack-<i>` (or just `cdk deploy --all`, which prunes the
now-undeclared stacks when you target them).

## Stack outputs consumed by the scripts

`PrivateSubnetIds`, `MskSecurityGroupId` (by the create-cluster script);
`ScriptsBucketName`, `HostInstanceId` (by `orchestration/run-on-host.sh`).
