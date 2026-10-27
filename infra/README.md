# infra: AWS CDK app (VPC + producer host + consumer fleet)

The AWS Cloud Development Kit (AWS CDK) app provisions **only networking and the client Amazon EC2
hosts**. The Amazon MSK cluster is created separately by `../create-msk-standard-cluster.sh`, so
nothing here calls the Amazon MSK control plane.

## Stacks

| Stack | What it is |
|---|---|
| `LoadBenchmarkNetworkStack` | VPC (3 AZs, public + private-egress subnets, one NAT gateway), client security group, MSK broker security group that allows port 9098 (IAM auth over TLS) from the client security group only. |
| `LoadBenchmarkProducerStack` | 1 producer host (default `c5n.2xlarge`). |
| `LoadBenchmarkConsumerStack-0 .. -(N-1)` | `consumerCount` consumer hosts, one stack each so you can create and destroy them independently. |

Every client host runs Amazon Linux 2023 and is managed by AWS Systems Manager (no SSH, no inbound
rules). User data installs Apache Kafka 3.9.2 and `aws-msk-iam-auth` 2.2.0. Each stack also creates
an Amazon S3 bucket for staging scripts and pulling results, and each host gets a 100 GiB encrypted
gp3 root volume and Kafka data-plane AWS IAM permissions scoped to the cluster **name**.

## Security defaults

- No security group allows ingress from `0.0.0.0/0`. The broker security group accepts only
  port 9098 from the client security group. The default VPC security group is restricted
  (`@aws-cdk/aws-ec2:restrictDefaultSecurityGroup`).
- Instances require IMDSv2 and use encrypted EBS root volumes.
- The S3 bucket blocks all public access, enforces TLS (`aws:SecureTransport`), uses SSE-S3
  encryption, and disables ACLs (bucket owner enforced).
- AWS IAM: data-plane (`kafka-cluster:*`) and control-plane (`kafka:DescribeCluster*`,
  `GetBootstrapBrokers`, `ListNodes`) actions are scoped to the benchmark cluster ARN pattern.
  Only the list/metric reads that do not support resource-level permissions
  (`kafka:List*`, `cloudwatch:GetMetricData`, `cloudwatch:GetMetricStatistics`) use `*`.
- The bucket and its objects are deleted with the stack (`autoDeleteObjects`). This is a
  short-lived benchmark environment; change the removal policy if you want to keep results.

## Deploy

```bash
npm ci
npx cdk bootstrap            # once per account/Region
npx cdk deploy --all
```

The account and Region come from CDK context (`-c account=... -c region=...`) or, if not given,
from the environment the CDK CLI resolves from your AWS profile (`CDK_DEFAULT_ACCOUNT` /
`CDK_DEFAULT_REGION`). Nothing is hard-coded. `cdk.context.json` is git-ignored because it
stores account-specific lookups.

## Configuration (CDK context; no file edits needed)

| Context key | Default | Meaning |
|---|---|---|
| `account` | `CDK_DEFAULT_ACCOUNT` | Target AWS account. |
| `region` | `CDK_DEFAULT_REGION` | Target Region (also fixes the 3 AZs `<region>a/b/c`). Required. |
| `clusterName` | `load-benchmark` | Cluster name IAM is scoped to. **Must match the create script.** |
| `consumerCount` | `1` | Number of consumer hosts / groups. Raise to fan out egress. |
| `producerInstanceType` | `c5n.2xlarge` | Producer host size. |
| `consumerInstanceType` | `c5n.2xlarge` | Each consumer host size. |
| `cidr` | `10.60.0.0/16` | VPC CIDR. |

Example: 3 consumer groups on larger hosts in another account and Region:

```bash
npx cdk deploy --all \
  -c account=111122223333 -c region=us-west-2 \
  -c consumerCount=3 -c consumerInstanceType=c5n.4xlarge
```

## Scaling the consumer fleet after the fact

Re-deploy with a higher `consumerCount`; existing consumer stacks are untouched and new
`LoadBenchmarkConsumerStack-N` stacks are added. To remove one, lower the count and run
`npx cdk destroy LoadBenchmarkConsumerStack-<i>`.

## Stack outputs consumed by the scripts

`PrivateSubnetIds`, `MskSecurityGroupId` (by the create-cluster script);
`ScriptsBucketName`, `HostInstanceId` (by `orchestration/run-on-host.sh`).

## Tests

```bash
npm test      # asserts the security defaults above against the synthesized templates
```
