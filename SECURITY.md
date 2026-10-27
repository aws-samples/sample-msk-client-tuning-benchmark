# Security policy

## Disclaimer

This project is benchmark scaffolding provided as sample code, for non-production usage. It drives sustained
load and creates, alters, and deletes topics and consumer groups, so run it only against a **dedicated test
Amazon MSK cluster in a dedicated test AWS account**, and tear everything down after each test. Work with your
security and legal teams to meet your organizational security, regulatory, and compliance requirements before
adapting any of it. See [Production hardening recommendations](#production-hardening-recommendations).

## Reporting vulnerabilities

If you discover a potential security issue in this project, notify AWS/Amazon Security through the
[vulnerability reporting page](https://aws.amazon.com/security/vulnerability-reporting/). Do **not** create a
public GitHub issue.

## AWS services used

- **Amazon MSK** - the test cluster under load (Standard brokers, IAM access control, TLS in transit, enhanced
  monitoring at `PER_BROKER`).
- **Amazon EC2** - client hosts for the EC2 harness, and the worker nodes of the Amazon EKS harness.
- **Amazon EKS** - optional; runs the Kafka clients as pods (`eks/`, `k8s/`).
- **Amazon VPC** - private subnets, a NAT gateway, and security groups for the clients and brokers.
- **AWS Identity and Access Management (IAM)** - instance role for the EC2 hosts; EKS Pod Identity (or IRSA)
  role for the pods.
- **AWS Systems Manager** - Run Command drives the EC2 hosts; the hosts have no inbound rules and no SSH keys.
- **Amazon S3** - staging bucket for host scripts and results.
- **Amazon CloudWatch** - broker metrics and the benchmark dashboard.
- **Amazon ECR** - repository for the client container image (EKS harness).
- **AWS CloudFormation** - through the AWS CDK app in `infra/` and through eksctl.

## Prerequisites and permissions

- An AWS account used only for testing, and a principal that can create the resources above. The AWS CDK app
  and eksctl create IAM roles, so the deploying principal needs IAM write permissions.
- Tools: AWS CLI v2, Node.js 20 or later (AWS CDK), Python 3.9 or later with boto3 for the analysis tools
  (`analysis/requirements.txt`), and, for the EKS harness, eksctl, kubectl, and Docker.

The roles that the sample creates are scoped as follows:

- **Client role** (`infra/lib/client-host-stack.ts`, `k8s/iam/msk-bench-client-policy.json`, `eks/cluster.yaml`):
  Amazon MSK data-plane actions only on the benchmark cluster's `cluster/`, `topic/`, `group/`, and
  `transactional-id/` ARNs. That includes `kafka-cluster:DeleteTopic`, because the harness recreates topics.
  Control-plane reads are scoped to the cluster ARN where the API supports it; `kafka:List*` and the CloudWatch
  metric reads use `*` because those APIs do not support resource-level permissions.
  `cloudwatch:PutDashboard` is limited to dashboards.
- **Client quotas** (test case 8) need `kafka-cluster:AlterClusterDynamicConfiguration` and
  `kafka-cluster:DescribeClusterDynamicConfiguration`. The shipped role does **not** grant them. Add them
  temporarily, in the test account only, to the principal that runs `scripts/09-set-client-quotas.sh`.

## Security defaults in this sample

- Client-to-broker and broker-to-broker traffic is encrypted in transit; clients authenticate with IAM on port
  9098, and the broker security group allows only that port from the client security group.
- EC2 hosts require IMDSv2, use encrypted Amazon EBS volumes, sit in private subnets, and are reached only
  through AWS Systems Manager.
- The S3 staging bucket blocks public access, requires TLS, disables ACLs, and uses server-side encryption.
- Downloads are pinned and verified: the Apache Kafka distribution (SHA-512), the aws-msk-iam-auth jar
  (SHA-256, matching the `.sha256` file published with the release), and the Maven distribution used by the
  Maven Wrapper (`distributionSha256Sum`). A mismatch fails the image build or host bootstrap.
- Container base images come from Amazon ECR Public and are pinned by digest.
- Client pods run as a non-root user with a read-only root filesystem, `allowPrivilegeEscalation: false`, and
  all Linux capabilities dropped.
- Client profiles cannot override authentication: the EC2 scripts and the container entrypoint refuse to start
  a client whose profile sets `security.protocol` or `sasl.*` settings.
- The analysis tools call Amazon CloudWatch through the AWS SDK for Python (boto3), not through a shell, and
  validate every argument before use.

## Known security considerations

These trade-offs were reviewed and accepted for a short-lived test harness. Reconsider each one before adapting
the code for anything longer-lived.

| Item | Category | Rationale |
|---|---|---|
| The Amazon EKS API endpoint in `eks/cluster.yaml` is public and private | Security debt | Operator convenience for a short-lived test cluster; access still requires IAM authentication. Restrict `publicAccessCIDRs` or make the endpoint private. |
| The client role can delete topics and alter groups on the benchmark cluster | Accepted (function) | The harness recreates topics and consumer groups between runs. The permission is scoped to the benchmark cluster's ARNs. |
| Operating-system packages (`dnf`, `apt-get`) are not version-pinned | Security debt | Installed from the distribution repositories over HTTPS at build or boot time. Pin or mirror them if your policy requires it. |
| Client hosts carry the `AmazonSSMManagedInstanceCore` managed policy | Accepted (function) | Required for AWS Systems Manager Run Command, which replaces SSH. |
| Client container images have no Docker `HEALTHCHECK` | Accepted (platform) | Kubernetes ignores Docker `HEALTHCHECK`; the clients exit non-zero on failure and Kubernetes restarts them. |
| The S3 staging bucket has no access logging or versioning | Accepted (scope) | The bucket holds only scripts copied from this repository and run results, and it is deleted with the stack. |
| AWS CDK-generated custom-resource Lambda functions (auto-delete objects, restrict default security group) run outside a VPC with no DLQ or concurrency limit | Accepted (framework) | AWS CDK framework handlers that run only during deploy and destroy; not part of the sample's own code. |

## Production hardening recommendations

- Do not point this harness at a production cluster. To test production-like behavior, copy the production
  client settings into a profile and run them against a test cluster.
- Make the Amazon EKS endpoint private, or restrict it to your operator CIDR ranges.
- Mirror container images and dependencies into private repositories with scan-on-push and immutable tags.
- Enable Amazon EKS control-plane logging and AWS CloudTrail data events if you keep a cluster for longer than a
  test window.
- Remove `kafka-cluster:DeleteTopic` and the quota permissions from any role that outlives the test.

## Resource cleanup

Follow [Cleanup](README.md#cleanup) in the README, in order: stop load, delete the Amazon EKS cluster, delete
the Amazon MSK cluster and then its configuration, destroy the AWS CDK stacks, and delete the Amazon ECR
repository and the pod IAM role. Then confirm that no benchmark EC2 instances, MSK clusters, or CloudFormation
stacks in `DELETE_FAILED` remain.

## Dependencies

| Dependency | Version | Notes |
|---|---|---|
| Apache Kafka (client tools and libraries) | 3.9.2 | Downloaded from archive.apache.org; SHA-512 verified. |
| aws-msk-iam-auth (`-all` jar) | 2.2.0 | Downloaded from the GitHub release; SHA-256 verified. |
| Eclipse Temurin base images | 17 (JDK and JRE) | `public.ecr.aws/docker/library/eclipse-temurin`, pinned by digest. |
| Apache Maven (through the Maven Wrapper) | 3.9.9 | SHA-256 verified by the wrapper. |
| AWS CDK (`aws-cdk-lib`, `aws-cdk`) | see `infra/package.json` | Locked by `infra/package-lock.json`; `npm audit` clean at publication. |
| Kafka Streams and Maven dependencies | see `kstreams-emulator/pom.xml` | Kafka 3.9.2. |
| boto3 (AWS SDK for Python) | `>=1.34,<2` (`analysis/requirements.txt`) | Workstation only, for the analysis tools; never installed on the client hosts. |

See [THIRD-PARTY.md](THIRD-PARTY.md) for licenses.
