# Amazon EKS cluster for the benchmark clients

`cluster.yaml` defines `msk-bench-eks`, an eksctl ClusterConfig. The cluster runs the Kafka client pods
(Java producers/consumers and the Kafka Streams emulator) inside an existing VPC, for example the one created
by the `infra/` stack `LoadBenchmarkNetworkStack`. The pods reach the Amazon Managed Streaming for Apache Kafka
(Amazon MSK) cluster `load-benchmark` with IAM auth on port 9098.

Before you start, replace every placeholder in `cluster.yaml`:

| Placeholder | Where | Replace with |
|---|---|---|
| `111122223333` | ARNs in the pod policy | your AWS account ID |
| `us-east-1` | `metadata.region`, the subnet AZ keys (`us-east-1a` ...), the ARNs | your Region and its Availability Zones |
| `REPLACE_ME_vpc-id` | `vpc.id` | `LoadBenchmarkNetworkStack` output `VpcId` |
| `REPLACE_ME_subnet-a`, `-b`, `-c` | `vpc.subnets.private` and the node group `subnets` | `LoadBenchmarkNetworkStack` output `PrivateSubnetIds` (comma-separated, one per AZ; key each by its AZ) |
| `REPLACE_ME_client-sg-id` | node group `securityGroups.attachIDs` | `LoadBenchmarkNetworkStack` output `ClientSecurityGroupId` |
| `load-benchmark` | ARNs in the pod policy | your Amazon MSK cluster name, if different |

Read the stack outputs (and the subnet AZs) with:

```bash
aws cloudformation describe-stacks --stack-name LoadBenchmarkNetworkStack --region us-east-1 --no-cli-pager \
  --query 'Stacks[0].Outputs[].[OutputKey,OutputValue]' --output table
aws ec2 describe-subnets --subnet-ids <PrivateSubnetIds, space-separated> --region us-east-1 --no-cli-pager \
  --query 'Subnets[].[SubnetId,AvailabilityZone]' --output table
```

Then check that no placeholder is left. The command ignores comment lines and must print nothing:

```bash
grep -nE 'REPLACE_ME|111122223333' eks/cluster.yaml | grep -vE '^[0-9]+:[[:space:]]*#'
```

| Item | Value |
|---|---|
| Account / Region | `111122223333` (example; use your AWS account ID) / `us-east-1` (example) |
| Kubernetes | Use a Kubernetes version currently in Amazon EKS standard support, within +/-1 minor of your `kubectl` |
| VPC | existing VPC (eksctl creates no VPC or NAT gateway) |
| Private subnets | one per Availability Zone |
| NAT | prerequisite, not a placeholder: each private route table needs a 0.0.0.0/0 route to a NAT gateway (`LoadBenchmarkNetworkStack` creates one). Verify it in the pre-flight checklist |
| Nodes | managed node group `kafka-clients`, c5n.2xlarge (x86_64), AL2023, 50 GiB gp3, label `role=kafka-client`. Set `desiredCapacity`/`maxSize` to your workload |
| MSK access | the client security group is attached to the nodes. The MSK security group (`LoadBenchmarkNetworkStack` output `MskSecurityGroupId`) must allow port 9098 (IAM over TLS) from it |
| Pod IAM | Amazon EKS Pod Identity. `cluster.yaml` creates the AWS IAM role `msk-bench-eks-client-pod-role`, the association, and the ServiceAccount `msk-bench/msk-bench-client`; nothing else is needed (`k8s/iam/` is only for bring-your-own clusters) |
| Pod policy | MSK data plane on the cluster's `cluster/`, `topic/`, `group/`, `transactional-id/` ARNs; `kafka:GetBootstrapBrokers`, `DescribeCluster`, `DescribeClusterV2`, `ListNodes` on the cluster ARN; only `kafka:ListClustersV2` uses `"*"` |
| Endpoint | public + private |
| Tags | `project=msk-load-benchmark` on the cluster, node group, ASG (propagateASGTags), and the pod-identity role |

## Pre-flight checklist

Run these read-only checks before creating the cluster.

- [ ] **Credentials**: `aws sts get-caller-identity --no-cli-pager` shows the intended account.
      Creating the cluster requires permissions to create Amazon EKS, Amazon EC2, AWS IAM, and AWS CloudFormation resources.
- [ ] **No placeholders left**: the `grep` command above prints nothing.
- [ ] **Client SG exists in this VPC**:
      `aws ec2 describe-security-groups --group-ids <ClientSecurityGroupId> --region us-east-1 --no-cli-pager --query 'SecurityGroups[].VpcId'`
      returns your `VpcId`.
- [ ] **MSK SG allows the client SG**:
      `aws ec2 describe-security-groups --group-ids <MskSecurityGroupId> --region us-east-1 --no-cli-pager --query 'SecurityGroups[].IpPermissions[].[FromPort,ToPort,UserIdGroupPairs[].GroupId]'`
      shows 9098 from the client SG.
- [ ] **NAT exists and is routed** (prerequisite; nothing to replace in `cluster.yaml`):
      `aws ec2 describe-route-tables --filters Name=association.subnet-id,Values=<subnet-a>,<subnet-b>,<subnet-c> --region us-east-1 --no-cli-pager --query 'RouteTables[].Routes[?DestinationCidrBlock==`0.0.0.0/0`].NatGatewayId'`
      returns a `nat-...` ID for every private route table. For each returned ID,
      `aws ec2 describe-nat-gateways --nat-gateway-ids <nat-id> --region us-east-1 --no-cli-pager --query 'NatGateways[].State'` returns `available`.
- [ ] **Subnet IP headroom**: VPC-CNI pre-allocates IPs per node (a c5n.2xlarge supports up to 58 pods).
      Check `AvailableIpAddressCount` on each subnet against the node and pod count you plan.
- [ ] **Keep the network stack** while the cluster exists. Deleting the VPC stack would fail or orphan the EKS ENIs.
- [ ] **Dry run passes**: `eksctl create cluster -f eks/cluster.yaml --dry-run` exits 0.

## Create

Creation takes about 15-25 minutes: roughly 10-12 minutes for the control plane, 3-5 minutes for addons and
pod identity, and 3-5 minutes for the node group.

```bash
# from the repo root
eksctl create cluster -f eks/cluster.yaml
```

eksctl writes the kubeconfig context automatically. If you need to refresh it:

```bash
aws eks update-kubeconfig --name msk-bench-eks --region us-east-1
```

## Verify

```bash
eksctl get cluster --name msk-bench-eks --region us-east-1
eksctl get nodegroup --cluster msk-bench-eks --region us-east-1
eksctl get addons --cluster msk-bench-eks --region us-east-1
eksctl get podidentityassociation --cluster msk-bench-eks --region us-east-1

kubectl get nodes -L role,topology.kubernetes.io/zone,node.kubernetes.io/instance-type
kubectl -n msk-bench get sa msk-bench-client
kubectl -n kube-system get pods
kubectl top nodes            # metrics-server

# Nodes carry the client SG
aws ec2 describe-instances --region us-east-1 --no-cli-pager \
  --filters Name=tag:eks:cluster-name,Values=msk-bench-eks Name=instance-state-name,Values=running \
  --query 'Reservations[].Instances[].[InstanceId,Placement.AvailabilityZone,SecurityGroups[].GroupId]'
```

Smoke test: reach Amazon MSK from a pod using the pod-identity ServiceAccount.

```bash
BOOTSTRAP=$(aws kafka get-bootstrap-brokers --region us-east-1 --no-cli-pager \
  --cluster-arn "$(aws kafka list-clusters-v2 --cluster-name-filter load-benchmark --region us-east-1 --no-cli-pager --query 'ClusterInfoList[0].ClusterArn' --output text)" \
  --query BootstrapBrokerStringSaslIam --output text)

# Image built and pushed per eks/ecr.md (111122223333 = your AWS account ID)
kubectl -n msk-bench run msk-smoke --rm -it --restart=Never \
  --image=111122223333.dkr.ecr.us-east-1.amazonaws.com/msk-bench-client:latest \
  --overrides='{"spec":{"serviceAccountName":"msk-bench-client","nodeSelector":{"role":"kafka-client"}}}' \
  --command -- bash -c "printf '%s\n' security.protocol=SASL_SSL sasl.mechanism=AWS_MSK_IAM \
      'sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;' \
      sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler > /tmp/iam.properties \
    && /opt/kafka/bin/kafka-topics.sh --bootstrap-server '$BOOTSTRAP' --command-config /tmp/iam.properties --list"
```

`--command` bypasses the image entrypoint, which otherwise expects the `ROLE`/`TOPICS` environment contract.

## Cost drivers (us-east-1, on-demand, approximate)

| Driver | Rate |
|---|---|
| Amazon EKS control plane (standard support) | $0.10/hr |
| c5n.2xlarge | ~$0.43/hr per node |
| Amazon EBS gp3 50 GiB | ~$4/mo per node |
| NAT gateway data processing | $0.045/GB (image pulls and AWS API traffic) |

For example, 10 nodes cost about $4.40/hr including the control plane. MSK traffic stays inside the VPC,
so it does not cross the NAT gateway. Cross-AZ data transfer between client pods and brokers costs $0.01/GB in
each direction and adds up quickly at benchmark throughput. Check current prices on the AWS pricing pages.

The control plane is billed even with zero nodes. Delete the cluster when it is idle. A cluster that stays on a
Kubernetes version after its standard support ends moves to extended support, which has a higher control-plane rate.

To pause the nodes without deleting the cluster:

```bash
eksctl scale nodegroup --cluster msk-bench-eks --name kafka-clients --nodes 0 --nodes-min 0 --region us-east-1
```

## Teardown

```bash
# 1. Delete workloads first so no LoadBalancer or PVC resources are orphaned
kubectl delete namespace msk-bench --wait

# 2. Delete the cluster. This removes the node group, addons, pod-identity role, and eksctl CFN stacks.
#    The existing VPC, subnets, NAT gateway, and client SG are NOT touched.
eksctl delete cluster -f eks/cluster.yaml --wait

# 3. Confirm nothing is left
aws cloudformation list-stacks --region us-east-1 --no-cli-pager \
  --query "StackSummaries[?starts_with(StackName,'eksctl-msk-bench-eks') && StackStatus!='DELETE_COMPLETE'].[StackName,StackStatus]"
```

Amazon ECR repository creation, image build/push, and ECR teardown are covered in `ecr.md`.
