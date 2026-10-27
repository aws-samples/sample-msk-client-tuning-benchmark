# Amazon ECR repositories and image build/push

These commands are for reference. Set `ACCOUNT` to your AWS account ID (`111122223333` below is an
example) and `AWS_REGION` to your Region.

```bash
export AWS_REGION=us-east-1
export ACCOUNT=111122223333          # REPLACE_ME: your AWS account ID
export REGISTRY=${ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com
export TAG=$(git rev-parse --short HEAD)
```

## 1. Create the repositories (one time)

```bash
for repo in msk-bench-client kstreams-emulator; do
  aws ecr create-repository \
    --repository-name "$repo" \
    --image-scanning-configuration scanOnPush=true \
    --image-tag-mutability MUTABLE \
    --encryption-configuration encryptionType=AES256 \
    --tags Key=project,Value=msk-load-benchmark \
    --region "$AWS_REGION" --no-cli-pager
done
```

Optional lifecycle policy that keeps the last 10 images:

```bash
for repo in msk-bench-client kstreams-emulator; do
  aws ecr put-lifecycle-policy --repository-name "$repo" --region "$AWS_REGION" --no-cli-pager \
    --lifecycle-policy-text '{"rules":[{"rulePriority":1,"description":"keep last 10","selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":10},"action":{"type":"expire"}}]}'
done
```

## 2. Log in to Amazon ECR

```bash
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"
```

## 3. Build and push (linux/amd64, since the c5n nodes are x86_64)

- `k8s/image/Dockerfile` builds the Java producer/consumer image. Its build context is the repo root.
- `kstreams-emulator/Dockerfile` builds the Kafka Streams app. Build the jar first
  (`./mvnw -q -DskipTests package` in `kstreams-emulator/`); the build context is `kstreams-emulator/`.

On an arm64 workstation (for example Apple silicon), Docker builds arm64 by default, so
`--platform linux/amd64` is required. Without it, pods fail with `exec format error`.

```bash
docker buildx create --name msk-bench --use 2>/dev/null || docker buildx use msk-bench

# Java producer/consumer client image (Kafka client libs + aws-msk-iam-auth jar)
docker buildx build --platform linux/amd64 \
  -f k8s/image/Dockerfile \
  -t ${REGISTRY}/msk-bench-client:${TAG} \
  -t ${REGISTRY}/msk-bench-client:latest \
  --push .

# Kafka Streams emulator
docker buildx build --platform linux/amd64 \
  -f kstreams-emulator/Dockerfile \
  -t ${REGISTRY}/kstreams-emulator:${TAG} \
  -t ${REGISTRY}/kstreams-emulator:latest \
  --push kstreams-emulator
```

Verify:

```bash
aws ecr describe-images --repository-name msk-bench-client  --region "$AWS_REGION" --no-cli-pager \
  --query 'imageDetails[].[imageTags,imagePushedAt]' --output table
aws ecr describe-images --repository-name kstreams-emulator --region "$AWS_REGION" --no-cli-pager \
  --query 'imageDetails[].[imageTags,imagePushedAt]' --output table
```

Nodes pull these images with `AmazonEC2ContainerRegistryReadOnly`, which is attached to the
node group role in `cluster.yaml`. The private subnets reach Amazon ECR through the NAT gateway.
No ECR VPC endpoints are needed, but adding them would cut NAT data charges if you pull images often.

## Image requirements for Amazon MSK IAM auth via Amazon EKS Pod Identity

- Java 17, the Kafka clients jar, and `aws-msk-iam-auth` 2.2.0 or later.
  Its bundled AWS SDK v2 resolves Pod Identity credentials through
  `AWS_CONTAINER_CREDENTIALS_FULL_URI` and `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`.
  If you shade or override the SDK, it must be version 2.21.30 or later.
- Client properties: `security.protocol=SASL_SSL`, `sasl.mechanism=AWS_MSK_IAM`,
  `sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;`,
  `sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler`,
  bootstrap on port 9098.

## Teardown (only when finished)

```bash
aws ecr delete-repository --repository-name msk-bench-client  --force --region "$AWS_REGION" --no-cli-pager
aws ecr delete-repository --repository-name kstreams-emulator --force --region "$AWS_REGION" --no-cli-pager
```
