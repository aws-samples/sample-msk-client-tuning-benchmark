# Third-party software

This repository contains original sample code licensed under MIT-0 (see [LICENSE](LICENSE)), plus the
Apache Maven Wrapper files listed below. It does **not** redistribute any other third-party software. The
components below are downloaded by you, at build time or at run time, directly from their upstream sources and
are subject to their own licenses. Review those licenses before you use them.

## Included in this repository

| Component | Version | License | Files |
|---|---|---|---|
| Apache Maven Wrapper | 3.3.2 | Apache License 2.0 | `kstreams-emulator/mvnw`, `kstreams-emulator/mvnw.cmd`, `kstreams-emulator/.mvn/wrapper/maven-wrapper.properties` |

These files keep their original Apache License 2.0 headers and are not covered by the MIT-0 license of this
repository.

## Downloaded at build or run time (not redistributed)

| Component | Version | License | Downloaded by | Source |
|---|---|---|---|---|
| Apache Kafka (binary distribution, Scala 2.13) | 3.9.2 | Apache License 2.0 | `k8s/image/Dockerfile` and Amazon EC2 client host user data in `infra/lib/client-host-stack.ts` (both verify a pinned SHA-512) | archive.apache.org |
| Amazon MSK Library for AWS IAM (`aws-msk-iam-auth`, shaded `-all` jar) | 2.2.0 | Apache License 2.0 | `k8s/image/Dockerfile`, EC2 client host user data | github.com/aws/aws-msk-iam-auth releases |
| Eclipse Temurin container images (`public.ecr.aws/docker/library/eclipse-temurin:17-jdk`, `:17-jre`; Amazon ECR Public mirror of the Docker official image) | 17 | GPL v2 with Classpath Exception (OpenJDK), plus the licenses of the base OS packages | `k8s/image/Dockerfile`, `kstreams-emulator/Dockerfile` | Amazon ECR Public (public.ecr.aws) |
| Apache Maven (fetched by the Maven Wrapper) | 3.9.9 | Apache License 2.0 | `kstreams-emulator/mvnw` | repo.maven.apache.org |
| Maven dependencies of `kstreams-emulator` | see `kstreams-emulator/pom.xml` | various (Apache License 2.0, MIT, EPL 2.0) | `./mvnw package` | Maven Central |
| boto3 (AWS SDK for Python) and its dependencies | see `analysis/requirements.txt` | Apache License 2.0 | `pip install -r analysis/requirements.txt` on the operator's workstation | PyPI |
| npm dependencies of the AWS CDK app | see `infra/package.json` and `infra/package-lock.json` | various (mostly Apache License 2.0 and MIT) | `npm install` in `infra/` | npm registry |
| Amazon Linux 2023 packages (Amazon Corretto 17, jq, Python 3, and others) | distribution versions | various | EC2 client host user data | Amazon Linux repositories |

### Maven dependencies (`kstreams-emulator/pom.xml`)

| Artifact | Version | License | Scope |
|---|---|---|---|
| `org.apache.kafka:kafka-streams` | 3.9.2 | Apache License 2.0 | compile |
| `software.amazon.msk:aws-msk-iam-auth` | 2.2.0 | Apache License 2.0 | compile |
| `org.slf4j:slf4j-api`, `org.slf4j:slf4j-simple` | 2.0.16 | MIT | compile |
| `org.apache.kafka:kafka-streams-test-utils` | 3.9.2 | Apache License 2.0 | test |
| `org.junit.jupiter:junit-jupiter` | 5.11.3 | Eclipse Public License 2.0 | test |
| `maven-compiler-plugin`, `maven-surefire-plugin`, `maven-shade-plugin` | 3.13.0, 3.5.2, 3.6.0 | Apache License 2.0 | build |

The shaded `kstreams-emulator.jar` that `./mvnw package` produces bundles these runtime dependencies and their
transitive dependencies. It is built on your machine and is not published by this repository. If you
redistribute a built jar or container image, you are responsible for meeting the license terms of everything
it contains.

### npm dependencies (`infra/package.json`)

| Package | Version range | License |
|---|---|---|
| `aws-cdk-lib` | ^2.275.0 | Apache License 2.0 |
| `constructs` | ^10.5.0 | Apache License 2.0 |
| `aws-cdk` (CLI, dev) | 2.1146.1 | Apache License 2.0 |
| `typescript` (dev) | ~5.6.3 | Apache License 2.0 |
| `jest`, `@types/jest`, `@types/node`, `@swc/core`, `@swc/jest`, `tsx` (dev) | see `infra/package.json` | MIT or Apache License 2.0 |
