# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
# checkov:skip=CKV_DOCKER_2: batch load-generator pods; liveness is managed by Kubernetes (exit non-zero on failure), and Kubernetes ignores Docker HEALTHCHECK.
#
# msk-bench-client -- container image for the plain producer / consumer side of the
# MSK load benchmark (scripts/MultiProducer.java, scripts/MultiGroupConsumer.java).
# Same client stack as the EC2 load hosts (infra/lib/client-host-stack.ts):
# Kafka 3.9.2 (kafka_2.13-3.9.2.tgz) client libs + aws-msk-iam-auth 2.2.0 "-all" jar.
#
# BUILD CONTEXT IS THE REPO ROOT (the Java sources live in scripts/):
#   docker build -f k8s/image/Dockerfile -t msk-bench-client:local .
# k8s/image/Dockerfile.dockerignore limits the context to the few files needed.
#
# JDK (not JRE) base on purpose: the tools run in single-file source mode
# (`java Foo.java`), which needs the in-JDK compiler (jdk.compiler module).

# Base image from the Amazon ECR Public mirror of the Docker official image, pinned by digest
# (multi-arch index) in both FROM lines below. Update the
# tag and digest together:  docker buildx imagetools inspect public.ecr.aws/docker/library/eclipse-temurin:17-jdk

# ---------------------------------------------------------------- fetch stage
FROM public.ecr.aws/docker/library/eclipse-temurin:17-jdk@sha256:bd5fd56be7550e10a1d55f18eda4027cf724f6a19fe5fa08d51b48c059663348 AS fetch

ARG KAFKA_VERSION=3.9.2
ARG SCALA_VERSION=2.13
ARG MSK_IAM_AUTH_VERSION=2.2.0
# Required pin for the IAM auth jar. Matches the .sha256 file published with the
# GitHub release. If you change MSK_IAM_AUTH_VERSION, update this as well.
ARG MSK_IAM_AUTH_SHA256=735dd058b81d088e767770d6cc4e7f1a1beea815ced18b66a23b35fd87651e77

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp/dl

# Kafka distribution from archive.apache.org, verified against the published .sha512.
# The .sha512 file is either "sha512sum" style or "gpg --print-md" style (uppercase,
# space-grouped, wrapped). Strip the file name, keep hex digits only, lowercase.
RUN set -eu; \
    tgz="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"; \
    base="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}"; \
    curl -fsSL --retry 5 --retry-delay 3 -o "${tgz}" "${base}/${tgz}" \
      || { echo "ERROR: download failed: ${base}/${tgz}" >&2; exit 1; }; \
    curl -fsSL --retry 5 --retry-delay 3 -o "${tgz}.sha512" "${base}/${tgz}.sha512" \
      || { echo "ERROR: download failed: ${base}/${tgz}.sha512" >&2; exit 1; }; \
    expected="$(tr -d '\r\n' < "${tgz}.sha512" | sed "s/${tgz}//" | tr -cd '0-9A-Fa-f' | tr 'A-F' 'a-f')"; \
    actual="$(sha512sum "${tgz}" | cut -d' ' -f1)"; \
    if [ "${#expected}" -ne 128 ]; then echo "ERROR: could not parse ${tgz}.sha512" >&2; cat "${tgz}.sha512" >&2; exit 1; fi; \
    if [ "${expected}" != "${actual}" ]; then \
      echo "ERROR: sha512 mismatch for ${tgz}: expected=${expected} actual=${actual}" >&2; exit 1; fi; \
    mkdir -p /opt/kafka; \
    tar -xzf "${tgz}" -C /opt/kafka --strip-components=1; \
    rm -rf /opt/kafka/site-docs /opt/kafka/bin/windows; \
    rm -f "${tgz}" "${tgz}.sha512"

# aws-msk-iam-auth shaded "-all" jar (bundles the AWS SDK v2 it needs), exactly the
# artifact the EC2 load hosts use. The thin Maven Central jar would also need the
# AWS SDK v2 dependency tree on the classpath, so it is not used here. The build
# fails if the jar is not a valid zip containing the IAM login module.
RUN set -eu; \
    jar="aws-msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar"; \
    url="https://github.com/aws/aws-msk-iam-auth/releases/download/v${MSK_IAM_AUTH_VERSION}/${jar}"; \
    curl -fsSL --retry 5 --retry-delay 3 -o "/opt/kafka/libs/${jar}" "${url}" \
      || { echo "ERROR: download failed: ${url}" >&2; exit 1; }; \
    if [ -z "${MSK_IAM_AUTH_SHA256}" ]; then echo "ERROR: MSK_IAM_AUTH_SHA256 is required" >&2; exit 1; fi; \
    echo "${MSK_IAM_AUTH_SHA256}  /opt/kafka/libs/${jar}" | sha256sum -c - \
      || { echo "ERROR: sha256 mismatch for ${jar}" >&2; exit 1; }; \
    jar tf "/opt/kafka/libs/${jar}" > /tmp/dl/jar-list.txt \
      || { echo "ERROR: ${jar} is not a readable jar" >&2; exit 1; }; \
    grep -q 'software/amazon/msk/auth/iam/IAMLoginModule.class' /tmp/dl/jar-list.txt \
      || { echo "ERROR: ${jar} does not contain IAMLoginModule" >&2; exit 1; }; \
    rm -f /tmp/dl/jar-list.txt

# ---------------------------------------------------------------- runtime stage
FROM public.ecr.aws/docker/library/eclipse-temurin:17-jdk@sha256:bd5fd56be7550e10a1d55f18eda4027cf724f6a19fe5fa08d51b48c059663348

ARG KAFKA_VERSION=3.9.2
ARG MSK_IAM_AUTH_VERSION=2.2.0
LABEL org.opencontainers.image.title="msk-bench-client" \
      org.opencontainers.image.description="MSK load benchmark producer/consumer (MultiProducer / MultiGroupConsumer)" \
      msk-bench.kafka-version="${KAFKA_VERSION}" \
      msk-bench.aws-msk-iam-auth-version="${MSK_IAM_AUTH_VERSION}"

# Non-root runtime user (fixed numeric uid/gid so runAsNonRoot can verify it).
RUN groupadd --gid 10001 bench \
 && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent \
            --shell /usr/sbin/nologin bench

COPY --from=fetch /opt/kafka /opt/kafka
COPY scripts/MultiProducer.java scripts/MultiGroupConsumer.java /opt/msk-bench/
COPY k8s/image/entrypoint.sh /opt/msk-bench/entrypoint.sh
RUN chmod 0555 /opt/msk-bench/entrypoint.sh \
 && chmod 0444 /opt/msk-bench/MultiProducer.java /opt/msk-bench/MultiGroupConsumer.java \
 && chmod -R a+rX /opt/kafka

ENV KAFKA_HOME=/opt/kafka \
    MSK_BENCH_HOME=/opt/msk-bench

# The entrypoint writes the effective client properties under $WORK_DIR (default
# /tmp), so the root filesystem can be mounted read-only with an emptyDir at /tmp.
USER 10001:10001
WORKDIR /tmp
ENTRYPOINT ["/opt/msk-bench/entrypoint.sh"]
