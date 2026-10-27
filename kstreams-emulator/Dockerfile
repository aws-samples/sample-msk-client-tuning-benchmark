# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
# checkov:skip=CKV_DOCKER_2: Kafka Streams emulator pods; liveness is managed by Kubernetes (exit non-zero on failure), and Kubernetes ignores Docker HEALTHCHECK.
#
# kstreams-emulator runtime image. Build the jar first (./mvnw -q -DskipTests package),
# then: docker build -t kstreams-emulator:0.1.0 .
# (This file is not built or pushed by any script in this repo.)
# Base image from the Amazon ECR Public mirror of the Docker official image, pinned by digest (multi-arch index). Update the tag and digest together:
#   docker buildx imagetools inspect public.ecr.aws/docker/library/eclipse-temurin:17-jre
FROM public.ecr.aws/docker/library/eclipse-temurin:17-jre@sha256:8b16f59326a08c566ced5e6250fcdb55112a2a736a19f1fb4ff12a15f4aa26ff

# Non-root runtime user. RocksDB state lives in STATE_DIR (default /tmp/kstreams-state);
# /var/lib/kstreams is provided as a writable alternative for a mounted volume.
RUN groupadd --system --gid 10001 kstreams \
 && useradd --system --uid 10001 --gid kstreams --home-dir /app --shell /usr/sbin/nologin kstreams \
 && mkdir -p /app /var/lib/kstreams \
 && chown -R kstreams:kstreams /app /var/lib/kstreams

WORKDIR /app
COPY --chown=kstreams:kstreams target/kstreams-emulator.jar /app/kstreams-emulator.jar

# RocksDB uses off-heap memory, so leave headroom below the container limit.
# Override at runtime with -e JAVA_TOOL_OPTIONS=...
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=50 -XX:+ExitOnOutOfMemoryError" \
    STATE_DIR=/tmp/kstreams-state

USER 10001:10001
ENTRYPOINT ["java", "-jar", "/app/kstreams-emulator.jar"]
