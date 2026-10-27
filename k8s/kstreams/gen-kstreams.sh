#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# gen-kstreams.sh <alos|eos> <num-apps> [input-topic] [tuning-profile] > manifests.yaml
# One Deployment (1 pod, 1 stream thread) per Kafka Streams app, each with its own
# application.id (= consumer group id + internal-topic prefix). Topology: selectKey ->
# repartition -> count (RocksDB) -> changelog (kstreams-emulator/README.md).
# Default input topic bcast-1. Keep its partition count small: every app adds one
# repartition and one changelog topic with the same partition count as the input.
# commit.interval.ms is left at the Kafka default (ALOS 30000, EOS 100).
# tuning-profile (default aws-tuned) selects ConfigMap client-tuning-<profile>.
# Image: set IMAGE, or AWS_ACCOUNT_ID / AWS_REGION to build the default ECR URI. A WARNING is
# printed on stderr while the image still holds the placeholder account 111122223333.
set -euo pipefail
MODE=${1:?alos|eos}; N=${2:?num-apps}; TOPIC=${3:-bcast-1}; TUNING=${4:-aws-tuned}
[[ "$TUNING" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || { echo "bad tuning profile name: $TUNING" >&2; exit 1; }
IMAGE=${IMAGE:-${AWS_ACCOUNT_ID:-111122223333}.dkr.ecr.${AWS_REGION:-us-east-1}.amazonaws.com/kstreams-emulator:latest}
[[ "$IMAGE" == *111122223333* ]] && echo "WARNING: image $IMAGE contains the placeholder account 111122223333; set IMAGE=<your ECR URI> (or AWS_ACCOUNT_ID / AWS_REGION), see eks/ecr.md" >&2
case $MODE in alos) PG=at_least_once;; eos) PG=exactly_once_v2;; *) echo "bad mode" >&2; exit 1;; esac
TS=$(date -u +%m%d%H%M)
for i in $(seq 0 $((N-1))); do
APP="kse-$MODE-$TS-$i"
cat <<Y
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kse-$MODE-$i
  namespace: msk-bench
  labels: {app.kubernetes.io/part-of: msk-bench, role: kstreams, guarantee: $MODE}
spec:
  replicas: 1
  selector: {matchLabels: {app: kse-$MODE-$i}}
  template:
    metadata: {labels: {app: kse-$MODE-$i, app.kubernetes.io/part-of: msk-bench, role: kstreams, guarantee: $MODE}}
    spec:
      serviceAccountName: msk-bench-client
      containers:
      - name: kse
        image: $IMAGE
        envFrom: [{configMapRef: {name: msk-bootstrap}}]
        env:
        - {name: INPUT_TOPIC, value: "$TOPIC"}
        - {name: APPLICATION_ID, value: "$APP"}
        - {name: PROCESSING_GUARANTEE, value: "$PG"}
        - {name: CLIENT_PROPS_FILE, value: /cfg/client-tuning.properties}
        - {name: STATE_DIR, value: /tmp/kstreams-state}
        - {name: JAVA_TOOL_OPTIONS, value: "-XX:MaxRAMPercentage=60 -XX:+ExitOnOutOfMemoryError"}
        resources: {requests: {cpu: 250m, memory: 768Mi}, limits: {cpu: "1", memory: 768Mi}}
        volumeMounts: [{name: cfg, mountPath: /cfg}, {name: tmp, mountPath: /tmp}]
      volumes:
      - {name: cfg, configMap: {name: client-tuning-$TUNING}}
      - {name: tmp, emptyDir: {}}
Y
done
