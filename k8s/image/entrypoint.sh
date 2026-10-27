#!/usr/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
# entrypoint.sh -- msk-bench-client container entrypoint.
#
# 1. Builds ONE effective client properties file:
#      mandatory MSK IAM auth props  (same 4 lines as /opt/kafka/config/client-iam.properties
#                                     on the EC2 hosts, infra/lib/client-host-stack.ts)
#    + client tuning                 (ConfigMap-mounted file, comments/blank lines stripped;
#                                     it must NOT set security.protocol, sasl.mechanism,
#                                     sasl.jaas.config or sasl.client.callback.handler.class,
#                                     same rule as scripts/0[1-8]-*.sh)
#    + EXTRA_PROPS                   (optional, newline-separated k=v, appended last)
# 2. execs `java -cp '/opt/kafka/libs/*' <Tool>.java <args>` built from env vars.
#
# Environment contract
#   Common:
#     ROLE                 producer | consumer                               (required)
#     BOOTSTRAP_SERVERS    host:9098,host:9098,...  (SASL_IAM bootstrap)     (required)
#     TOPICS               comma-separated topic list                        (required)
#     TUNING_FILE          default /etc/msk-bench/tuning/client-tuning.properties
#     EXTRA_PROPS          optional newline-separated overrides (appended last)
#     REPORT_INTERVAL_MS   default 60000
#     STAGGER_MS           default: tool default (producer 100, consumer 200)
#     JAVA_OPTS            default "-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
#     LOG4J_OPTS           default -Dlog4j.configuration=file:$KAFKA_HOME/config/tools-log4j.properties
#     WORK_DIR             default /tmp (effective props are written here)
#     POD_NAME             injected via the downward API (used for client ids)
#   ROLE=producer  (MultiProducer.java):
#     NUM_PRODUCERS            KafkaProducer instances in this pod             (required)
#     TOTAL_RECORDS_PER_SEC    records/s for THIS POD, split over its producers (required)
#     RECORD_SIZE              bytes                                            (required)
#     TOPIC_MODE               single | all            (default single)
#     CLIENT_PREFIX            default $POD_NAME (unique per pod)
#     PAYLOAD_POOL             default 4096
#   ROLE=consumer  (MultiGroupConsumer.java):
#     GROUP_PREFIX             group ids are <prefix>-<START_INDEX + i>          (required)
#     NUM_GROUPS               default 1 (one pod = one consumer group)
#     WORKER_THREADS           processing threads per group (default 0 = poll thread)
#     MEMBERS_PER_GROUP        KafkaConsumer members per group id in this pod (default 1)
#     START_INDEX              default 0
#     LATENCY_REPORT_SEC       E2E latency report interval in s (default 0 = off); prints
#                              E2E_LATENCY lines (now - record CreateTime) per group and stage
#   (NUM_GROUPS, not GROUPS: GROUPS is a reserved bash array variable.)
#
# Never silent: every problem prints "ERROR: ..." on stderr and exits non-zero.
set -euo pipefail

log() { echo "[entrypoint] $*" >&2; }
die() { echo "ERROR: [entrypoint] $*" >&2; exit 1; }
trap 'rc=$?; [[ $rc -ne 0 ]] && echo "ERROR: [entrypoint] failed (exit $rc) at line $LINENO" >&2' ERR

require() {
  local name="$1"
  [[ -n "${!name:-}" ]] || die "required environment variable $name is not set"
}
require_int() {
  local name="$1"
  require "$name"
  [[ "${!name}" =~ ^[0-9]+$ ]] || die "$name must be a non-negative integer, got '${!name}'"
}
require_number() {
  local name="$1"
  require "$name"
  [[ "${!name}" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "$name must be a number, got '${!name}'"
}

KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
MSK_BENCH_HOME="${MSK_BENCH_HOME:-/opt/msk-bench}"
TUNING_FILE="${TUNING_FILE:-/etc/msk-bench/tuning/client-tuning.properties}"
WORK_DIR="${WORK_DIR:-/tmp}"
REPORT_INTERVAL_MS="${REPORT_INTERVAL_MS:-60000}"
JAVA_OPTS="${JAVA_OPTS:--XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError}"
LOG4J_OPTS="${LOG4J_OPTS:--Dlog4j.configuration=file:${KAFKA_HOME}/config/tools-log4j.properties}"
POD_NAME="${POD_NAME:-${HOSTNAME:-unknown-pod}}"

require ROLE
require BOOTSTRAP_SERVERS
require TOPICS
require_int REPORT_INTERVAL_MS

# Refuse to start on an unfilled placeholder. The shipped ConfigMap value is
# REPLACE_ME_BOOTSTRAP_SERVERS (k8s/base/20-configmap-msk-bootstrap.yaml);
# PLACEHOLDER catches hand-written ones. Nothing else is treated as a placeholder.
case "$BOOTSTRAP_SERVERS" in
  *REPLACE_ME*|*PLACEHOLDER*)
    die "BOOTSTRAP_SERVERS still holds the placeholder ('$BOOTSTRAP_SERVERS'); fill ConfigMap msk-bootstrap (see k8s/README.md)";;
esac
[[ "$BOOTSTRAP_SERVERS" == *:9098* ]] \
  || log "WARNING: BOOTSTRAP_SERVERS has no :9098 endpoint; MSK IAM auth listens on 9098 (got '$BOOTSTRAP_SERVERS')"

[[ -d "$KAFKA_HOME/libs" ]] || die "Kafka libs not found at $KAFKA_HOME/libs"
ls "$KAFKA_HOME"/libs/aws-msk-iam-auth-*.jar >/dev/null 2>&1 \
  || die "aws-msk-iam-auth jar missing from $KAFKA_HOME/libs"
[[ -r "$TUNING_FILE" ]] \
  || die "tuning file not readable: $TUNING_FILE (is ConfigMap client-tuning-<profile> mounted?)"
[[ -d "$WORK_DIR" && -w "$WORK_DIR" ]] || die "WORK_DIR $WORK_DIR is not a writable directory"

# The IAM auth block is fixed. The tuning file and EXTRA_PROPS must not set any
# of its keys (same rule as the EC2 host scripts, scripts/lib-msk-env.sh).
IAM_KEYS_RE='^[[:space:]]*(security\.protocol|sasl\.mechanism|sasl\.jaas\.config|sasl\.client\.callback\.handler\.class)[[:space:]]*[=:]'
if bad="$(grep -nE "$IAM_KEYS_RE" "$TUNING_FILE")"; then
  die "tuning file $TUNING_FILE sets MSK IAM auth keys (security.protocol, sasl.mechanism, sasl.jaas.config, sasl.client.callback.handler.class are fixed by the image); remove them: ${bad//$'\n'/; }"
fi
if [[ -n "${EXTRA_PROPS:-}" ]] && bad="$(printf '%s\n' "$EXTRA_PROPS" | grep -nE "$IAM_KEYS_RE")"; then
  die "EXTRA_PROPS sets MSK IAM auth keys (security.protocol, sasl.mechanism, sasl.jaas.config, sasl.client.callback.handler.class are fixed by the image); remove them: ${bad//$'\n'/; }"
fi

EFFECTIVE_PROPS="$WORK_DIR/client-effective.properties"
{
  echo "# Generated by entrypoint.sh at $(date -u +%Y-%m-%dT%H:%M:%SZ) for pod $POD_NAME (ROLE=$ROLE)"
  echo "# ---- MSK IAM auth (mandatory) ----"
  echo "security.protocol=SASL_SSL"
  echo "sasl.mechanism=AWS_MSK_IAM"
  echo "sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;"
  echo "sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler"
  echo "# ---- client tuning: $TUNING_FILE ----"
  # grep exits 1 when nothing matches; an all-comment tuning file is legal.
  grep -Ev '^[[:space:]]*(#|!|$)' "$TUNING_FILE" || true
  if [[ -n "${EXTRA_PROPS:-}" ]]; then
    echo "# ---- EXTRA_PROPS (env) ----"
    printf '%s\n' "$EXTRA_PROPS" | grep -Ev '^[[:space:]]*(#|!|$)' || true
  fi
} > "$EFFECTIVE_PROPS" || die "could not write $EFFECTIVE_PROPS"

# Defense in depth: the effective file must still end up with IAM auth.
for key in security.protocol sasl.mechanism; do
  last="$(grep -E "^[[:space:]]*${key//./\\.}[[:space:]]*[=:]" "$EFFECTIVE_PROPS" | tail -n1 | sed 's/^[^=:]*[=:][[:space:]]*//')"
  case "$key=$last" in
    security.protocol=SASL_SSL|sasl.mechanism=AWS_MSK_IAM) ;;
    *) die "tuning/EXTRA_PROPS overrides $key to '$last'; MSK IAM auth needs SASL_SSL / AWS_MSK_IAM";;
  esac
done

ARGS=(--bootstrap "$BOOTSTRAP_SERVERS" --topics "$TOPICS" --config "$EFFECTIVE_PROPS"
      --report-interval-ms "$REPORT_INTERVAL_MS")
if [[ -n "${STAGGER_MS:-}" ]]; then
  require_int STAGGER_MS
  ARGS+=(--stagger-ms "$STAGGER_MS")
fi

case "$ROLE" in
  producer)
    TOOL=MultiProducer
    require_int NUM_PRODUCERS
    require_number TOTAL_RECORDS_PER_SEC
    require_int RECORD_SIZE
    TOPIC_MODE="${TOPIC_MODE:-single}"
    [[ "$TOPIC_MODE" == single || "$TOPIC_MODE" == all ]] \
      || die "TOPIC_MODE must be single or all, got '$TOPIC_MODE'"
    CLIENT_PREFIX="${CLIENT_PREFIX:-$POD_NAME}"
    ARGS+=(--producers "$NUM_PRODUCERS" --total-records-per-sec "$TOTAL_RECORDS_PER_SEC"
           --record-size "$RECORD_SIZE" --topic-mode "$TOPIC_MODE" --client-prefix "$CLIENT_PREFIX")
    if [[ -n "${PAYLOAD_POOL:-}" ]]; then
      require_int PAYLOAD_POOL
      ARGS+=(--payload-pool "$PAYLOAD_POOL")
    fi
    ;;
  consumer)
    TOOL=MultiGroupConsumer
    require GROUP_PREFIX
    NUM_GROUPS="${NUM_GROUPS:-1}"
    START_INDEX="${START_INDEX:-0}"
    require_int NUM_GROUPS
    require_int START_INDEX
    ARGS+=(--group-prefix "$GROUP_PREFIX" --groups "$NUM_GROUPS" --start-index "$START_INDEX")
    if [[ -n "${WORKER_THREADS:-}" ]]; then require_int WORKER_THREADS; ARGS+=(--worker-threads "$WORKER_THREADS"); fi
    if [[ -n "${LATENCY_REPORT_SEC:-}" ]]; then require_int LATENCY_REPORT_SEC; ARGS+=(--latency-report-sec "$LATENCY_REPORT_SEC"); fi
    if [[ -n "${MEMBERS_PER_GROUP:-}" ]]; then require_int MEMBERS_PER_GROUP; ARGS+=(--members-per-group "$MEMBERS_PER_GROUP" --member-tag "${POD_NAME##*-}-"); fi
    ;;
  *)
    die "ROLE must be 'producer' or 'consumer', got '$ROLE'"
    ;;
esac

SRC="$MSK_BENCH_HOME/$TOOL.java"
[[ -r "$SRC" ]] || die "tool source not found: $SRC"

log "pod=$POD_NAME role=$ROLE tool=$TOOL tuning=$TUNING_FILE"
log "effective properties ($EFFECTIVE_PROPS):"
sed 's/^/[entrypoint]   /' "$EFFECTIVE_PROPS" >&2
log "exec java $JAVA_OPTS $LOG4J_OPTS -cp '$KAFKA_HOME/libs/*' $SRC ${ARGS[*]}"

trap - ERR
# shellcheck disable=SC2086  # JAVA_OPTS / LOG4J_OPTS are intentionally word-split
exec java $JAVA_OPTS $LOG4J_OPTS -cp "$KAFKA_HOME/libs/*" "$SRC" "${ARGS[@]}"
