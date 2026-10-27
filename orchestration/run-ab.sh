#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Runs a client-profile A/B end to end on the Amazon EC2 harness, from your laptop.
# For each profile, in order A then B:
#   1. copies scripts/profiles/<p>.properties over scripts/client-tuning.properties
#   2. starts producers (01) on the producer host and one consumer group (02) on the
#      first consumer host, both through run-on-host.sh --background
#   3. sleeps the warm-up, records the UTC start, sleeps the measurement window,
#      records the UTC end
#   4. stops the load (03) on both hosts
#   5. runs analysis/measure_cluster.py with label <p>-<timestamp>
# Then it writes analysis/compare.py <label-a> <label-b> --format markdown to
# results/ab-<timestamp>.md and restores the original scripts/client-tuning.properties
# (also on Ctrl-C or any error, through a trap; on Ctrl-C it also stops the load).
#
# Usage:
#   ./orchestration/run-ab.sh <cluster-arn> <profile-a> <profile-b>
#       [--mbps 100] [--topics 12] [--warmup-min 15] [--measure-min 30]
#       [--region REGION] [--aws-profile PROFILE] [--brokers N]
#       [--producer-stack LoadBenchmarkProducerStack] [--consumer-stack LoadBenchmarkConsumerStack-0]
#       [--group bench-cg-0] [--dry-run]
#
# Examples:
#   # Codec A/B: zstd (aws-tuned) vs snappy, everything else identical. Print the plan first:
#   ./orchestration/run-ab.sh "$CLUSTER_ARN" aws-tuned aws-tuned-snappy --dry-run
#   ./orchestration/run-ab.sh "$CLUSTER_ARN" aws-tuned aws-tuned-snappy
#   # Your production settings vs the recommended profile:
#   ./orchestration/run-ab.sh "$CLUSTER_ARN" baseline aws-tuned --mbps 150 --measure-min 30
#
# A profile name <p> maps to scripts/profiles/<p>.properties. Profiles that set
# authentication keys are refused (same rule as the host-side start scripts).
# --region defaults to the Region in the cluster ARN; --aws-profile defaults to
# $AWS_PROFILE or "default"; --brokers (passed to measure_cluster.py as NUM_BROKERS)
# defaults to $NUM_BROKERS or 3. CW_SETTLE_SEC (default 180) is how long to wait after
# a window ends before reading Amazon CloudWatch, so the last minutes have arrived.
#
# One A/B at the defaults takes about 2 x (15 + 30 + 3) minutes plus the SSM round trips.
# Test clusters only: it drives sustained load. Never point it at a production cluster.
#
# Exit codes: 0 = A/B ran and compare.py reported OK; 1 = compare.py reported SUSPECT
# (or a step failed); 2 = usage or input error.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SELF="$REPO_ROOT/orchestration/$(basename "${BASH_SOURCE[0]}")"
# Work from the repo root so every printed command uses short relative paths.
cd "$REPO_ROOT"
ACTIVE_PROPS=scripts/client-tuning.properties
PROFILES_DIR=scripts/profiles
RUN_ON_HOST=./orchestration/run-on-host.sh
MEASURE=analysis/measure_cluster.py
COMPARE=analysis/compare.py
RESULTS_DIR=results

usage() {
  sed -n '/^# Usage:/,/^# (or a step failed)/p' "$SELF" | sed 's/^# \{0,1\}//' >&2
}
die_usage() { echo "ERROR: $*" >&2; echo "Run with --help for usage." >&2; exit 2; }

# ---- Parse arguments ------------------------------------------------------
POSITIONAL=()
MBPS=100 TOPICS=12 WARMUP_MIN=15 MEASURE_MIN=30
REGION="" AWS_PROFILE_NAME="${AWS_PROFILE:-default}" BROKERS="${NUM_BROKERS:-3}"
PRODUCER_STACK=LoadBenchmarkProducerStack CONSUMER_STACK=LoadBenchmarkConsumerStack-0
GROUP=bench-cg-0 DRY_RUN=false
CW_SETTLE_SEC="${CW_SETTLE_SEC:-180}"
TOPIC_PREFIX=load-bench

need_value() { [[ $# -ge 2 && -n "$2" ]] || die_usage "$1 needs a value"; }
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --mbps) need_value "$@"; MBPS="$2"; shift 2 ;;
    --topics) need_value "$@"; TOPICS="$2"; shift 2 ;;
    --warmup-min) need_value "$@"; WARMUP_MIN="$2"; shift 2 ;;
    --measure-min) need_value "$@"; MEASURE_MIN="$2"; shift 2 ;;
    --region) need_value "$@"; REGION="$2"; shift 2 ;;
    --aws-profile) need_value "$@"; AWS_PROFILE_NAME="$2"; shift 2 ;;
    --brokers) need_value "$@"; BROKERS="$2"; shift 2 ;;
    --producer-stack) need_value "$@"; PRODUCER_STACK="$2"; shift 2 ;;
    --consumer-stack) need_value "$@"; CONSUMER_STACK="$2"; shift 2 ;;
    --group) need_value "$@"; GROUP="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --) shift; POSITIONAL+=("$@"); break ;;
    -*) die_usage "unknown option: $1" ;;
    *) POSITIONAL+=("$1"); shift ;;
  esac
done
[[ ${#POSITIONAL[@]} -eq 3 ]] || { usage; die_usage "expected <cluster-arn> <profile-a> <profile-b>"; }
CLUSTER_ARN="${POSITIONAL[0]}" PROFILE_A="${POSITIONAL[1]}" PROFILE_B="${POSITIONAL[2]}"

# ---- Validate every input before anything runs ----------------------------
# Same character rules as analysis/measure_cluster.py (_PATTERNS) for cluster name, Region and profile.
ARN_RE='^arn:aws[a-z-]*:kafka:([a-z]{2}(-[a-z]+)+-[0-9]):[0-9]{12}:cluster/([A-Za-z0-9][A-Za-z0-9-]{0,63})/[A-Za-z0-9-]+$'
[[ "$CLUSTER_ARN" =~ $ARN_RE ]] || die_usage "invalid cluster ARN (expected arn:aws:kafka:<region>:<account>:cluster/<name>/<uuid>)"
ARN_REGION="${BASH_REMATCH[1]}"
CLUSTER_NAME="${BASH_REMATCH[3]}"
REGION="${REGION:-$ARN_REGION}"
[[ "$REGION" =~ ^[a-z]{2}(-[a-z]+)+-[0-9]$ ]] || die_usage "invalid --region: $REGION"
[[ "$REGION" == "$ARN_REGION" ]] || die_usage "--region $REGION does not match the cluster ARN's Region $ARN_REGION"
[[ "$AWS_PROFILE_NAME" =~ ^[A-Za-z0-9._-]{1,64}$ ]] || die_usage "invalid --aws-profile: $AWS_PROFILE_NAME"
for pair in "mbps:$MBPS" "topics:$TOPICS" "warmup-min:$WARMUP_MIN" "measure-min:$MEASURE_MIN" \
            "brokers:$BROKERS" "CW_SETTLE_SEC:$CW_SETTLE_SEC"; do
  name="${pair%%:*}" value="${pair#*:}"
  [[ "$value" =~ ^[0-9]{1,6}$ ]] || die_usage "--$name must be a non-negative integer, got '$value'"
done
for pair in "mbps:$MBPS" "topics:$TOPICS" "measure-min:$MEASURE_MIN" "brokers:$BROKERS"; do
  name="${pair%%:*}" value="${pair#*:}"
  (( 10#$value > 0 )) || die_usage "--$name must be greater than 0"
done
[[ "$PRODUCER_STACK" =~ ^[A-Za-z][A-Za-z0-9-]{0,127}$ ]] || die_usage "invalid --producer-stack: $PRODUCER_STACK"
[[ "$CONSUMER_STACK" =~ ^[A-Za-z][A-Za-z0-9-]{0,127}$ ]] || die_usage "invalid --consumer-stack: $CONSUMER_STACK"
[[ "$GROUP" =~ ^[A-Za-z0-9._-]{1,200}$ ]] || die_usage "invalid --group: $GROUP"

# shellcheck source=../scripts/lib-msk-env.sh
source scripts/lib-msk-env.sh 2>/dev/null
for p in "$PROFILE_A" "$PROFILE_B"; do
  [[ "$p" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ ]] \
    || die_usage "invalid profile name '$p' (use the name only, e.g. aws-tuned for scripts/profiles/aws-tuned.properties)"
  [[ -f "$PROFILES_DIR/$p.properties" ]] || die_usage "profile file not found: scripts/profiles/$p.properties"
  # msk_check_tuning_props exits 1 on auth keys; run it in a subshell so we can report and exit 2.
  ( msk_check_tuning_props "$PROFILES_DIR/$p.properties" ) \
    || die_usage "profile '$p' sets authentication keys; remove them (auth comes from the IAM auth file)"
done
for f in "$RUN_ON_HOST" "$MEASURE" "$COMPARE"; do
  [[ -f "$f" ]] || die_usage "missing $f"
done

TS="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ "$PROFILE_A" == "$PROFILE_B" ]]; then
  # A/A run (noise check): keep the two labels distinct.
  LABEL_A="$PROFILE_A-A-$TS" LABEL_B="$PROFILE_B-B-$TS"
else
  LABEL_A="$PROFILE_A-$TS" LABEL_B="$PROFILE_B-$TS"
fi
REPORT="$RESULTS_DIR/ab-$TS.md"

# ---- Helpers --------------------------------------------------------------
# run <cmd...>: print the command (shell-quoted), then run it unless --dry-run.
run() {
  printf '+'; printf ' %q' "$@"; printf '\n'
  $DRY_RUN || "$@"
}
pause() {  # pause <seconds> <why>
  echo "==> Waiting $1 s ($2)"
  run sleep "$1"
}
utc_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
on_host() {  # on_host <stack> <script> [args...]
  local stack="$1"; shift
  run "$RUN_ON_HOST" "$stack" "$REGION" "$AWS_PROFILE_NAME" "$@"
}
stop_load() {
  on_host "$PRODUCER_STACK" 03-stop-load.sh
  on_host "$CONSUMER_STACK" 03-stop-load.sh
}

# ---- Clean up on exit, error or Ctrl-C ---------------------------------------
# Always restore the original scripts/client-tuning.properties; if the script stops while load is
# running (Ctrl-C or a failed step), also stop the load on both hosts, best effort.
[[ -f "$ACTIVE_PROPS" ]] || die_usage "missing $ACTIVE_PROPS (restore it with git checkout first)"
BACKUP=""
LOAD_RUNNING=false
if ! $DRY_RUN; then
  BACKUP="$(mktemp "${TMPDIR:-/tmp}/client-tuning.properties.XXXXXX")"
  cp -p "$ACTIVE_PROPS" "$BACKUP"
fi
cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  set +e
  if $LOAD_RUNNING && ! $DRY_RUN; then
    echo "==> Stopping load on both hosts (best effort)..." >&2
    stop_load || echo "WARN: stopping load failed; run 03-stop-load.sh on both hosts yourself." >&2
  fi
  if [[ -n "$BACKUP" && -f "$BACKUP" ]]; then
    cp -p "$BACKUP" "$ACTIVE_PROPS" && rm -f "$BACKUP" \
      && echo "==> Restored the original scripts/client-tuning.properties" >&2
  fi
  exit "$rc"
}
trap cleanup EXIT
trap 'echo; echo "==> Interrupted." >&2; exit 130' INT TERM

# ---- Plan -----------------------------------------------------------------
echo "==> A/B on cluster $CLUSTER_NAME ($REGION, AWS profile $AWS_PROFILE_NAME)$($DRY_RUN && echo ' [DRY RUN: nothing is executed]')"
echo "    A = $PROFILE_A (label $LABEL_A)   B = $PROFILE_B (label $LABEL_B)"
echo "    load: $MBPS MB/s over $TOPICS topics; warm-up $WARMUP_MIN min; window $MEASURE_MIN min; $BROKERS brokers"
echo "    hosts: producer $PRODUCER_STACK, consumer $CONSUMER_STACK (group $GROUP)"
echo "    report: results/ab-$TS.md"

# ---- One leg per profile --------------------------------------------------
run_leg() {  # run_leg <profile> <label>
  local profile="$1" label="$2" start end
  echo
  echo "======== Profile $profile (label $label) ========"
  run cp "$PROFILES_DIR/$profile.properties" "$ACTIVE_PROPS"
  LOAD_RUNNING=true
  on_host "$PRODUCER_STACK" 01-start-producers.sh --background "$CLUSTER_ARN" "$TOPIC_PREFIX" "$TOPICS" "$MBPS"
  on_host "$CONSUMER_STACK" 02-start-consumers.sh --background "$CLUSTER_ARN" "$TOPIC_PREFIX" "$TOPICS" "$GROUP"
  pause $((10#$WARMUP_MIN * 60)) "warm-up, $WARMUP_MIN min"
  if $DRY_RUN; then start="<start-utc>"; else start="$(utc_now)"; fi
  echo "==> Window start: $start"
  pause $((10#$MEASURE_MIN * 60)) "measurement window, $MEASURE_MIN min"
  if $DRY_RUN; then end="<end-utc>"; else end="$(utc_now)"; fi
  echo "==> Window end:   $end"
  stop_load
  LOAD_RUNNING=false
  pause "$CW_SETTLE_SEC" "let the last CloudWatch datapoints arrive"
  run env NUM_BROKERS="$BROKERS" python3 "$MEASURE" "$CLUSTER_NAME" "$label" "$start" "$end" \
    --region "$REGION" --profile "$AWS_PROFILE_NAME"
}

run_leg "$PROFILE_A" "$LABEL_A"
run_leg "$PROFILE_B" "$LABEL_B"

# ---- Compare --------------------------------------------------------------
echo
echo "======== Compare ========"
run mkdir -p "$RESULTS_DIR"
printf '+ %q %q %q %q --format markdown > %q\n' python3 "$COMPARE" "$LABEL_A" "$LABEL_B" "$REPORT"
rc=0
if ! $DRY_RUN; then
  python3 "$COMPARE" "$LABEL_A" "$LABEL_B" --format markdown > "$REPORT" || rc=$?
  cat "$REPORT"
  echo
  case "$rc" in
    0) echo "==> Comparison OK. Report: $REPORT" ;;
    1) echo "==> Comparison SUSPECT: fix the run before reading the numbers. Report: $REPORT" ;;
    *) echo "ERROR: compare.py failed (exit $rc)" >&2; rc=1 ;;
  esac
fi
exit "$rc"
