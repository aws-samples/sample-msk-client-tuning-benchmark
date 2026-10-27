#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Runs a host-side script on a client host (producer or consumer) via SSM,
# without an interactive session:
#   1. Syncs scripts/ up to the staging S3 bucket
#   2. Tells the host (via SSM RunShellScript) to pull scripts/ down from S3
#   3. Runs the requested script with the given args, streaming output back
#   4. Pushes anything written to /opt/load-benchmark/results back up to S3 and
#      pulls it down locally to ./results/
#
# Usage:
#   ./run-on-host.sh <stack-name> <region> <profile> <script-name> [script-args...]
#
# Runs commands through AWS Systems Manager Run Command (AWS-RunShellScript).
#
# Examples:
#   ./run-on-host.sh LoadBenchmarkProducerStack us-east-1 default 00-setup-topics.sh <cluster-arn>
#   ./run-on-host.sh LoadBenchmarkProducerStack us-east-1 default 01-start-producers.sh --background <cluster-arn>
#
# For long-running scripts (01-start-producers.sh, 02-start-consumers.sh) pass
# --background as the first script-arg to launch it detached (nohup) on the host
# and return immediately; check on it later with ./tail-command.sh.
set -euo pipefail

STACK_NAME="${1:?Usage: $0 <stack-name> <region> <profile> <script-name> [args...]}"
REGION="${2:?Usage: $0 <stack-name> <region> <profile> <script-name> [args...]}"
PROFILE="${3:?Usage: $0 <stack-name> <region> <profile> <script-name> [args...]}"
SCRIPT_NAME="${4:?Usage: $0 <stack-name> <region> <profile> <script-name> [args...]}"
shift 4
SCRIPT_ARGS=("$@")

RUN_BACKGROUND=false
if [[ "${SCRIPT_ARGS[0]:-}" == "--background" ]]; then
  RUN_BACKGROUND=true
  SCRIPT_ARGS=("${SCRIPT_ARGS[@]:1}")
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_SCRIPTS_DIR="$SCRIPT_DIR/scripts"
LOCAL_RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$LOCAL_RESULTS_DIR"

if [[ ! -f "$LOCAL_SCRIPTS_DIR/$SCRIPT_NAME" ]]; then
  echo "ERROR: $LOCAL_SCRIPTS_DIR/$SCRIPT_NAME not found." >&2
  exit 1
fi

AWSCLI=(aws --region "$REGION" --profile "$PROFILE")

echo "==> Looking up stack outputs for $STACK_NAME..."
OUTPUTS_FILE=$(mktemp)
trap 'rm -f "$OUTPUTS_FILE"' EXIT
"${AWSCLI[@]}" cloudformation describe-stacks --stack-name "$STACK_NAME" \
  --query 'Stacks[0].Outputs' --output json > "$OUTPUTS_FILE"

get_output() {
  python3 -c "
import json, sys
with open('$OUTPUTS_FILE') as f:
    outputs = json.load(f)
for o in outputs:
    if o['OutputKey'] == sys.argv[1]:
        print(o['OutputValue'])
        sys.exit(0)
sys.exit(1)
" "$1"
}

BUCKET=$(get_output ScriptsBucketName)
INSTANCE_ID=$(get_output HostInstanceId)

if [[ -z "$BUCKET" || -z "$INSTANCE_ID" ]]; then
  echo "ERROR: could not resolve ScriptsBucketName / HostInstanceId from stack outputs." >&2
  exit 1
fi

echo "==> Bucket: $BUCKET   Instance: $INSTANCE_ID"

echo "==> Syncing local scripts/ -> s3://$BUCKET/scripts/"
"${AWSCLI[@]}" s3 sync "$LOCAL_SCRIPTS_DIR" "s3://$BUCKET/scripts/" --exclude "README.md" --only-show-errors

# Build the remote command: pull scripts, chmod, then run the requested one.
QUOTED_ARGS=""
for a in "${SCRIPT_ARGS[@]+"${SCRIPT_ARGS[@]}"}"; do
  QUOTED_ARGS+=" $(printf '%q' "$a")"
done

if $RUN_BACKGROUND; then
  RUN_LINE="nohup /opt/load-benchmark/scripts/$SCRIPT_NAME$QUOTED_ARGS > /opt/load-benchmark/results/${SCRIPT_NAME%.sh}.out 2>&1 & echo \"Started PID \$! (log: /opt/load-benchmark/results/${SCRIPT_NAME%.sh}.out)\""
else
  RUN_LINE="/opt/load-benchmark/scripts/$SCRIPT_NAME$QUOTED_ARGS"
fi

# Optional: forward MSK_ENDPOINT_URL to the host to point the host-side
# `aws kafka` calls at a non-default Amazon MSK endpoint. Leave it unset to use
# the standard regional endpoint. See scripts/lib-msk-env.sh.
ENV_LINE=""
if [[ -n "${MSK_ENDPOINT_URL:-}" ]]; then
  ENV_LINE="export MSK_ENDPOINT_URL=$(printf '%q' "$MSK_ENDPOINT_URL")"
  echo "==> Forwarding MSK_ENDPOINT_URL=$MSK_ENDPOINT_URL to the host"
fi

REMOTE_COMMANDS=$(cat <<EOF
set -e
$ENV_LINE
aws s3 sync s3://$BUCKET/scripts/ /opt/load-benchmark/scripts/ --only-show-errors
chmod +x /opt/load-benchmark/scripts/*.sh
cd /opt/load-benchmark/results
$RUN_LINE
EOF
)

echo "==> Sending command to host ($SCRIPT_NAME ${SCRIPT_ARGS[*]:-})..."
SEND_COMMAND_INPUT=$(mktemp)
trap 'rm -f "$OUTPUTS_FILE" "$SEND_COMMAND_INPUT"' EXIT
python3 -c "
import json, sys
with open('$SEND_COMMAND_INPUT', 'w') as f:
    json.dump({
        'InstanceIds': ['$INSTANCE_ID'],
        'DocumentName': 'AWS-RunShellScript',
        'Comment': 'msk-load-benchmark: $SCRIPT_NAME'[:100],
        'Parameters': {'commands': [sys.stdin.read()]},
    }, f)
" <<< "$REMOTE_COMMANDS"

COMMAND_ID=$("${AWSCLI[@]}" ssm send-command \
  --cli-input-json "file://$SEND_COMMAND_INPUT" \
  --output text --query 'Command.CommandId')

echo "==> Command ID: $COMMAND_ID"
echo "$COMMAND_ID" > "$LOCAL_RESULTS_DIR/.last-command-id"

if $RUN_BACKGROUND; then
  echo "==> Background launch requested; not waiting. Check status with:"
  echo "    ./orchestration/tail-command.sh $STACK_NAME $REGION $PROFILE $COMMAND_ID"
  exit 0
fi

echo "==> Waiting for command to finish (polling every 5s)..."
while true; do
  STATUS=$("${AWSCLI[@]}" ssm get-command-invocation \
    --command-id "$COMMAND_ID" --instance-id "$INSTANCE_ID" \
    --query 'Status' --output text 2>/dev/null || echo "Pending")
  case "$STATUS" in
    Success|Failed|Cancelled|TimedOut) break ;;
    *) sleep 5 ;;
  esac
done

echo "==> Status: $STATUS"
echo "----- stdout -----"
"${AWSCLI[@]}" ssm get-command-invocation \
  --command-id "$COMMAND_ID" --instance-id "$INSTANCE_ID" \
  --query 'StandardOutputContent' --output text
echo "----- stderr -----"
"${AWSCLI[@]}" ssm get-command-invocation \
  --command-id "$COMMAND_ID" --instance-id "$INSTANCE_ID" \
  --query 'StandardErrorContent' --output text

echo "==> Pulling back /opt/load-benchmark/results/ -> $LOCAL_RESULTS_DIR/"
"$SCRIPT_DIR/orchestration/pull-results.sh" "$STACK_NAME" "$REGION" "$PROFILE"

if [[ "$STATUS" != "Success" ]]; then
  echo "Command did not succeed (status: $STATUS)." >&2
  exit 1
fi
