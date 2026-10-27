#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Pushes /opt/load-benchmark/results from a client host up to S3, then syncs it
# down to the local results/ dir. Safe to run any time (e.g. to check on a
# --background job started by run-on-host.sh).
#
# Usage: ./pull-results.sh <stack-name> <region> <profile>
set -euo pipefail

STACK_NAME="${1:?Usage: $0 <stack-name> <region> <profile>}"
REGION="${2:?Usage: $0 <stack-name> <region> <profile>}"
PROFILE="${3:?Usage: $0 <stack-name> <region> <profile>}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$LOCAL_RESULTS_DIR"

AWSCLI=(aws --region "$REGION" --profile "$PROFILE")

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

echo "==> Pushing host results -> s3://$BUCKET/results/"
SEND_COMMAND_INPUT=$(mktemp)
trap 'rm -f "$OUTPUTS_FILE" "$SEND_COMMAND_INPUT"' EXIT
python3 -c "
import json
with open('$SEND_COMMAND_INPUT', 'w') as f:
    json.dump({
        'InstanceIds': ['$INSTANCE_ID'],
        'DocumentName': 'AWS-RunShellScript',
        'Comment': 'msk-load-benchmark: push results to s3',
        'Parameters': {'commands': ['mkdir -p /opt/load-benchmark/results && aws s3 sync /opt/load-benchmark/results/ s3://$BUCKET/results/ --only-show-errors']},
    }, f)
"

COMMAND_ID=$("${AWSCLI[@]}" ssm send-command \
  --cli-input-json "file://$SEND_COMMAND_INPUT" \
  --output text --query 'Command.CommandId')

while true; do
  STATUS=$("${AWSCLI[@]}" ssm get-command-invocation \
    --command-id "$COMMAND_ID" --instance-id "$INSTANCE_ID" \
    --query 'Status' --output text 2>/dev/null || echo "Pending")
  case "$STATUS" in
    Success|Failed|Cancelled|TimedOut) break ;;
    *) sleep 3 ;;
  esac
done

if [[ "$STATUS" != "Success" ]]; then
  echo "WARNING: push-to-s3 command status was $STATUS" >&2
  "${AWSCLI[@]}" ssm get-command-invocation \
    --command-id "$COMMAND_ID" --instance-id "$INSTANCE_ID" \
    --query 'StandardErrorContent' --output text >&2
fi

# Skip the live, uncompressed *.log files: over a multi-day run each one grows
# to ~10 GB and syncing them all fills the local disk. Rotated *.gz files and
# other result files are still pulled. To fetch one live log on purpose:
#   aws s3 cp s3://$BUCKET/results/<name>.log ./results/
echo "==> Syncing s3://$BUCKET/results/ -> $LOCAL_RESULTS_DIR/ (excluding live *.log)"
"${AWSCLI[@]}" s3 sync "s3://$BUCKET/results/" "$LOCAL_RESULTS_DIR/" \
  --exclude "*.log" --only-show-errors

echo "==> Done. Local results in $LOCAL_RESULTS_DIR/"
ls -la "$LOCAL_RESULTS_DIR/"
