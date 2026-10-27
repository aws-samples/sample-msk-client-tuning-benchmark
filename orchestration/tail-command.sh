#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Checks the status/output of a previously issued SSM command (e.g. one started
# with --background). If no command-id is given, uses the last one recorded by
# run-on-host.sh.
#
# Usage: ./tail-command.sh <stack-name> <region> <profile> [command-id]
set -euo pipefail

STACK_NAME="${1:?Usage: $0 <stack-name> <region> <profile> [command-id]}"
REGION="${2:?Usage: $0 <stack-name> <region> <profile> [command-id]}"
PROFILE="${3:?Usage: $0 <stack-name> <region> <profile> [command-id]}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMMAND_ID="${4:-$(cat "$SCRIPT_DIR/results/.last-command-id" 2>/dev/null || true)}"

if [[ -z "$COMMAND_ID" ]]; then
  echo "ERROR: no command-id given and no $SCRIPT_DIR/results/.last-command-id found." >&2
  exit 1
fi

AWSCLI=(aws --region "$REGION" --profile "$PROFILE")

OUTPUTS_FILE=$(mktemp)
trap 'rm -f "$OUTPUTS_FILE"' EXIT
"${AWSCLI[@]}" cloudformation describe-stacks --stack-name "$STACK_NAME" \
  --query 'Stacks[0].Outputs' --output json > "$OUTPUTS_FILE"
INSTANCE_ID=$(python3 -c "
import json
with open('$OUTPUTS_FILE') as f:
    outputs = json.load(f)
for o in outputs:
    if o['OutputKey'] == 'HostInstanceId':
        print(o['OutputValue'])
")

"${AWSCLI[@]}" ssm get-command-invocation \
  --command-id "$COMMAND_ID" --instance-id "$INSTANCE_ID" \
  --query '{Status:Status,Stdout:StandardOutputContent,Stderr:StandardErrorContent}' \
  --output json
