#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Quick console readout of one measurement window: per-broker averages of the
# key Amazon MSK broker metrics, peak IAM connections per broker, a snap.py
# summary, and the consumer groups with the highest EstimatedMaxTimeLag.
#
# Usage: ./measure.sh <label> <start-utc> [end-utc]
#   end-utc defaults to now. Times are ISO-8601 UTC, e.g. 2026-01-01T10:00:00Z.
# Environment:
#   CLUSTER_NAME  (default load-benchmark)
#   NUM_BROKERS   (default 3)
#   AWS_REGION / AWS_PROFILE are honored by the AWS CLI as usual.
set -euo pipefail

L="${1:?Usage: $0 <label> <start-utc> [end-utc]}"
S="${2:?Usage: $0 <label> <start-utc> [end-utc]}"
E="${3:-$(date -u +%FT%TZ)}"
CLUSTER_NAME="${CLUSTER_NAME:-load-benchmark}"
NUM_BROKERS="${NUM_BROKERS:-3}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CW=(aws cloudwatch --no-cli-pager)

echo "$L  $S -> $E  (cluster: $CLUSTER_NAME, brokers: $NUM_BROKERS)"

printf "%-32s" "metric \\ broker"
for b in $(seq 1 "$NUM_BROKERS"); do printf "%9s" "$b"; done; echo
for M in RequestHandlerAvgIdlePercent NetworkProcessorAvgIdlePercent CpuUser CpuSystem; do
  printf "%-32s" "$M"
  for b in $(seq 1 "$NUM_BROKERS"); do
    "${CW[@]}" get-metric-statistics --namespace AWS/Kafka --metric-name "$M" \
      --dimensions "Name=Cluster Name,Value=$CLUSTER_NAME" "Name=Broker ID,Value=$b" \
      --start-time "$S" --end-time "$E" --period 60 --statistics Average \
      --query 'Datapoints[].Average' --output json |
      python3 -c "import json,sys;v=json.load(sys.stdin);print(f'{sum(v)/len(v):9.3f}' if v else '      n/a',end='')"
  done
  echo
done

printf "%-32s" "IAM conns (max)"
for b in $(seq 1 "$NUM_BROKERS"); do
  "${CW[@]}" get-metric-statistics --namespace AWS/Kafka --metric-name ClientConnectionCount \
    --dimensions "Name=Cluster Name,Value=$CLUSTER_NAME" "Name=Broker ID,Value=$b" "Name=Client Authentication,Value=IAM" \
    --start-time "$S" --end-time "$E" --period 60 --statistics Sum \
    --query 'max(Datapoints[].Sum)' --output text | awk '{printf "%9s",$1}'
done
echo

CLUSTER_NAME="$CLUSTER_NAME" NUM_BROKERS="$NUM_BROKERS" \
  python3 "$SCRIPT_DIR/snap.py" "$L" "$S" "$E" | grep -E 'bytes|msgs|cpu|req_h|net_p|volume_q' || true

Q=$(python3 -c "
import json, sys
c = sys.argv[1]
print(json.dumps([{'Id': 'q', 'Period': 300, 'Expression':
  'SELECT MAX(EstimatedMaxTimeLag) FROM SCHEMA(\"AWS/Kafka\", \"Cluster Name\",\"Consumer Group\",Topic) '
  f'WHERE \"Cluster Name\" = \'{c}\' GROUP BY \"Consumer Group\" ORDER BY MAX() DESC LIMIT 6'}]))
" "$CLUSTER_NAME")
"${CW[@]}" get-metric-data --start-time "$S" --end-time "$E" --output json --metric-data-queries "$Q" |
  python3 -c "
import json, sys
for r in json.load(sys.stdin)['MetricDataResults']:
    v = sorted(zip(r['Timestamps'], r['Values']))
    print('lag', f\"{r['Label'][:30]:32s}\", ' '.join(f'{x:.0f}' for _, x in v))"
