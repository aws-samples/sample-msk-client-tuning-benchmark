#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Run on any client host. Stops all producer/consumer processes started on THIS
# host (reads PIDs from /opt/load-benchmark/load-pids.txt). Safe to run even if
# some processes already exited. Does not affect other hosts in the fleet.
#
# Usage: ./03-stop-load.sh
set -uo pipefail

PID_FILE=/opt/load-benchmark/load-pids.txt

if [[ ! -f "$PID_FILE" ]]; then
  echo "No $PID_FILE found -- nothing to stop on this host."
  exit 0
fi

while read -r role topic pid; do
  [[ -z "$pid" ]] && continue
  if kill -0 "$pid" 2>/dev/null; then
    echo "Stopping $role for $topic (PID $pid)..."
    kill "$pid" 2>/dev/null || true
  else
    echo "$role for $topic (PID $pid) already stopped."
  fi
done < "$PID_FILE"

sleep 2
echo "Done. Remaining kafka-perf-test processes on this host:"
pgrep -af 'kafka-.*-perf-test' || echo "  (none)"
