#!/bin/bash
# Run on any client host. Snapshot of the running producer/consumer processes on
# THIS host and the tail of each log, so you can eyeball throughput/errors
# without pulling full logs back via pull-results.sh.
#
# Usage: ./04-load-status.sh [lines]
set -uo pipefail

LINES="${1:-5}"
PID_FILE=/opt/load-benchmark/load-pids.txt
RESULTS_DIR=/opt/load-benchmark/results

if [[ ! -f "$PID_FILE" ]]; then
  echo "No $PID_FILE found -- load not started on this host (or PID file missing)."
  exit 0
fi

echo "=== Process status ==="
while read -r role topic pid; do
  [[ -z "$pid" ]] && continue
  if kill -0 "$pid" 2>/dev/null; then
    echo "RUNNING  $role  $topic  (PID $pid)"
  else
    echo "STOPPED  $role  $topic  (PID $pid)"
  fi
done < "$PID_FILE"

echo
echo "=== Last ${LINES} lines per log ==="
for f in "$RESULTS_DIR"/producer-*.log "$RESULTS_DIR"/consumer-*.log; do
  [[ -f "$f" ]] || continue
  echo "--- $f ---"
  tail -n "$LINES" "$f"
  echo
done
