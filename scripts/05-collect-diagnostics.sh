#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Run on a client host (via run-on-host.sh). READ-ONLY diagnostic snapshot to
# explain consumer lag / client bottlenecks before resizing the host:
#   - CPU (per core), memory, load, OOM kills
#   - per perf-test JVM: RSS, CPU, heap limit (-Xmx) and GC activity
#   - ENA network allowance counters (bandwidth / PPS throttling)
#   - consumer-group lag per partition (top offenders + per-topic totals)
#   - rebalance / error counts in the perf-test logs
#   - effective client config in use
# Writes /opt/load-benchmark/results/diag-<host>-<utc>.txt and prints it.
#
# Usage: ./05-collect-diagnostics.sh [group-name]
set -uo pipefail

GROUP="${1:-group-0}"
KAFKA_HOME=/opt/kafka
RESULTS_DIR=/opt/load-benchmark/results
IAM_PROPS=/opt/kafka/config/client-iam.properties
mkdir -p "$RESULTS_DIR"
OUT="$RESULTS_DIR/diag-$(hostname -s)-$(date -u +%Y%m%dT%H%M%SZ).txt"

sec() { echo; echo "===== $* ====="; }

{
sec "HOST"
date -u; hostname
TOKEN=$(curl -s -m 2 -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' || true)
echo "instance-type: $(curl -s -m 2 -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-type || echo n/a)"
echo "vCPUs: $(nproc)"; uptime

sec "MEMORY (MB)"
free -m

sec "VMSTAT 1s x5 (r=run queue, si/so=swap, wa=iowait, st=steal)"
vmstat 1 5

sec "PER-CORE CPU (5s sample from /proc/stat)"
python3 - <<'PY'
import time
def snap():
    d={}
    for l in open('/proc/stat'):
        if l.startswith('cpu') and l[3]!=' ':
            f=l.split(); v=list(map(int,f[1:])); d[f[0]]=(sum(v), v[3]+v[4], v[6]+v[5], v[7] if len(v)>7 else 0)
    return d
a=snap(); time.sleep(5); b=snap()
for k in sorted(a,key=lambda x:int(x[3:])):
    t=b[k][0]-a[k][0] or 1
    busy=100*(1-(b[k][1]-a[k][1])/t); irq=100*(b[k][2]-a[k][2])/t; st=100*(b[k][3]-a[k][3])/t
    print(f"{k:>6}: busy {busy:5.1f}%  irq+softirq {irq:5.1f}%  steal {st:4.1f}%")
PY

sec "TOP CPU PROCESSES"
top -b -n1 -o %CPU | head -25

sec "PERF-TEST JVMs (pid, cpu%, rss MB, heap flag, uptime)"
for P in $(pgrep -f 'ConsumerPerformance|ProducerPerformance'); do
  ARGS=$(tr '\0' ' ' < /proc/$P/cmdline)
  XMX=$(grep -oE -- '-Xmx[0-9]+[mMgG]' <<<"$ARGS" | tail -1)
  TOPIC=$(grep -oE -- '--topic [^ ]+' <<<"$ARGS" | awk '{print $2}')
  ps -o pid=,pcpu=,rss=,etime= -p "$P" | awk -v x="${XMX:-default}" -v t="$TOPIC" '{printf "%-7s cpu %5s%%  rss %6.0f MB  heap %-8s up %-12s %s\n",$1,$2,$3/1024,x,$4,t}'
done

sec "JVM GC (jstat -gcutil; O=old-gen %, FGC=full GCs, GCT=total GC seconds)"
if command -v jstat >/dev/null 2>&1; then
  for P in $(pgrep -f 'ConsumerPerformance|ProducerPerformance' | head -3); do
    echo "pid $P:"; jstat -gcutil "$P" 1000 3 2>&1
  done
else
  echo "jstat not installed (headless JDK). Install with: dnf install -y java-17-amazon-corretto-devel"
fi

sec "OOM / KERNEL (last 20 relevant dmesg lines)"
dmesg -T 2>/dev/null | grep -iE 'oom|killed process|ena|link is down|nf_conntrack' | tail -20 || echo "(none)"

sec "ENA ALLOWANCE COUNTERS (non-zero and growing = host is being network-throttled)"
IF=$(ip route show default | awk '{print $5; exit}')
echo "interface: $IF"
ethtool -S "$IF" 2>/dev/null | grep -E 'allowance_exceeded|allowance_available' || echo "ethtool stats unavailable"
S1=$(ethtool -S "$IF" 2>/dev/null | grep -E 'allowance_exceeded'); sleep 10
S2=$(ethtool -S "$IF" 2>/dev/null | grep -E 'allowance_exceeded')
echo "-- delta over 10s --"
paste <(echo "$S1") <(echo "$S2") | awk '{d=$4-$2; printf "%-34s +%d\n",$1,d}'

sec "NETWORK THROUGHPUT (10s sample)"
R1=$(cat /sys/class/net/$IF/statistics/rx_bytes); T1=$(cat /sys/class/net/$IF/statistics/tx_bytes); sleep 10
R2=$(cat /sys/class/net/$IF/statistics/rx_bytes); T2=$(cat /sys/class/net/$IF/statistics/tx_bytes)
awk -v r=$((R2-R1)) -v t=$((T2-T1)) 'BEGIN{printf "rx %.1f MB/s (%.2f Gbps)   tx %.1f MB/s\n", r/10/1048576, r*8/10/1e9, t/10/1048576}'

sec "CONSUMER GROUP LAG: $GROUP"
if [[ -f /opt/kafka/bootstrap-brokers.txt ]]; then
  BS=$(cat /opt/kafka/bootstrap-brokers.txt)
  DESC=$(timeout 90 "$KAFKA_HOME/bin/kafka-consumer-groups.sh" --bootstrap-server "$BS" \
    --command-config "$IAM_PROPS" --describe --group "$GROUP" 2>/dev/null | awk 'NF>=6 && $1!="GROUP"')
  if [[ -n "$DESC" ]]; then
    echo "-- per-topic totals (topic, partitions, total lag, max partition lag, distinct consumers) --"
    awk '{lag[$2]+=$6; n[$2]++; if($6>mx[$2])mx[$2]=$6; c[$2" "$7]=1} END{for(t in lag){k=0; for(x in c) if(index(x,t" ")==1)k++; printf "%-16s %4d  %12d  %10d  %d\n",t,n[t],lag[t],mx[t],k}}' <<<"$DESC" | sort -k3 -nr
    echo "-- top 15 partitions by lag --"
    sort -k6 -nr <<<"$DESC" | head -15 | awk '{printf "%-16s p%-3s lag %10s  consumer %s\n",$2,$3,$6,$7}'
    echo "-- partitions with no consumer assigned --"
    awk '$7=="-" {print $2, "p"$3}' <<<"$DESC" | head -20
  else
    echo "describe returned nothing (group inactive or tool timed out)"
  fi
else
  echo "no bootstrap-brokers.txt; start the consumers first"
fi

sec "PERF-TEST LOG SIGNALS (per log: rebalance warnings, errors, last rebalance line)"
for f in "$RESULTS_DIR"/consumer-*.log "$RESULTS_DIR"/producer-*.log; do
  [[ -f "$f" ]] || continue
  # WARN lines about rebalances (skip the CSV header, which contains "rebalance.time.ms")
  RB=$(grep -v '^time,' "$f" | grep -c 'rebalanc')
  ER=$(grep -cE 'ERROR|Exception' "$f")
  # Consumer stats rows whose rebalance.time.ms (column 7) is non-zero; the first
  # row is the initial group join, so anything after it is a real rebalance.
  RBROWS=$(awk -F', ' 'NF>=10 && $1 ~ /^[0-9]/ && $7+0>0 {n++} END{print (n>0?n-1:0)}' "$f")
  printf "%-44s rebalance-warns %5d  rebalances-after-start %4d  error-lines %5d\n" "$(basename "$f")" "$RB" "$RBROWS" "$ER"
done
LAST=$(grep -hv '^time,' "$RESULTS_DIR"/consumer-*.log 2>/dev/null | grep 'rebalanc' | grep -oE '^\[[0-9-]+ [0-9:]+' | sort | tail -3)
echo "latest rebalance WARN timestamps: ${LAST:-none}"
echo "-- consumer throughput, last 3 intervals of topic 1 (MB.sec is column 4) --"
tail -3 "$RESULTS_DIR"/consumer-*-load-bench-1.log 2>/dev/null | cut -c1-160

sec "EFFECTIVE CLIENT CONFIG (auth lines hidden)"
for f in /opt/kafka/config/consumer-effective.properties /opt/kafka/config/producer-effective.properties; do
  [[ -f "$f" ]] && { echo "$f:"; grep -Ev '^\s*(#|$)|sasl|security' "$f"; }
done

sec "DISK"
df -h / ; du -sh "$RESULTS_DIR" 2>/dev/null
} 2>&1 | tee "$OUT"

echo
echo "Diagnostics saved to $OUT"
