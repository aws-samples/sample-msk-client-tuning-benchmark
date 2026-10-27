// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
//
// MultiGroupConsumer.java -- many independent consumer GROUPS in one JVM.
//
// Why it exists: clusters behind microservice fleets often carry thousands of
// consumer groups, most of them per-instance -- e.g. one group
// `<service>-broadcast-<N>` per service instance, each reading every message
// of the same topic ("broadcast" fan-out). Group count drives coordinator,
// heartbeat, offset-commit and fetch request load on broker request-handler
// threads independent of bytes. kafka-consumer-perf-test is one JVM per group,
// so 100+ groups needs this: one thread + one KafkaConsumer per group.
//
// Run (Java 17+, single-file source mode, Kafka client jars on the classpath):
//   java -cp '/opt/kafka/libs/*' MultiGroupConsumer.java \
//     --bootstrap <servers> --topics t1[,t2...] --group-prefix <prefix> \
//     --groups <N> [--start-index 0] --config <props> [--report-interval-ms 60000] \
//     [--stagger-ms 200]
// Group ids are <prefix>-<start-index + i>, i = 0..N-1 (per-instance naming,
// one group per service instance). All client tuning comes from --config; only bootstrap,
// group.id, client.id and deserializers are set here.
//
// Output (stdout, one CSV line per report interval, aggregated across groups):
//   time, prefix, groups_configured, groups_running, groups_failed, records,
//   bytes, records_per_sec, MB_per_sec, polls, empty_polls
// Optional E2E latency (--latency-report-sec N / env LATENCY_REPORT_SEC, 0 = off):
// for every consumed record, latencyMs = now - record.timestamp(). record.timestamp() is the
// producer CreateTime, set when send() is called, so the value includes producer linger/batching,
// the produce request, replication wait (acks=all), the fetch and the consumer poll cycle.
// It is only valid if the topic uses message.timestamp.type=CreateTime (the Kafka default);
// with LogAppendTime the broker overwrites the timestamp and producer-side batching delay is lost.
// Producer and consumer pods may run on different nodes: accuracy depends on node clock sync
// (EKS nodes use chrony with the Amazon Time Sync Service, typically sub-millisecond).
// Every N seconds one line per group and stage is printed on stdout and the window is reset:
//   E2E_LATENCY group=<g> stage=poll|processed count=<n> p50=<ms> p90=<ms> p99=<ms> p999=<ms> max=<ms> window_s=<s>
// stage=poll is measured when poll() returns (before processing); stage=processed after the
// worker threads (or the poll thread, if --worker-threads 0) finished processing the record.
//
// Any per-group failure prints "ERROR: group=<id> ..." on stderr and is counted
// in groups_failed -- never silent. Exit code is non-zero if every group fails.

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MultiGroupConsumer {

    private static final DateTimeFormatter TS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS").withZone(ZoneOffset.UTC);
    private static final String HEADER = "time, prefix, groups_configured, groups_running, "
        + "groups_failed, records, bytes, records_per_sec, MB_per_sec, polls, empty_polls";

    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static final AtomicLong records = new AtomicLong();
    private static final AtomicLong bytes = new AtomicLong();
    private static final AtomicLong polls = new AtomicLong();
    private static final AtomicLong emptyPolls = new AtomicLong();
    private static final AtomicInteger groupsRunning = new AtomicInteger();
    private static final AtomicInteger groupsFailed = new AtomicInteger();
    private static final List<KafkaConsumer<byte[], byte[]>> consumers = new CopyOnWriteArrayList<>();

    // ---- E2E latency histograms (opt-in) --------------------------------------------------
    private static volatile boolean latencyOn = false;
    /** per group id: [0] = stage poll, [1] = stage processed; swapped (reset) at each report */
    private static final Map<String, AtomicReference<LatHist>[]> lat = new ConcurrentHashMap<>();

    /** Lock-free histogram: 1 ms buckets 0..999 ms, then 1 %-wide log buckets up to 600,000 ms. */
    static final class LatHist {
        static final int LINEAR = 1000;
        static final double STEP = Math.log(1.01);
        static final int LOG = (int) Math.ceil(Math.log(600000.0 / LINEAR) / STEP) + 1;
        final AtomicLongArray b = new AtomicLongArray(LINEAR + LOG + 1); // last = overflow
        final AtomicLong count = new AtomicLong();
        final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
        final long startMs = System.currentTimeMillis();

        void add(long v) {
            if (v < 0) v = 0; // clock skew between nodes can produce small negatives
            int i;
            if (v < LINEAR) i = (int) v;
            else {
                int k = (int) (Math.log((double) v / LINEAR) / STEP);
                i = Math.min(LINEAR + k, b.length() - 1);
            }
            b.incrementAndGet(i);
            count.incrementAndGet();
            long m;
            while (v > (m = max.get()) && !max.compareAndSet(m, v)) { }
        }
        /** upper edge (ms) of bucket i */
        static long edge(int i) {
            if (i < LINEAR) return i;
            return (long) Math.ceil(LINEAR * Math.exp((i - LINEAR + 1) * STEP));
        }
        long quantile(double q) {
            long n = count.get();
            if (n == 0) return 0;
            long target = (long) Math.ceil(q * n), cum = 0;
            for (int i = 0; i < b.length(); i++) {
                cum += b.get(i);
                if (cum >= target) return Math.min(edge(i), max.get());
            }
            return max.get();
        }
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<LatHist>[] latFor(String groupId) {
        return lat.computeIfAbsent(groupId, g -> new AtomicReference[] {
            new AtomicReference<>(new LatHist()), new AtomicReference<>(new LatHist())});
    }

    private static void recordLatency(String groupId, int stage, Iterable<ConsumerRecord<byte[], byte[]>> recs) {
        if (!latencyOn) return;
        LatHist h = latFor(groupId)[stage].get();
        long now = System.currentTimeMillis();
        for (ConsumerRecord<byte[], byte[]> rec : recs) h.add(now - rec.timestamp());
    }

    private static void reportLatency() {
        long now = System.currentTimeMillis();
        String[] stages = {"poll", "processed"};
        for (Map.Entry<String, AtomicReference<LatHist>[]> e : lat.entrySet()) {
            for (int st = 0; st < 2; st++) {
                LatHist h = e.getValue()[st].getAndSet(new LatHist());
                long n = h.count.get();
                System.out.printf(Locale.ROOT,
                    "E2E_LATENCY group=%s stage=%s count=%d p50=%d p90=%d p99=%d p999=%d max=%d window_s=%.1f%n",
                    e.getKey(), stages[st], n, h.quantile(0.50), h.quantile(0.90), h.quantile(0.99),
                    h.quantile(0.999), n == 0 ? 0 : h.max.get(), (now - h.startMs) / 1000.0);
            }
        }
        System.out.flush();
    }

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Throwable t) {
            System.err.println("ERROR: " + t.getClass().getName() + ": " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void run(String[] args) throws Exception {
        Map<String, String> cli = parseArgs(args);
        String bootstrap = required(cli, "--bootstrap");
        List<String> topics = Arrays.asList(required(cli, "--topics").split(","));
        String prefix = required(cli, "--group-prefix");
        int groups = Integer.parseInt(required(cli, "--groups"));
        int startIndex = Integer.parseInt(cli.getOrDefault("--start-index", "0"));
        String configFile = required(cli, "--config");
        long reportMs = Long.parseLong(cli.getOrDefault("--report-interval-ms", "60000"));
        long staggerMs = Long.parseLong(cli.getOrDefault("--stagger-ms", "200"));
        // --worker-threads N: each group's poll thread
        // hands the polled batch to N processing threads (split by partition) and waits for
        // them before the next poll, like a microservice with internal processing threads.
        // Waiting keeps auto-commit at-least-once safe. 0 = process on the poll thread.
        final int workers = Integer.parseInt(cli.getOrDefault("--worker-threads", "0"));
        // --members-per-group M: M KafkaConsumer instances per group id in this
        // JVM (all join the same group), so a group can have as many members as partitions.
        final int members = Integer.parseInt(cli.getOrDefault("--members-per-group", "1"));
        final String memberTag = cli.getOrDefault("--member-tag", "");
        final long latencySec = Long.parseLong(cli.getOrDefault("--latency-report-sec",
            System.getenv().getOrDefault("LATENCY_REPORT_SEC", "0")));
        latencyOn = latencySec > 0;
        if (groups <= 0) throw new IllegalArgumentException("--groups must be > 0");

        Properties base = loadProperties(configFile);
        System.err.println("[MultiGroupConsumer] prefix=" + prefix + " groups=" + groups
            + " startIndex=" + startIndex + " topics=" + topics + " staggerMs=" + staggerMs
            + " max.poll.records=" + base.getProperty("max.poll.records", "<default>")
            + " assignor=" + base.getProperty("partition.assignment.strategy", "<default>")
            + " workerThreads=" + workers + " membersPerGroup=" + members
            + " latencyReportSec=" + latencySec);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < groups; i++) {
            String groupId = prefix + "-" + (startIndex + i);
            for (int m = 0; m < members; m++) {
                Properties p = new Properties();
                p.putAll(base);
                p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
                p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                p.put(ConsumerConfig.CLIENT_ID_CONFIG, "mgc-" + groupId + (members > 1 ? "-" + memberTag + "m" + m : ""));
                p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
                p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
                Thread t = new Thread(() -> loop(groupId, topics, p, workers), "mgc-" + groupId + "-m" + m);
                t.setDaemon(false);
                threads.add(t);
                t.start();
                if (staggerMs > 0) Thread.sleep(staggerMs); // avoid a connection storm at start
            }
        }
        final int groupsTotal = groups * members; // reported counts are consumers (members)

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            for (KafkaConsumer<byte[], byte[]> c : consumers) {
                try { c.wakeup(); } catch (Exception ignored) { }
            }
            for (Thread t : threads) {
                try { t.join(30000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            report(prefix, groupsTotal, System.currentTimeMillis(), reportMs);
            if (latencyOn) reportLatency();
            System.err.println("[MultiGroupConsumer] shutdown complete");
        }, "mgc-shutdown"));

        if (latencyOn) {
            Thread lt = new Thread(() -> {
                while (running.get()) {
                    try { Thread.sleep(latencySec * 1000); } catch (InterruptedException ie) { return; }
                    reportLatency();
                }
            }, "mgc-latency-report");
            lt.setDaemon(true);
            lt.start();
        }
        System.out.println(HEADER);
        long last = System.currentTimeMillis();
        while (running.get()) {
            Thread.sleep(1000);
            long now = System.currentTimeMillis();
            if (now - last >= reportMs) {
                report(prefix, groupsTotal, now, now - last);
                last = now;
            }
            if (groupsFailed.get() >= groupsTotal) {
                System.err.println("ERROR: all " + groupsTotal + " consumers failed; exiting");
                System.exit(2);
            }
        }
    }

    private static long[] process(String groupId, Iterable<ConsumerRecord<byte[], byte[]>> recs) {
        long r = 0, b = 0;
        for (ConsumerRecord<byte[], byte[]> rec : recs) {
            r++;
            if (rec.serializedValueSize() > 0) b += rec.serializedValueSize();
            if (rec.serializedKeySize() > 0) b += rec.serializedKeySize();
        }
        recordLatency(groupId, 1, recs); // stage=processed
        return new long[] {r, b};
    }

    private static void loop(String groupId, List<String> topics, Properties p, int workers) {
        final java.util.concurrent.ExecutorService pool = workers > 0
            ? java.util.concurrent.Executors.newFixedThreadPool(workers, rr -> {
                Thread w = new Thread(rr, "mgc-" + groupId + "-worker"); w.setDaemon(true); return w; })
            : null;
        KafkaConsumer<byte[], byte[]> c;
        try {
            c = new KafkaConsumer<>(p);
        } catch (Exception e) {
            groupsFailed.incrementAndGet();
            System.err.println("ERROR: group=" + groupId + " failed to create consumer: " + e);
            return;
        }
        consumers.add(c);
        groupsRunning.incrementAndGet();
        try {
            c.subscribe(topics);
            while (running.get()) {
                ConsumerRecords<byte[], byte[]> batch = c.poll(Duration.ofMillis(1000));
                polls.incrementAndGet();
                if (batch.isEmpty()) {
                    emptyPolls.incrementAndGet();
                    continue;
                }
                recordLatency(groupId, 0, batch); // stage=poll
                long r = 0, b = 0;
                if (pool == null) {
                    long[] rb = process(groupId, batch); r = rb[0]; b = rb[1];
                } else {
                    // split the batch by partition across the workers; wait for all of them
                    List<List<ConsumerRecord<byte[], byte[]>>> parts = new ArrayList<>();
                    for (int w = 0; w < workers; w++) parts.add(new ArrayList<>());
                    int k = 0;
                    for (org.apache.kafka.common.TopicPartition tp : batch.partitions()) {
                        parts.get(k++ % workers).addAll(batch.records(tp));
                    }
                    List<java.util.concurrent.Future<long[]>> fs = new ArrayList<>();
                    for (List<ConsumerRecord<byte[], byte[]>> part : parts) {
                        if (!part.isEmpty()) fs.add(pool.submit(() -> process(groupId, part)));
                    }
                    for (java.util.concurrent.Future<long[]> f : fs) { long[] rb = f.get(); r += rb[0]; b += rb[1]; }
                }
                records.addAndGet(r);
                bytes.addAndGet(b);
            }
        } catch (WakeupException e) {
            if (running.get()) {
                groupsFailed.incrementAndGet();
                System.err.println("ERROR: group=" + groupId + " unexpected wakeup");
            }
        } catch (Exception e) {
            groupsFailed.incrementAndGet();
            System.err.println("ERROR: group=" + groupId + " " + e.getClass().getName() + ": " + e.getMessage());
        } finally {
            groupsRunning.decrementAndGet();
            if (pool != null) pool.shutdownNow();
            try { c.close(Duration.ofSeconds(10)); } catch (Exception e) {
                System.err.println("ERROR: group=" + groupId + " close failed: " + e);
            }
        }
    }

    private static void report(String prefix, int groups, long now, long elapsedMs) {
        long r = records.getAndSet(0), b = bytes.getAndSet(0);
        long pl = polls.getAndSet(0), ep = emptyPolls.getAndSet(0);
        double sec = Math.max(1L, elapsedMs) / 1000.0;
        System.out.printf(Locale.ROOT, "%s, %s, %d, %d, %d, %d, %d, %.2f, %.4f, %d, %d%n",
            TS.format(Instant.ofEpochMilli(now)), prefix, groups, groupsRunning.get(),
            groupsFailed.get(), r, b, r / sec, (b / 1048576.0) / sec, pl, ep);
        System.out.flush();
    }

    private static Properties loadProperties(String file) throws IOException {
        Path path = Paths.get(file);
        if (!Files.isReadable(path)) throw new IOException("config file not readable: " + path.toAbsolutePath());
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(path)) { p.load(in); }
        return p;
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--")) throw new IllegalArgumentException("unexpected argument: " + args[i]);
            if (i + 1 >= args.length) throw new IllegalArgumentException("missing value for " + args[i]);
            m.put(args[i], args[++i]);
        }
        return m;
    }

    private static String required(Map<String, String> m, String k) {
        String v = m.get(k);
        if (v == null || v.isEmpty()) throw new IllegalArgumentException("missing required " + k);
        return v;
    }
}
