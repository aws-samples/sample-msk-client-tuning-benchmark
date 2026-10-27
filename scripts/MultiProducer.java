// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
//
// MultiProducer.java -- many independent KafkaProducer instances in one JVM.
//
// Why it exists: microservice fleets often run hundreds of services against one
// cluster, i.e. many producer instances each sending a small share of the
// traffic. For the same total bytes, many small producers send many more, smaller
// produce requests (fewer records per partition batch) and hold many more broker
// connections than a few big producers -- request-handler work that throughput
// alone does not show. kafka-producer-perf-test is one JVM per producer, so this
// runs N KafkaProducer instances (each with its own connections and sender thread)
// in one JVM, splitting a fixed total record rate evenly across them.
//
// Run (Java 17+, single-file source mode, Kafka client jars on the classpath):
//   java -cp '/opt/kafka/libs/*' MultiProducer.java \
//     --bootstrap <servers> --topics t1,t2,... --producers <N> \
//     --total-records-per-sec <R> --record-size <bytes> --config <props> \
//     [--client-prefix mp] [--report-interval-ms 60000] [--stagger-ms 100] [--payload-pool 4096] [--topic-mode single|all]
// Each producer sends round-robin across all topics with null keys (default
// partitioner). All client tuning (linger.ms, batch.size, compression.type, acks,
// buffer.memory, ...) comes from --config; only bootstrap, client.id and
// serializers are set here.
//
// Output (stdout, one CSV line per interval, aggregated across producers):
//   time, producers_configured, producers_running, producers_failed, records_sent,
//   records_acked, send_errors, records_per_sec, MB_per_sec, avg_ack_latency_ms,
//   max_ack_latency_ms
// Any failure prints "ERROR: producer=<id> ..." on stderr -- never silent.

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

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
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class MultiProducer {

    private static final DateTimeFormatter TS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS").withZone(ZoneOffset.UTC);
    private static final String HEADER = "time, producers_configured, producers_running, "
        + "producers_failed, records_sent, records_acked, send_errors, records_per_sec, "
        + "MB_per_sec, avg_ack_latency_ms, max_ack_latency_ms";

    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static final AtomicLong sent = new AtomicLong();
    private static final AtomicLong acked = new AtomicLong();
    private static final AtomicLong ackedBytes = new AtomicLong();
    private static final AtomicLong errors = new AtomicLong();
    private static final AtomicLong latencySumMs = new AtomicLong();
    private static final AtomicLong latencyMaxMs = new AtomicLong();
    private static final AtomicInteger producersRunning = new AtomicInteger();
    private static final AtomicInteger producersFailed = new AtomicInteger();
    private static final List<KafkaProducer<byte[], byte[]>> producers = new CopyOnWriteArrayList<>();

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
        int n = Integer.parseInt(required(cli, "--producers"));
        double totalRps = Double.parseDouble(required(cli, "--total-records-per-sec"));
        int recordSize = Integer.parseInt(required(cli, "--record-size"));
        String configFile = required(cli, "--config");
        String prefix = cli.getOrDefault("--client-prefix", "mp");
        long reportMs = Long.parseLong(cli.getOrDefault("--report-interval-ms", "60000"));
        // topic-mode: "all" = each producer round-robins over every topic (each producer
        // then tracks every partition -> heavy client CPU at thousands of partitions);
        // "single" = producer i writes only topics[i % topics] (one service, one topic).
        final String topicMode = cli.getOrDefault("--topic-mode", "single");
        long staggerMs = Long.parseLong(cli.getOrDefault("--stagger-ms", "100"));
        if (n <= 0 || totalRps <= 0 || recordSize <= 0) {
            throw new IllegalArgumentException("--producers, --total-records-per-sec, --record-size must be > 0");
        }
        double perProducerRps = totalRps / n;

        Properties base = loadProperties(configFile);
        System.err.println("[MultiProducer] producers=" + n + " totalRps=" + totalRps
            + " perProducerRps=" + String.format(Locale.ROOT, "%.1f", perProducerRps)
            + " recordSize=" + recordSize + " topics=" + topics.size()
            + " topicMode=" + cli.getOrDefault("--topic-mode", "single")
            + " linger.ms=" + base.getProperty("linger.ms", "<default>")
            + " batch.size=" + base.getProperty("batch.size", "<default>")
            + " compression.type=" + base.getProperty("compression.type", "<default>")
            + " acks=" + base.getProperty("acks", "<default>"));

        // A pool of DISTINCT random payloads (uppercase A-Z, same alphabet as
        // kafka-producer-perf-test, which randomises every record). Reusing one fixed
        // payload made batches near-identical, so snappy/zstd compressed them ~5x and
        // on-wire bytes no longer matched the perf-test runs. A pool much larger than
        // the records-per-batch keeps batches free of repeats (each batch is compressed
        // on its own) while avoiding per-record RNG cost on the client CPU.
        final int poolSize = Integer.parseInt(cli.getOrDefault("--payload-pool", "4096"));
        final byte[][] pool = new byte[poolSize][recordSize];
        Random rnd = new Random(42);
        for (byte[] b : pool) for (int i = 0; i < b.length; i++) b[i] = (byte) ('A' + rnd.nextInt(26));

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String id = prefix + "-" + i;
            Properties p = new Properties();
            p.putAll(base);
            p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            p.put(ProducerConfig.CLIENT_ID_CONFIG, id);
            p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            p.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all"); // same as the perf-test runs
            final int offset = i;
            final List<String> myTopics = "all".equals(topicMode)
                ? topics : java.util.Collections.singletonList(topics.get(i % topics.size()));
            Thread t = new Thread(() -> loop(id, myTopics, offset, p, perProducerRps, pool), id);
            threads.add(t);
            t.start();
            if (staggerMs > 0) Thread.sleep(staggerMs);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            for (Thread t : threads) {
                try { t.join(30000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            report(n, System.currentTimeMillis(), reportMs);
            System.err.println("[MultiProducer] shutdown complete");
        }, "mp-shutdown"));

        System.out.println(HEADER);
        long last = System.currentTimeMillis();
        while (running.get()) {
            Thread.sleep(1000);
            long now = System.currentTimeMillis();
            if (now - last >= reportMs) {
                report(n, now, now - last);
                last = now;
            }
            if (producersFailed.get() >= n) {
                System.err.println("ERROR: all " + n + " producers failed; exiting");
                System.exit(2);
            }
        }
    }

    private static void loop(String id, List<String> topics, int offset, Properties p,
                             double rps, byte[][] pool) {
        KafkaProducer<byte[], byte[]> producer;
        try {
            producer = new KafkaProducer<>(p);
        } catch (Exception e) {
            producersFailed.incrementAndGet();
            System.err.println("ERROR: producer=" + id + " failed to create: " + e);
            return;
        }
        producers.add(producer);
        producersRunning.incrementAndGet();
        long intervalNanos = (long) (1_000_000_000L / rps);
        long next = System.nanoTime();
        long count = 0;
        try {
            while (running.get()) {
                String topic = topics.get((int) ((offset + count) % topics.size()));
                final byte[] payload = pool[(int) ((count * 7919 + offset * 104729) % pool.length)];
                final long start = System.currentTimeMillis();
                // No explicit timestamp: the client stamps CreateTime = now when send() is called.
                // MultiGroupConsumer --latency-report-sec relies on this (topics must keep the default
                // message.timestamp.type=CreateTime; LogAppendTime would hide producer batching delay).
                producer.send(new ProducerRecord<>(topic, payload), (md, ex) -> {
                    if (ex != null) {
                        long e = errors.incrementAndGet();
                        if (e <= 20 || e % 10000 == 0) {
                            System.err.println("ERROR: producer=" + id + " send failed (#" + e + "): " + ex);
                        }
                    } else {
                        acked.incrementAndGet();
                        ackedBytes.addAndGet(payload.length);
                        long lat = System.currentTimeMillis() - start;
                        latencySumMs.addAndGet(lat);
                        latencyMaxMs.accumulateAndGet(lat, Math::max);
                    }
                });
                sent.incrementAndGet();
                count++;
                next += intervalNanos;
                long wait = next - System.nanoTime();
                if (wait > 0) {
                    LockSupport.parkNanos(wait);
                } else if (wait < -1_000_000_000L) {
                    next = System.nanoTime(); // >1 s behind: don't burst to catch up
                }
            }
        } catch (Exception e) {
            producersFailed.incrementAndGet();
            System.err.println("ERROR: producer=" + id + " " + e.getClass().getName() + ": " + e.getMessage());
        } finally {
            producersRunning.decrementAndGet();
            try { producer.close(Duration.ofSeconds(20)); } catch (Exception e) {
                System.err.println("ERROR: producer=" + id + " close failed: " + e);
            }
        }
    }

    private static void report(int n, long now, long elapsedMs) {
        long s = sent.getAndSet(0), a = acked.getAndSet(0), b = ackedBytes.getAndSet(0);
        long er = errors.getAndSet(0), ls = latencySumMs.getAndSet(0), lm = latencyMaxMs.getAndSet(0);
        double sec = Math.max(1L, elapsedMs) / 1000.0;
        System.out.printf(Locale.ROOT, "%s, %d, %d, %d, %d, %d, %d, %.2f, %.4f, %.2f, %d%n",
            TS.format(Instant.ofEpochMilli(now)), n, producersRunning.get(), producersFailed.get(),
            s, a, er, a / sec, (b / 1048576.0) / sec, a > 0 ? (double) ls / a : 0.0, lm);
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
