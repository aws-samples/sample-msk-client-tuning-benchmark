// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
//
// KStreamsEmulator -- a small Kafka Streams app that emulates a typical stateful Kafka
// Streams microservice workload on Amazon MSK, to measure the broker-side cost
// (RequestHandlerAvgIdlePercent, ProduceTotalTimeMsMean, ...) of
// processing.guarantee=at_least_once vs exactly_once_v2, of commit.interval.ms, and of
// the number of stream (worker) threads.
//
// Topology (one pod = one KafkaStreams instance = one application.id = one consumer group):
//
//   INPUT_TOPIC (byte[]/byte[])
//     -> selectKey(key derived from the value)            => <app>-rekey-repartition   (internal)
//     -> groupByKey().count() into persistent RocksDB     => <app>-<store>-changelog   (internal)
//     -> toStream().to(OUTPUT_TOPIC)                       (optional; skipped if OUTPUT_TOPIC is empty)
//
// Every setting can come from an env var or a CLI flag (--input-topic for INPUT_TOPIC,
// etc.). Env vars WIN over CLI flags (container use). Run with --help for the list.
//
// Output contract (same format as ../scripts/MultiProducer.java):
//   stdout: CSV header once at startup, then one CSV line every REPORT_INTERVAL_MS (60 s).
//   stderr: "INFO: ..." / "WARN: ..." / "ERROR: ..." lines. Never fails silently.
//   exit code: 0 on clean shutdown (or 143 on SIGTERM via the JVM), 1 if the app reached
//              ERROR state, 2 on invalid configuration.

package com.amazonaws.samples.msk.kstreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class KStreamsEmulator {

    private static final DateTimeFormatter TS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS").withZone(ZoneOffset.UTC);

    static final String HEADER = "time, app_id, state, processing_guarantee, commit_interval_ms, "
        + "threads_alive, records_processed_delta, process_rate, commit_rate, commit_latency_avg_ms, "
        + "consumer_offset_commit_rate, producer_record_send_rate, producer_request_rate, "
        + "txn_commit_time_ms_delta, txn_send_offsets_time_ms_delta";

    /** Keys Kafka Streams owns; setting them from a tuning file would be ignored or break the app. */
    private static final Set<String> STREAMS_CONTROLLED = Set.of(
        ConsumerConfig.GROUP_ID_CONFIG,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        ConsumerConfig.ISOLATION_LEVEL_CONFIG,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ProducerConfig.TRANSACTIONAL_ID_CONFIG,
        ProducerConfig.CLIENT_ID_CONFIG,
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        StreamsConfig.APPLICATION_ID_CONFIG);

    // ----- settings: name, default, help. Flag = "--" + lower-case name with '-' -----
    private record Setting(String env, String def, String help) {
        String flag() { return "--" + env.toLowerCase(Locale.ROOT).replace('_', '-'); }
    }

    private static final List<Setting> SETTINGS = List.of(
        new Setting("BOOTSTRAP_SERVERS", null, "REQUIRED. Broker list, e.g. b-1.x.kafka.us-east-1.amazonaws.com:9098"),
        new Setting("INPUT_TOPIC", "load-bench-3", "Source topic (consumed as byte[]/byte[])"),
        new Setting("OUTPUT_TOPIC", "", "Sink topic for the count stream (String/Long). Empty = no sink. Must already exist."),
        new Setting("APPLICATION_ID", "kstreams-emulator", "application.id = consumer group id = internal topic prefix"),
        new Setting("PROCESSING_GUARANTEE", "at_least_once", "at_least_once | exactly_once_v2"),
        new Setting("COMMIT_INTERVAL_MS", "", "commit.interval.ms. Empty = Kafka default (30000 ALOS, 100 EOS)"),
        new Setting("NUM_STREAM_THREADS", "1", "num.stream.threads"),
        new Setting("REPLICATION_FACTOR", "2", "replication.factor for internal (repartition + changelog) topics"),
        new Setting("STATE_DIR", "/tmp/kstreams-state", "state.dir (RocksDB location)"),
        new Setting("STATESTORE_CACHE_MAX_BYTES", "", "statestore.cache.max.bytes. Empty = Kafka default"),
        new Setting("STORE_NAME", "key-count-store", "Name of the RocksDB count store (changelog = <app>-<store>-changelog)"),
        new Setting("KEY_CARDINALITY", "10000", "Number of distinct derived keys"),
        new Setting("KEY_PREFIX_BYTES", "8", "KEY_MODE=value: hash the first N value bytes"),
        new Setting("KEY_MODE", "value", "value = key from hash(first N value bytes) mod cardinality; "
            + "counter = round-robin per thread (use when the source payloads are not diverse)"),
        new Setting("AUTO_OFFSET_RESET", "latest", "Main consumer auto.offset.reset when the group has no committed "
            + "offsets (latest|earliest). Kafka Streams' own default is earliest; latest avoids a backlog burst"),
        new Setting("CLIENT_PROPS_FILE", "", "Client tuning properties file (same format as ../scripts/client-tuning.properties)"),
        new Setting("AUTH_PROPS_FILE", "", "Security/IAM properties file (merged unprefixed, applies to every client)"),
        new Setting("MSK_IAM", "auto", "true = inject MSK IAM SASL_SSL props; false = never; auto = only if no "
            + "security.protocol is configured AND BOOTSTRAP_SERVERS uses port 9098"),
        new Setting("REPORT_INTERVAL_MS", "60000", "CSV stats interval"),
        new Setting("MAX_THREAD_REPLACEMENTS", "10", "Uncaught exceptions handled with REPLACE_THREAD up to this many "
            + "times, then SHUTDOWN_CLIENT (-> ERROR state, exit 1)"),
        new Setting("LOG_LEVEL", "warn", "slf4j-simple default log level (trace|debug|info|warn|error)"),
        new Setting("DUMP_METRIC_NAMES", "false", "true = print every available metric group:name once on stderr (debug aid)"));

    private static final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private static final AtomicBoolean reachedError = new AtomicBoolean(false);

    private KStreamsEmulator() { }

    public static void main(String[] args) {
        int rc;
        try {
            rc = run(args);
        } catch (ConfigError e) {
            System.err.println("ERROR: invalid configuration: " + e.getMessage());
            System.err.println("ERROR: run with --help for usage");
            rc = 2;
        } catch (Throwable t) {
            System.err.println("ERROR: " + t.getClass().getName() + ": " + t.getMessage());
            t.printStackTrace(System.err);
            rc = 1;
        }
        if (!shutdownRequested.get()) {
            System.exit(rc);
        }
        // On SIGTERM the JVM is already exiting through the shutdown hook; just return.
    }

    static final class ConfigError extends RuntimeException {
        ConfigError(String msg) { super(msg); }
        ConfigError(String msg, Throwable cause) { super(msg, cause); }
    }

    // =====================================================================================
    private static int run(String[] args) throws Exception {
        Map<String, String> cli = parseCli(args);
        if (cli.containsKey("--help") || cli.containsKey("-h")) {
            printHelp();
            return 0;
        }
        Map<String, String> cfg = resolveSettings(cli);

        // Must happen before the first Logger is created.
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", cfg.get("LOG_LEVEL"));

        final String bootstrap = require(cfg, "BOOTSTRAP_SERVERS");
        final String inputTopic = require(cfg, "INPUT_TOPIC");
        final String outputTopic = cfg.get("OUTPUT_TOPIC");
        final String appId = require(cfg, "APPLICATION_ID");
        final String guarantee = cfg.get("PROCESSING_GUARANTEE");
        if (!guarantee.equals(StreamsConfig.AT_LEAST_ONCE) && !guarantee.equals(StreamsConfig.EXACTLY_ONCE_V2)) {
            throw new ConfigError("PROCESSING_GUARANTEE must be " + StreamsConfig.AT_LEAST_ONCE + " or "
                + StreamsConfig.EXACTLY_ONCE_V2 + ", got '" + guarantee + "'");
        }
        final int keyCardinality = positiveInt(cfg, "KEY_CARDINALITY");
        final int keyPrefixBytes = positiveInt(cfg, "KEY_PREFIX_BYTES");
        final String keyMode = cfg.get("KEY_MODE");
        if (!keyMode.equals("value") && !keyMode.equals("counter")) {
            throw new ConfigError("KEY_MODE must be value or counter, got '" + keyMode + "'");
        }
        final long reportMs = positiveLong(cfg, "REPORT_INTERVAL_MS");
        final int maxReplacements = nonNegativeInt(cfg, "MAX_THREAD_REPLACEMENTS");
        final String storeName = require(cfg, "STORE_NAME");

        // ---------- build StreamsConfig properties ----------
        Properties props = new Properties();
        List<String> notes = new ArrayList<>();
        if (!cfg.get("CLIENT_PROPS_FILE").isEmpty()) {
            mergeClientProps(props, loadProps(cfg.get("CLIENT_PROPS_FILE")), cfg.get("CLIENT_PROPS_FILE"), notes);
        }
        if (!cfg.get("AUTH_PROPS_FILE").isEmpty()) {
            Properties auth = loadProps(cfg.get("AUTH_PROPS_FILE"));
            for (String k : auth.stringPropertyNames()) {
                props.setProperty(k, auth.getProperty(k)); // unprefixed => producer, consumers, admin
            }
            notes.add("merged " + auth.size() + " auth props (unprefixed) from " + cfg.get("AUTH_PROPS_FILE"));
        }
        applyIam(props, cfg.get("MSK_IAM"), bootstrap, notes);

        // Explicit settings always win over anything in the properties files.
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, guarantee);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, String.valueOf(positiveInt(cfg, "NUM_STREAM_THREADS")));
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, String.valueOf(positiveInt(cfg, "REPLICATION_FACTOR")));
        props.put(StreamsConfig.STATE_DIR_CONFIG, require(cfg, "STATE_DIR"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class.getName());
        final String offsetReset = cfg.get("AUTO_OFFSET_RESET");
        if (!offsetReset.equals("latest") && !offsetReset.equals("earliest")) {
            throw new ConfigError("AUTO_OFFSET_RESET must be latest or earliest, got '" + offsetReset + "'");
        }
        props.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), offsetReset);
        final boolean commitExplicit = !cfg.get("COMMIT_INTERVAL_MS").isEmpty();
        if (commitExplicit) {
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, String.valueOf(nonNegativeLong(cfg, "COMMIT_INTERVAL_MS")));
        } else {
            props.remove(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG); // let Kafka pick its guarantee-specific default
        }
        if (!cfg.get("STATESTORE_CACHE_MAX_BYTES").isEmpty()) {
            props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG,
                String.valueOf(nonNegativeLong(cfg, "STATESTORE_CACHE_MAX_BYTES")));
        }

        final StreamsConfig streamsConfig;
        try {
            streamsConfig = new StreamsConfig(props);
        } catch (org.apache.kafka.common.config.ConfigException e) {
            throw new ConfigError(e.getMessage(), e);
        }
        final long effectiveCommitMs = streamsConfig.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);
        final long effectiveCacheBytes = streamsConfig.getLong(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG);

        // ---------- startup log (stderr) ----------
        info("kstreams-emulator starting; kafka-streams " + org.apache.kafka.common.utils.AppInfoParser.getVersion());
        for (Setting s : SETTINGS) {
            info("setting " + s.env() + "=" + cfg.get(s.env()));
        }
        for (String n : notes) {
            info(n);
        }
        info("effective commit.interval.ms=" + effectiveCommitMs
            + (commitExplicit ? " (explicit)" : " (Kafka default for " + guarantee + ")"));
        info("effective statestore.cache.max.bytes=" + effectiveCacheBytes);
        for (String k : new TreeMap<>(toMap(props)).keySet()) {
            info("streams-prop " + k + "=" + mask(k, String.valueOf(props.get(k))));
        }

        // ---------- topology ----------
        Topology topology = buildTopology(inputTopic, outputTopic, storeName, keyMode, keyCardinality, keyPrefixBytes);
        info("topology:\n" + topology.describe());
        info("internal topics: " + appId + "-rekey-repartition, " + appId + "-" + storeName + "-changelog"
            + (outputTopic.isEmpty() ? "; no sink (OUTPUT_TOPIC empty)" : "; sink " + outputTopic));

        // ---------- KafkaStreams ----------
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicInteger replacements = new AtomicInteger();

        streams.setStateListener((newState, oldState) -> {
            info("state " + oldState + " -> " + newState);
            if (newState == KafkaStreams.State.ERROR) {
                reachedError.set(true);
                System.err.println("ERROR: KafkaStreams entered ERROR state (app_id=" + appId + ")");
                done.countDown();
            } else if (newState == KafkaStreams.State.NOT_RUNNING) {
                done.countDown();
            }
        });
        streams.setUncaughtExceptionHandler(exception -> {
            Throwable root = rootCause(exception);
            System.err.println("ERROR: uncaught exception in stream thread " + Thread.currentThread().getName()
                + ": " + exception.getClass().getName() + ": " + exception.getMessage()
                + (root != exception ? " (root cause " + root.getClass().getName() + ": " + root.getMessage() + ")" : ""));
            exception.printStackTrace(System.err);
            if (hasCause(exception, MissingSourceTopicException.class)) {
                System.err.println("ERROR: source topic missing (INPUT_TOPIC=" + inputTopic
                    + "); replacing the thread would loop forever -> SHUTDOWN_CLIENT");
                return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            }
            int n = replacements.incrementAndGet();
            if (n > maxReplacements) {
                System.err.println("ERROR: " + n + " uncaught exceptions > MAX_THREAD_REPLACEMENTS="
                    + maxReplacements + " -> SHUTDOWN_CLIENT");
                return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            }
            System.err.println("ERROR: responding REPLACE_THREAD (" + n + "/" + maxReplacements + ")");
            return StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        // ---------- reporter ----------
        Reporter reporter = new Reporter(streams, appId, guarantee, effectiveCommitMs,
            Boolean.parseBoolean(cfg.get("DUMP_METRIC_NAMES")));
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "csv-reporter");
            t.setDaemon(true);
            return t;
        });
        System.out.println(HEADER);
        System.out.flush();
        scheduler.scheduleAtFixedRate(reporter::reportSafely, reportMs, reportMs, TimeUnit.MILLISECONDS);

        // ---------- SIGTERM ----------
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shutdownRequested.set(true);
            info("shutdown signal received; closing KafkaStreams (timeout 30 s)");
            scheduler.shutdownNow();
            reporter.reportSafely(); // final line, taken while the client metrics still exist
            closeStreams(streams);
            done.countDown();
            info("shutdown complete");
        }, "shutdown-hook"));

        try {
            streams.start();
        } catch (Exception e) {
            System.err.println("ERROR: KafkaStreams.start() failed: " + e);
            closeStreams(streams);
            return 1;
        }
        info("KafkaStreams started; CSV every " + reportMs + " ms on stdout");

        done.await();
        if (shutdownRequested.get()) {
            return 0; // hook owns the close
        }
        scheduler.shutdownNow();
        closeStreams(streams);
        if (reachedError.get()) {
            System.err.println("ERROR: exiting with status 1 because the application reached ERROR state");
            return 1;
        }
        info("KafkaStreams stopped (NOT_RUNNING)");
        return 0;
    }

    private static void closeStreams(KafkaStreams streams) {
        try {
            boolean ok = streams.close(Duration.ofSeconds(30));
            if (!ok) {
                System.err.println("ERROR: KafkaStreams.close() did not complete within 30 s");
            }
        } catch (Exception e) {
            System.err.println("ERROR: KafkaStreams.close() failed: " + e);
        }
    }

    // =====================================================================================
    static Topology buildTopology(String inputTopic, String outputTopic, String storeName, String keyMode,
                                  int keyCardinality, int keyPrefixBytes) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<byte[], byte[]> source = builder.stream(inputTopic, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()));

        // A per-thread counter for KEY_MODE=counter (each stream thread runs its own tasks).
        final ThreadLocal<long[]> counter = ThreadLocal.withInitial(() -> new long[1]);
        KStream<String, byte[]> rekeyed = source.selectKey((k, v) -> {
            long slot;
            if ("counter".equals(keyMode)) {
                slot = counter.get()[0]++;
            } else {
                slot = prefixHash(v, keyPrefixBytes);
            }
            return "k-" + Math.floorMod(slot, (long) keyCardinality);
        });

        KTable<String, Long> counts = rekeyed
            .groupByKey(Grouped.with("rekey", Serdes.String(), Serdes.ByteArray()))
            .count(Materialized.<String, Long>as(Stores.persistentKeyValueStore(storeName))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        if (outputTopic != null && !outputTopic.isEmpty()) {
            counts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        }
        return builder.build();
    }

    /** FNV-1a over the first n bytes; null/empty value => 0. */
    static long prefixHash(byte[] v, int n) {
        if (v == null) {
            return 0;
        }
        long h = 0xcbf29ce484222325L;
        int len = Math.min(n, v.length);
        for (int i = 0; i < len; i++) {
            h ^= (v[i] & 0xff);
            h *= 0x100000001b3L;
        }
        return h;
    }

    // =====================================================================================
    /**
     * Classify each tuning key and route it with the right StreamsConfig prefix:
     *  - already prefixed (producer./consumer./main.consumer./restore.consumer./global.consumer./admin.): as-is
     *  - security.protocol, sasl.*, ssl.*: unprefixed (every client, incl. admin + restore consumer)
     *  - keys Kafka Streams controls (group.id, auto commit, assignor, serdes, ...): skipped with WARN
     *  - producer-only keys (linger.ms, batch.size, compression.type, acks, ...): producer.
     *  - consumer-only keys (fetch.min.bytes, fetch.max.wait.ms, max.poll.records, ...): main.consumer.
     *    (main consumer only, so state-restore fetch behaviour stays at Streams defaults)
     *  - keys valid for both producer and consumer (send/receive.buffer.bytes, connections.max.idle.ms, ...)
     *    and Streams-level keys: unprefixed
     *  - anything else: unprefixed with WARN (Kafka will log it as unknown)
     */
    static void mergeClientProps(Properties target, Properties src, String file, List<String> notes) {
        Set<String> producerNames = ProducerConfig.configNames();
        Set<String> consumerNames = ConsumerConfig.configNames();
        Set<String> streamsNames = StreamsConfig.configDef().names();
        int prod = 0, cons = 0, both = 0, skipped = 0, pass = 0;
        for (String k : new TreeMap<>(toMap(src)).keySet()) {
            String v = src.getProperty(k);
            if (k.startsWith(StreamsConfig.PRODUCER_PREFIX) || k.startsWith(StreamsConfig.CONSUMER_PREFIX)
                || k.startsWith(StreamsConfig.MAIN_CONSUMER_PREFIX) || k.startsWith(StreamsConfig.RESTORE_CONSUMER_PREFIX)
                || k.startsWith(StreamsConfig.GLOBAL_CONSUMER_PREFIX) || k.startsWith(StreamsConfig.ADMIN_CLIENT_PREFIX)
                || k.startsWith(StreamsConfig.TOPIC_PREFIX)) {
                target.setProperty(k, v);
                pass++;
            } else if (k.equals("security.protocol") || k.startsWith("sasl.") || k.startsWith("ssl.")) {
                target.setProperty(k, v);
                both++;
            } else if (STREAMS_CONTROLLED.contains(k)) {
                warn("skipping '" + k + "' from " + file + " (controlled by Kafka Streams / this app)");
                skipped++;
            } else if (producerNames.contains(k) && !consumerNames.contains(k)) {
                target.setProperty(StreamsConfig.producerPrefix(k), v);
                prod++;
            } else if (consumerNames.contains(k) && !producerNames.contains(k)) {
                target.setProperty(StreamsConfig.mainConsumerPrefix(k), v);
                cons++;
            } else if (producerNames.contains(k) || consumerNames.contains(k) || streamsNames.contains(k)) {
                target.setProperty(k, v);
                both++;
            } else {
                warn("'" + k + "' from " + file + " is not a known producer/consumer/streams config; passing unprefixed");
                target.setProperty(k, v);
                both++;
            }
        }
        notes.add("merged " + file + ": producer.=" + prod + " main.consumer.=" + cons + " unprefixed=" + both
            + " pre-prefixed=" + pass + " skipped=" + skipped);
    }

    static void applyIam(Properties props, String mode, String bootstrap, List<String> notes) {
        boolean inject;
        switch (mode) {
            case "true": inject = true; break;
            case "false": inject = false; break;
            case "auto":
                inject = !props.containsKey("security.protocol") && bootstrap.contains(":9098");
                break;
            default:
                throw new ConfigError("MSK_IAM must be true|false|auto, got '" + mode + "'");
        }
        if (inject) {
            props.setProperty("security.protocol", "SASL_SSL");
            props.setProperty("sasl.mechanism", "AWS_MSK_IAM");
            props.setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
            props.setProperty("sasl.client.callback.handler.class",
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
            notes.add("MSK_IAM=" + mode + ": injected SASL_SSL / AWS_MSK_IAM client props");
        } else if (!props.containsKey("security.protocol")) {
            warn("no security.protocol configured (CLIENT_PROPS_FILE / AUTH_PROPS_FILE / MSK_IAM) -> PLAINTEXT");
        }
    }

    // =====================================================================================
    /** Pulls metrics from KafkaStreams#metrics() and prints one CSV line. Never throws. */
    static final class Reporter {
        private final KafkaStreams streams;
        private final String appId;
        private final String guarantee;
        private final long commitMs;
        private final boolean dumpNames;
        private boolean dumped;
        private final boolean eos;
        // Monotonic totals per (group,name,tags) so thread replacement does not produce negative deltas.
        private final Map<String, Double> lastTotals = new HashMap<>();

        Reporter(KafkaStreams streams, String appId, String guarantee, long commitMs, boolean dumpNames) {
            this.streams = streams;
            this.appId = appId;
            this.guarantee = guarantee;
            this.commitMs = commitMs;
            this.dumpNames = dumpNames;
            this.eos = StreamsConfig.EXACTLY_ONCE_V2.equals(guarantee);
        }

        synchronized void reportSafely() {
            try {
                report();
            } catch (Throwable t) {
                System.err.println("ERROR: stats reporter failed: " + t);
            }
        }

        private void report() {
            Map<MetricName, ? extends Metric> m = streams.metrics();
            if (dumpNames && !dumped) {
                dumped = true;
                m.keySet().stream().map(n -> n.group() + ":" + n.name()).distinct().sorted()
                    .forEach(n -> info("metric " + n));
            }
            String state;
            try {
                state = streams.state().name();
            } catch (Exception e) {
                state = "na";
            }
            String threads;
            try {
                threads = String.valueOf(streams.metadataForLocalThreads().size());
            } catch (Exception e) {
                threads = "na";
            }
            String line = String.join(", ",
                TS.format(Instant.now()),
                appId,
                state,
                guarantee,
                String.valueOf(commitMs),
                threads,
                fmt0(delta(m, "stream-thread-metrics", "process-total")),
                fmt(sum(m, "stream-thread-metrics", "process-rate")),
                fmt(sum(m, "stream-thread-metrics", "commit-rate")),
                fmt(avg(m, "stream-thread-metrics", "commit-latency-avg")),
                fmt(sum(m, "consumer-coordinator-metrics", "commit-rate")),
                fmt(sum(m, "producer-metrics", "record-send-rate")),
                fmt(sum(m, "producer-metrics", "request-rate")),
                eos ? fmtNsToMs(delta(m, "producer-metrics", "txn-commit-time-ns-total")) : "na",
                eos ? fmtNsToMs(delta(m, "producer-metrics", "txn-send-offsets-time-ns-total")) : "na");
            System.out.println(line);
            System.out.flush();
        }

        private static List<Double> values(Map<MetricName, ? extends Metric> m, String group, String name,
                                           Map<String, Double> keyed) {
            List<Double> out = new ArrayList<>();
            for (Map.Entry<MetricName, ? extends Metric> e : m.entrySet()) {
                MetricName n = e.getKey();
                if (!n.group().equals(group) || !n.name().equals(name)) {
                    continue;
                }
                Object v;
                try {
                    v = e.getValue().metricValue();
                } catch (Exception ex) {
                    continue;
                }
                if (v instanceof Number num) {
                    double d = num.doubleValue();
                    if (!Double.isNaN(d) && !Double.isInfinite(d)) {
                        out.add(d);
                        if (keyed != null) {
                            keyed.put(group + "|" + name + "|" + new TreeMap<>(n.tags()), d);
                        }
                    }
                }
            }
            return out;
        }

        private static Double sum(Map<MetricName, ? extends Metric> m, String group, String name) {
            List<Double> v = values(m, group, name, null);
            return v.isEmpty() ? null : v.stream().mapToDouble(Double::doubleValue).sum();
        }

        private static Double avg(Map<MetricName, ? extends Metric> m, String group, String name) {
            List<Double> v = values(m, group, name, null);
            return v.isEmpty() ? null : v.stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);
        }

        /** Sum over all tag-sets of (current - previous) for a cumulative metric; a reset counts from 0. */
        private Double delta(Map<MetricName, ? extends Metric> m, String group, String name) {
            Map<String, Double> now = new LinkedHashMap<>();
            values(m, group, name, now);
            if (now.isEmpty()) {
                return null;
            }
            double d = 0;
            for (Map.Entry<String, Double> e : now.entrySet()) {
                Double prev = lastTotals.get(e.getKey());
                double cur = e.getValue();
                d += (prev == null || cur < prev) ? cur : cur - prev;
                lastTotals.put(e.getKey(), cur);
            }
            return d;
        }

        private static String fmt(Double d) {
            return d == null ? "na" : String.format(Locale.ROOT, "%.2f", d);
        }

        private static String fmt0(Double d) {
            return d == null ? "na" : String.format(Locale.ROOT, "%.0f", d);
        }

        private static String fmtNsToMs(Double d) {
            return d == null ? "na" : String.format(Locale.ROOT, "%.2f", d / 1_000_000.0);
        }
    }

    // =====================================================================================
    static Map<String, String> parseCli(String[] args) {
        Map<String, String> out = new HashMap<>();
        Set<String> known = new java.util.HashSet<>();
        SETTINGS.forEach(s -> known.add(s.flag()));
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.equals("--help") || a.equals("-h")) {
                out.put("--help", "");
                continue;
            }
            String key;
            String val;
            int eq = a.indexOf('=');
            if (a.startsWith("--") && eq > 0) {
                key = a.substring(0, eq);
                val = a.substring(eq + 1);
            } else if (a.startsWith("--")) {
                key = a;
                if (i + 1 >= args.length) {
                    throw new ConfigError("missing value for " + a);
                }
                val = args[++i];
            } else {
                throw new ConfigError("unexpected argument '" + a + "'");
            }
            if (!known.contains(key)) {
                throw new ConfigError("unknown flag " + key);
            }
            out.put(key, val);
        }
        return out;
    }

    /** env var (if set, even to "") wins over the CLI flag; blank falls back to the default. */
    static Map<String, String> resolveSettings(Map<String, String> cli) {
        Map<String, String> out = new LinkedHashMap<>();
        for (Setting s : SETTINGS) {
            String env = System.getenv(s.env());
            String v = env != null ? env : cli.get(s.flag());
            if (v == null || v.isBlank()) {
                v = s.def() == null ? "" : s.def();
            }
            out.put(s.env(), v.trim());
        }
        return out;
    }

    private static void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("kstreams-emulator -- Kafka Streams workload emulator for Amazon MSK\n\n")
          .append("Topology: INPUT_TOPIC -> selectKey (repartition topic) -> groupByKey().count() ")
          .append("(RocksDB + changelog topic) -> [OUTPUT_TOPIC]\n\n")
          .append("Usage: java -jar kstreams-emulator.jar [--flag value | --flag=value ...]\n")
          .append("Every setting is ENV_VAR or --flag; the env var wins if both are set.\n\n");
        for (Setting s : SETTINGS) {
            sb.append(String.format(Locale.ROOT, "  %-30s %-28s %s%s%n", s.env(), s.flag(),
                s.help(), s.def() == null || s.def().isEmpty() ? "" : " [default: " + s.def() + "]"));
        }
        sb.append("\nstdout: CSV header, then one line per REPORT_INTERVAL_MS:\n  ").append(HEADER).append('\n')
          .append("stderr: INFO/WARN/ERROR lines. Exit 1 if the app reaches ERROR state, 2 on bad config.\n");
        System.out.print(sb);
    }

    private static Properties loadProps(String file) {
        Path p = Paths.get(file);
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(p)) {
            props.load(in);
        } catch (IOException e) {
            throw new ConfigError("cannot read properties file " + file + ": " + e);
        }
        return props;
    }

    private static Map<String, Object> toMap(Properties p) {
        Map<String, Object> m = new HashMap<>();
        for (String k : p.stringPropertyNames()) {
            m.put(k, p.getProperty(k));
        }
        return m;
    }

    private static String require(Map<String, String> cfg, String key) {
        String v = cfg.get(key);
        if (v == null || v.isEmpty()) {
            throw new ConfigError(key + " is required (env " + key + " or --"
                + key.toLowerCase(Locale.ROOT).replace('_', '-') + ")");
        }
        return v;
    }

    private static long parseLong(Map<String, String> cfg, String key) {
        try {
            return Long.parseLong(require(cfg, key));
        } catch (NumberFormatException e) {
            throw new ConfigError(key + " must be an integer, got '" + cfg.get(key) + "'");
        }
    }

    private static long positiveLong(Map<String, String> cfg, String key) {
        long v = parseLong(cfg, key);
        if (v <= 0) {
            throw new ConfigError(key + " must be > 0, got " + v);
        }
        return v;
    }

    private static long nonNegativeLong(Map<String, String> cfg, String key) {
        long v = parseLong(cfg, key);
        if (v < 0) {
            throw new ConfigError(key + " must be >= 0, got " + v);
        }
        return v;
    }

    private static int positiveInt(Map<String, String> cfg, String key) {
        long v = positiveLong(cfg, key);
        if (v > Integer.MAX_VALUE) {
            throw new ConfigError(key + " too large: " + v);
        }
        return (int) v;
    }

    private static int nonNegativeInt(Map<String, String> cfg, String key) {
        long v = nonNegativeLong(cfg, key);
        if (v > Integer.MAX_VALUE) {
            throw new ConfigError(key + " too large: " + v);
        }
        return (int) v;
    }

    private static String mask(String key, String value) {
        String k = key.toLowerCase(Locale.ROOT);
        return (k.contains("password") || k.contains("secret")) ? "****" : value;
    }

    private static Throwable rootCause(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c;
    }

    private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            if (type.isInstance(c)) {
                return true;
            }
        }
        return false;
    }

    static void info(String msg) {
        System.err.println("INFO: " + TS.format(Instant.now()) + " " + msg);
    }

    static void warn(String msg) {
        System.err.println("WARN: " + TS.format(Instant.now()) + " " + msg);
    }
}
