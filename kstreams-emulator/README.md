# kstreams-emulator

A small Kafka Streams application that emulates a typical stateful Kafka Streams microservice
workload on Amazon Managed Streaming for Apache Kafka (Amazon MSK). It accompanies the optional Amazon
Elastic Kubernetes Service (Amazon EKS) factors of Test 2, "Find what drives the load, one factor at a time"
([runbook](../docs/test-cases/test-2-one-factor-at-a-time.md)), in the blog post "Right-sizing Amazon MSK by
tuning your Kafka clients", and is used to measure what `processing.guarantee=at_least_once` (ALOS) and `exactly_once_v2` (EOS) cost the brokers,
what `commit.interval.ms` costs them, and how the number of stream (worker) threads
(`NUM_STREAM_THREADS`) changes the picture, mainly through the Amazon CloudWatch metric
`RequestHandlerAvgIdlePercent`.

One process (pod) is one `KafkaStreams` instance, with one `application.id` and one consumer group.

## What it emulates

```
INPUT_TOPIC (byte[]/byte[])
  -> selectKey(key derived from the value)       => <APPLICATION_ID>-rekey-repartition          (internal topic)
  -> groupByKey().count()  [persistent RocksDB]  => <APPLICATION_ID>-<STORE_NAME>-changelog     (internal topic)
  -> toStream().to(OUTPUT_TOPIC)                  (optional; skipped when OUTPUT_TOPIC is empty)
```

For each input record, the app does what a typical stateful Streams service does:

- It fetches the record from the source topic.
- It produces the record again to a repartition topic, then fetches it back from there.
- It updates a RocksDB store, which writes to a changelog topic.
- If a sink is configured, it emits the updated count to `OUTPUT_TOPIC`.
- It commits on every `commit.interval.ms`.

The record cache (`statestore.cache.max.bytes`) is flushed on each commit. As a result,
changelog and sink traffic depend on the commit interval as well as on the input rate.

The internal topic names above were verified with `TopologyTestDriver` in
`src/test/.../KStreamsEmulatorTest.java`. With the defaults they are
`kstreams-emulator-rekey-repartition` and `kstreams-emulator-key-count-store-changelog`.
Kafka Streams creates both, with `REPLICATION_FACTOR` (default 2; set it to match your cluster).
Because of this, the AWS Identity and Access Management (IAM) principal needs topic-create
permission on `<APPLICATION_ID>-*`.
`INPUT_TOPIC` and `OUTPUT_TOPIC` must already exist, because Streams does not create them.
(Whether Amazon MSK auto-creates topics depends on `auto.create.topics.enable` in the cluster
configuration. Do not rely on it.)

### Key derivation (`KEY_MODE`)

- `value` (default): `k-<FNV1a(first KEY_PREFIX_BYTES bytes of the value) mod KEY_CARDINALITY>`.
  The key depends only on the value.
- `counter`: round-robin `k-0 ... k-(KEY_CARDINALITY-1)` for each stream thread.
  Use this when the source payloads are not diverse.

The number of distinct keys you actually get is capped by the number of distinct payloads
in the input topic:

- `../scripts/MultiProducer.java` uses a pool of 4096 distinct payloads (`--payload-pool`),
  so you get at most 4096 keys.
- `kafka-producer-perf-test` without `--payload-file` sends the same random payload for every
  record, so in `value` mode you would get very few keys (possibly one hot key). Use
  `KEY_MODE=counter` with that producer.

## Build

Maven is not required on the host. The Maven wrapper downloads Maven 3.9.9 from Maven Central.

```bash
cd kstreams-emulator
./mvnw -q -DskipTests package          # -> target/kstreams-emulator.jar (fat jar, ~90 MB incl. RocksDB natives)
./mvnw -q package                      # same, plus TopologyTestDriver unit tests (no broker needed)
java -jar target/kstreams-emulator.jar --help
```

The fat jar requires Java 17 or later. The bytecode targets release 17.

The jar is built with these dependencies:

- `org.apache.kafka:kafka-streams:3.9.2`
- `software.amazon.msk:aws-msk-iam-auth:2.2.0` (includes AWS SDK v2 STS, for IRSA)
- `slf4j-simple 2.0.16`

## Configuration

Every setting can be given as an environment variable or as a CLI flag. The flag name is the
variable name in lower case with `-` instead of `_` (`--input-topic` for `INPUT_TOPIC`). If both
are set, **the environment variable wins**, so container env overrides the image's args. An env
var that is set to an empty string still wins: for example, `OUTPUT_TOPIC=` disables the sink.

| Env var | Default | Maps to / meaning |
|---|---|---|
| `BOOTSTRAP_SERVERS` | (required) | `bootstrap.servers` |
| `INPUT_TOPIC` | `load-bench-3` | source topic, ByteArray serdes |
| `OUTPUT_TOPIC` | empty | sink topic (String key, Long count). Empty means no sink. |
| `APPLICATION_ID` | `kstreams-emulator` | `application.id`. This is also the consumer group id and the internal topic prefix. |
| `PROCESSING_GUARANTEE` | `at_least_once` | `processing.guarantee`: `at_least_once` or `exactly_once_v2` |
| `COMMIT_INTERVAL_MS` | empty | `commit.interval.ms`. Empty uses the Kafka default: **30000 for ALOS, 100 for EOS** (verified, see below). The effective value is logged at startup and printed in every CSV line. |
| `NUM_STREAM_THREADS` | `1` | `num.stream.threads` |
| `REPLICATION_FACTOR` | `2` | `replication.factor` for the repartition and changelog topics |
| `STATE_DIR` | `/tmp/kstreams-state` | `state.dir` (RocksDB) |
| `STATESTORE_CACHE_MAX_BYTES` | empty | `statestore.cache.max.bytes`. Empty uses the Kafka default of 10485760 (verified). |
| `STORE_NAME` | `key-count-store` | name of the count store. The changelog topic is `<app>-<store>-changelog`. |
| `KEY_CARDINALITY` | `10000` | number of distinct derived keys |
| `KEY_PREFIX_BYTES` | `8` | for `KEY_MODE=value`, the number of leading value bytes that are hashed |
| `KEY_MODE` | `value` | `value` or `counter` (see above) |
| `AUTO_OFFSET_RESET` | `latest` | `main.consumer.auto.offset.reset`, used only when the group has no committed offsets. The Kafka Streams default is `earliest` (verified). `latest` avoids a backlog burst at the start of a test. |
| `CLIENT_PROPS_FILE` | empty | client tuning file, for example `../scripts/profiles/aws-tuned.properties` or `../scripts/client-tuning.properties` (routing described below) |
| `AUTH_PROPS_FILE` | empty | security properties file, merged unprefixed so it applies to every client (producer, consumers, admin) |
| `MSK_IAM` | `auto` | `true` injects the four MSK IAM props. `auto` does the same only if no `security.protocol` is configured and `BOOTSTRAP_SERVERS` contains `:9098`. `false` never injects them. |
| `REPORT_INTERVAL_MS` | `60000` | interval between CSV stats lines |
| `MAX_THREAD_REPLACEMENTS` | `10` | uncaught exceptions are answered with `REPLACE_THREAD` this many times, then with `SHUTDOWN_CLIENT` (ERROR state, exit 1). A missing source topic goes straight to `SHUTDOWN_CLIENT`. |
| `LOG_LEVEL` | `warn` | slf4j-simple default level for Kafka and AWS SDK logs |
| `DUMP_METRIC_NAMES` | `false` | `true` prints every available metric `group:name` once on stderr |

The MSK IAM props that are injected (or that you can put in `AUTH_PROPS_FILE`):

```properties
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

### How `CLIENT_PROPS_FILE` keys are routed

Unprefixed tuning keys are routed so that, for example, producer `linger.ms` does not reach the
restore consumer, and Streams-owned settings are never overridden:

| Key kind | Routed as | Examples |
|---|---|---|
| already prefixed | unchanged | `producer.linger.ms`, `main.consumer.fetch.min.bytes`, `restore.consumer.*`, `admin.*`, `topic.*` |
| security | unprefixed (all clients) | `security.protocol`, `sasl.*`, `ssl.*` |
| producer-only | `producer.` (`StreamsConfig.producerPrefix`) | `linger.ms`, `batch.size`, `compression.type`, `acks`, `buffer.memory`, `enable.idempotence` |
| consumer-only | `main.consumer.` (`StreamsConfig.mainConsumerPrefix`) | `fetch.min.bytes`, `fetch.max.wait.ms`, `max.poll.records`, `max.partition.fetch.bytes`, `session.timeout.ms` |
| producer and consumer, or Streams | unprefixed | `send.buffer.bytes`, `receive.buffer.bytes`, `connections.max.idle.ms` |
| owned by Streams | **skipped, with a WARN** | `group.id`, `enable.auto.commit`, `auto.commit.interval.ms`, `partition.assignment.strategy`, `isolation.level`, `transactional.id`, `client.id`, serializers and deserializers, `bootstrap.servers`, `application.id` |

Explicit settings such as `APPLICATION_ID` and `PROCESSING_GUARANTEE` always override the
properties files. All effective `streams-prop` values are logged on stderr at startup. With
the `aws-tuned` profile (`../scripts/profiles/aws-tuned.properties`), the log shows `producer.=7 main.consumer.=7
unprefixed=3 skipped=2`. The two skipped keys are `auto.commit.interval.ms` and
`partition.assignment.strategy`, which Streams controls.

## Example runs

```bash
# ALOS, Kafka default commit interval (30 s), aws-tuned client profile, IAM auto-injected (port 9098)
BOOTSTRAP_SERVERS=REPLACE_ME_BOOTSTRAP_SERVERS \
INPUT_TOPIC=load-bench-3 APPLICATION_ID=kse-alos-1 \
CLIENT_PROPS_FILE=../scripts/profiles/aws-tuned.properties \
java -jar target/kstreams-emulator.jar > kse-alos-1.csv

# EOS v2, Kafka default commit interval (100 ms), with a sink topic
BOOTSTRAP_SERVERS=... PROCESSING_GUARANTEE=exactly_once_v2 APPLICATION_ID=kse-eos-1 \
OUTPUT_TOPIC=kse-out CLIENT_PROPS_FILE=../scripts/profiles/aws-tuned.properties \
java -jar target/kstreams-emulator.jar > kse-eos-1.csv

# EOS v2 with the commit interval stretched to 1 s (isolates the commit-interval effect)
... PROCESSING_GUARANTEE=exactly_once_v2 COMMIT_INTERVAL_MS=1000 ...

# Same workload with 4 stream (worker) threads in one instance
... NUM_STREAM_THREADS=4 ...

# A/B against your own production settings: use the baseline profile (or your own profile)
... CLIENT_PROPS_FILE=../scripts/profiles/baseline.properties ...

# Container (image built from the Dockerfile; never built or pushed by this repo's scripts)
docker run --rm -e BOOTSTRAP_SERVERS=... -e APPLICATION_ID=kse-eos-1 -e PROCESSING_GUARANTEE=exactly_once_v2 \
  -v $PWD/../scripts/profiles/aws-tuned.properties:/app/client-tuning.properties:ro \
  -e CLIENT_PROPS_FILE=/app/client-tuning.properties kstreams-emulator:0.1.0
```

Use a new `APPLICATION_ID` for each run. Otherwise the group resumes from committed
offsets and reuses the internal topics and the local state from the previous run.

## Output

**stdout**: one CSV header at startup, then one line every `REPORT_INTERVAL_MS`, plus a final
line at shutdown. The format matches the other tools in `../scripts/`:

```
time, app_id, state, processing_guarantee, commit_interval_ms, threads_alive, records_processed_delta,
process_rate, commit_rate, commit_latency_avg_ms, consumer_offset_commit_rate, producer_record_send_rate,
producer_request_rate, txn_commit_time_ms_delta, txn_send_offsets_time_ms_delta
```

| Column | Source in `KafkaStreams#metrics()` (group:name), summed over threads and clients |
|---|---|
| `records_processed_delta` | `stream-thread-metrics:process-total`, delta since the last line (reset-safe when a thread is replaced) |
| `process_rate` | `stream-thread-metrics:process-rate` |
| `commit_rate` | `stream-thread-metrics:commit-rate` (Streams commits per second) |
| `commit_latency_avg_ms` | `stream-thread-metrics:commit-latency-avg` (averaged over threads) |
| `consumer_offset_commit_rate` | `consumer-coordinator-metrics:commit-rate` (OffsetCommit requests from the main consumer; expected near 0 under EOS, because offsets go through the transaction. Check it on your own run.) |
| `producer_record_send_rate` | `producer-metrics:record-send-rate` |
| `producer_request_rate` | `producer-metrics:request-rate` (all producer requests, including transactional ones) |
| `txn_commit_time_ms_delta` | EOS only: delta of `producer-metrics:txn-commit-time-ns-total`, in ms |
| `txn_send_offsets_time_ms_delta` | EOS only: delta of `producer-metrics:txn-send-offsets-time-ns-total`, in ms |

A metric that is missing (or is NaN, for example commit latency before the first commit) is
printed as `na`. The reporter never crashes the app. All of these metric names were confirmed
present in a Kafka Streams 3.9.x run with `DUMP_METRIC_NAMES=true`; set it yourself to list the
names your client version exposes.

**stderr**: `INFO:` (config, topology, state transitions), `WARN:`, `ERROR:` (uncaught
exceptions, ERROR state, close timeouts), and Kafka and AWS SDK logs at `LOG_LEVEL`.

**Exit codes**:

- 0: clean stop.
- 1: the app reached the ERROR state, or `start()` failed.
- 2: invalid configuration.
- 143: SIGTERM. A shutdown hook calls `close(Duration.ofSeconds(30))` first, then the JVM
  reports 143.

## ALOS vs EOS: what changes for the brokers

| | `at_least_once` | `exactly_once_v2` |
|---|---|---|
| default `commit.interval.ms` | **30000** (verified) | **100** (verified) |
| what a commit is | flush producer, then the main consumer sends **OffsetCommit** | flush, then `AddOffsetsToTxn` / `TxnOffsetCommit` / `EndTxn` to the transaction coordinator, which writes **transaction markers** to every partition the transaction touched (repartition, changelog, sink, `__consumer_offsets`) (protocol behaviour per KIP-98/KIP-447; not measured here, see the per-request-type rates below) |
| producers | one per stream thread | one **transactional** producer per stream thread (`transactional.id=<app>-<processId>-<threadIdx>`, seen in the local run log) |
| producer overrides set by Streams | `linger.ms=100` (verified) | `linger.ms=100`, `enable.idempotence=true`, `delivery.timeout.ms=2147483647`, `transaction.timeout.ms=10000` (all verified) |
| main consumer | `max.poll.records=1000`, `enable.auto.commit=false`, `auto.offset.reset=earliest` (verified) | the same, plus `isolation.level=read_committed` (verified) |
| cache flush | every 30 s, so changelog and sink writes are heavily de-duplicated | every 100 ms, so many more changelog and sink records and smaller batches |
| internal topics | repartition and changelog (the same in both modes) | the same, plus `__transaction_state` traffic |

The CLIENT_PROPS_FILE values (for example `linger.ms=25` in the `aws-tuned` profile) override the Streams `linger.ms=100`
default because user-supplied producer configs win. This was checked with
`StreamsConfig#getProducerConfigs` and is visible in the startup `streams-prop` log.

Other verified Kafka Streams 3.9.x defaults:

- `statestore.cache.max.bytes=10485760`
- `repartition.purge.interval.ms=30000`. Streams periodically calls DeleteRecords on the
  repartition topic. This is additional admin traffic.
- `num.standby.replicas=0`
- `topology.optimization=none`
- `replication.factor=-1` (broker default). This app overrides it with `REPLICATION_FACTOR` (default 2).

## Broker metrics to watch (Amazon CloudWatch `AWS/Kafka`, per broker)

- **`RequestHandlerAvgIdlePercent`**: the main KPI. A lower value means busier request handler threads.
- **`ProduceTotalTimeMsMean`** (and `FetchConsumerTotalTimeMsMean`): request latency as seen by the broker.
- **`BytesInPerSec` / `BytesOutPerSec`**: the repartition, changelog and sink topics add both ingress and egress.
- **`ConnectionCount`** (and `ConnectionCreationRate`): each instance holds connections for
  its producer(s), main and restore consumers, and admin client.
- Optional: `MessagesInPerSec`, `ProduceMessageConversionsPerSec`, and `NetworkProcessorAvgIdlePercent`.
  Per-request-type rates such as EndTxn, AddOffsetsToTxn, TxnOffsetCommit and WriteTxnMarkers
  are visible in Open Monitoring (Prometheus JMX `kafka.network:type=RequestMetrics,name=RequestsPerSec,request=*`)
  but not in the default CloudWatch metrics (check your cluster's monitoring level).

Suggested matrix, with the same input load and tuning each time:

1. ALOS with the default 30 s commit interval
2. ALOS with `COMMIT_INTERVAL_MS=100`
3. EOS with the default 100 ms commit interval
4. EOS with `COMMIT_INTERVAL_MS=1000`
5. EOS with `COMMIT_INTERVAL_MS=30000`

Compare 1 vs 2 and 3 vs 5 to separate the cost of transactions from the cost of the commit frequency.
Repeat a case with a different `NUM_STREAM_THREADS` (for example 1 vs 4) to see the effect of the
worker thread count: each stream thread has its own producer (transactional under EOS), so more
threads mean more producers, connections and, under EOS, more transactions per commit interval.

## How the defaults were verified

The values were first read from the kafka-streams 3.9.0 classes inside the fat jar. The pom now pins
Kafka 3.9.2; the commit-interval constants below were re-checked against the kafka-streams 3.9.2 jar
and are unchanged. Re-run these checks if you change `kafka.version`:

```bash
javap -cp target/kstreams-emulator.jar -constants -p org.apache.kafka.streams.StreamsConfig | grep COMMIT_INTERVAL
#   private static final long DEFAULT_COMMIT_INTERVAL_MS = 30000l;
#   private static final long EOS_DEFAULT_COMMIT_INTERVAL_MS = 100l;
```

They were also read at runtime, with jshell on the same jar:
`new StreamsConfig(props).getLong("commit.interval.ms")` returns `30000` for `at_least_once`
and `100` for `exactly_once_v2`. The other defaults in the tables came from
`getProducerConfigs` and `getMainConsumerConfigs`. The app logs the effective value at startup:
`INFO: effective commit.interval.ms=100 (Kafka default for exactly_once_v2)`.

## Files

- `pom.xml`: Maven build with the shade plugin, producing `target/kstreams-emulator.jar`.
- `mvnw`, `mvnw.cmd`, `.mvn/wrapper/maven-wrapper.properties`: the Maven wrapper (Maven 3.9.9).
- `src/main/java/com/amazonaws/samples/msk/kstreams/KStreamsEmulator.java`: the app.
- `src/main/resources/simplelogger.properties`: slf4j-simple defaults (WARN, to stderr).
- `src/test/java/.../KStreamsEmulatorTest.java`: topology and RocksDB tests that run without a broker.
- `Dockerfile` and `.dockerignore`: runtime image (eclipse-temurin:17-jre, non-root uid 10001).

## Known limitations

- Automated tests (`TopologyTestDriver`) run without a broker. They cover the topology, the
  internal topic names and the RocksDB store, but not broker interaction.
- The exit-1-on-ERROR path (for example a missing source topic) needs a broker to exercise.
- `KEY_MODE=counter` is not deterministic across restarts. This is fine for load emulation,
  but it is not a correct "count" semantically.
- The fat jar is about 90 MB because it bundles RocksDB natives for every platform.
- With a local `~/.aws/config` whose profile names the AWS SDK rejects, you see repeated
  `ProfileFileReader` WARN lines. These are harmless and do not occur in containers without
  that file.
