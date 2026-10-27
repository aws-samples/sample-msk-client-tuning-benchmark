// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package com.amazonaws.samples.msk.kstreams;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KStreamsEmulatorTest {

    @TempDir
    Path stateDir;

    private Properties props(String guarantee) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        p.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, guarantee);
        return p;
    }

    @Test
    void topologyHasRepartitionAndChangelogAndCountsPerDerivedKey() {
        Topology t = KStreamsEmulator.buildTopology("in", "out", "key-count-store", "value", 100, 8);
        String desc = t.describe().toString();
        assertTrue(desc.contains("rekey-repartition"), desc);
        assertTrue(desc.contains("key-count-store"), desc);

        for (String g : List.of(StreamsConfig.AT_LEAST_ONCE, StreamsConfig.EXACTLY_ONCE_V2)) {
            try (TopologyTestDriver d = new TopologyTestDriver(t, props(g))) {
                TestInputTopic<byte[], byte[]> in = d.createInputTopic("in", new ByteArraySerializer(), new ByteArraySerializer());
                TestOutputTopic<String, Long> out = d.createOutputTopic("out", new StringDeserializer(), new LongDeserializer());
                byte[] a = "AAAAAAAAxxxx".getBytes(StandardCharsets.US_ASCII);
                byte[] a2 = "AAAAAAAAyyyy".getBytes(StandardCharsets.US_ASCII); // same 8-byte prefix -> same key
                byte[] b = "BBBBBBBBxxxx".getBytes(StandardCharsets.US_ASCII);
                in.pipeInput(null, a);
                in.pipeInput(null, a2);
                in.pipeInput(null, b);
                in.pipeInput((byte[]) null, (byte[]) null);
                KeyValueStore<String, Long> store = d.getKeyValueStore("key-count-store");
                String ka = "k-" + Math.floorMod(KStreamsEmulator.prefixHash(a, 8), 100L);
                String kb = "k-" + Math.floorMod(KStreamsEmulator.prefixHash(b, 8), 100L);
                assertEquals(2L, store.get(ka), g);
                assertEquals(1L, store.get(kb), g);
                Map<String, Long> latest = out.readKeyValuesToMap();
                assertEquals(2L, latest.get(ka), g);
                assertTrue(d.producedTopicNames().contains("test-app-key-count-store-changelog"), g + " " + d.producedTopicNames());
                assertTrue(d.producedTopicNames().contains("test-app-rekey-repartition"), g + " " + d.producedTopicNames());
            }
        }
    }

    @Test
    void counterModeSpreadsKeysAndNoSinkWhenOutputEmpty() {
        Topology t = KStreamsEmulator.buildTopology("in", "", "key-count-store", "counter", 10, 8);
        assertTrue(!t.describe().toString().contains("KSTREAM-SINK"), t.describe().toString());
        try (TopologyTestDriver d = new TopologyTestDriver(t, props(StreamsConfig.AT_LEAST_ONCE))) {
            TestInputTopic<byte[], byte[]> in = d.createInputTopic("in", new ByteArraySerializer(), new ByteArraySerializer());
            byte[] same = "identical-payload".getBytes(StandardCharsets.US_ASCII);
            for (int i = 0; i < 100; i++) {
                in.pipeInput(null, same);
            }
            KeyValueStore<String, Long> store = d.getKeyValueStore("key-count-store");
            List<Long> counts = new ArrayList<>();
            store.all().forEachRemaining(kv -> counts.add(kv.value));
            assertEquals(10, counts.size());
            counts.forEach(c -> assertEquals(10L, c));
        }
    }
}
