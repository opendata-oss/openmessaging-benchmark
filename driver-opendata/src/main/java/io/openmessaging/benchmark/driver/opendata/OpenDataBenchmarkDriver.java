/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.opendata;

import dev.opendata.LogDb;
import dev.opendata.LogDbConfig;
import dev.opendata.LogDbReader;
import dev.opendata.LogDbReaderConfig;
import dev.opendata.LogRead;
import dev.opendata.Logging;
import dev.opendata.ReadVisibility;
import dev.opendata.SegmentConfig;
import dev.opendata.Telemetry;
import dev.opendata.common.ObjectStoreConfig;
import dev.opendata.common.StorageConfig;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OpenMessaging Benchmark driver for OpenData Log.
 *
 * <p>Maps OMB concepts to Log operations:
 * <ul>
 *   <li>Topic + Partitions → Log partition-keys: "{topic}/0", "{topic}/1", ...</li>
 *   <li>Producer → Log.append() with key routing</li>
 *   <li>Consumer → LogReader (polling-based initially)</li>
 * </ul>
 *
 * <p>Configuration is loaded from a YAML file. See {@link OpenDataConfig} for options.
 */
public class OpenDataBenchmarkDriver implements BenchmarkDriver {

    private static final Logger log = LoggerFactory.getLogger(OpenDataBenchmarkDriver.class);

    /**
     * SlateDB metrics surfaced by the telemetry print loop. Order is preserved
     * in the log line. The native tracing subscriber rewrites dotted metric
     * names (e.g. {@code slatedb.db.l0_sst_count}) to underscores via
     * metrics-exporter-prometheus.
     */
    private static final String[] TELEMETRY_METRICS = {
            "slatedb_db_l0_sst_count",
            "slatedb_db_segment_max_l0_sst_count",
            "slatedb_db_backpressure_count",
            "slatedb_db_total_mem_size_bytes",
            "slatedb_db_immutable_memtable_flushes",
            "slatedb_db_l0_flush_bytes",
    };

    private LogDb logDb;
    private OpenDataConfig config;
    private ScheduledExecutorService telemetryExecutor;

    /** Tracks partition count per topic for producer/consumer creation. */
    private final Map<String, Integer> topicPartitions = new ConcurrentHashMap<>();

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        this.config = OpenDataConfig.load(configurationFile);

        // Tracing subscriber must be installed before any LogDb open emits logs.
        if (config.telemetry.logFilter != null) {
            Logging.enable(config.telemetry.logFilter);
        }

        StorageConfig storageConfig = buildStorageConfig(config.storage);
        ReadVisibility readVisibility = ReadVisibility.valueOf(config.storage.readVisibility);
        LogDbConfig logDbConfig = new LogDbConfig(storageConfig, SegmentConfig.DEFAULT, readVisibility);
        this.logDb = LogDb.open(logDbConfig);

        if (config.telemetry.enabled) {
            Telemetry.init();
            startTelemetryLoop(config.telemetry.printIntervalMs);
        }

        log.info("OpenData driver initialized; telemetry.enabled={}, telemetry.logFilter={}, telemetry.printIntervalMs={}",
                config.telemetry.enabled,
                config.telemetry.logFilter,
                config.telemetry.printIntervalMs);
    }

    private StorageConfig buildStorageConfig(OpenDataConfig.StorageConfig storage) {
        if ("in-memory".equalsIgnoreCase(storage.type)) {
            return new StorageConfig.InMemory();
        }

        // SlateDB storage
        ObjectStoreConfig objectStoreConfig = buildObjectStoreConfig(storage);
        return new StorageConfig.SlateDb(storage.path, objectStoreConfig, storage.settingsPath);
    }

    private ObjectStoreConfig buildObjectStoreConfig(OpenDataConfig.StorageConfig storage) {
        if ("in-memory".equalsIgnoreCase(storage.objectStore)) {
            return new ObjectStoreConfig.InMemory();
        } else if ("s3".equalsIgnoreCase(storage.objectStore)) {
            return new ObjectStoreConfig.Aws(storage.s3Region, storage.s3Bucket);
        } else {
            // Default to local
            return new ObjectStoreConfig.Local(storage.path);
        }
    }

    @Override
    public String getTopicNamePrefix() {
        return "opendata-";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        // Log doesn't require explicit topic/key creation
        // Keys are created implicitly on first append
        // Track partition count for later producer/consumer creation
        topicPartitions.put(topic, partitions);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        int partitions = topicPartitions.getOrDefault(topic, 1);
        BenchmarkProducer producer = new OpenDataBenchmarkProducer(logDb, topic, partitions);
        return CompletableFuture.completedFuture(producer);
    }

    @Override
    public CompletableFuture<Void> ensureTopicsAreReady(List<BenchmarkProducer> producers) {
        // LogDb keys are created on first append and LogDbReader sees writes as soon as the
        // underlying SlateDB makes them visible — no broker-side priming needed. Skipping the
        // default probe also avoids stalling on durability when WAL is disabled, since a single
        // probe message wouldn't fill the L0 threshold.
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic,
            String subscriptionName,
            ConsumerCallback callback) {
        int partitions = topicPartitions.getOrDefault(topic, 1);

        // Create reader based on configuration
        LogRead reader;
        LogDbReader ownedReader = null;
        if (config.consumer.separateReader) {
            // Create separate LogDbReader for realistic e2e latency measurement
            StorageConfig storageConfig = buildStorageConfig(config.storage);
            LogDbReaderConfig readerConfig = new LogDbReaderConfig(
                    storageConfig, Long.valueOf(config.consumer.refreshIntervalMs));
            ownedReader = LogDbReader.open(readerConfig);
            reader = ownedReader;
        } else {
            // Share the producer's LogDb instance
            reader = logDb;
        }

        // Consumer reads from all partitions for this topic
        BenchmarkConsumer consumer = new OpenDataBenchmarkConsumer(
                reader,
                ownedReader,  // null if sharing LogDb, non-null if we created a LogDbReader
                topic,
                partitions,
                config.consumer,
                callback);
        return CompletableFuture.completedFuture(consumer);
    }

    @Override
    public void close() throws Exception {
        if (telemetryExecutor != null) {
            telemetryExecutor.shutdownNow();
            telemetryExecutor.awaitTermination(2, TimeUnit.SECONDS);
        }
        if (logDb != null) {
            logDb.close();
        }
    }

    private void startTelemetryLoop(long intervalMs) {
        telemetryExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "opendata-telemetry");
            t.setDaemon(true);
            return t;
        });
        telemetryExecutor.scheduleAtFixedRate(
                this::printTelemetrySnapshot, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void printTelemetrySnapshot() {
        try {
            String text = Telemetry.renderMetrics();
            if (text.isEmpty()) {
                log.warn("[telemetry] renderMetrics returned empty — foreign metrics recorder?");
                return;
            }
            StringBuilder line = new StringBuilder("[telemetry]");
            for (String metric : TELEMETRY_METRICS) {
                line.append(' ').append(shortName(metric)).append('=')
                        .append(extractMetric(text, metric));
            }
            log.info(line.toString());
        } catch (Throwable t) {
            // Never let a telemetry tick kill the executor.
            log.warn("telemetry snapshot failed", t);
        }
    }

    /** Strips the {@code slatedb_db_} prefix for terser log lines. */
    private static String shortName(String metric) {
        return metric.startsWith("slatedb_db_") ? metric.substring("slatedb_db_".length()) : metric;
    }

    private static String extractMetric(String text, String metricName) {
        Pattern p = Pattern.compile(
                "^" + Pattern.quote(metricName) + "(?:\\{[^}]*\\})?\\s+(\\S+)\\s*$",
                Pattern.MULTILINE);
        Matcher m = p.matcher(text);
        return m.find() ? m.group(1) : "-";
    }
}
