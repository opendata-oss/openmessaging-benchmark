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
import dev.opendata.common.ObjectStoreConfig;
import dev.opendata.common.StorageConfig;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.bookkeeper.stats.StatsLogger;

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

    private LogDb log;
    private OpenDataConfig config;

    /** Tracks partition count per topic for producer/consumer creation. */
    private final Map<String, Integer> topicPartitions = new ConcurrentHashMap<>();

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        this.config = OpenDataConfig.load(configurationFile);

        StorageConfig storageConfig = buildStorageConfig(config.storage);
        LogDbConfig logDbConfig = new LogDbConfig(storageConfig);
        this.log = LogDb.open(logDbConfig);
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
        BenchmarkProducer producer = new OpenDataBenchmarkProducer(log, topic, partitions);
        return CompletableFuture.completedFuture(producer);
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
            reader = log;
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
        if (log != null) {
            log.close();
        }
    }
}
