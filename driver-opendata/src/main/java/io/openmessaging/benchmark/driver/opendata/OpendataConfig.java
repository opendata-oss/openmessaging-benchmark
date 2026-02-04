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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;

/**
 * Configuration for the OpenData OMB driver.
 *
 * <p>Example YAML configuration:
 * <pre>
 * name: OpenData
 * driverClass: io.openmessaging.benchmark.driver.opendata.OpendataBenchmarkDriver
 *
 * # Log storage configuration
 * storage:
 *   type: in-memory  # or "slatedb"
 *   path: /tmp/opendata-benchmark
 *
 * # Producer settings
 * producer:
 *   # Reserved for future batching options
 *
 * # Consumer settings
 * consumer:
 *   pollIntervalMs: 10
 *   pollBatchSize: 1000
 *   queueCapacity: 10000
 * </pre>
 */
public class OpendataConfig {

    /** Driver name (displayed in benchmark results). */
    public String name = "OpenData";

    /** Driver class (used by OMB to instantiate the driver). */
    public String driverClass = "io.openmessaging.benchmark.driver.opendata.OpendataBenchmarkDriver";

    /** Log storage configuration. */
    public StorageConfig storage = new StorageConfig();

    /** Producer configuration. */
    public ProducerConfig producer = new ProducerConfig();

    /** Consumer configuration. */
    public ConsumerConfig consumer = new ConsumerConfig();

    /**
     * Loads configuration from a YAML file.
     *
     * @param configFile the YAML configuration file to load
     * @return the parsed configuration
     * @throws IOException if the file cannot be read or parsed
     */
    public static OpendataConfig load(File configFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readValue(configFile, OpendataConfig.class);
    }

    /**
     * Storage backend configuration.
     */
    public static class StorageConfig {
        /**
         * Storage type: "in-memory" or "slatedb".
         * Default: "in-memory" for testing.
         */
        public String type = "in-memory";

        /**
         * Path for SlateDB storage (ignored for in-memory).
         * Default: "/tmp/opendata-benchmark"
         */
        public String path = "/tmp/opendata-benchmark";

        /**
         * Object store type for SlateDB: "in-memory", "local", or "s3".
         * Default: "local"
         */
        @JsonProperty("objectStore")
        public String objectStore = "local";

        /**
         * S3 bucket name (only used when objectStore is "s3").
         */
        @JsonProperty("s3Bucket")
        public String s3Bucket;

        /**
         * S3 region (only used when objectStore is "s3").
         */
        @JsonProperty("s3Region")
        public String s3Region;

        /**
         * Path to SlateDB settings file (optional).
         * Used to configure SlateDB parameters like l0_sst_size_bytes.
         */
        @JsonProperty("settingsPath")
        public String settingsPath;
    }

    /**
     * Producer configuration.
     */
    public static class ProducerConfig {
        // Reserved for future options like:
        // - batchingEnabled
        // - batchingMaxMessages
        // - batchingMaxBytes
        // - lingerMs
    }

    /**
     * Consumer configuration.
     */
    public static class ConsumerConfig {
        /**
         * Use a separate LogDb instance for the consumer.
         * When true, creates an independent reader that accesses storage directly,
         * simulating a separate process. This provides more realistic end-to-end
         * latency measurements.
         * When false, shares the producer's LogDb instance (faster but less realistic).
         * Default: true
         */
        @JsonProperty("separateReader")
        public boolean separateReader = true;

        /**
         * Interval between polls when no data is available (ms).
         * Lower values reduce latency but increase CPU usage.
         * Default: 10ms
         */
        @JsonProperty("pollIntervalMs")
        public long pollIntervalMs = 10;

        /**
         * Maximum entries to read per poll.
         * Default: 1000
         */
        @JsonProperty("pollBatchSize")
        public int pollBatchSize = 1000;

        /**
         * Capacity of the internal queue between pollers and dispatcher.
         * Provides backpressure when consumer callback is slow.
         * Default: 10000
         */
        @JsonProperty("queueCapacity")
        public int queueCapacity = 10_000;
    }
}
