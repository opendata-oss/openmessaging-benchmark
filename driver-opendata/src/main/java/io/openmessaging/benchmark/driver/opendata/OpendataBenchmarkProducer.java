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

import dev.opendata.Log;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * OMB producer implementation backed by Log.append().
 *
 * <p>Maps OMB's key distribution model to Log partition-keys:
 * <ul>
 *   <li>Producer manages N partition-keys: "{topic}/0", "{topic}/1", ..., "{topic}/N-1"</li>
 *   <li>OMB message keys are hashed to select a partition-key</li>
 *   <li>NO_KEY messages are round-robined across partitions</li>
 * </ul>
 */
public class OpendataBenchmarkProducer implements BenchmarkProducer {

    private final Log log;
    private final byte[][] partitionKeys;
    private final int numPartitions;
    private final AtomicLong roundRobinCounter = new AtomicLong(0);

    /**
     * Creates a producer for a single partition (Phase 1).
     *
     * @param log   the Log instance
     * @param topic the topic name (used as the Log key)
     */
    public OpendataBenchmarkProducer(Log log, String topic) {
        this(log, topic, 1);
    }

    /**
     * Creates a producer with multiple partitions.
     *
     * @param log           the Log instance
     * @param topic         the topic name (prefix for partition-keys)
     * @param numPartitions number of partitions to distribute messages across
     */
    public OpendataBenchmarkProducer(Log log, String topic, int numPartitions) {
        this.log = log;
        this.numPartitions = numPartitions;
        this.partitionKeys = new byte[numPartitions][];

        for (int i = 0; i < numPartitions; i++) {
            String partitionKey = numPartitions == 1 ? topic : topic + "/" + i;
            this.partitionKeys[i] = partitionKey.getBytes(StandardCharsets.UTF_8);
        }
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> optionalKey, byte[] payload) {
        byte[] partitionKey = selectPartitionKey(optionalKey);
        return log.appendAsync(partitionKey, payload)
                .thenApply(result -> null);
    }

    /**
     * Selects the partition-key based on the OMB message key.
     *
     * <p>If a key is provided, it's hashed to deterministically select a partition
     * (same key always routes to same partition for ordering guarantees).
     * If no key, round-robin across partitions.
     */
    private byte[] selectPartitionKey(Optional<String> optionalKey) {
        if (numPartitions == 1) {
            return partitionKeys[0];
        }

        int partitionIndex;
        if (optionalKey.isPresent()) {
            // Hash the key to select partition (deterministic routing)
            partitionIndex = Math.abs(optionalKey.get().hashCode()) % numPartitions;
        } else {
            // Round-robin for NO_KEY
            partitionIndex = (int) (roundRobinCounter.getAndIncrement() % numPartitions);
        }

        return partitionKeys[partitionIndex];
    }

    @Override
    public void close() throws Exception {
        // Producer doesn't own the Log, so nothing to close
    }
}
