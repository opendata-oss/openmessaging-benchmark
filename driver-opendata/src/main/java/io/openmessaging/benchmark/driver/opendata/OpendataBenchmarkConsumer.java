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
import dev.opendata.LogEntry;
import dev.opendata.LogReader;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * OMB consumer implementation backed by LogReader.
 *
 * <p>Uses polling to simulate push-based consumption until a native push API
 * is available.
 *
 * <p>Architecture:
 * <ul>
 *   <li>One poller thread per partition-key, each tracking its own sequence</li>
 *   <li>Shared LogReader instance for all partition reads</li>
 *   <li>Shared BlockingQueue for entries from all partitions</li>
 *   <li>Single dispatcher thread calls the OMB callback (ensures thread-safety)</li>
 * </ul>
 *
 * <p>Configuration options (via {@link OpendataConfig.ConsumerConfig}):
 * <ul>
 *   <li>pollIntervalMs - sleep duration when no data available</li>
 *   <li>pollBatchSize - max entries per poll</li>
 *   <li>queueCapacity - backpressure threshold</li>
 * </ul>
 */
public class OpendataBenchmarkConsumer implements BenchmarkConsumer {

    // Default values (used when config not provided)
    private static final int DEFAULT_POLL_BATCH_SIZE = 1000;
    private static final long DEFAULT_POLL_INTERVAL_MS = 10;
    private static final int DEFAULT_QUEUE_CAPACITY = 10_000;

    private final LogReader reader;
    private final ConsumerCallback callback;
    private final BlockingQueue<LogEntry> entryQueue;
    private final ExecutorService pollerExecutor;
    private final ExecutorService dispatcherExecutor;
    private final List<PartitionPoller> pollers;
    private final AtomicBoolean running = new AtomicBoolean(true);

    // Configurable parameters
    private final int pollBatchSize;
    private final long pollIntervalMs;

    /**
     * Creates a consumer for a single partition with default config.
     */
    public OpendataBenchmarkConsumer(Log log, String topic, ConsumerCallback callback) {
        this(log, topic, 1, null, callback);
    }

    /**
     * Creates a consumer for multiple partitions with default config.
     */
    public OpendataBenchmarkConsumer(Log log, String topic, int numPartitions, ConsumerCallback callback) {
        this(log, topic, numPartitions, null, callback);
    }

    /**
     * Creates a consumer for multiple partitions with custom config.
     *
     * @param log           the Log instance
     * @param topic         the topic name (prefix for partition-keys)
     * @param numPartitions number of partitions to consume from
     * @param config        consumer configuration (nullable, uses defaults if null)
     * @param callback      the OMB callback for received messages
     */
    public OpendataBenchmarkConsumer(Log log, String topic, int numPartitions,
                            OpendataConfig.ConsumerConfig config, ConsumerCallback callback) {
        // Apply configuration or defaults
        this.pollBatchSize = config != null ? config.pollBatchSize : DEFAULT_POLL_BATCH_SIZE;
        this.pollIntervalMs = config != null ? config.pollIntervalMs : DEFAULT_POLL_INTERVAL_MS;
        int queueCapacity = config != null ? config.queueCapacity : DEFAULT_QUEUE_CAPACITY;

        this.reader = log.reader();
        this.callback = callback;
        this.entryQueue = new LinkedBlockingQueue<>(queueCapacity);
        this.pollers = new ArrayList<>(numPartitions);

        // Create poller for each partition
        for (int i = 0; i < numPartitions; i++) {
            String partitionKey = numPartitions == 1 ? topic : topic + "/" + i;
            pollers.add(new PartitionPoller(partitionKey));
        }

        // Thread pool for pollers (one thread per partition)
        this.pollerExecutor = Executors.newFixedThreadPool(numPartitions, r -> {
            Thread t = new Thread(r);
            t.setName("opendata-poller-" + topic);
            t.setDaemon(true);
            return t;
        });

        // Single dispatcher thread for callback invocation
        this.dispatcherExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "opendata-dispatcher-" + topic);
            t.setDaemon(true);
            return t;
        });

        start();
    }

    private void start() {
        // Start all partition pollers
        for (PartitionPoller poller : pollers) {
            pollerExecutor.submit(poller);
        }

        // Start dispatcher
        dispatcherExecutor.submit(this::dispatchLoop);
    }

    /**
     * Dispatcher loop: takes entries from queue and invokes callback.
     * Runs on a single thread to ensure callback thread-safety.
     */
    private void dispatchLoop() {
        while (running.get() || !entryQueue.isEmpty()) {
            try {
                LogEntry entry = entryQueue.poll(pollIntervalMs, TimeUnit.MILLISECONDS);
                if (entry != null) {
                    callback.messageReceived(entry.value(), entry.timestamp());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @Override
    public void close() throws Exception {
        running.set(false);

        // Shutdown pollers first
        pollerExecutor.shutdownNow();
        pollerExecutor.awaitTermination(5, TimeUnit.SECONDS);

        // Then shutdown dispatcher (let it drain the queue)
        dispatcherExecutor.shutdown();
        dispatcherExecutor.awaitTermination(5, TimeUnit.SECONDS);

        reader.close();
    }

    /**
     * Polls a single partition-key and enqueues entries.
     */
    private class PartitionPoller implements Runnable {
        private final byte[] partitionKey;
        private long currentSequence = 0;

        PartitionPoller(String partitionKey) {
            this.partitionKey = partitionKey.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void run() {
            while (running.get()) {
                try {
                    List<LogEntry> entries = reader.read(partitionKey, currentSequence, pollBatchSize);

                    if (entries.isEmpty()) {
                        // No data available, sleep before next poll
                        Thread.sleep(pollIntervalMs);
                    } else {
                        for (LogEntry entry : entries) {
                            // Block if queue is full (backpressure)
                            entryQueue.put(entry);
                            currentSequence = entry.sequence() + 1;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    if (running.get()) {
                        System.err.println("Error polling partition " +
                            new String(partitionKey, StandardCharsets.UTF_8) + ": " + e.getMessage());
                        try {
                            Thread.sleep(pollIntervalMs);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
        }
    }
}
