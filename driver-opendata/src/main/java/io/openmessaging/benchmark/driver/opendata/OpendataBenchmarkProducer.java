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
import dev.opendata.Record;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * OMB producer implementation backed by Log.append().
 *
 * <p>Uses a batching writer pattern for efficiency and backpressure:
 * <ul>
 *   <li>Writes are queued with their timestamps (captured at sendAsync time)</li>
 *   <li>A background thread drains the queue and batches writes</li>
 *   <li>Bounded queue provides natural backpressure</li>
 * </ul>
 *
 * <p>Maps OMB's key distribution model to Log partition-keys:
 * <ul>
 *   <li>Producer manages N partition-keys: "{topic}/0", "{topic}/1", ..., "{topic}/N-1"</li>
 *   <li>OMB message keys are hashed to select a partition-key</li>
 *   <li>NO_KEY messages are round-robined across partitions</li>
 * </ul>
 */
public class OpendataBenchmarkProducer implements BenchmarkProducer {

    /** Maximum records per batch. */
    private static final int MAX_BATCH_SIZE = 1000;

    /** Queue capacity - provides backpressure when full. */
    private static final int QUEUE_CAPACITY = 10_000;

    private final LogAppender appender;
    private final byte[][] partitionKeys;
    private final int numPartitions;
    private final AtomicLong roundRobinCounter = new AtomicLong(0);

    private final BlockingQueue<PendingWrite> writeQueue;
    private final Thread writerThread;
    private volatile boolean closed = false;

    /**
     * A pending write waiting to be batched and sent to the Log.
     */
    private static class PendingWrite {
        final Record record;
        final CompletableFuture<Void> future;

        PendingWrite(Record record) {
            this.record = record;
            this.future = new CompletableFuture<>();
        }
    }

    /**
     * Creates a producer for a single partition.
     *
     * @param log   the LogDb instance
     * @param topic the topic name (used as the Log key)
     */
    public OpendataBenchmarkProducer(LogDb log, String topic) {
        this(LogAppender.wrap(log), topic, 1);
    }

    /**
     * Creates a producer with multiple partitions.
     *
     * @param log           the LogDb instance
     * @param topic         the topic name (prefix for partition-keys)
     * @param numPartitions number of partitions to distribute messages across
     */
    public OpendataBenchmarkProducer(LogDb log, String topic, int numPartitions) {
        this(LogAppender.wrap(log), topic, numPartitions);
    }

    /**
     * Creates a producer with a LogAppender (for testing).
     *
     * @param appender      the LogAppender to use
     * @param topic         the topic name (prefix for partition-keys)
     * @param numPartitions number of partitions to distribute messages across
     */
    OpendataBenchmarkProducer(LogAppender appender, String topic, int numPartitions) {
        this.appender = appender;
        this.numPartitions = numPartitions;
        this.partitionKeys = new byte[numPartitions][];

        for (int i = 0; i < numPartitions; i++) {
            String partitionKey = numPartitions == 1 ? topic : topic + "/" + i;
            this.partitionKeys[i] = partitionKey.getBytes(StandardCharsets.UTF_8);
        }

        this.writeQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        this.writerThread = new Thread(this::writerLoop, "opendata-producer-writer");
        this.writerThread.setDaemon(true);
        this.writerThread.start();
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> optionalKey, byte[] payload) {
        if (closed) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Producer is closed"));
            return future;
        }

        byte[] partitionKey = selectPartitionKey(optionalKey);
        Record record = new Record(partitionKey, payload);
        PendingWrite pending = new PendingWrite(record);

        try {
            writeQueue.put(pending); // blocks if queue is full = backpressure
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pending.future.completeExceptionally(e);
        }

        return pending.future;
    }

    @Override
    public void close() throws Exception {
        if (!closed) {
            closed = true;

            // Interrupt writer thread and wait for it to finish
            writerThread.interrupt();
            try {
                writerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Fail any remaining queued writes
            PendingWrite pending;
            while ((pending = writeQueue.poll()) != null) {
                pending.future.completeExceptionally(
                        new IllegalStateException("Producer closed before write completed"));
            }
        }
        // Note: Producer doesn't own the Log, so we don't close it
    }

    /**
     * Background writer loop that batches and flushes writes.
     *
     * <p>No linger time - we grab whatever is available and write immediately.
     * SlateDB handles batching/buffering internally.
     */
    private void writerLoop() {
        List<PendingWrite> batch = new ArrayList<>(MAX_BATCH_SIZE);

        while (!closed) {
            try {
                // Block until at least one item is available
                PendingWrite first = writeQueue.take();
                batch.add(first);

                // Grab whatever else is available (non-blocking), up to batch limit
                writeQueue.drainTo(batch, MAX_BATCH_SIZE - 1);

                // Write the batch immediately
                writeBatch(batch);

            } catch (InterruptedException e) {
                // Shutdown requested
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Fail all futures in the batch
                for (PendingWrite pw : batch) {
                    pw.future.completeExceptionally(e);
                }
            } finally {
                batch.clear();
            }
        }

        // Drain remaining items on shutdown
        writeQueue.drainTo(batch);
        if (!batch.isEmpty()) {
            try {
                writeBatch(batch);
            } catch (Exception e) {
                for (PendingWrite pw : batch) {
                    pw.future.completeExceptionally(e);
                }
            }
        }
    }

    /**
     * Writes a batch of records to the Log and completes their futures.
     *
     * @param batch the batch of pending writes to process
     */
    private void writeBatch(List<PendingWrite> batch) {
        if (batch.isEmpty()) {
            return;
        }

        int size = batch.size();
        Record[] records = new Record[size];
        for (int i = 0; i < size; i++) {
            records[i] = batch.get(i).record;
        }

        try {
            appender.append(records);

            // Complete all futures
            for (PendingWrite pw : batch) {
                pw.future.complete(null);
            }
        } catch (Exception e) {
            for (PendingWrite pw : batch) {
                pw.future.completeExceptionally(e);
            }
            throw e;
        }
    }

    /**
     * Selects the partition-key based on the OMB message key.
     *
     * <p>If a key is provided, it's hashed to deterministically select a partition
     * (same key always routes to same partition for ordering guarantees).
     * If no key, round-robin across partitions.
     *
     * @param optionalKey the optional message key for partition selection
     * @return the partition key bytes
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
}
