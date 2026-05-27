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

import dev.opendata.AppendResult;
import dev.opendata.LogDb;
import dev.opendata.Record;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * OMB producer backed by {@link LogDb#appendTimeout(Record[], long)}.
 *
 * <p>Each {@code sendAsync} appends a single record and chains the returned OMB
 * future to {@link AppendResult#durable()} so completion signals durability,
 * matching Kafka's {@code acks=all} semantics.
 *
 * <p>OMB message keys map to log partition-keys named
 * {@code "{topic}/{partitionIndex}"}; when {@code numPartitions == 1} the topic
 * name is used directly. Keyed messages hash to a deterministic partition;
 * keyless messages round-robin.
 */
public class OpenDataBenchmarkProducer implements BenchmarkProducer {

    /** Matches Kafka producer's {@code max.block.ms} default. */
    private static final long APPEND_TIMEOUT_MS = 60_000;

    /** Process-wide instrumentation. Sufficient because the bench runs a single producer. */
    private static final AtomicLong APPEND_CALLS = new AtomicLong();
    private static final AtomicLong APPEND_NANOS = new AtomicLong();

    public static long getAppendCalls() {
        return APPEND_CALLS.get();
    }

    public static long getAppendNanos() {
        return APPEND_NANOS.get();
    }

    private final LogDb log;
    private final byte[][] partitionKeys;
    private final int numPartitions;
    private final AtomicLong roundRobinCounter = new AtomicLong(0);
    private volatile boolean closed = false;

    public OpenDataBenchmarkProducer(LogDb log, String topic, int numPartitions) {
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
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("Producer is closed"));
        }

        byte[] partitionKey = selectPartitionKey(optionalKey);
        Record record = new Record(partitionKey, payload, System.currentTimeMillis());

        AppendResult result;
        long t0 = System.nanoTime();
        try {
            result = log.appendTimeout(new Record[]{record}, APPEND_TIMEOUT_MS);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        } finally {
            APPEND_NANOS.addAndGet(System.nanoTime() - t0);
            APPEND_CALLS.incrementAndGet();
        }

        CompletableFuture<Void> ombFuture = new CompletableFuture<>();
        result.durable().whenComplete((unused, ex) -> {
            if (ex != null) {
                ombFuture.completeExceptionally(ex);
            } else {
                ombFuture.complete(null);
            }
        });
        return ombFuture;
    }

    @Override
    public CompletableFuture<Void> flush() {
        if (closed) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            log.flush();
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    private byte[] selectPartitionKey(Optional<String> optionalKey) {
        if (numPartitions == 1) {
            return partitionKeys[0];
        }
        int partitionIndex;
        if (optionalKey.isPresent()) {
            partitionIndex = Math.abs(optionalKey.get().hashCode()) % numPartitions;
        } else {
            partitionIndex = (int) (roundRobinCounter.getAndIncrement() % numPartitions);
        }
        return partitionKeys[partitionIndex];
    }
}
