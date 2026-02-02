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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.opendata.AppendResult;
import dev.opendata.Record;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class OpendataBenchmarkProducerTest {

    private LogAppender mockAppender;
    private OpendataBenchmarkProducer producer;

    @BeforeEach
    void setUp() {
        mockAppender = mock(LogAppender.class);
        when(mockAppender.append(any(Record[].class)))
                .thenReturn(new AppendResult(0, System.currentTimeMillis()));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    void sendAsync_shouldCompleteSuccessfully() throws Exception {
        producer = new OpendataBenchmarkProducer(mockAppender, "test-topic", 1);

        CompletableFuture<Void> future = producer.sendAsync(Optional.empty(), "test-payload".getBytes());

        // Should complete within reasonable time
        future.get(5, TimeUnit.SECONDS);

        // Verify append was called
        verify(mockAppender, timeout(1000).atLeastOnce()).append(any(Record[].class));
    }

    @Test
    void sendAsync_shouldBatchMultipleWrites() throws Exception {
        // Use a latch to control when append completes, allowing writes to queue up
        CountDownLatch appendLatch = new CountDownLatch(1);
        AtomicInteger appendCount = new AtomicInteger(0);
        List<Integer> batchSizes = new ArrayList<>();

        when(mockAppender.append(any(Record[].class))).thenAnswer(invocation -> {
            Record[] records = invocation.getArgument(0);
            batchSizes.add(records.length);
            appendCount.incrementAndGet();
            // First call waits, subsequent calls proceed immediately
            if (appendCount.get() == 1) {
                appendLatch.await(5, TimeUnit.SECONDS);
            }
            return new AppendResult(0, System.currentTimeMillis());
        });

        producer = new OpendataBenchmarkProducer(mockAppender, "test-topic", 1);

        // Send multiple messages quickly
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(producer.sendAsync(Optional.empty(), ("payload-" + i).getBytes()));
        }

        // Release the latch to allow batching
        Thread.sleep(50); // Give time for writes to queue
        appendLatch.countDown();

        // Wait for all futures to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);

        // Should have batched - fewer append calls than messages
        assertThat(appendCount.get()).isLessThan(100);
        // At least one batch should have multiple records
        assertThat(batchSizes.stream().anyMatch(size -> size > 1)).isTrue();
    }

    @Test
    void sendAsync_shouldPropagateErrors() throws Exception {
        RuntimeException testException = new RuntimeException("Test error");
        when(mockAppender.append(any(Record[].class))).thenThrow(testException);

        producer = new OpendataBenchmarkProducer(mockAppender, "test-topic", 1);

        CompletableFuture<Void> future = producer.sendAsync(Optional.empty(), "test-payload".getBytes());

        assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Test error");
    }

    @Test
    void sendAsync_withKey_shouldRouteToSamePartition() throws Exception {
        ArgumentCaptor<Record[]> recordsCaptor = ArgumentCaptor.forClass(Record[].class);
        when(mockAppender.append(recordsCaptor.capture()))
                .thenReturn(new AppendResult(0, System.currentTimeMillis()));

        producer = new OpendataBenchmarkProducer(mockAppender, "test-topic", 4);

        // Send multiple messages with the same key
        String key = "consistent-key";
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(producer.sendAsync(Optional.of(key), ("payload-" + i).getBytes()));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);

        // All records should have the same partition key
        List<Record[]> allBatches = recordsCaptor.getAllValues();
        String firstPartitionKey = null;
        for (Record[] batch : allBatches) {
            for (Record record : batch) {
                String partitionKey = new String(record.key());
                if (firstPartitionKey == null) {
                    firstPartitionKey = partitionKey;
                } else {
                    assertThat(partitionKey).isEqualTo(firstPartitionKey);
                }
            }
        }
    }

    @Test
    void sendAsync_withoutKey_shouldRoundRobin() throws Exception {
        ArgumentCaptor<Record[]> recordsCaptor = ArgumentCaptor.forClass(Record[].class);

        // Slow down appends to prevent batching
        when(mockAppender.append(recordsCaptor.capture())).thenAnswer(invocation -> {
            Thread.sleep(10);
            return new AppendResult(0, System.currentTimeMillis());
        });

        producer = new OpendataBenchmarkProducer(mockAppender, "test-topic", 4);

        // Send messages without keys
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            futures.add(producer.sendAsync(Optional.empty(), ("payload-" + i).getBytes()));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);

        // Collect all partition keys used
        List<String> partitionKeys = new ArrayList<>();
        for (Record[] batch : recordsCaptor.getAllValues()) {
            for (Record record : batch) {
                partitionKeys.add(new String(record.key()));
            }
        }

        // Should have used multiple partitions (round-robin)
        long distinctPartitions = partitionKeys.stream().distinct().count();
        assertThat(distinctPartitions).isGreaterThan(1);
    }

    @Test
    void close_shouldCompleteRemainingWrites() throws Exception {
        CountDownLatch appendStarted = new CountDownLatch(1);
        CountDownLatch appendCanProceed = new CountDownLatch(1);

        when(mockAppender.append(any(Record[].class))).thenAnswer(invocation -> {
            appendStarted.countDown();
            appendCanProceed.await(5, TimeUnit.SECONDS);
            return new AppendResult(0, System.currentTimeMillis());
        });

        producer = new OpendataBenchmarkProducer(mockAppender, "test-topic", 1);

        // Send a message
        CompletableFuture<Void> future = producer.sendAsync(Optional.empty(), "test-payload".getBytes());

        // Wait for append to start
        appendStarted.await(1, TimeUnit.SECONDS);

        // Close should wait for in-flight writes
        new Thread(() -> {
            try {
                Thread.sleep(100);
                appendCanProceed.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();

        producer.close();

        // Future should have completed
        assertThat(future.isDone()).isTrue();
    }

    @Test
    void close_shouldFailPendingWritesAfterShutdown() throws Exception {
        // Block the first append indefinitely
        CountDownLatch blockForever = new CountDownLatch(1);
        AtomicInteger appendCount = new AtomicInteger(0);

        when(mockAppender.append(any(Record[].class))).thenAnswer(invocation -> {
            if (appendCount.incrementAndGet() == 1) {
                // First append blocks
                blockForever.await();
            }
            return new AppendResult(0, System.currentTimeMillis());
        });

        producer = new OpendataBenchmarkProducer(mockAppender, "test-topic", 1);

        // Send first message (will block in append)
        CompletableFuture<Void> first = producer.sendAsync(Optional.empty(), "first".getBytes());

        // Wait a bit for the writer thread to pick it up
        Thread.sleep(50);

        // Send more messages that will queue up
        List<CompletableFuture<Void>> queued = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            queued.add(producer.sendAsync(Optional.empty(), ("queued-" + i).getBytes()));
        }

        // Close the producer (should interrupt the blocked append)
        producer.close();

        // Queued writes should fail with IllegalStateException
        for (CompletableFuture<Void> future : queued) {
            if (!future.isDone()) {
                assertThatThrownBy(() -> future.get(1, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class);
            }
        }
    }

    @Test
    void sendAsync_afterClose_shouldFail() throws Exception {
        producer = new OpendataBenchmarkProducer(mockAppender, "test-topic", 1);
        producer.close();

        CompletableFuture<Void> future = producer.sendAsync(Optional.empty(), "test".getBytes());

        assertThatThrownBy(() -> future.get(1, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void sendAsync_shouldCaptureTimestampAtSubmission() throws Exception {
        ArgumentCaptor<Record[]> recordsCaptor = ArgumentCaptor.forClass(Record[].class);

        // Delay the append
        when(mockAppender.append(recordsCaptor.capture())).thenAnswer(invocation -> {
            Thread.sleep(100);
            return new AppendResult(0, System.currentTimeMillis());
        });

        producer = new OpendataBenchmarkProducer(mockAppender, "test-topic", 1);

        long beforeSend = System.currentTimeMillis();
        CompletableFuture<Void> future = producer.sendAsync(Optional.empty(), "test".getBytes());
        long afterSend = System.currentTimeMillis();

        future.get(5, TimeUnit.SECONDS);

        // Get the captured record's timestamp
        Record[] records = recordsCaptor.getValue();
        long recordTimestamp = records[0].timestampMs();

        // Timestamp should be captured at send time, not append time
        assertThat(recordTimestamp).isBetween(beforeSend, afterSend);
    }
}
