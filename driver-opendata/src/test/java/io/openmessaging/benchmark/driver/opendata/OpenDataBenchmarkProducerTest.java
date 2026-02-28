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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.opendata.AppendResult;
import dev.opendata.RecordBatch;
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

class OpenDataBenchmarkProducerTest {

    private LogAppender mockAppender;
    private OpenDataBenchmarkProducer producer;

    @BeforeEach
    void setUp() {
        mockAppender = mock(LogAppender.class);
        when(mockAppender.tryAppend(any(RecordBatch.class)))
                .thenReturn(new AppendResult(0, System.currentTimeMillis()));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (producer != null) {
            producer.close();
        }
    }

    @Test
    void sendAsyncShouldCompleteSuccessfully() throws Exception {
        producer = new OpenDataBenchmarkProducer(mockAppender, "test-topic", 1);

        CompletableFuture<Void> future = producer.sendAsync(Optional.empty(), "test-payload".getBytes());

        // Should complete within reasonable time
        future.get(5, TimeUnit.SECONDS);

        // Verify tryAppend was called
        verify(mockAppender, timeout(1000).atLeastOnce()).tryAppend(any(RecordBatch.class));
    }

    @Test
    void sendAsyncShouldBatchMultipleWrites() throws Exception {
        // Use a latch to control when tryAppend completes, allowing writes to queue up
        CountDownLatch appendLatch = new CountDownLatch(1);
        AtomicInteger appendCount = new AtomicInteger(0);

        when(mockAppender.tryAppend(any(RecordBatch.class))).thenAnswer(invocation -> {
            appendCount.incrementAndGet();
            // First call waits, subsequent calls proceed immediately
            if (appendCount.get() == 1) {
                appendLatch.await(5, TimeUnit.SECONDS);
            }
            return new AppendResult(0, System.currentTimeMillis());
        });

        producer = new OpenDataBenchmarkProducer(mockAppender, "test-topic", 1);

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

        // Should have batched - fewer tryAppend calls than messages
        assertThat(appendCount.get()).isLessThan(100);
    }

    @Test
    void sendAsyncShouldPropagateErrors() throws Exception {
        RuntimeException testException = new RuntimeException("Test error");
        when(mockAppender.tryAppend(any(RecordBatch.class))).thenThrow(testException);

        producer = new OpenDataBenchmarkProducer(mockAppender, "test-topic", 1);

        CompletableFuture<Void> future = producer.sendAsync(Optional.empty(), "test-payload".getBytes());

        assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Test error");
    }

    @Test
    void sendAsyncAfterCloseShouldFail() throws Exception {
        producer = new OpenDataBenchmarkProducer(mockAppender, "test-topic", 1);
        producer.close();

        CompletableFuture<Void> future = producer.sendAsync(Optional.empty(), "test".getBytes());

        assertThatThrownBy(() -> future.get(1, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void sendAsyncShouldCaptureTimestampAtSubmission() throws Exception {
        // Track the RecordBatch passed to tryAppend so we can inspect its first timestamp
        List<Long> capturedTimestamps = new ArrayList<>();

        when(mockAppender.tryAppend(any(RecordBatch.class))).thenAnswer(invocation -> {
            RecordBatch batch = invocation.getArgument(0);
            capturedTimestamps.add(batch.firstTimestampMs());
            return new AppendResult(0, System.currentTimeMillis());
        });

        producer = new OpenDataBenchmarkProducer(mockAppender, "test-topic", 1);

        long beforeSend = System.currentTimeMillis();
        CompletableFuture<Void> future = producer.sendAsync(Optional.empty(), "test".getBytes());
        long afterSend = System.currentTimeMillis();

        future.get(5, TimeUnit.SECONDS);

        // Timestamp should be captured at send time, not append time
        assertThat(capturedTimestamps).isNotEmpty();
        assertThat(capturedTimestamps.get(0)).isBetween(beforeSend, afterSend);
    }

    @Test
    void flushShouldDrainQueueAndCallAppenderFlush() throws Exception {
        producer = new OpenDataBenchmarkProducer(mockAppender, "test-topic", 1);

        // Send some messages
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures.add(producer.sendAsync(Optional.empty(), ("payload-" + i).getBytes()));
        }

        // Flush should wait for all writes and call appender.flush()
        producer.flush().get(5, TimeUnit.SECONDS);

        // All futures should be complete
        for (CompletableFuture<Void> future : futures) {
            assertThat(future.isDone()).isTrue();
        }

        // Appender.flush() should have been called
        verify(mockAppender, timeout(1000)).flush();
    }

    @Test
    void flushAfterCloseShouldNotThrow() throws Exception {
        producer = new OpenDataBenchmarkProducer(mockAppender, "test-topic", 1);
        producer.close();

        // Flush after close should complete immediately
        CompletableFuture<Void> flushFuture = producer.flush();
        assertThat(flushFuture.isDone()).isTrue();
        assertThatCode(() -> flushFuture.get()).doesNotThrowAnyException();

        // Appender.flush() should not have been called
        verify(mockAppender, never()).flush();
    }

    @Test
    void flushShouldPropagateAppenderFlushErrors() throws Exception {
        RuntimeException testException = new RuntimeException("Flush failed");
        doThrow(testException).when(mockAppender).flush();

        producer = new OpenDataBenchmarkProducer(mockAppender, "test-topic", 1);

        // Send a message first
        producer.sendAsync(Optional.empty(), "test".getBytes()).get(5, TimeUnit.SECONDS);

        // Flush should propagate the exception via the future
        CompletableFuture<Void> flushFuture = producer.flush();
        assertThatThrownBy(() -> flushFuture.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseMessage("Flush failed");
    }
}
