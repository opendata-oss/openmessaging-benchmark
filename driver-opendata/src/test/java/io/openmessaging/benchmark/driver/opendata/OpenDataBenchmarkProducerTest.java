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

import dev.opendata.LogDb;
import dev.opendata.LogEntry;
import dev.opendata.LogScanIterator;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests against an in-memory {@link LogDb}.
 *
 * <p>Requires the native log-c library on the load path. The opendata-java
 * Gradle build publishes a SNAPSHOT artifact whose tests document the same
 * requirement (see {@code LogDbIntegrationTest}).
 */
class OpenDataBenchmarkProducerTest {

    private static final long AWAIT_MS = 5_000;
    private static final String TOPIC = "topic";

    private LogDb log;
    private OpenDataBenchmarkProducer producer;

    @BeforeEach
    void setUp() {
        log = LogDb.openInMemory();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (producer != null) {
            producer.close();
        }
        if (log != null) {
            log.close();
        }
    }

    @Test
    void sendAsyncCompletesWhenDurable() throws Exception {
        producer = new OpenDataBenchmarkProducer(log, TOPIC, 1);

        CompletableFuture<Void> future =
                producer.sendAsync(Optional.empty(), "payload".getBytes(StandardCharsets.UTF_8));

        future.get(AWAIT_MS, TimeUnit.MILLISECONDS);
        assertThat(future.isCompletedExceptionally()).isFalse();
    }

    @Test
    void sendAsyncCapturesTimestampAtSubmission() throws Exception {
        producer = new OpenDataBenchmarkProducer(log, TOPIC, 1);

        long beforeSend = System.currentTimeMillis();
        producer.sendAsync(Optional.empty(), "payload".getBytes(StandardCharsets.UTF_8))
                .get(AWAIT_MS, TimeUnit.MILLISECONDS);
        long afterSend = System.currentTimeMillis();

        LogScanIterator iter = log.scan(TOPIC.getBytes(StandardCharsets.UTF_8), 0);
        assertThat(iter.hasNext()).isTrue();
        LogEntry entry = iter.next();
        assertThat(entry.timestamp()).isBetween(beforeSend, afterSend);
    }

    @Test
    void sendAsyncAfterCloseFails() throws Exception {
        producer = new OpenDataBenchmarkProducer(log, TOPIC, 1);
        producer.close();

        CompletableFuture<Void> future =
                producer.sendAsync(Optional.empty(), "payload".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> future.get(AWAIT_MS, TimeUnit.MILLISECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void keyedMessagesRouteToSamePartition() throws Exception {
        int numPartitions = 4;
        producer = new OpenDataBenchmarkProducer(log, TOPIC, numPartitions);

        for (int i = 0; i < 8; i++) {
            producer.sendAsync(Optional.of("stable-key"), ("v" + i).getBytes(StandardCharsets.UTF_8))
                    .get(AWAIT_MS, TimeUnit.MILLISECONDS);
        }

        Set<Integer> partitionsHit = new HashSet<>();
        for (int p = 0; p < numPartitions; p++) {
            byte[] partitionKey = (TOPIC + "/" + p).getBytes(StandardCharsets.UTF_8);
            LogScanIterator iter = log.scan(partitionKey, 0);
            if (iter.hasNext()) {
                partitionsHit.add(p);
            }
        }
        assertThat(partitionsHit).hasSize(1);
    }

    @Test
    void keylessMessagesRoundRobinAcrossPartitions() throws Exception {
        int numPartitions = 4;
        producer = new OpenDataBenchmarkProducer(log, TOPIC, numPartitions);

        for (int i = 0; i < numPartitions * 4; i++) {
            producer.sendAsync(Optional.empty(), ("v" + i).getBytes(StandardCharsets.UTF_8))
                    .get(AWAIT_MS, TimeUnit.MILLISECONDS);
        }

        for (int p = 0; p < numPartitions; p++) {
            byte[] partitionKey = (TOPIC + "/" + p).getBytes(StandardCharsets.UTF_8);
            LogScanIterator iter = log.scan(partitionKey, 0);
            int count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
            assertThat(count).isEqualTo(4);
        }
    }

    @Test
    void flushSucceedsAfterAppends() throws Exception {
        producer = new OpenDataBenchmarkProducer(log, TOPIC, 1);
        producer.sendAsync(Optional.empty(), "p".getBytes(StandardCharsets.UTF_8))
                .get(AWAIT_MS, TimeUnit.MILLISECONDS);

        producer.flush().get(AWAIT_MS, TimeUnit.MILLISECONDS);
    }

    @Test
    void flushAfterCloseCompletesImmediately() {
        producer = new OpenDataBenchmarkProducer(log, TOPIC, 1);
        producer.close();

        CompletableFuture<Void> flushFuture = producer.flush();
        assertThat(flushFuture.isDone()).isTrue();
        assertThatCode(() -> flushFuture.get(AWAIT_MS, TimeUnit.MILLISECONDS))
                .doesNotThrowAnyException();
    }
}
