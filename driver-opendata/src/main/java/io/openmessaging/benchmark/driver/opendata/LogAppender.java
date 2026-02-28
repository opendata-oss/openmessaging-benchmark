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
import dev.opendata.RecordBatch;

/**
 * Interface for appending records to a Log.
 *
 * <p>This abstraction allows the producer to be tested without loading the native library.
 */
public interface LogAppender {

    /**
     * Appends a batch of records to the log.
     *
     * <p>The batch is not closed by this method — the caller retains ownership.
     *
     * @param batch the record batch to append
     * @return the result of the append operation
     */
    AppendResult tryAppend(RecordBatch batch);

    /**
     * Flushes buffered data to durable storage.
     *
     * <p>Default implementation is a no-op for testing with mocks.
     */
    default void flush() {
        // Default no-op
    }

    /**
     * Creates a LogAppender that delegates to the given LogDb instance.
     *
     * @param log the LogDb instance
     * @return a LogAppender wrapping the LogDb
     */
    static LogAppender wrap(LogDb log) {
        return new LogAppender() {
            @Override
            public AppendResult tryAppend(RecordBatch batch) {
                return log.tryAppend(batch);
            }

            @Override
            public void flush() {
                log.flush();
            }
        };
    }
}
