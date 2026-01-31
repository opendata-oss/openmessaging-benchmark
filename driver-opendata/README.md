# OpenData Driver for OpenMessaging Benchmark

This module provides an OpenMessaging Benchmark driver for [OpenData Log](https://github.com/opendata-oss/opendata).

## Overview

The driver maps OMB concepts to OpenData Log operations:

| OMB Concept |              OpenData Mapping              |
|-------------|--------------------------------------------|
| Topic       | Log key prefix                             |
| Partition   | Key suffix (`{topic}/0`, `{topic}/1`, ...) |
| Producer    | `Log.append()` with partition routing      |
| Consumer    | `LogReader` with polling                   |

## Prerequisites

1. Build and install the [opendata-java](https://github.com/opendata-oss/opendata-java) library:

```bash
cd opendata-java
cd log/native && cargo build --release && cd ../..
mvn clean install
```

2. Ensure the native library is in your library path:

```bash
export LD_LIBRARY_PATH=/path/to/opendata-java/log/native/target/release:$LD_LIBRARY_PATH
# or on macOS:
export DYLD_LIBRARY_PATH=/path/to/opendata-java/log/native/target/release:$DYLD_LIBRARY_PATH
```

## Configuration

Create a driver configuration YAML file:

```yaml
name: OpenData
driverClass: io.openmessaging.benchmark.driver.opendata.OpendataBenchmarkDriver

storage:
  type: slatedb           # or "in-memory"
  path: /tmp/opendata-benchmark
  objectStore: local      # "local", "in-memory", or "s3"
  # For S3:
  # objectStore: s3
  # s3Bucket: my-bucket
  # s3Region: us-east-1
  # settingsPath: /path/to/slatedb-settings.toml  # optional

consumer:
  pollIntervalMs: 10      # Polling interval when no data
  pollBatchSize: 1000     # Max entries per poll
  queueCapacity: 10000    # Internal queue size
```

### Storage Options

|    Type     |            Description             |
|-------------|------------------------------------|
| `in-memory` | Fast, non-persistent (for testing) |
| `slatedb`   | Persistent storage via SlateDB     |

### Object Store Options (for SlateDB)

|    Type     |      Description       |
|-------------|------------------------|
| `in-memory` | In-memory object store |
| `local`     | Local filesystem       |
| `s3`        | Amazon S3              |

## Running a Benchmark

```bash
cd openmessaging-benchmark

# Build the project
mvn clean install -DskipTests

# Run a benchmark
bin/benchmark \
  --drivers driver-opendata/opendata.yaml \
  --workers 1 \
  workloads/1-topic-1-partition-1kb.yaml
```

## Architecture

### Producer

- Routes messages to partition keys based on OMB message key
- Keys are hashed for deterministic routing
- No-key messages use round-robin distribution

### Consumer

- Uses polling-based consumption (push API pending upstream)
- One poller thread per partition
- Single dispatcher thread for callback invocation
- Configurable backpressure via queue capacity

## Limitations

- **Polling-based consumption**: True push-based consumption awaits upstream API
- **JNI overhead**: See opendata-java documentation for performance characteristics
- **Single Log instance**: All topics share one Log (keys provide isolation)

## Troubleshooting

### `UnsatisfiedLinkError: no opendata_log_jni in java.library.path`

The native library is not in your library path. Set `LD_LIBRARY_PATH` (Linux) or `DYLD_LIBRARY_PATH` (macOS) to include the directory containing `libopendata_log_jni.so` or `libopendata_log_jni.dylib`.

### Slow S3 writes

S3 latency from local development machines can be high. For realistic benchmarks, run from EC2 in the same region as your S3 bucket. Consider using SlateDB settings to tune flush behavior:

```toml
# slatedb-settings.toml
l0_sst_size_bytes = 4194304  # 4MB
```

## Related

- [opendata-java](https://github.com/opendata-oss/opendata-java) - Java bindings library
- [OpenData](https://github.com/opendata-oss/opendata) - Upstream Rust implementation

