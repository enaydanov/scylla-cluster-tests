# Plan for pystress.py Implementation

## Overview
`pystress.py` is a Python-based benchmarking tool designed to mimic the `cassandra-stress` CLI interface while leveraging the ScyllaDB Python driver. The tool aims to provide a lightweight and flexible alternative for stress testing ScyllaDB clusters, with features such as customizable workloads, traffic control, schema flexibility, and metrics generation.

---

## Implementation Plan

### 1. Initial Tool Implementation
- **Objective**: Create a Python script (`pystress.py`) that implements the basic `cassandra-stress` CLI interface.
- **Features**:
  - Support for `write`, `read`, and `mixed` workloads.
  - Generate HDR Histogram files for latency metrics.
  - Use the ScyllaDB Python driver for database interactions.

### 2. Add Support for `-mode` CLI Option
- **Objective**: Implement the `-mode` option to configure connection settings.
- **Details**:
  - Parse `user` and `password` for authentication.
  - Use `PlainTextAuthProvider` when credentials are provided.
  - Parse `requestTimeout=<ms>` for request timeout configuration.
    - Value specified in milliseconds (converted to seconds internally for Python driver).
    - Default: 12000ms (12 seconds) - matches cassandra-stress default.
    - Case-insensitive parsing (e.g., `requestTimeout`, `REQUESTTIMEOUT`, `RequestTimeout`).
  - Parse `connectTimeout=<ms>` for initial cluster connection timeout.
    - Value specified in milliseconds (converted to seconds internally for Python driver).
    - Only passed to driver if explicitly specified (uses driver default otherwise).
    - Case-insensitive parsing.
  - Parse `controlConnectionTimeout=<ms>` for control connection timeout.
    - Value specified in milliseconds (converted to seconds internally for Python driver).
    - Only passed to driver if explicitly specified (uses driver default otherwise).
    - Case-insensitive parsing.
  - Parse `protocolVersion=<version>` for CQL protocol version.
    - Integer value (e.g., 3, 4, 5).
    - Only passed to driver if explicitly specified (uses driver default otherwise).
    - Case-insensitive parsing.
  - Examples:
    - `-mode user=cassandra password=secret`
    - `-mode requestTimeout=30000`
    - `-mode user=cassandra password=secret requestTimeout=60000`
    - `-mode connectTimeout=15000 controlConnectionTimeout=10000`
    - `-mode protocolVersion=4`

### 3. Add Support for `-port` CLI Option
- **Objective**: Implement the `-port` option to configure the native CQL port.
- **Details**:
  - Parse `native=<port>` for native CQL port configuration.
    - Integer value (e.g., 9042, 9043).
    - Only passed to driver if explicitly specified (uses driver default 9042 otherwise).
    - Case-insensitive parsing.
  - Examples:
    - `-port native=9043`
    - `-port native=19042`

### 4. Add Support for `-col` CLI Option
- **Objective**: Implement column configuration options such as `size=FIXED(1024)` and `n=FIXED(1)`.
- **Details**:
  - Parse and apply column size and count settings.
  - Support both fixed and uniform distributions.

### 5. Add Support for `duration=` CLI Option
- **Objective**: Allow users to specify test duration.
- **Details**:
  - Use `time.perf_counter()` for accurate time measurement.
  - Ensure compatibility with other workload parameters.
  - **Duration vs n=X behavior**:
    - `duration=X`: Test stops immediately when time expires. Pending operations are cancelled.
    - `n=X`: Test waits for all N operations to complete before stopping.
  - **Implementation**:
    - `stop_event` (multiprocessing.Event) signals immediate stop to workers
    - Workers check `stop_event` in their loop and exit within ~100ms
    - `executor.shutdown(wait=False, cancel_futures=True)` cancels pending tasks
  - **Timing accuracy**: Total operation time should be within ~1s of requested duration.

### 6. Add Support for `-rate` CLI Option
- **Objective**: Implement rate limiting and concurrency control.
- **Details**:
  - `threads=N`: Number of concurrent threads per worker process.
  - `processes=N`: Number of worker processes (replaces old `connectionsPerHost`).
  - `throttle=N/s`: Support `N/s` syntax for throttling (e.g., `throttle=1000/s`).

### 7. Add Support for `-log` CLI Option
- **Objective**: Enable users to specify HDR Histogram output files and interval statistics.
- **Details**:
  - Parse `hdrfile=<path>` to specify HDR histogram output file.
  - Parse `interval=<time>` to configure periodic statistics output interval (default: 10 seconds).
    - Supports suffixes: `s` (seconds), `ms` (milliseconds), or no suffix (assumes seconds).
    - Examples: `interval=10`, `interval=10s`, `interval=500ms`.
  - Ensure proper file handling and error reporting.

### 8. Add Help for CLI Options
- **Objective**: Provide detailed usage instructions.
- **Details**:
  - Implement a `--help` flag.
  - Document all supported options and their usage.

### 9. Cassandra-Stress Compatible Output Format
- **Objective**: Match cassandra-stress output format for easy integration with existing tooling.
- **Details**:
  - **Summary output for single operation type** (write or read):
    ```
    Results:
    Op rate                   : 125,468 op/s  [WRITE: 125,468 op/s]
    Latency mean              :  1.6 ms [WRITE: 1.6 ms]
    Latency median            :  1.5 ms [WRITE: 1.5 ms]
    Latency 95th percentile   :  2.7 ms [WRITE: 2.7 ms]
    Latency 99th percentile   :  3.7 ms [WRITE: 3.7 ms]
    Latency 99.9th percentile :  6.7 ms [WRITE: 6.7 ms]
    Latency max               :  116.9 ms [WRITE: 116.9 ms]
    Total operation time      : 00:16:36
    ```
  - **Summary output for mixed workload** (combined stats with READ/WRITE breakdown):
    ```
    Results:
    Op rate                   :   29,187 op/s  [READ: 14,594 op/s, WRITE: 14,594 op/s]
    Latency mean              :    3.4 ms [READ: 4.8 ms, WRITE: 2.0 ms]
    Latency median            :    2.1 ms [READ: 2.9 ms, WRITE: 1.5 ms]
    Latency 95th percentile   :    9.1 ms [READ: 12.2 ms, WRITE: 3.9 ms]
    Latency 99th percentile   :   20.9 ms [READ: 25.9 ms, WRITE: 6.8 ms]
    Latency 99.9th percentile :   93.5 ms [READ: 182.8 ms, WRITE: 44.3 ms]
    Latency max               : 5,041.6 ms [READ: 5,041.6 ms, WRITE: 2,043.7 ms]
    Total operation time      : 12:00:00
    ```
  - **Periodic interval statistics** printed every N seconds (configurable via `-log interval=N`):
    ```
     total ops,    op/s,  mean,     med,     .95,     .99,    .999,     max,   time,  errors
       1443348,  144335,   1.3,     0.9,     3.0,     6.8,    23.8,   116.9,   10.0        0
       3292404,  184906,   1.1,     0.9,     2.3,     3.0,     7.8,    15.5,   20.0        0
       4924818,  163241,   1.2,     1.0,     2.4,     3.1,     8.8,    99.8,   30.0        0
    ```
  - **Strict interval timing**: The `time` column uses target interval boundaries (10.0, 20.0, 30.0)
    rather than actual print time, preventing drift from processing overhead.
  - Latencies displayed in milliseconds for consistency with cassandra-stress.
  - Duration formatted as `HH:MM:SS`.

### 10. Add Support for `-schema` Option (cassandra-stress format)
- **Objective**: Support the full cassandra-stress `-schema` option format.
- **Details**:
  - Parse nested function-style options matching cassandra-stress format:
    - `replication(strategy=? replication_factor=?)` - Replication settings
      - `strategy=?` (default: SimpleStrategy)
      - `replication_factor=?` (default: 1)
    - `keyspace=?` (default: keyspace1)
    - `compaction(strategy=?)` - Compaction strategy (e.g., LeveledCompactionStrategy)
    - `compression=?` - Compression setting (e.g., LZ4Compressor)
  - Examples:
    - `-schema 'replication(replication_factor=3)'`
    - `-schema 'replication(strategy=NetworkTopologyStrategy replication_factor=3) keyspace=test'`
    - `-schema 'replication(replication_factor=3) compaction(strategy=SizeTieredCompactionStrategy)'`
    - `-schema 'keyspace=mystress compaction(strategy=LeveledCompactionStrategy) compression=LZ4Compressor'`

### 11. Add Support for `-pop dist=` Option (Population Distributions)
- **Objective**: Support cassandra-stress style population distribution options.
- **Details**:
  - Supported distribution formats:
    - `seq=min..max` - Sequential range (default)
    - `dist=UNIFORM(min..max)` - Uniform random distribution
    - `dist=GAUSSIAN(min..max,stdvrng)` - Gaussian distribution
      - `mean = (min + max) / 2`
      - `stdev = (mean - min) / stdvrng`
    - `dist=GAUSSIAN(min..max,mean,stdev)` - Gaussian with explicit parameters
  - Aliases: `gauss`, `normal`, `norm` (all map to GAUSSIAN)
  - Distribution names are case-insensitive
  - Gaussian values are clamped to [min, max] range
  - Examples:
    - `-pop 'seq=1..1000000'`
    - `-pop 'dist=UNIFORM(1..1000000)'`
    - `-pop 'dist=GAUSSIAN(1..1000000,5)'` (stdvrng=5)
    - `-pop 'dist=gauss(1..1000000,500000,100000)'` (explicit mean=500000, stdev=100000)

### 12. Cassandra-Stress Compatible HDR File Format
- **Objective**: Generate HDR histogram files matching cassandra-stress format.
- **Details**:
  - Custom `TaggedHistogramLogWriter` class extends `HistogramLogWriter` to support tag output
    - The standard library's `output_interval_histogram()` does not output tags
    - Our subclass overrides this method to include `Tag=<tag>,` prefix when histogram has a tag
  - Uses `HistogramLogWriter` methods for proper formatting:
    - `output_comment()` for first comment line
    - `output_log_format_version()` for version header (1.2 - Python library limitation)
    - `output_base_time()` for BaseTime header
    - `output_start_time()` for StartTime header
    - `output_legend()` for CSV column header
    - `output_interval_histogram()` (overridden) for histogram data with tag
  - Header format:
    ```
    #Logging op latencies for Cassandra Stress
    #[Histogram log format version 1.2]
    #[BaseTime: 1768994585.000 (seconds since epoch)]
    #[StartTime: 1768994585.000 (seconds since epoch), 2026-02-13 12:00:00]
    "StartTimestamp","Interval_Length","Interval_Max","Interval_Compressed_Histogram"
    Tag=WRITE-st,0.000000,60.000000,999,<base64_histogram>
    ```
  - Tags for histogram records:
    - `WRITE-st` for write operations
    - `READ-st` for read operations
    - The `-st` suffix means "service time" (request to response time)
  - Note: `mixed` command outputs both `WRITE-st` and `READ-st` histograms
  - Note: Format version 1.2 is a Python hdrhistogram library limitation (cassandra-stress uses 1.3)

### 13. Separate Read/Write Histograms with Interval-Based HDR Output
- **Objective**: Maintain separate histograms for READ and WRITE operations, output to HDR file periodically.
- **Details**:
  - Separate histogram tracking:
    - `write_summary_histogram` / `read_summary_histogram` - Accumulated across all intervals for final summary
    - `write_interval_histogram` / `read_interval_histogram` - Reset after each interval output
  - HDR file initialization:
    - File opened and headers written at start of run via `_init_hdr_writer()`
    - Uses `TaggedHistogramLogWriter` for tag support
  - Interval-based output:
    - `_output_interval_histograms()` called every `log_interval` seconds
    - Outputs `WRITE-st` histogram if write operations occurred in interval
    - Outputs `READ-st` histogram if read operations occurred in interval
    - Histograms reset after each interval output
  - Mixed workload support:
    - Selects read or write based on configured ratio (default: 50/50)
    - Both `WRITE-st` and `READ-st` histograms output in same interval
  - Final summary:
    - Prints separate statistics for READ and WRITE operations
    - Uses accumulated summary histograms

### 14. Add Support for `ratio(write=N,read=M)` Option
- **Objective**: Allow configuring the read/write ratio for mixed workloads.
- **Details**:
  - Parse `ratio(write=N,read=M)` syntax after `mixed` command
  - Default ratio is 1:1 (50% writes, 50% reads)
  - Ratio determines probability of selecting write vs read operation
  - Order of write/read in ratio string doesn't matter
  - Case-insensitive parsing
  - Examples:
    - `mixed 'ratio(write=1,read=1)'` - 50% writes, 50% reads (default)
    - `mixed 'ratio(write=1,read=2)'` - 33% writes, 67% reads
    - `mixed 'ratio(read=3,write=1)'` - 25% writes, 75% reads

### 15. Nanosecond Latency Storage (cassandra-stress compatibility)
- **Objective**: Store latency values in nanoseconds to match cassandra-stress implementation.
- **Details**:
  - Latency values captured using `time.perf_counter()` and converted to nanoseconds
  - Histogram configuration: `HdrHistogram(1, 60 * 60 * 1_000_000_000, 3)` (1ns to 1 hour, 3 sig figs)
  - Recording: `int((time.perf_counter() - t0) * 1_000_000_000)` nanoseconds
  - Display conversion: `value / 1_000_000.0` (ns → ms) for console output
  - HDR output: `max_value_unit_ratio=1000000000.0` (ns → seconds) for file output
  - Benefits:
    - Exact compatibility with cassandra-stress histogram semantics
    - Higher precision for sub-microsecond latencies
    - Easier comparison when analyzing HDR files from both tools

### 16. Code Architecture and Maintainability
- **Objective**: Keep code maintainable and compliant with linting rules.
- **Details**:
  - **CLI Parsing** - Modular helper functions for each flag type:
    - `_parse_key_value_arg()` - handles `n=`, `duration=`, `cl=` args
    - `_parse_rate_args()` - handles `-rate` flag
    - `_parse_col_args()` - handles `-col` flag
    - `_parse_schema_args()` - handles `-schema` flag
    - `_parse_pop_args()` - handles `-pop` flag
    - `_parse_log_args()` - handles `-log` flag
    - `_parse_mode_arg()` - handles `-mode` flag options
  - **Multiprocess Execution** (always used):
    - `create_cluster_connection()` - helper function to create Cluster with common config (auth, timeout, policies)
    - `ThreadConnection` - per-thread connection state (Cluster, Session, prepared statements)
    - `WorkerContext` - class holding worker state with thread-local connections
    - `worker_process()` - standalone function spawned as subprocess
    - `StatsCollectorContext` - class holding stats aggregation state
    - `stats_collector_process()` - standalone function for stats collection
    - `ProcessPoolManager` - manages worker and stats collector lifecycle and IPC
    - `run()` - main entry point, contains workload execution logic (inlined)
    - `_create_key_generator()` - creates key generator based on distribution config
  - **Statistics and Output**:
    - `_get_histogram_stats()` - extracts stats from histogram as dict
    - `_print_op_summary()` - prints single operation type summary
    - `_print_mixed_summary()` - prints mixed workload summary
    - `print_interval_stats()` - prints periodic interval statistics (in StatsCollectorContext)
  - **Error Handling** - Specific exception types instead of blind catches:
    - `OSError` for file/network operations
    - `RuntimeError` for execution failures
    - `ValueError` for data conversion issues
    - `ConnectionError` for connection failures (in workers)

### 17. Multiprocess Mode
- **Objective**: Use separate processes for true parallelism and decouple stats processing.
- **Details**:
  - **Always enabled** - single execution path for simplicity
  - **Per-Thread Connections**:
    - Each thread in a worker process has its own ScyllaDB Cluster/Session
    - Connections are created upfront via `connect_all()` before workload starts
    - Uses thread index assignment for O(1) connection lookup
    - Total connections = `processes * threads`
    - No latency impact from connection initialization during workload
  - **Worker Processes**:
    - Coordinate per-thread connections via `WorkerContext` and `ThreadConnection`
    - Report histograms to stats collector via IPC queue
    - Run ThreadPoolExecutor for concurrent operations
    - Check `stop_event` for immediate termination (duration mode)
  - **Stats Collector Process**:
    - Receives histograms from all workers
    - Aggregates results into summary and interval histograms
    - Prints periodic interval statistics with strict timing
    - Writes HDR histogram file
    - Ensures main loop is not blocked by heavy stats processing
  - **Main Process**:
    - Coordinates task distribution via `ProcessPoolManager`
    - Submits operations to workers via bounded queues
    - Handles backpressure and shutdown sequences
    - Sets `stop_event` for immediate stop (duration mode)
  - **Shutdown behavior**:
    - Duration mode (`duration=X`): Sets `stop_event`, workers exit immediately, cancel pending tasks
    - Fixed ops mode (`n=X`):
      1. Main process waits for ops_queue to drain (all operations picked up by workers)
      2. Sends "graceful" signal, workers wait for pending ops to complete
    - Workers send "done" signal BEFORE `cluster.shutdown()` to avoid blocking stats collector
    - After joining all workers, main process sends "all_joined" signal to stats collector
    - Stats collector exits when all "done" messages received OR "all_joined" signal received
    - Stats collector is joined in `get_summary()`, not in `stop_workers()` (avoids premature termination)
  - Implementation classes/functions:
    - `WorkerContext` - holds worker state (connection, histograms, counters)
    - `worker_process()` - standalone function spawned as subprocess, accepts `stop_event`
    - `StatsCollectorContext` - holds aggregation and printing state
    - `stats_collector_process()` - standalone function for stats collection
    - `ProcessPoolManager` - manages worker and stats collector lifecycle, holds `stop_event`
    - `run()` - main entry point, contains multiprocess workload execution logic (inlined)
  - Message protocol:
    - **Results Queue** (Workers → Stats Collector): `("histogram", ...)`, `("done", ...)`, `("error", ...)`
    - **Results Queue** (Main → Stats Collector): `("all_joined", ...)` - signals all workers have been joined
    - **Summary Queue** (Stats Collector → Main): `{"total_ops": ..., "latency_hist": ...}`
  - Example:
    ```bash
    # Uses 4 worker processes + 1 stats collector process
    pystress.py write n=100000 -rate threads=40 processes=4

    # Duration mode - stops immediately at 15s
    pystress.py write duration=15s -rate threads=40 processes=4
    ```

---

## CLI Options Reference

| Option | Description | Example |
|--------|-------------|---------|
| `write` | Write workload | `pystress.py write n=10000` |
| `read` | Read workload | `pystress.py read n=10000` |
| `mixed` | Mixed read/write workload with optional ratio | `pystress.py mixed 'ratio(write=1,read=2)' n=10000` |
| `n=<num>` | Number of operations | `n=1000000` |
| `duration=<time>` | Test duration (e.g., `30s`, `5m`, `1h`) | `duration=5m` |
| `cl=<level>` | Consistency level | `cl=QUORUM` |
| `-node <hosts>` | Comma-separated list of nodes | `-node 10.0.0.1,10.0.0.2` |
| `-port` | Port configuration (native=port) | `-port native=9043` |
| `-mode` | Connection mode options (user, password, requestTimeout, connectTimeout, controlConnectionTimeout, protocolVersion) | `-mode user=cassandra password=secret requestTimeout=30000` |
| `-rate` | Rate limiting options | `-rate threads=10 processes=4 throttle=1000/s` |
| `-col` | Column configuration | `-col 'size=FIXED(1024) n=FIXED(5)'` |
| `-log` | Logging options | `-log hdrfile=output.hdr interval=30s` |
| `-schema` | Schema options (keyspace, replication, compaction) | `-schema 'replication(replication_factor=3) keyspace=test'` |
| `-pop` | Population distribution | `-pop 'dist=GAUSSIAN(1..1000000,5)'` |
| `--help` | Show help message | `pystress.py --help` |

---

## Summary
The `pystress.py` tool is a versatile and lightweight benchmarking solution for ScyllaDB. By mimicking the `cassandra-stress` CLI interface and output format, it ensures ease of use and compatibility with existing tooling while providing advanced features such as HDR Histogram generation, flexible schema configurations, periodic interval statistics with strict timing, separate read/write histogram tracking, configurable read/write ratios, multiprocess execution for true parallelism, accurate duration enforcement via stop_event signaling, and precise traffic control. This plan outlines the key steps and features implemented to achieve a robust and user-friendly tool.
