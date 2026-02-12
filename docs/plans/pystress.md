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
  - Parse `connectionsPerHost` and other parameters.
  - Use multiple `Cluster`/`Session` instances to simulate multiple connections per host.
  - Document how `connectionsPerHost` maps to multiple clusters/sessions.

### 3. Add Support for `-col` CLI Option
- **Objective**: Implement column configuration options such as `size=FIXED(1024)` and `n=FIXED(1)`.
- **Details**:
  - Parse and apply column size and count settings.
  - Support both fixed and uniform distributions.

### 4. Add Support for `duration=` CLI Option
- **Objective**: Allow users to specify test duration.
- **Details**:
  - Use `time.perf_counter()` for accurate time measurement.
  - Ensure compatibility with other workload parameters.

### 5. Add Support for `-rate` CLI Option
- **Objective**: Implement rate limiting for operations.
- **Details**:
  - Support `N/s` syntax for throttling (e.g., `throttle=1000/s`).

### 6. Add Support for `-log` CLI Option
- **Objective**: Enable users to specify HDR Histogram output files and interval statistics.
- **Details**:
  - Parse `hdrfile=<path>` to specify HDR histogram output file.
  - Parse `interval=<time>` to configure periodic statistics output interval (default: 10 seconds).
    - Supports suffixes: `s` (seconds), `ms` (milliseconds), or no suffix (assumes seconds).
    - Examples: `interval=10`, `interval=10s`, `interval=500ms`.
  - Ensure proper file handling and error reporting.

### 7. Add Help for CLI Options
- **Objective**: Provide detailed usage instructions.
- **Details**:
  - Implement a `--help` flag.
  - Document all supported options and their usage.

### 8. Cassandra-Stress Compatible Output Format
- **Objective**: Match cassandra-stress output format for easy integration with existing tooling.
- **Details**:
  - **Summary output** matches cassandra-stress format:
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
  - **Periodic interval statistics** printed every N seconds (configurable via `-log interval=N`):
    ```
     total ops,    op/s,  mean,     med,     .95,     .99,    .999,     max,   time,  errors
       1443348,  144335,   1.3,     0.9,     3.0,     6.8,    23.8,   116.9,   10.0        0
       3292404,  184906,   1.1,     0.9,     2.3,     3.0,     7.8,    15.5,   20.0        0
       4924818,  163241,   1.2,     1.0,     2.4,     3.1,     8.8,    99.8,   30.0        0
    ```
  - Latencies displayed in milliseconds for consistency with cassandra-stress.
  - Duration formatted as `HH:MM:SS`.

### 9. Add Support for `-schema` Option (cassandra-stress format)
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

---

## CLI Options Reference

| Option | Description | Example |
|--------|-------------|---------|
| `write` | Write workload | `pystress.py write n=10000` |
| `read` | Read workload | `pystress.py read n=10000` |
| `mixed` | Mixed read/write workload | `pystress.py mixed n=10000` |
| `n=<num>` | Number of operations | `n=1000000` |
| `duration=<time>` | Test duration (e.g., `30s`, `5m`, `1h`) | `duration=5m` |
| `cl=<level>` | Consistency level | `cl=QUORUM` |
| `-node <hosts>` | Comma-separated list of nodes | `-node 10.0.0.1,10.0.0.2` |
| `-mode` | Connection mode options | `-mode connectionsPerHost=5` |
| `-rate` | Rate limiting options | `-rate threads=10 throttle=1000/s` |
| `-col` | Column configuration | `-col 'size=FIXED(1024) n=FIXED(5)'` |
| `-log` | Logging options | `-log hdrfile=output.hdr interval=30s` |
| `-schema` | Schema options (keyspace, replication) | `-schema 'keyspace=mystress replication_factor=3'` |
| `-pop` | Population distribution | `-pop 'seq=1..1000000'` |
| `--help` | Show help message | `pystress.py --help` |

---

## Summary
The `pystress.py` tool is a versatile and lightweight benchmarking solution for ScyllaDB. By mimicking the `cassandra-stress` CLI interface and output format, it ensures ease of use and compatibility with existing tooling while providing advanced features such as HDR Histogram generation, flexible schema configurations, periodic interval statistics, and precise traffic control. This plan outlines the key steps and features implemented to achieve a robust and user-friendly tool.
