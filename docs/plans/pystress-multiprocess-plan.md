# Plan: Multiprocess Execution with Per-Thread Connections
**Status: IMPLEMENTED**
## Objective
Use separate processes for workload execution, providing:
1. True parallelism by bypassing Python's Global Interpreter Lock (GIL)
2. Better simulation of real cassandra-stress behavior where each connection is independent
3. Improved performance on multi-core systems
4. Responsive statistics reporting decoupled from workload execution
## Architecture
Pystress uses a multiprocess architecture with dedicated workers and a stats collector:
```
┌─────────────────────────────────────────────────────────────────────┐
│                         Main Process                                 │
│  - Coordinates workers via ProcessPoolManager                       │
│  - Submits operations to workers via ops_queue (shared)             │
│  - Receives final summary from Stats Collector                      │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               │  ops_queue (shared)
                               ▼
           ┌─────────────────────────────────────────┐
           │                                         │
           ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  Worker Process 1│  │  Worker Process 2│  │  Worker Process N│
│  ┌─────────────┐ │  │  ┌─────────────┐ │  │  ┌─────────────┐ │
│  │WorkerContext│ │  │  │WorkerContext│ │  │  │WorkerContext│ │
│  │  - Session  │ │  │  │  - Session  │ │  │  │  - Session  │ │
│  │  - Histos   │ │  │  │  - Histos   │ │  │  │  - Histos   │ │
│  └─────────────┘ │  │  └─────────────┘ │  │  └─────────────┘ │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                    │
         │        results_queue (histograms)       │
         └────────────────────┼────────────────────┘
                              ▼
              ┌───────────────────────────────┐
              │     Stats Collector Process    │
              │  ┌─────────────────────────┐  │
              │  │ StatsCollectorContext   │  │
              │  │  - Aggregates results   │  │
              │  │  - Prints interval stats│  │
              │  │  - Writes HDR file      │  │
              │  └─────────────────────────┘  │
              └───────────────┬───────────────┘
                              │ summary_queue
                              ▼
                        (Final Summary)
```
## Implementation Details
### Stats Collector Process
A dedicated process responsible for:
1. Receiving histogram data from all workers via `results_queue`
2. Aggregates histograms into summary and interval containers
3. Printing periodic interval statistics to stdout
4. Writing interval histograms to the HDR log file
5. Sending the final summary back to the main process via `summary_queue`

This separation ensures that heavy histogram aggregation and I/O operations do not block the main process's operation submission loop or the workers' execution.

```python
def stats_collector_process(
    config_dict: dict,
    results_queue: multiprocessing.Queue,
    summary_queue: multiprocessing.Queue,
    start_barrier: multiprocessing.synchronize.Barrier,
):
    \"\"\"Stats collector process.\"\"\"
    ctx = StatsCollectorContext(config_dict)
    start_barrier.wait()  # Synchronize with workers and main process
    ctx.start_time = time.perf_counter()
    ctx.start_timestamp = time.time() * 1000
    ctx.init_hdr_writer()
    # Main loop: aggregate results, print stats at intervals, exit when all workers done
    ...
```

### WorkerContext Class
Holds all worker process state with per-thread connections. Connections are pre-created upfront for predictable latencies:
```python
class ThreadConnection:
    \"\"\"Per-thread connection state for ScyllaDB.\"\"\"
    def __init__(self, config_dict: dict, col_count: int, payload_cache: bytes):
        self.cluster = None
        self.session = None
        self.prepared_write = None
        self.prepared_read = None
    def connect(self): ...
    def shutdown(self): ...

class WorkerContext:
    \"\"\"Context holder for worker process state with per-thread connections.\"\"\"
    def __init__(self, worker_id: int, config_dict: dict):
        # Shared payload cache (read-only, generated once)
        self.payload_cache = b"x" * config_dict["col_size"]
        # Pre-allocated connections (one per thread)
        self._connections: list[ThreadConnection] = []
        # Thread-local storage for connection index assignment
        self._thread_local = threading.local()
        # Histograms and counters (protected by lock, shared across threads)
        self.write_histogram = HdrHistogram(1, 60 * 60 * 1_000_000_000, 3)
        self.read_histogram = HdrHistogram(1, 60 * 60 * 1_000_000_000, 3)
        self.histogram_interval = min(1.0, config_dict.get("log_interval", 10) / 5.0)
    def connect_all(self):
        \"\"\"Pre-create connections for all threads before workload starts.\"\"\"
        for i in range(self.threads_per_worker):
            conn = ThreadConnection(...)
            conn.connect()
            self._connections.append(conn)
    def _get_connection(self) -> ThreadConnection:
        \"\"\"Get pre-allocated connection for current thread (no connection overhead).\"\"\"
        ...
    def process_task(self, task):
        \"\"\"Process task using thread's pre-allocated connection.\"\"\"
        conn = self._get_connection()
        # Execute using conn.session
        ...
    def shutdown(self):
        \"\"\"Shutdown all thread connections.\"\"\"
        ...
```
### worker_process Function
Standalone function spawned as a separate process. Connections are created upfront before workload:
```python
def worker_process(
    worker_id: int,
    config_dict: dict,
    ops_queue: multiprocessing.Queue,
    results_queue: multiprocessing.Queue,
    start_barrier: multiprocessing.synchronize.Barrier,
    stop_event: multiprocessing.synchronize.Event,
):
    \"\"\"Worker process with per-thread connections.\"\"\"
    ctx = WorkerContext(worker_id, config_dict)
    threads_per_worker = config_dict["threads_per_worker"]

    # Create all connections upfront before workload starts
    ctx.connect_all()

    start_barrier.wait()  # All connections established

    executor = ThreadPoolExecutor(max_workers=threads_per_worker)
    try:
        while True:
            # Check for immediate stop signal (duration mode)
            if stop_event.is_set():
                executor.shutdown(wait=False, cancel_futures=True)
                break

            # Periodic histogram send
            if time.perf_counter() >= next_histogram_send:
                ctx.send_histogram_data(results_queue)
                next_histogram_send += ctx.histogram_interval

            task = ops_queue.get(timeout=0.1)
            if task == "graceful":  # n=X mode - wait for pending
                for f in pending_futures:
                    f.result(timeout=30)
                executor.shutdown(wait=True)
                break
            if task is not None:
                # Thread gets pre-allocated connection via _get_connection()
                executor.submit(ctx.process_task, task)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    # Send done signal BEFORE cluster shutdown (allows stats collector to proceed)
    ctx.send_histogram_data(results_queue)
    results_queue.put((\"done\", worker_id, None, None, 0, 0))
    ctx.shutdown()  # Shuts down all thread connections
```

### ProcessPoolManager Class
Manages worker processes and stats collector:
```python
class ProcessPoolManager:
    def __init__(self, config):
        self.config = config  # StressConfig object with num_workers and threads_per_worker properties
        self.workers = []
        self.ops_queue = None  # Single shared queue for all workers
        self.results_queue = None  # Shared with stats collector
        self.summary_queue = None # For receiving summary
        self.stats_process = None
        self.stop_event = multiprocessing.Event()  # For immediate stop signaling

    def start_workers(self):
        \"\"\"Spawn stats collector and worker processes.

        Uses a barrier to synchronize startup of all workers + stats collector + main process.
        Timing is handled internally - stats collector captures start_time after barrier.
        \"\"\"
        ...
    def submit_operation(self, key: int, op_type: str, timeout=None):
        \"\"\"Submit operation to workers (via shared queue).\"\"\"
        ...
    def stop_workers(self, graceful: bool = False):
        \"\"\"Signal workers to stop and wait for them to terminate.

        Args:
            graceful: If True (n=X mode), wait for queue to drain, then send 'graceful' signal.
                      If False (duration mode), set stop_event - workers exit immediately.
        \"\"\"
        ...
    def get_summary(self):
        \"\"\"Get summary data from stats collector and cleanup the process.\"\"\"
        ...
```
## Mode Selection
Multiprocess mode is **always used**. Configuration is handled via `StressConfig` properties:
```python
class StressConfig:
    # self.processes initialized to 1 by default

    @property
    def threads_per_worker(self):
        return max(1, self.threads // self.processes)

def run(self):
    # ...
    manager = ProcessPoolManager(self.config)
    manager.start_workers()
    # ...
```
## CLI Usage
```bash
# Single worker with 40 threads = 40 connections
pystress.py write n=100000 -rate threads=40

# 4 workers with 10 threads each = 40 connections total
pystress.py write n=100000 -rate threads=40 processes=4
# Creates 4 worker processes, each with 10 threads (40/4)
# Each thread has its own ScyllaDB connection
# Total connections = processes * threads = 4 * 10 = 40
```
## Execution Flow
The multiprocess workload executes in two phases:
### Phase 1: Submit Operations
```
Main Process                     Worker Processes
    │                                  │
    ├─► Submit op to shared queue ──►│ (Automatic Load Balancing)
    │   (Handle backpressure)          │
    │   ...                           │
    └─► All ops submitted             │
```
(Stats Collector runs concurrently, receiving results and printing stats)
### Phase 2: Drain and Shutdown
```
Main Process                     Worker Processes
    │                                  │
    ├─► Signal Stop (Robustly) ──────►│
    │                                  ├─► Finish pending ops
    │                                  │
    ├─► Join Workers                  │
    └─► Join Stats Collector          │
```
Key points:
- Interval stats are printed by **Stats Collector Process**, ensuring no drift or blockage from main loop logic.
- Main process focuses on submitting operations.
- Queues are bounded (`maxsize`) to provide backpressure and prevent OOM/buffering delays.
## Message Protocol
Workers communicate with stats collector via `results_queue`:
| Message Type | Format | Description |
|--------------|--------|-------------|
| histogram | `("histogram", worker_id, op_type, encoded_hist, ops_count, err_count)` | Periodic histogram data |
| done | `("done", worker_id, None, None, 0, 0)` | Worker completed |
| error | `("error", worker_id, error_msg, None, 0, 0)` | Worker error |
| all_joined | `("all_joined", None, None, None, 0, 0)` | Main process signals all workers have been joined |

Stats collector communicates with Main process via `summary_queue`:
| Message Type | Format | Description |
|--------------|--------|-------------|
| summary | Dict with total ops, errors, decoded histograms | Final summary data |
## Thread/Process Safety
### Worker-side (WorkerContext)
- `self.lock` protects access to histograms and counters
- `process_task()` acquires lock when updating histogram/counters (runs in thread pool)
- `send_histogram_data()` acquires lock when reading/resetting (runs in main worker thread)
### Stats Collector
- Single-threaded process (no locks needed for aggregation logic)
- Reads from thread-safe `multiprocessing.Queue`
### Main Process
- Uses bounded queues with timeout/nowait operations to avoid blocking/deadlocks

## Timing and Shutdown Optimizations
### Strict Interval Timing
Stats collector uses target interval time for reporting instead of actual time:
```python
# Uses next_interval_time - start_time for clean output (1.0, 2.0, 3.0...)
# Instead of now - start_time which causes drift (1.003, 2.007, 3.012...)
elapsed = next_interval_time - start_time
```
### Immediate Stop for Duration Mode
When `duration=X` is specified:
1. Main process sets `stop_event` when duration expires
2. Workers check `stop_event` in their loop and exit immediately
3. Workers call `executor.shutdown(wait=False, cancel_futures=True)` to abandon pending tasks
4. Workers send "done" signal BEFORE `cluster.shutdown()` to avoid blocking stats collector
5. Stats collector can finish immediately; worker cleanup happens in background
### Graceful Stop for n=X Mode
When `n=X` is specified (fixed operation count):
1. Main process submits all N operations to the queue
2. Main process waits for queue to drain (become empty)
3. Main process sends "graceful" signal via queue
4. Workers wait for all pending operations to complete
5. Workers then shutdown cleanly
### Short Timeouts
- `join_workers()`: 5s timeout per worker
- `get_summary()`: 30s timeout (allows stats collector to finish processing)
- Workers are killed if they exceed timeout (data already collected)
- Stats collector is NOT killed in `stop_workers()` - it finishes processing and is joined in `get_summary()`
- `all_joined` signal is sent to stats collector after all workers are joined, ensuring it can exit even if some workers were killed before sending "done"

## Benefits
1. **True Parallelism**: Bypasses GIL for CPU-bound histogram operations
2. **Isolation**: Each session runs in isolated memory space
3. **Responsive Stats**: Stats printing and HDR writing happens in a dedicated process, decoupled from operation submission
4. **Backpressure Handling**: Bounded queues prevent excessive buffering and ensure timely shutdown
5. **Accurate Duration**: stop_event ensures duration=X tests stop within ~1s of requested time
6. **Clean Interval Reporting**: Strict interval timing prevents drift in time column output
7. **Fast Shutdown**: Workers signal completion before slow cluster cleanup

## Files Changed
- `utils/pystress.py`:
  - Added `StatsCollectorContext` and `stats_collector_process`
  - Updated `ProcessPoolManager` to handle stats process and stop_event
  - Updated `WorkerContext` and `worker_process` with stop_event handling
  - Inlined workload execution into `run()` method
  - Optimized shutdown sequence to send "done" before cluster cleanup
