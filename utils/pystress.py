#!/usr/bin/env python3

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""
pystress.py - A Python implementation of cassandra-stress using the Python Driver.

Usage:
    ./pystress.py write n=100000 cl=QUORUM -rate threads=50 -node 192.168.1.10

Note:
    connectionsPerHost is implemented by creating multiple Cluster/Session instances and round-robin
    dispatching operations across them in this driver.
    duration= supports suffixes s (seconds), m (minutes), h (hours), e.g., duration=5m.
"""

import sys
import time
import logging
import random
import threading
import re
from concurrent.futures import ThreadPoolExecutor

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy
from cassandra import ConsistencyLevel
from hdrh.histogram import HdrHistogram
from hdrh.log import HistogramLogWriter


logger = logging.getLogger(__name__)

TABLE_NAME = "standard1"


class StressConfig:
    def __init__(self):
        self.command = None
        self.num_operations = 10000  # Default n=
        self.consistency_level = ConsistencyLevel.ONE
        self.threads = 1
        self.col_size = 1024
        self.col_count = 1
        # Distributions
        self.col_size_dist = {"type": "FIXED", "value": 1024}
        self.col_count_dist = {"type": "FIXED", "value": 1}
        self.nodes = ["127.0.0.1"]
        self.replication_factor = 1
        self.replication_strategy = "SimpleStrategy"
        self.keyspace_name = "keyspace1"
        self.compaction_strategy = None
        self.compression = None
        self.pop_dist = "seq"
        self.pop_min = 1
        self.pop_max = 1000000
        self.output_file = "pystress.hdr"
        self.user = None
        self.password = None
        self.connections_per_host = None
        self.duration = None
        self.throttle = None
        self.log_interval = 10  # Default interval for periodic stats (seconds)


def parse_metric_suffix(val_str):
    """Parses suffixes like 100M, 1k."""
    multiplier = 1
    val_str = val_str.upper()
    if val_str.endswith("M"):
        multiplier = 1_000_000
        val_str = val_str[:-1]
    elif val_str.endswith("K"):
        multiplier = 1_000
        val_str = val_str[:-1]
    elif val_str.endswith("B"):
        multiplier = 1_000_000_000
        val_str = val_str[:-1]
    return int(val_str) * multiplier


def parse_distribution(dist_str):
    """
    Parses distribution strings like 'FIXED(1024)' or 'UNIFORM(1..10)'.
    """
    dist_str = dist_str.upper()
    if "FIXED" in dist_str:
        val = re.findall(r"\d+", dist_str)
        if val:
            return {"type": "FIXED", "value": int(val[0])}
    elif "UNIFORM" in dist_str:
        vals = re.findall(r"\d+", dist_str)
        if len(vals) >= 2:
            return {"type": "UNIFORM", "min": int(vals[0]), "max": int(vals[1])}
    # Fallback/Default for simple numbers "size=10"
    vals = re.findall(r"\d+", dist_str)
    if vals:
        return {"type": "FIXED", "value": int(vals[0])}
    return None


def parse_duration(val_str):
    """
    Parses duration strings like '10s', '5m', '1h'.
    """
    val_str = val_str.lower()
    if val_str.endswith("s"):
        return int(val_str[:-1])
    elif val_str.endswith("m"):
        return int(val_str[:-1]) * 60
    elif val_str.endswith("h"):
        return int(val_str[:-1]) * 3600
    else:
        return int(val_str)  # assume seconds


def parse_interval(val_str):
    """
    Parses interval strings like '10s', '500ms', or '10' (seconds).
    Only supports 's' (seconds) and 'ms' (milliseconds) suffixes.
    """
    val_str = val_str.lower()
    if val_str.endswith("ms"):
        return int(val_str[:-2]) / 1000.0
    elif val_str.endswith("s"):
        return int(val_str[:-1])
    else:
        return int(val_str)  # assume seconds


def parse_schema_option(schema_str):
    """
    Parse cassandra-stress style schema options.

    Format:
        replication(strategy=? replication_factor=?)
        keyspace=?
        compaction(strategy=?)
        compression=?

    Examples:
        "replication(replication_factor=3) keyspace=test"
        "replication(strategy=NetworkTopologyStrategy replication_factor=3)"
        "compaction(strategy=LeveledCompactionStrategy)"
    """
    result = {
        "keyspace": "keyspace1",
        "replication_strategy": "SimpleStrategy",
        "replication_factor": 1,
        "compaction_strategy": None,
        "compression": None,
    }

    # Parse replication(...)
    replication_match = re.search(r"replication\s*\(([^)]+)\)", schema_str, re.IGNORECASE)
    if replication_match:
        repl_content = replication_match.group(1)
        # Extract strategy=
        strategy_match = re.search(r"strategy\s*=\s*(\S+)", repl_content, re.IGNORECASE)
        if strategy_match:
            strategy = strategy_match.group(1)
            # Handle full class names or short names
            if "NetworkTopologyStrategy" in strategy:
                result["replication_strategy"] = "NetworkTopologyStrategy"
            else:
                result["replication_strategy"] = "SimpleStrategy"
        # Extract replication_factor=
        factor_match = re.search(r"replication_factor\s*=\s*(\d+)", repl_content, re.IGNORECASE)
        if factor_match:
            result["replication_factor"] = int(factor_match.group(1))

    # Parse keyspace=
    keyspace_match = re.search(r"keyspace\s*=\s*([a-zA-Z0-9_]+)", schema_str, re.IGNORECASE)
    if keyspace_match:
        result["keyspace"] = keyspace_match.group(1)

    # Parse compaction(...)
    compaction_match = re.search(r"compaction\s*\(([^)]+)\)", schema_str, re.IGNORECASE)
    if compaction_match:
        comp_content = compaction_match.group(1)
        strategy_match = re.search(r"strategy\s*=\s*(\S+)", comp_content, re.IGNORECASE)
        if strategy_match:
            result["compaction_strategy"] = strategy_match.group(1)

    # Parse compression=
    compression_match = re.search(r"compression\s*=\s*(\S+)", schema_str, re.IGNORECASE)
    if compression_match:
        result["compression"] = compression_match.group(1)

    return result


def parse_cli_args(args):
    """
    Parses cassandra-stress style arguments manually since argparse
    doesn't handle `key=value` mixed with `-flags` well.
    """
    if "--help" in args or "-h" in args:
        print_help()
        sys.exit(0)

    config = StressConfig()

    if not args:
        print("No command provided (write, read, mixed)")
        sys.exit(1)

    config.command = args[0]
    if config.command not in ["write", "read", "mixed"]:
        print(f"Unknown command: {config.command}")
        sys.exit(1)

    i = 1
    while i < len(args):
        arg = args[i]

        # Handle key=value args
        if "=" in arg and not arg.startswith("-"):
            key, val = arg.split("=", 1)
            if key == "n":
                config.num_operations = parse_metric_suffix(val)
            elif key == "duration":
                config.duration = parse_duration(val)
            elif key == "cl":
                try:
                    config.consistency_level = getattr(ConsistencyLevel, val.upper())
                except AttributeError:
                    logger.warning(f"Unknown CL {val}, defaulting to ONE")
            # Ignore other kv args for simplicity in this scaffold
            i += 1
            continue

        # Handle standard flags
        if arg == "-rate":
            i += 1
            if i < len(args):
                rate_arg = args[i]
                # If rate_arg does not contain commas but we expect multiple options, it might be space separated in one arg or multiple args?
                # Usually cassandra-stress takes -rate threads=N,throttle=N...
                # But sometimes users might pass -rate threads=N throttle=N if they didn't quote correctly or if it consumes adjacent args.
                # However, parse_cli_args iterates args.

                # If the user did "-rate threads=10 throttle=50/s" (two separate args), our loop structure handles distinct key=value args, but flags expect one value usually?
                # Actually, cassandra-stress options usually consume until next main flag.

                # Let's adjust to consume multiple sub-arguments for -rate similar to -mode or -col which consume multiple tokens.
                while i < len(args) and not args[i].startswith("-"):
                    rate_arg = args[i]
                    # It might be comma separated inside one chunk: "threads=50,throttle=100/s"
                    parts = rate_arg.replace(",", " ").split()
                    for part in parts:
                        if "threads=" in part:
                            config.threads = int(part.split("=")[1])
                        elif "throttle=" in part:
                            val = part.split("=")[1]
                            if val.endswith("/s"):
                                config.throttle = float(val[:-2])
                            else:
                                logger.warning(f"Ignoring throttle value '{val}': only 'N/s' format is supported")
                    i += 1
                # The outer loop increments i at the end, but we already consumed everything for -rate.
                # Since we used a while loop to consume non-flag args, current args[i] starts with "-" or we are at end.
                # The outer loop logic is:
                # while i < len(args):
                #   arg = args[i]
                #   ... checks ...
                #   increment i for next iteration

                # If we use a while loop inside, `i` points to the next flag or len(args).
                # We should NOT increment `i` again in the outer logic for this block if we already advanced it correctly.
                # But existing structure is `elif ... i+=1`.

                # Let's rewrite the -rate block to match the structure of -mode and -col which consume multiple tokens.
                continue

        elif arg == "-node":
            i += 1
            if i < len(args):
                config.nodes = args[i].split(",")
            i += 1

        elif arg == "-col":
            i += 1
            # Consume arguments until next flag
            while i < len(args) and not args[i].startswith("-"):
                col_arg = args[i]
                for part in col_arg.split():  # handle 'size=FIXED(1024) n=FIXED(1)'
                    if "size=" in part:
                        _, val = part.split("size=", 1)
                        dist = parse_distribution(val)
                        if dist:
                            config.col_size_dist = dist
                            if dist["type"] == "FIXED":
                                config.col_size = dist["value"]
                            elif dist["type"] == "UNIFORM":
                                config.col_size = dist["max"]  # Use max for buffering/schema

                    if "n=" in part:
                        _, val = part.split("n=", 1)
                        dist = parse_distribution(val)
                        if dist:
                            config.col_count_dist = dist
                            if dist["type"] == "FIXED":
                                config.col_count = dist["value"]
                            elif dist["type"] == "UNIFORM":
                                config.col_count = dist["max"]  # Use max for schema
                i += 1

        elif arg == "-schema":
            i += 1
            schema_parts = []
            while i < len(args) and not args[i].startswith("-"):
                schema_parts.append(args[i])
                i += 1
            if schema_parts:
                schema_str = " ".join(schema_parts)
                parsed = parse_schema_option(schema_str)
                config.keyspace_name = parsed["keyspace"]
                config.replication_strategy = parsed["replication_strategy"]
                config.replication_factor = parsed["replication_factor"]
                config.compaction_strategy = parsed["compaction_strategy"]
                config.compression = parsed["compression"]

        elif arg == "-pop":
            i += 1
            if i < len(args):
                pop_arg = args[i]
                # Basic handling for seq=X..Y or dist=gauss(...)
                if "seq=" in pop_arg:
                    vals = re.findall(r"\d+", pop_arg)
                    if len(vals) >= 2:
                        config.pop_min = int(vals[0])
                        config.pop_max = int(vals[1])
                elif "dist=gauss" in pop_arg:
                    # map gauss to random for this scaffold
                    config.pop_dist = "random"
                    vals = re.findall(r"\d+", pop_arg)
                    if len(vals) >= 1:
                        config.pop_max = int(vals[0])
            i += 1

        elif arg == "-log":
            i += 1
            while i < len(args) and not args[i].startswith("-"):
                log_arg = args[i]
                parts = log_arg.replace(",", " ").split()
                for part in parts:
                    if "hdrfile=" in part:
                        config.output_file = part.split("=", 1)[1]
                    elif "interval=" in part:
                        config.log_interval = parse_interval(part.split("=", 1)[1])
                i += 1

        elif arg == "-mode":
            i += 1
            while i < len(args) and not args[i].startswith("-"):
                mode_part = args[i]
                _parse_mode_arg(config, mode_part)
                i += 1

        else:
            # Skip unknown
            i += 1

    return config


def _parse_mode_arg(config, mode_arg):
    if "user=" in mode_arg:
        config.user = mode_arg.split("=", 1)[1]
    elif "password=" in mode_arg:
        config.password = mode_arg.split("=", 1)[1]
    elif "connectionsPerHost=" in mode_arg:
        config.connections_per_host = int(mode_arg.split("=", 1)[1])


def print_help():
    """Print detailed help information for pystress.py."""
    help_text = """
pystress.py - A Python implementation of cassandra-stress using the Python Driver.

USAGE:
    ./pystress.py <command> [positional_args] [flags]

COMMANDS:
    write       Perform write operations
    read        Perform read operations
    mixed       Perform mixed operations (stub)

POSITIONAL ARGUMENTS (key=value):
    n=<number>              Number of operations (default: 10000)
                            Supports suffixes: K (thousands), M (millions), B (billions)
    duration=<time>         Run for specified duration instead of fixed operations
                            Supports suffixes: s (seconds), m (minutes), h (hours)
                            Example: duration=5m
    cl=<consistency>        Consistency level (default: ONE)
                            Options: ONE, QUORUM, LOCAL_QUORUM, etc.

FLAGS:
    -rate <options>         Rate control options
                            threads=<number>    Number of threads (default: 1)
                            throttle=<number>/s Operation rate limit (ops/sec)
                            Example: -rate threads=50,throttle=1000/s
    -node <nodes>           Comma-separated list of node IPs (default: 127.0.0.1)
                            Example: -node 192.168.1.10,192.168.1.11
    -col <options>          Column specification
                            size=<dist>         Column size distribution
                            n=<dist>            Number of columns distribution
                            Distributions: FIXED(value), UNIFORM(min..max)
                            Example: -col size=FIXED(1024) n=FIXED(5)
    -schema <options>       Schema options
                            replication(strategy=? replication_factor=?)  Replication settings
                                strategy=? (default: SimpleStrategy)
                                replication_factor=? (default: 1)
                            keyspace=? (default: keyspace1)
                            compaction(strategy=?)  Compaction strategy
                            compression=?           Compression setting
                            Example: -schema 'replication(replication_factor=3) keyspace=test'
                            Example: -schema 'replication(strategy=NetworkTopologyStrategy replication_factor=3) compaction(strategy=LeveledCompactionStrategy)'
    -pop <options>          Population distribution
                            seq=<min>..<max>    Sequential range (default)
                            dist=gauss(<mean>)  Gaussian distribution
                            Example: -pop seq=1..1000000
    -log <options>          Logging options
                            hdrfile=<path>      Output HDR histogram file (default: pystress.hdr)
                            interval=<time>     Interval for periodic stats output (default: 10)
                                                Supports suffixes: s (seconds), ms (milliseconds)
                            Example: -log hdrfile=custom.hdr interval=5s
    -mode <options>         Connection mode options
                            user=<username>     Authentication username
                            password=<password> Authentication password
                            connectionsPerHost=<number> Multiple connections per host
                            Example: -mode user=cassandra password=cassandra connectionsPerHost=5
    -h, --help              Show this help message

EXAMPLES:
    Basic write test:
        ./pystress.py write n=10000 -node 127.0.0.1

    Write with custom rate and duration:
        ./pystress.py write duration=30s -rate threads=10,throttle=500/s -node 192.168.1.10

    Read test with custom columns:
        ./pystress.py read n=50000 -col size=UNIFORM(512..2048) n=FIXED(10) -rate threads=20

    Mixed operations with authentication:
        ./pystress.py mixed n=100000 -mode user=cassandra password=secret -node 10.0.0.1,10.0.0.2

NOTES:
    - connectionsPerHost creates multiple Cluster/Session instances for load distribution.
    - Throttle format must be <number>/s (e.g., 1000/s).
    - HDR histogram is saved to the specified file for latency analysis.
"""
    print(help_text)


class CassandraStressPy:
    def __init__(self, config):
        self.config = config
        self.clusters = []
        self.sessions = []
        self.prepared_writes = []
        self.prepared_reads = []
        # 1 microsec to 1 hour, 3 sig figs
        self.histogram = HdrHistogram(1, 60 * 60 * 1000 * 1000, 3)
        self.histogram_lock = threading.Lock()

        # Interval statistics tracking
        self.interval_histogram = HdrHistogram(1, 60 * 60 * 1000 * 1000, 3)
        self.interval_ops = 0
        self.interval_errors = 0
        self.total_ops = 0
        self.total_errors = 0
        self.stats_lock = threading.Lock()
        self.last_interval_time = 0
        self.stats_header_printed = False

        # Pre-cache max payload size if fixed, or just max for buffer if we use it consistently
        if self.config.col_size_dist["type"] == "FIXED":
            self._payload_cache = b"x" * self.config.col_size_dist["value"]
        else:
            # For UNIFORM, we might need variable payloads, but we can cache the max size and slice it?
            # Or just generate fresh. Slicing from max cache is faster.
            # Only if Uniform max is reasonable.
            max_size = self.config.col_size
            self._payload_cache = b"x" * max_size

    def connect(self):
        logger.info(f"Connecting to {self.config.nodes}...")

        auth_provider = None
        if self.config.user and self.config.password:
            from cassandra.auth import PlainTextAuthProvider

            auth_provider = PlainTextAuthProvider(username=self.config.user, password=self.config.password)

        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
            consistency_level=self.config.consistency_level,
        )

        cluster_count = 1
        if self.config.connections_per_host:
            cluster_count = max(1, int(self.config.connections_per_host))
            logger.info(f"Creating {cluster_count} cluster connections for connectionsPerHost={cluster_count}")

        for _ in range(cluster_count):
            cluster = Cluster(
                contact_points=self.config.nodes,
                execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                protocol_version=4,
                auth_provider=auth_provider,
            )
            session = cluster.connect()
            self.clusters.append(cluster)
            self.sessions.append(session)

        logger.info("Connected.")

    def setup_schema(self):
        logger.info("Setting up schema...")
        if self.config.replication_strategy == "NetworkTopologyStrategy":
            strategy = f"{{'class': 'NetworkTopologyStrategy', 'replication_factor': {self.config.replication_factor}}}"
        else:
            strategy = f"{{'class': 'SimpleStrategy', 'replication_factor': {self.config.replication_factor}}}"

        keyspace_name = self.config.keyspace_name
        session = self.sessions[0]
        session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {strategy}")
        session.execute(f"USE {keyspace_name}")

        # Mimic standard1 table
        # We start columns from C1 because C0 is often used but cassandra-stress uses C0..CN.
        # Standard1 in cassandra-stress usually (key, C0, C1, C2, C3, C4)
        cols = ", ".join([f'"C{i}" blob' for i in range(self.config.col_count)])
        stmt = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (key blob PRIMARY KEY, {cols})"

        # Add table options (compaction and compression)
        table_options = []
        if self.config.compaction_strategy:
            table_options.append(f"compaction = {{'class': '{self.config.compaction_strategy}'}}")
        if self.config.compression:
            table_options.append(f"compression = {{'sstable_compression': '{self.config.compression}'}}")

        if table_options:
            stmt += " WITH " + " AND ".join(table_options)

        session.execute(stmt)

        placeholders = ", ".join(["?"] * self.config.col_count)
        col_names = ", ".join([f'"C{i}"' for i in range(self.config.col_count)])

        write_q = f"INSERT INTO {TABLE_NAME} (key, {col_names}) VALUES (?, {placeholders})"
        read_q = f"SELECT * FROM {TABLE_NAME} WHERE key = ?"

        self.prepared_writes = []
        self.prepared_reads = []
        for prep_session in self.sessions:
            prep_session.set_keyspace(keyspace_name)
            logger.info(f"Prepare Write: {write_q}")
            self.prepared_writes.append(prep_session.prepare(write_q))
            logger.info(f"Prepare Read: {read_q}")
            self.prepared_reads.append(prep_session.prepare(read_q))

    def _generate_payloads(self):
        # Determine actual count for this op
        actual_count = self.config.col_count
        if self.config.col_count_dist["type"] == "UNIFORM":
            actual_count = random.randint(self.config.col_count_dist["min"], self.config.col_count_dist["max"])

        payloads = []
        for i in range(self.config.col_count):  # Iterate over schema columns (max)
            if i < actual_count:
                if self.config.col_size_dist["type"] == "FIXED":
                    payloads.append(self._payload_cache)
                else:
                    # Variable size
                    size = self.config.col_size
                    if self.config.col_size_dist["type"] == "UNIFORM":
                        size = random.randint(self.config.col_size_dist["min"], self.config.col_size_dist["max"])

                    # Optimization: slice from cache if possible
                    if len(self._payload_cache) >= size:
                        payloads.append(self._payload_cache[:size])
                    else:
                        payloads.append(b"x" * size)
            else:
                payloads.append(None)
        return payloads

    def _generate_key(self, idx):
        # cassandra-stress keys are blobs, often string reps of numbers
        return str(idx).encode("utf-8")

    def run(self):
        self.connect()
        # Only setup schema if writing? Or assume it exists for read?
        # cassandra-stress usually ensures schema unless specified otherwise.
        # We will attempt schema setup.
        try:
            self.setup_schema()
        except Exception as e:
            logger.error(f"Schema setup failed; aborting run: {e}")
            return

        ops_total = self.config.num_operations
        threads = self.config.threads

        logger.info(
            f"Starting {self.config.command} workload: {ops_total} ops, {threads} threads, "
            f"CL={self.config.consistency_level}, ColSize={self.config.col_size}, ColCount={self.config.col_count}"
        )

        start_time = time.perf_counter()

        # Generator for keys
        if self.config.pop_dist == "random":

            def key_gen():
                idx = self.config.pop_min
                while True:
                    if self.config.pop_dist == "random":
                        yield random.randint(self.config.pop_min, self.config.pop_max)
                    else:
                        yield idx
                        idx += 1
        else:
            # Sequence
            def key_gen():
                idx = self.config.pop_min
                while True:
                    yield idx
                    idx += 1

        keys = key_gen()

        # We prepare the payload args once if they are same every time (random data not supported properly yet)
        # For multiple columns, we need a list of payloads
        # Optimization: if everything is fixed, precompute args
        fixed_payloads = None
        if self.config.col_size_dist["type"] == "FIXED" and self.config.col_count_dist["type"] == "FIXED":
            fixed_payloads = [self._payload_cache for _ in range(self.config.col_count)]

        sessions_count = len(self.sessions)
        session_index_lock = threading.Lock()
        session_index = 0

        end_time = start_time + self.config.duration if self.config.duration else None

        # Rate limiting variables
        throttle_delay = 1.0 / self.config.throttle if self.config.throttle else 0
        next_op_time = start_time

        # Interval stats tracking
        self.last_interval_time = 0
        next_interval_time = start_time + self.config.log_interval

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = []
            submitted = 0

            # Helper to process result
            def on_done(f):
                try:
                    latency_micros = f.result()
                    with self.histogram_lock:
                        self.histogram.record_value(latency_micros)
                    with self.stats_lock:
                        self.interval_histogram.record_value(latency_micros)
                        self.interval_ops += 1
                        self.total_ops += 1
                except Exception as e:
                    logger.error(f"Op failed: {e}")
                    with self.stats_lock:
                        self.interval_errors += 1
                        self.total_errors += 1

            def pick_session_index():
                nonlocal session_index
                with session_index_lock:
                    idx = session_index
                    session_index = (session_index + 1) % sessions_count
                return idx

            # Define task
            def do_write(key_int, session_idx):
                # construct args: key + [payloads...]
                if fixed_payloads:
                    current_payloads = fixed_payloads
                else:
                    current_payloads = self._generate_payloads()

                args = [self._generate_key(key_int)] + current_payloads

                t0 = time.perf_counter()
                self.sessions[session_idx].execute(self.prepared_writes[session_idx], args)
                return int((time.perf_counter() - t0) * 1_000_000)

            def do_read(key_int, session_idx):
                args = (self._generate_key(key_int),)

                t0 = time.perf_counter()
                self.sessions[session_idx].execute(self.prepared_reads[session_idx], args)
                return int((time.perf_counter() - t0) * 1_000_000)

            task = do_write if self.config.command == "write" else do_read

            # Main Loop - Submit tasks
            # To avoid exploding memory with millions of futures, we control the queue depth
            max_pending = threads * 50

            while True:
                now = time.perf_counter()
                if end_time and now >= end_time:
                    break
                if not self.config.duration and submitted >= self.config.num_operations:
                    break

                # Rate limiting
                if throttle_delay > 0:
                    if now < next_op_time:
                        sleep_time = next_op_time - now
                        if sleep_time > 0:
                            time.sleep(sleep_time)
                        next_op_time += throttle_delay
                    else:
                        # We are behind schedule or just starting.
                        # Do not try to burst catch up too much if we are very far behind?
                        # For simple limiting, we just set next op time.
                        # To avoid drift, we increment from next_op_time, but if we fall way behind
                        # (e.g. system pause), we might reset to now.
                        # Simple implementation:
                        next_op_time = max(next_op_time + throttle_delay, now + throttle_delay)

                try:
                    k = next(keys)
                except StopIteration:
                    break

                idx = pick_session_index()
                f = executor.submit(task, k, idx)
                f.add_done_callback(on_done)
                futures.append(f)
                submitted += 1

                if len(futures) >= max_pending:
                    # Prune finished futures
                    futures = [fut for fut in futures if not fut.done()]
                    # if still full, sleep a bit
                    while len(futures) >= max_pending:
                        time.sleep(0.005)
                        futures = [fut for fut in futures if not fut.done()]

                # Print interval stats periodically
                now = time.perf_counter()
                if now >= next_interval_time:
                    elapsed = now - start_time
                    self.print_interval_stats(elapsed)
                    next_interval_time = now + self.config.log_interval

            # Wait for remaining
            logger.info("Waiting for pending tasks...")
            for f in futures:
                try:
                    f.result()
                except Exception:
                    pass  # Handled in callback

        total_duration = time.perf_counter() - start_time
        print("\nDone.")
        self.save_hdr()
        self.print_summary(total_duration, submitted)
        for cluster in self.clusters:
            cluster.shutdown()

    def save_hdr(self):
        logger.info(f"Saving HDR histogram to {self.config.output_file}...")
        try:
            with open(self.config.output_file, "w") as f:
                writer = HistogramLogWriter(f)
                writer.output_log_format_version()
                writer.output_comment(f"pystress command={self.config.command}")
                writer.output_legend()
                # We output one interval covering the whole test
                writer.output_interval_histogram(self.histogram)
        except Exception as e:
            logger.error(f"Failed to write HDR file: {e}")

    def print_interval_stats(self, elapsed_time):
        """Print interval statistics in cassandra-stress format."""
        with self.stats_lock:
            if self.interval_ops == 0:
                return

            # Calculate ops/s for this interval
            interval_duration = elapsed_time - self.last_interval_time
            if interval_duration <= 0:
                return

            ops_per_sec = self.interval_ops / interval_duration

            # Get latency percentiles from interval histogram (convert from us to ms)
            mean_ms = self.interval_histogram.get_mean_value() / 1000.0
            median_ms = self.interval_histogram.get_value_at_percentile(50.0) / 1000.0
            p95_ms = self.interval_histogram.get_value_at_percentile(95.0) / 1000.0
            p99_ms = self.interval_histogram.get_value_at_percentile(99.0) / 1000.0
            p999_ms = self.interval_histogram.get_value_at_percentile(99.9) / 1000.0
            max_ms = self.interval_histogram.get_max_value() / 1000.0

            # Print header once
            if not self.stats_header_printed:
                print(
                    f"{'total ops':>10}, {'op/s':>8}, {'mean':>6}, {'med':>7}, {'.95':>7}, "
                    f"{'.99':>7}, {'.999':>7}, {'max':>7}, {'time':>6}, {'errors':>7}"
                )
                self.stats_header_printed = True

            # Print interval stats
            print(
                f"{self.total_ops:>10}, {ops_per_sec:>8.0f}, {mean_ms:>6.1f}, {median_ms:>7.1f}, "
                f"{p95_ms:>7.1f}, {p99_ms:>7.1f}, {p999_ms:>7.1f}, {max_ms:>7.1f}, "
                f"{elapsed_time:>6.1f}, {self.interval_errors:>7}"
            )

            # Reset interval counters
            self.interval_histogram.reset()
            self.interval_ops = 0
            self.interval_errors = 0
            self.last_interval_time = elapsed_time

    def print_summary(self, duration, ops):
        op_type = self.config.command.upper()
        op_rate = ops / duration if duration > 0 else 0

        # Convert microseconds to milliseconds for display
        latency_mean = self.histogram.get_mean_value() / 1000.0
        latency_median = self.histogram.get_value_at_percentile(50.0) / 1000.0
        latency_p95 = self.histogram.get_value_at_percentile(95.0) / 1000.0
        latency_p99 = self.histogram.get_value_at_percentile(99.0) / 1000.0
        latency_p999 = self.histogram.get_value_at_percentile(99.9) / 1000.0
        latency_max = self.histogram.get_max_value() / 1000.0

        # Format duration as HH:MM:SS
        hours, remainder = divmod(int(duration), 3600)
        minutes, seconds = divmod(remainder, 60)
        duration_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        print("\nResults:")
        print(f"Op rate                   : {op_rate:,.0f} op/s  [{op_type}: {op_rate:,.0f} op/s]")
        print(f"Latency mean              :  {latency_mean:.1f} ms [{op_type}: {latency_mean:.1f} ms]")
        print(f"Latency median            :  {latency_median:.1f} ms [{op_type}: {latency_median:.1f} ms]")
        print(f"Latency 95th percentile   :  {latency_p95:.1f} ms [{op_type}: {latency_p95:.1f} ms]")
        print(f"Latency 99th percentile   :  {latency_p99:.1f} ms [{op_type}: {latency_p99:.1f} ms]")
        print(f"Latency 99.9th percentile :  {latency_p999:.1f} ms [{op_type}: {latency_p999:.1f} ms]")
        print(f"Latency max               :  {latency_max:.1f} ms [{op_type}: {latency_max:.1f} ms]")
        print(f"Total operation time      : {duration_str}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    config = parse_cli_args(sys.argv[1:])
    tool = CassandraStressPy(config)
    tool.run()
