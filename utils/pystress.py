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
    The processes option (-rate processes=N) spawns multiple worker processes for true parallelism.
    Each process has per-thread Cluster/Session connections, bypassing Python's GIL.
    Total connections = processes * threads (e.g., processes=4 threads=10 = 40 connections).
    duration= supports suffixes s (seconds), m (minutes), h (hours), e.g., duration=5m.
"""

import sys
import time
import logging
import random
import threading
import re
import multiprocessing
import multiprocessing.synchronize
import queue
from concurrent.futures import ThreadPoolExecutor
from typing import TypedDict
from datetime import datetime

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel, consistency_value_to_name
from hdrh.histogram import HdrHistogram
from hdrh.log import HistogramLogWriter


logger = logging.getLogger(__name__)

TABLE_NAME = "standard1"


class TaggedHistogramLogWriter(HistogramLogWriter):
    """Extended HistogramLogWriter that supports tag output in histogram lines.

    The standard HistogramLogWriter does not output the histogram's tag even when set.
    This subclass overrides output_interval_histogram() to include the tag prefix
    in the format: Tag=<tag>,<start>,<interval>,<max>,<encoded>
    """

    def output_start_time(self, start_time_msec):
        """Log a start time in the log.
        Params:
            start_time_msec time (in milliseconds) since the absolute start time (the epoch)
        """
        start_time_sec = float(start_time_msec) / 1000.0
        datetime_formatted = datetime.fromtimestamp(start_time_sec).isoformat(" ")
        self.log.write(f"#[StartTime: {start_time_sec} (seconds since epoch), {datetime_formatted}]\n")

    def output_interval_histogram(
        self, histogram, start_time_stamp_sec=0, end_time_stamp_sec=0, max_value_unit_ratio=1000000000.0
    ):
        """Output an interval histogram with optional tag support.

        If the histogram has a tag set (via histogram.set_tag()), it will be
        included in the output line as: Tag=<tag>,<start>,<interval>,<max>,<encoded>
        """
        if not start_time_stamp_sec:
            start_time_stamp_sec = (histogram.get_start_time_stamp() - self.base_time) / 1000.0
        if not end_time_stamp_sec:
            end_time_stamp_sec = (histogram.get_end_time_stamp() - self.base_time) / 1000.0

        cpayload = histogram.encode()

        # Check if histogram has a tag and include it in output
        tag = histogram.get_tag()
        if tag:
            self.log.write(
                "Tag=%s,%f,%f,%f,%s\n"
                % (
                    tag,
                    start_time_stamp_sec,
                    end_time_stamp_sec - start_time_stamp_sec,
                    histogram.get_max_value() // max_value_unit_ratio,
                    cpayload.decode("utf-8"),
                )
            )
        else:
            self.log.write(
                "%f,%f,%f,%s\n"
                % (
                    start_time_stamp_sec,
                    end_time_stamp_sec - start_time_stamp_sec,
                    histogram.get_max_value() // max_value_unit_ratio,
                    cpayload.decode("utf-8"),
                )
            )


class PopDistribution(TypedDict):
    type: str
    min: int
    max: int
    mean: int | float | None
    stdev: int | float | None


class StressConfig:
    def __init__(self):
        self.command = None
        self.num_operations = 10000  # Default n=
        self.consistency_level = ConsistencyLevel.ONE
        self.threads = 1
        self.col_size = 1024
        self.col_count = 1
        # Distributions
        self.nodes = ["127.0.0.1"]
        self.replication_factor = 1
        self.replication_strategy = "SimpleStrategy"
        self.keyspace_name = "keyspace1"
        self.compaction_strategy = None
        self.compression = None
        self.pop_dist = "seq"
        self.pop_min = 1
        self.pop_max = 1000000
        self.pop_mean = None
        self.pop_stdev = None
        self.output_file = "pystress.hdr"
        self.user = None
        self.password = None
        self.processes = 1
        self.duration = None
        self.throttle = None
        self.log_interval = 10  # Default interval for periodic stats (seconds)
        self.ratio_write = 1  # Write ratio for mixed workload
        self.ratio_read = 1  # Read ratio for mixed workload
        self.request_timeout = None  # Request timeout in seconds (None = use default 12s like cassandra-stress)

    @property
    def threads_per_worker(self):
        return max(1, self.threads // self.processes)


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


def parse_pop_distribution(pop_str) -> PopDistribution:
    """
    Parse cassandra-stress style population distribution.

    Supported formats:
        seq=min..max
        dist=GAUSSIAN(min..max,stdvrng)
        dist=GAUSSIAN(min..max,mean,stdev)
        dist=UNIFORM(min..max)

    Aliases: gauss, normal, norm (all map to GAUSSIAN)
    Distribution names are case-insensitive.
    """
    result: PopDistribution = {
        "type": "seq",
        "min": 1,
        "max": 1000000,
        "mean": None,
        "stdev": None,
    }

    # Parse seq=min..max
    if "seq=" in pop_str.lower():
        vals = re.findall(r"\d+", pop_str)
        if len(vals) >= 2:
            result["type"] = "seq"
            result["min"] = int(vals[0])
            result["max"] = int(vals[1])
        return result

    # Parse dist=DISTRIBUTION(...)
    dist_match = re.search(r"dist\s*=\s*(\w+)\s*\(([^)]+)\)", pop_str, re.IGNORECASE)
    if dist_match:
        dist_name = dist_match.group(1).lower()
        params = dist_match.group(2)

        # Extract all numbers (handles min..max,param1,param2)
        vals = re.findall(r"\d+", params)

        if dist_name in ("gaussian", "gauss", "normal", "norm"):
            result["type"] = "gaussian"
            if len(vals) >= 2:
                result["min"] = int(vals[0])
                result["max"] = int(vals[1])
                mean = (result["min"] + result["max"]) / 2

                if len(vals) == 3:
                    # GAUSSIAN(min..max,stdvrng) format
                    stdvrng = float(vals[2])
                    result["mean"] = mean
                    if stdvrng > 0:
                        result["stdev"] = (mean - result["min"]) / stdvrng
                    else:
                        result["stdev"] = (mean - result["min"]) / 3
                elif len(vals) >= 4:
                    # GAUSSIAN(min..max,mean,stdev) format
                    result["mean"] = float(vals[2])
                    result["stdev"] = float(vals[3])
                else:
                    # Default: stdvrng=3 (covers ~99.7% within range)
                    result["mean"] = mean
                    result["stdev"] = (mean - result["min"]) / 3

        elif dist_name == "uniform":
            result["type"] = "uniform"
            if len(vals) >= 2:
                result["min"] = int(vals[0])
                result["max"] = int(vals[1])

    return result


def parse_ratio_option(ratio_str):
    """
    Parse cassandra-stress style ratio option for mixed workloads.

    Format: ratio(write=N,read=M) or ratio(read=M,write=N)

    Examples:
        "ratio(write=1,read=1)"  -> write=1, read=1 (50/50)
        "ratio(write=1,read=2)"  -> write=1, read=2 (33/67)
        "ratio(read=3,write=1)"  -> write=1, read=3 (25/75)
    """
    result = {"write": 1, "read": 1}

    ratio_match = re.search(r"ratio\s*\(([^)]+)\)", ratio_str, re.IGNORECASE)
    if ratio_match:
        content = ratio_match.group(1)

        write_match = re.search(r"write\s*=\s*(\d+)", content, re.IGNORECASE)
        if write_match:
            result["write"] = int(write_match.group(1))

        read_match = re.search(r"read\s*=\s*(\d+)", content, re.IGNORECASE)
        if read_match:
            result["read"] = int(read_match.group(1))

    return result


def _parse_key_value_arg(config, key, val):
    """Parse a key=value argument and update config."""
    if key == "n":
        config.num_operations = parse_metric_suffix(val)
    elif key == "duration":
        config.duration = parse_duration(val)
    elif key == "cl":
        try:
            config.consistency_level = getattr(ConsistencyLevel, val.upper())
        except AttributeError:
            logger.warning(f"Unknown CL {val}, defaulting to ONE")


def _parse_rate_args(config, args, start_idx):
    """Parse -rate arguments, returns next index to process."""
    i = start_idx
    while i < len(args) and not args[i].startswith("-"):
        rate_arg = args[i]
        parts = rate_arg.replace(",", " ").split()
        for part in parts:
            if "threads=" in part:
                config.threads = int(part.split("=")[1])
            elif "processes=" in part:
                config.processes = int(part.split("=")[1])
            elif "throttle=" in part:
                val = part.split("=")[1]
                if val.endswith("/s"):
                    config.throttle = float(val[:-2])
                else:
                    logger.warning(f"Ignoring throttle value '{val}': only 'N/s' format is supported")
        i += 1
    return i


def _parse_col_args(config, args, start_idx):
    """Parse -col arguments, returns next index to process."""
    i = start_idx
    while i < len(args) and not args[i].startswith("-"):
        col_arg = args[i]
        for part in col_arg.split():
            if "size=" in part:
                _, val = part.split("size=", 1)
                dist = parse_distribution(val)
                if dist:
                    if dist["type"] == "FIXED":
                        config.col_size = dist["value"]
                    elif dist["type"] == "UNIFORM":
                        config.col_size = dist["max"]
            if "n=" in part:
                _, val = part.split("n=", 1)
                dist = parse_distribution(val)
                if dist:
                    if dist["type"] == "FIXED":
                        config.col_count = dist["value"]
                    elif dist["type"] == "UNIFORM":
                        config.col_count = dist["max"]
        i += 1
    return i


def _parse_schema_args(config, args, start_idx):
    """Parse -schema arguments, returns next index to process."""
    i = start_idx
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
    return i


def _parse_pop_args(config, args, start_idx):
    """Parse -pop arguments, returns next index to process."""
    i = start_idx
    pop_parts = []
    while i < len(args) and not args[i].startswith("-"):
        pop_parts.append(args[i])
        i += 1
    if pop_parts:
        pop_str = " ".join(pop_parts)
        parsed = parse_pop_distribution(pop_str)
        config.pop_dist = parsed["type"]
        config.pop_min = parsed["min"]
        config.pop_max = parsed["max"]
        config.pop_mean = parsed["mean"]
        config.pop_stdev = parsed["stdev"]
    return i


def _parse_log_args(config, args, start_idx):
    """Parse -log arguments, returns next index to process."""
    i = start_idx
    while i < len(args) and not args[i].startswith("-"):
        log_arg = args[i]
        parts = log_arg.replace(",", " ").split()
        for part in parts:
            if "hdrfile=" in part:
                config.output_file = part.split("=", 1)[1]
            elif "interval=" in part:
                config.log_interval = parse_interval(part.split("=", 1)[1])
        i += 1
    return i


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

    # Check for ratio option immediately after mixed command
    if config.command == "mixed" and i < len(args) and "ratio(" in args[i].lower():
        parsed_ratio = parse_ratio_option(args[i])
        config.ratio_write = parsed_ratio["write"]
        config.ratio_read = parsed_ratio["read"]
        i += 1

    while i < len(args):
        arg = args[i]

        # Handle key=value args
        if "=" in arg and not arg.startswith("-"):
            key, val = arg.split("=", 1)
            _parse_key_value_arg(config, key, val)
            i += 1
            continue

        # Handle standard flags
        if arg == "-rate":
            i = _parse_rate_args(config, args, i + 1)
        elif arg == "-node":
            i += 1
            if i < len(args):
                config.nodes = args[i].split(",")
            i += 1
        elif arg == "-col":
            i = _parse_col_args(config, args, i + 1)
        elif arg == "-schema":
            i = _parse_schema_args(config, args, i + 1)
        elif arg == "-pop":
            i = _parse_pop_args(config, args, i + 1)
        elif arg == "-log":
            i = _parse_log_args(config, args, i + 1)
        elif arg == "-mode":
            i += 1
            while i < len(args) and not args[i].startswith("-"):
                _parse_mode_arg(config, args[i])
                i += 1
        else:
            # Skip unknown
            i += 1

    return config


def _parse_mode_arg(config, mode_arg):
    mode_arg_lower = mode_arg.lower()
    if mode_arg_lower.startswith("user="):
        config.user = mode_arg.split("=", 1)[1]
    elif mode_arg_lower.startswith("password="):
        config.password = mode_arg.split("=", 1)[1]
    elif mode_arg_lower.startswith("requesttimeout="):
        # cassandra-stress uses milliseconds, Python driver uses seconds
        timeout_ms = int(mode_arg.split("=", 1)[1])
        config.request_timeout = timeout_ms / 1000.0


def print_help():
    """Print detailed help information for pystress.py."""
    help_text = """
pystress.py - A Python implementation of cassandra-stress using the Python Driver.

USAGE:
    ./pystress.py <command> [positional_args] [flags]

COMMANDS:
    write       Perform write operations
    read        Perform read operations
    mixed       Perform mixed read/write operations
                ratio(write=N,read=M)  Ratio of operations (default: 1:1)
                Example: mixed 'ratio(write=1,read=2)'  (33% writes, 67% reads)

POSITIONAL ARGUMENTS (key=value):
    n=<number>              Number of operations (default: 10000)
                            Supports suffixes: K (thousands), M (millions), B (billions)
    duration=<time>         Run for specified duration instead of fixed operations
                            Supports suffixes: s (seconds), m (minutes), h (hours)
                            Example: duration=5m
    cl=<consistency>        Consistency level (default: ONE)
                            Options: ONE, QUORUM, LOCAL_QUORUM, etc.

FLAGS:
    -rate <options>         Rate and parallelism options
                            threads=<number>    Number of threads (default: 1)
                            processes=<number>  Number of worker processes (default: 1)
                            throttle=<number>/s Operation rate limit (ops/sec)
                            Example: -rate threads=50 processes=4 throttle=1000/s
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
                            dist=UNIFORM(min..max)  Uniform random distribution
                            dist=GAUSSIAN(min..max,stdvrng)  Gaussian distribution
                                mean=(min+max)/2, stdev=(mean-min)/stdvrng
                            dist=GAUSSIAN(min..max,mean,stdev)  Gaussian with explicit params
                            Aliases: gauss, normal, norm (for GAUSSIAN)
                            Example: -pop 'seq=1..1000000'
                            Example: -pop 'dist=UNIFORM(1..1000000)'
                            Example: -pop 'dist=GAUSSIAN(1..1000000,5)'
                            Example: -pop 'dist=gauss(1..1000000,500000,100000)'
    -log <options>          Logging options
                            hdrfile=<path>      Output HDR histogram file (default: pystress.hdr)
                            interval=<time>     Interval for periodic stats output (default: 10)
                                                Supports suffixes: s (seconds), ms (milliseconds)
                            Example: -log hdrfile=custom.hdr interval=5s
    -mode <options>         Connection mode options
                            user=<username>     Authentication username
                            password=<password> Authentication password
                            requestTimeout=<ms> Request timeout in milliseconds (default: 12000)
                            Example: -mode user=cassandra password=cassandra
                            Example: -mode requestTimeout=30000
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
    - processes creates multiple worker processes, each with per-thread Cluster/Session connections.
    - Each thread within a worker process has its own independent ScyllaDB connection.
    - Total connections = processes * threads (e.g., 4 * 10 = 40 connections).
    - Throttle format must be <number>/s (e.g., 1000/s).
    - HDR histogram is saved to the specified file for latency analysis.
"""
    print(help_text)


def create_cluster_connection(
    nodes: list,
    user: str | None = None,
    password: str | None = None,
    consistency_level=ConsistencyLevel.ONE,
    request_timeout: float | None = None,
) -> Cluster:
    """Create a Cluster instance with common configuration.

    Args:
        nodes: List of contact point IP addresses
        user: Authentication username (optional)
        password: Authentication password (optional)
        consistency_level: CQL consistency level
        request_timeout: Request timeout in seconds (default: 12s like cassandra-stress)

    Returns:
        Configured Cluster instance (not connected)
    """
    auth_provider = None
    if user and password:
        auth_provider = PlainTextAuthProvider(username=user, password=password)

    profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
        consistency_level=consistency_level,
        request_timeout=request_timeout or 12.0,  # Default 12s like cassandra-stress
    )

    return Cluster(
        contact_points=nodes,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        protocol_version=4,
        auth_provider=auth_provider,
        connect_timeout=11,  # Timeout for initial cluster connection
        control_connection_timeout=6,  # Timeout for heartbeat responses
    )


class ThreadConnection:
    """Per-thread connection state for ScyllaDB.

    Each thread in the worker's thread pool gets its own Cluster/Session
    to avoid contention and provide true connection isolation.
    """

    def __init__(self, config_dict: dict, col_count: int, payload_cache: bytes):
        self.config_dict = config_dict
        self.col_count = col_count
        self.payload_cache = payload_cache
        self.cluster = None
        self.session = None
        self.prepared_write = None
        self.prepared_read = None

    def connect(self):
        """Establish connection and prepare statements."""
        self.cluster = create_cluster_connection(
            nodes=self.config_dict["nodes"],
            user=self.config_dict.get("user"),
            password=self.config_dict.get("password"),
            consistency_level=getattr(ConsistencyLevel, self.config_dict["consistency_level"]),
            request_timeout=self.config_dict.get("request_timeout"),
        )
        self.session = self.cluster.connect()
        self.session.use_client_timestamp = False
        self.session.set_keyspace(self.config_dict["keyspace_name"])

        # Prepare statements
        placeholders = ", ".join(["?"] * self.col_count)
        col_names = ", ".join([f'"C{i}"' for i in range(self.col_count)])
        write_q = f"INSERT INTO {TABLE_NAME} (key, {col_names}) VALUES (?, {placeholders})"
        read_q = f"SELECT * FROM {TABLE_NAME} WHERE key = ?"

        self.prepared_write = self.session.prepare(write_q)
        self.prepared_read = self.session.prepare(read_q)

    def shutdown(self):
        """Close connection."""
        if self.cluster:
            self.cluster.shutdown()
            self.cluster = None
            self.session = None


class WorkerContext:
    """Context holder for worker process state with per-thread connections.

    Each thread in the worker's thread pool gets its own ThreadConnection.
    Connections are pre-created before workload starts to avoid latency impact.
    Histograms and counters are shared across threads with lock protection.
    """

    def __init__(self, worker_id: int, config_dict: dict):
        self.worker_id = worker_id
        self.config_dict = config_dict
        self.col_count = config_dict["col_count"]
        self.threads_per_worker = config_dict["threads_per_worker"]

        # Shared payload cache (read-only, generated once)
        self.payload_cache = b"x" * config_dict["col_size"]

        # Pre-allocated connections (one per thread, indexed by thread number)
        self._connections: list[ThreadConnection] = []

        # Thread-local storage for connection index assignment
        self._thread_local = threading.local()
        self._next_thread_idx = 0
        self._thread_idx_lock = threading.Lock()

        # Histograms and counters (protected by lock, shared across threads)
        self.lock = threading.Lock()
        self.write_histogram = HdrHistogram(1, 60 * 60 * 1_000_000_000, 3)
        self.read_histogram = HdrHistogram(1, 60 * 60 * 1_000_000_000, 3)
        self.write_ops = 0
        self.read_ops = 0
        self.errors = 0

        # Send histograms frequently (at least 5 times per log interval or every 1s)
        # to ensure main process has data for aggregation before printing stats
        self.histogram_interval = min(1.0, config_dict.get("log_interval", 10) / 5.0)

        # Logger
        self.logger = logging.getLogger(f"worker-{worker_id}")

    def connect_all(self):
        """Pre-create connections for all threads before workload starts."""
        self.logger.debug(f"Creating {self.threads_per_worker} connections...")
        for i in range(self.threads_per_worker):
            conn = ThreadConnection(self.config_dict, self.col_count, self.payload_cache)
            conn.connect()
            self._connections.append(conn)
        self.logger.debug(f"Created {self.threads_per_worker} connections")

    def _get_connection(self) -> ThreadConnection:
        """Get the pre-allocated connection for the current thread."""
        thread_idx = getattr(self._thread_local, "idx", None)
        if thread_idx is None:
            # Assign a thread index on first access
            with self._thread_idx_lock:
                thread_idx = self._next_thread_idx
                self._next_thread_idx += 1
            self._thread_local.idx = thread_idx
        return self._connections[thread_idx]

    @staticmethod
    def _generate_key(idx: int) -> bytes:
        return str(idx).encode("utf-8")

    def _do_write(self, conn: ThreadConnection, key_int: int) -> int:
        """Execute write using given connection."""
        args = [self._generate_key(key_int)] + [conn.payload_cache] * conn.col_count
        t0 = time.perf_counter()
        conn.session.execute(conn.prepared_write, args)
        return int((time.perf_counter() - t0) * 1_000_000_000)

    def _do_read(self, conn: ThreadConnection, key_int: int) -> int:
        """Execute read using given connection."""
        args = (self._generate_key(key_int),)
        t0 = time.perf_counter()
        conn.session.execute(conn.prepared_read, args)
        return int((time.perf_counter() - t0) * 1_000_000_000)

    def process_task(self, task):
        """Process a single task using thread-local connection."""
        key, op_type = task
        conn = self._get_connection()

        try:
            if op_type == "write":
                latency = self._do_write(conn, key)
                with self.lock:
                    self.write_histogram.record_value(latency)
                    self.write_ops += 1
            else:
                latency = self._do_read(conn, key)
                with self.lock:
                    self.read_histogram.record_value(latency)
                    self.read_ops += 1
        except (OSError, RuntimeError, ValueError):
            with self.lock:
                self.errors += 1

    def send_histogram_data(self, results_queue):
        """Send histogram data to results queue."""
        with self.lock:
            if self.write_histogram.get_total_count() > 0:
                results_queue.put(
                    (
                        "histogram",
                        self.worker_id,
                        "write",
                        self.write_histogram.encode(),
                        self.write_ops,
                        self.errors,
                    )
                )
                self.write_histogram.reset()

            if self.read_histogram.get_total_count() > 0:
                results_queue.put(
                    (
                        "histogram",
                        self.worker_id,
                        "read",
                        self.read_histogram.encode(),
                        self.read_ops,
                        0,
                    )
                )
                self.read_histogram.reset()

            self.write_ops = 0
            self.read_ops = 0
            self.errors = 0

    def shutdown(self):
        """Shutdown all thread connections."""
        for conn in self._connections:
            try:
                conn.shutdown()
            except (OSError, RuntimeError):
                pass
        self._connections.clear()
        self.logger.debug("Shutdown complete")


def worker_process(
    worker_id: int,
    config_dict: dict,
    ops_queue: multiprocessing.Queue,
    results_queue: multiprocessing.Queue,
    start_barrier: multiprocessing.synchronize.Barrier,
    stop_event: multiprocessing.synchronize.Event,
):
    """
    Worker process that handles operations with per-thread connections.

    Each thread in the worker's thread pool has its own Cassandra connection,
    created upfront before the workload starts. This provides true connection
    isolation without latency impact from lazy initialization.

    Operations are received via ops_queue and results sent via results_queue.
    """
    # Setup logging for this worker
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(levelname)s  [Worker-{worker_id}] %(asctime)s %(filename)s:%(lineno)s - %(message)s",
    )

    ctx = WorkerContext(worker_id, config_dict)
    threads_per_worker = config_dict["threads_per_worker"]

    try:
        # Create all connections upfront before workload starts
        ctx.connect_all()

        # Use thread pool within this process
        executor = ThreadPoolExecutor(max_workers=threads_per_worker)

        # Signal ready (all connections established)
        start_barrier.wait()
        ctx.logger.debug("Started processing operations")

        try:
            next_histogram_send = time.perf_counter() + ctx.histogram_interval
            pending_futures = []

            while True:
                # Check if it's time to send histogram data
                if time.perf_counter() >= next_histogram_send:
                    ctx.send_histogram_data(results_queue)
                    next_histogram_send += ctx.histogram_interval

                # Check for immediate stop signal (duration limit reached)
                if stop_event.is_set():
                    # Cancel pending futures and don't wait
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                # Get task from queue
                try:
                    task = ops_queue.get(timeout=0.1)

                    if task == "graceful":  # Graceful stop - wait for pending (n=X mode)
                        # Wait for pending operations to complete
                        for f in pending_futures:
                            try:
                                f.result(timeout=30)
                            except (OSError, RuntimeError, ValueError, TimeoutError):
                                pass
                        executor.shutdown(wait=True)
                        break

                    if task is not None:
                        f = executor.submit(ctx.process_task, task)
                        pending_futures.append(f)

                        # Prune completed futures
                        if len(pending_futures) > threads_per_worker * 10:
                            pending_futures = [fut for fut in pending_futures if not fut.done()]
                except queue.Empty:
                    continue
        finally:
            # Ensure executor is shutdown if logic breaks out unexpectedly
            # (e.g. exceptions). If already shutdown, this is no-op.
            # We use wait=False here to assume if we crashed out, we want fast exit.
            # If we exited cleanly loop via 'break', we already called shutdown appropriately above
            # (or for None case, we explicitly called wait=False).
            # However, checking if it's shutdown is internal API.
            # Simplest is just to call it with wait=False as a safety net.
            # But wait: if we did graceful shutdown, we don't want to double call?
            # Double call is fine.
            # But if we did graceful, we waited. If we call wait=False now, it's fine.
            executor.shutdown(wait=False, cancel_futures=True)

        # Send final histogram data and signal completion BEFORE cluster shutdown
        # This allows stats collector to proceed without waiting for Cassandra cleanup
        ctx.send_histogram_data(results_queue)
        results_queue.put(("done", worker_id, None, None, 0, 0))

        # Now shutdown cluster (this may take time but doesn't block stats)
        ctx.shutdown()

    except (OSError, RuntimeError, ValueError, ConnectionError) as e:
        ctx.logger.error(f"Worker failed: {e}")
        results_queue.put(("error", worker_id, str(e), None, 0, 0))


class StatsCollectorContext:
    """Context holder for stats collector process state."""

    def __init__(self, config_dict: dict):
        self.config_dict = config_dict
        self.log_interval = config_dict.get("log_interval", 10)
        self.output_file = config_dict.get("output_file", "pystress.hdr")

        # Summary histograms - accumulated across all intervals for final summary
        self.write_summary_histogram = HdrHistogram(1, 60 * 60 * 1_000_000_000, 3)
        self.read_summary_histogram = HdrHistogram(1, 60 * 60 * 1_000_000_000, 3)

        # Interval histograms - reset after each interval output
        self.write_interval_histogram = HdrHistogram(1, 60 * 60 * 1_000_000_000, 3)
        self.read_interval_histogram = HdrHistogram(1, 60 * 60 * 1_000_000_000, 3)

        # Counters
        self.write_interval_ops = 0
        self.read_interval_ops = 0
        self.interval_errors = 0
        self.total_write_ops = 0
        self.total_read_ops = 0
        self.total_errors = 0

        # Timing
        self.start_time = None
        self.start_timestamp = None
        self.last_interval_time = 0
        self.stats_header_printed = False

        # HDR file writer
        self.hdr_file = None
        self.hdr_writer = None

        # Logger
        self.logger = logging.getLogger("stats-collector")

    def init_hdr_writer(self):
        """Initialize HDR histogram file writer."""
        try:
            self.hdr_file = open(self.output_file, "w")
            self.hdr_writer = TaggedHistogramLogWriter(self.hdr_file)
            self.hdr_writer.output_comment("Logging op latencies for Cassandra Stress")
            self.hdr_writer.output_log_format_version()
            self.hdr_writer.output_base_time(self.start_timestamp)
            self.hdr_writer.output_start_time(self.start_timestamp)
            self.hdr_writer.output_legend()
        except OSError as e:
            self.logger.error(f"Failed to open HDR file: {e}")

    def aggregate_result(self, result):
        """Aggregate a single worker result into histograms and counters.

        Returns True if this was a 'done' or 'error' message.
        """
        msg_type = result[0]
        if msg_type == "histogram":
            _, worker_id, op_type, encoded, ops_count, err_count = result
            temp_hist = HdrHistogram.decode(encoded)
            if op_type == "write":
                self.write_summary_histogram.add(temp_hist)
                self.write_interval_histogram.add(temp_hist)
                self.write_interval_ops += ops_count
                self.total_write_ops += ops_count
            else:
                self.read_summary_histogram.add(temp_hist)
                self.read_interval_histogram.add(temp_hist)
                self.read_interval_ops += ops_count
                self.total_read_ops += ops_count
            self.interval_errors += err_count
            self.total_errors += err_count
            return False
        elif msg_type == "done":
            return True
        elif msg_type == "error":
            _, worker_id, error_msg, _, _, _ = result
            self.logger.error(f"Worker {worker_id} error: {error_msg}")
            return True
        return False

    def output_interval_histograms(self, elapsed_time):
        """Output interval histograms to HDR file."""
        if not self.hdr_writer:
            return

        interval_start_ms = int(self.last_interval_time * 1000)
        interval_end_ms = int(elapsed_time * 1000)

        if self.write_interval_histogram.get_total_count() > 0:
            self.write_interval_histogram.set_tag("WRITE-st")
            self.write_interval_histogram.set_start_time_stamp(interval_start_ms)
            self.write_interval_histogram.set_end_time_stamp(interval_end_ms)
            self.hdr_writer.output_interval_histogram(self.write_interval_histogram)

        if self.read_interval_histogram.get_total_count() > 0:
            self.read_interval_histogram.set_tag("READ-st")
            self.read_interval_histogram.set_start_time_stamp(interval_start_ms)
            self.read_interval_histogram.set_end_time_stamp(interval_end_ms)
            self.hdr_writer.output_interval_histogram(self.read_interval_histogram)

    def print_interval_stats(self, elapsed_time):
        """Print interval statistics in cassandra-stress format."""
        total_interval_ops = self.write_interval_ops + self.read_interval_ops
        if total_interval_ops == 0:
            return

        interval_duration = elapsed_time - self.last_interval_time
        if interval_duration <= 0:
            return

        ops_per_sec = total_interval_ops / interval_duration
        total_ops = self.total_write_ops + self.total_read_ops
        interval_errors = self.interval_errors

        # Reset interval counters
        self.write_interval_ops = 0
        self.read_interval_ops = 0
        self.interval_errors = 0
        self.last_interval_time = elapsed_time

        # Extract histogram data
        write_count = self.write_interval_histogram.get_total_count()
        read_count = self.read_interval_histogram.get_total_count()

        if write_count > 0 and read_count > 0:
            total_count = write_count + read_count
            mean_ms = (
                (
                    self.write_interval_histogram.get_mean_value() * write_count
                    + self.read_interval_histogram.get_mean_value() * read_count
                )
                / total_count
                / 1_000_000.0
            )
            hist = self.write_interval_histogram if write_count >= read_count else self.read_interval_histogram
            median_ms = hist.get_value_at_percentile(50.0) / 1_000_000.0
            p95_ms = hist.get_value_at_percentile(95.0) / 1_000_000.0
            p99_ms = hist.get_value_at_percentile(99.0) / 1_000_000.0
            p999_ms = hist.get_value_at_percentile(99.9) / 1_000_000.0
            max_ms = (
                max(
                    self.write_interval_histogram.get_max_value(),
                    self.read_interval_histogram.get_max_value(),
                )
                / 1_000_000.0
            )
        elif write_count > 0:
            mean_ms = self.write_interval_histogram.get_mean_value() / 1_000_000.0
            median_ms = self.write_interval_histogram.get_value_at_percentile(50.0) / 1_000_000.0
            p95_ms = self.write_interval_histogram.get_value_at_percentile(95.0) / 1_000_000.0
            p99_ms = self.write_interval_histogram.get_value_at_percentile(99.0) / 1_000_000.0
            p999_ms = self.write_interval_histogram.get_value_at_percentile(99.9) / 1_000_000.0
            max_ms = self.write_interval_histogram.get_max_value() / 1_000_000.0
        elif read_count > 0:
            mean_ms = self.read_interval_histogram.get_mean_value() / 1_000_000.0
            median_ms = self.read_interval_histogram.get_value_at_percentile(50.0) / 1_000_000.0
            p95_ms = self.read_interval_histogram.get_value_at_percentile(95.0) / 1_000_000.0
            p99_ms = self.read_interval_histogram.get_value_at_percentile(99.0) / 1_000_000.0
            p999_ms = self.read_interval_histogram.get_value_at_percentile(99.9) / 1_000_000.0
            max_ms = self.read_interval_histogram.get_max_value() / 1_000_000.0
        else:
            mean_ms = median_ms = p95_ms = p99_ms = p999_ms = max_ms = 0.0

        # Reset interval histograms
        self.write_interval_histogram.reset()
        self.read_interval_histogram.reset()

        # Print stats
        if not self.stats_header_printed:
            print(
                f"{'total ops':>10}, {'op/s':>8}, {'mean':>6}, {'med':>7}, {'.95':>7}, "
                f"{'.99':>7}, {'.999':>7}, {'max':>7}, {'time':>6}, {'errors':>7}"
            )
            self.stats_header_printed = True

        print(
            f"{total_ops:>10}, {ops_per_sec:>8.0f}, {mean_ms:>6.1f}, {median_ms:>7.1f}, "
            f"{p95_ms:>7.1f}, {p99_ms:>7.1f}, {p999_ms:>7.1f}, {max_ms:>7.1f}, "
            f"{elapsed_time:>6.1f}, {interval_errors:>7}"
        )

    def save_hdr(self):
        """Close HDR histogram file."""
        if self.hdr_file:
            try:
                self.hdr_file.close()
                self.logger.info(f"HDR histogram saved to {self.output_file}")
            except OSError as e:
                self.logger.error(f"Failed to close HDR file: {e}")

    def get_summary_data(self):
        """Return summary data for the main process to print."""
        return {
            "total_write_ops": self.total_write_ops,
            "total_read_ops": self.total_read_ops,
            "total_errors": self.total_errors,
            "write_hist_encoded": self.write_summary_histogram.encode()
            if self.write_summary_histogram.get_total_count() > 0
            else None,
            "read_hist_encoded": self.read_summary_histogram.encode()
            if self.read_summary_histogram.get_total_count() > 0
            else None,
        }


def stats_collector_process(
    config_dict: dict,
    results_queue: multiprocessing.Queue,
    summary_queue: multiprocessing.Queue,
    start_barrier: multiprocessing.synchronize.Barrier,
):
    """
    Stats collector process that aggregates histograms and prints periodic statistics.

    Receives histogram data from workers via results_queue.
    Sends final summary data to main process via summary_queue.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s  [StatsCollector] %(asctime)s %(filename)s:%(lineno)s - %(message)s",
    )

    ctx = StatsCollectorContext(config_dict)

    start_barrier.wait()

    ctx.start_time = time.perf_counter()
    ctx.start_timestamp = time.time() * 1000

    # Re-capture start time to ignore process spawn overhead
    ctx.init_hdr_writer()

    next_interval_time = ctx.start_time + ctx.log_interval
    workers_running = config_dict["num_workers"]

    try:
        all_workers_joined = False
        while workers_running > 0 and not all_workers_joined:
            # Collect results with timeout
            try:
                result = results_queue.get(timeout=0.1)

                msg_type = result[0]
                if msg_type == "all_joined":
                    # Main process has joined all workers - exit even if not all "done" received
                    # This handles cases where workers were killed before sending "done"
                    all_workers_joined = True
                elif ctx.aggregate_result(result):
                    workers_running -= 1
            except queue.Empty:
                pass

            # Check if it's time to print stats
            if time.perf_counter() >= next_interval_time:
                # Use strict interval timing for reporting to avoid drift (e.g. print 1.0 instead of 1.003)
                report_time = next_interval_time
                elapsed = report_time - ctx.start_time

                ctx.output_interval_histograms(elapsed)
                ctx.print_interval_stats(elapsed)
                next_interval_time += ctx.log_interval

        # Final stats output
        elapsed = time.perf_counter() - ctx.start_time
        ctx.output_interval_histograms(elapsed)
        ctx.print_interval_stats(elapsed)

        ctx.save_hdr()

        # Send summary data to main process
        summary_queue.put(ctx.get_summary_data())

    except (OSError, RuntimeError, ValueError) as e:
        ctx.logger.error(f"Stats collector failed: {e}")
        summary_queue.put({"error": str(e)})


class ProcessPoolManager:
    """Manages worker processes and stats collector for parallel operation execution."""

    def __init__(self, config):
        self.config = config
        self.workers = []
        self.ops_queue = None
        self.results_queue = None
        self.summary_queue = None
        self.stats_process = None
        self.stop_event = multiprocessing.Event()

    def _config_to_dict(self):
        """Convert config to a picklable dict."""
        return {
            "nodes": self.config.nodes,
            "user": self.config.user,
            "password": self.config.password,
            "consistency_level": consistency_value_to_name(self.config.consistency_level),
            "keyspace_name": self.config.keyspace_name,
            "col_count": self.config.col_count,
            "col_size": self.config.col_size,
            "log_interval": self.config.log_interval,
            "output_file": self.config.output_file,
            "num_workers": self.config.processes,
            "threads_per_worker": self.config.threads_per_worker,
            "request_timeout": self.config.request_timeout,
        }

    def start_workers(self):
        """Spawn worker processes and stats collector."""
        self.results_queue = multiprocessing.Queue()
        self.summary_queue = multiprocessing.Queue()
        start_barrier = multiprocessing.Barrier(self.config.processes + 2)
        config_dict = self._config_to_dict()

        # Start stats collector process first
        self.stats_process = multiprocessing.Process(
            target=stats_collector_process,
            args=(
                config_dict,
                self.results_queue,
                self.summary_queue,
                start_barrier,
            ),
        )
        self.stats_process.start()

        # Bound the queue size to prevent infinite buffering (which causes long shutdown times)
        # Size proportional to the total number of threads to ensure backpressure works
        self.ops_queue = multiprocessing.Queue(maxsize=self.config.processes * self.config.threads_per_worker * 100)

        for i in range(self.config.processes):
            p = multiprocessing.Process(
                target=worker_process,
                args=(
                    i,
                    config_dict,
                    self.ops_queue,
                    self.results_queue,
                    start_barrier,
                    self.stop_event,
                ),
            )
            p.start()
            self.workers.append(p)

        # Wait for all workers to be ready
        logger.debug(f"Waiting for {self.config.processes} workers to initialize...")
        start_barrier.wait()
        logger.debug("All workers ready")

    def submit_operation(self, key: int, op_type: str, timeout=None):
        """Submit operation to workers."""
        self.ops_queue.put((key, op_type), timeout=timeout)

    def stop_workers(self, graceful: bool = False):
        """Signal workers to stop and wait for them to terminate.

        Args:
            graceful: If True, workers wait for pending operations (n=X mode).
                      If False, workers exit immediately (duration mode).
        """
        if graceful:
            # In fixed ops mode, wait for queue to drain before sending stop signals
            logger.info("Waiting for operations queue to drain...")
            while not self.ops_queue.empty():
                time.sleep(0.1)
        else:
            # Signal workers to stop
            self.stop_event.set()

        # Send one stop signal per worker to the shared queue
        # We still send None to wake up workers stuck in queue.get()
        stop_signal = "graceful" if graceful else None
        for _ in self.workers:
            try:
                self.ops_queue.put_nowait(stop_signal)
            except queue.Full:
                # If queue is full, worker is busy processing and will check stop_event soon
                pass

        # Wait for worker processes to terminate
        for p in self.workers:
            # Workers signal 'done' before cluster shutdown, so they should exit quickly
            p.join(timeout=5)
            if p.is_alive():
                # Worker is stuck in cluster.shutdown() - kill it, data is already collected
                p.kill()
                p.join(timeout=1)

        # Signal stats collector that all workers have been joined
        # This helps if any worker was killed before sending its "done" message
        if self.results_queue:
            self.results_queue.put(("all_joined", None, None, None, 0, 0))

        self.ops_queue.cancel_join_thread()

    def get_summary(self):
        """Get summary data from stats collector and cleanup the process."""
        if self.summary_queue is None:
            return None
        try:
            summary = self.summary_queue.get(timeout=30)
        except queue.Empty:
            logger.error("Timeout waiting for summary from stats collector")
            summary = None

        # Join the stats collector process after getting summary (or timeout)
        if self.stats_process:
            self.stats_process.join(timeout=5)
            if self.stats_process.is_alive():
                logger.warning("Stats collector process did not exit cleanly, killing it")
                self.stats_process.kill()
                self.stats_process.join(timeout=1)

        return summary


class CassandraStressPy:
    def __init__(self, config):
        self.config = config

    def setup_schema(self):
        """Connect to cluster, create keyspace and table, then disconnect.

        Workers create their own connections for actual workload execution.
        """
        logger.info(f"Connecting to {self.config.nodes} for schema setup...")

        cluster = create_cluster_connection(
            nodes=self.config.nodes,
            user=self.config.user,
            password=self.config.password,
            consistency_level=self.config.consistency_level,
            request_timeout=self.config.request_timeout,
        )
        session = cluster.connect()
        session.use_client_timestamp = False

        try:
            if self.config.replication_strategy == "NetworkTopologyStrategy":
                strategy = (
                    f"{{'class': 'NetworkTopologyStrategy', 'replication_factor': {self.config.replication_factor}}}"
                )
            else:
                strategy = f"{{'class': 'SimpleStrategy', 'replication_factor': {self.config.replication_factor}}}"

            keyspace_name = self.config.keyspace_name
            session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {strategy}")
            session.execute(f"USE {keyspace_name}")

            # Mimic standard1 table
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
            logger.info("Schema setup complete.")
        finally:
            cluster.shutdown()

    def _create_key_generator(self):
        """Create a key generator based on population distribution config."""
        if self.config.pop_dist == "gaussian":
            mean = self.config.pop_mean
            stdev = self.config.pop_stdev
            pop_min = self.config.pop_min
            pop_max = self.config.pop_max

            def key_gen():
                while True:
                    val = random.gauss(mean, stdev)
                    val = int(round(val))
                    val = max(pop_min, min(pop_max, val))
                    yield val

            return key_gen()

        if self.config.pop_dist == "uniform":

            def key_gen():
                while True:
                    yield random.randint(self.config.pop_min, self.config.pop_max)

            return key_gen()

        # Sequential (default)
        def key_gen():
            idx = self.config.pop_min
            while True:
                yield idx
                idx += 1
                if idx > self.config.pop_max:
                    idx = self.config.pop_min

        return key_gen()

    def run(self):
        try:
            self.setup_schema()
        except (OSError, RuntimeError) as e:
            logger.error(f"Schema setup failed; aborting run: {e}")
            return

        keys = self._create_key_generator()

        logger.info(
            f"Starting {self.config.command} workload: "
            f"{self.config.num_operations or 'unlimited'} ops, "
            f"{self.config.processes} worker(s) x {self.config.threads_per_worker} threads, "
            f"CL={self.config.consistency_level}, ColSize={self.config.col_size}, ColCount={self.config.col_count}"
        )

        current_key = None
        current_op = self.config.command
        is_mixed = current_op == "mixed"
        ratio_write = self.config.ratio_write / (self.config.ratio_write + self.config.ratio_read)

        throttle_delay = 1.0 / self.config.throttle if self.config.throttle else 0
        to_submit = self.config.num_operations if not self.config.duration else -1

        # Start worker processes and stats collector
        manager = ProcessPoolManager(self.config)
        manager.start_workers()

        next_op_time = start_time = time.perf_counter()
        end_time = start_time + self.config.duration if self.config.duration else float("inf")

        try:
            while True:
                now = time.perf_counter()

                # Check termination conditions
                if now >= end_time or to_submit == 0:
                    break

                # Rate limiting
                if throttle_delay > 0:
                    if now < next_op_time:
                        time.sleep(next_op_time - now)
                    next_op_time = next_op_time + throttle_delay

                # Get next key only if we successfully submitted the previous one
                if current_key is None:
                    try:
                        current_key = next(keys)
                    except StopIteration:
                        break

                    if is_mixed:
                        current_op = "write" if random.random() < ratio_write else "read"

                try:
                    manager.submit_operation(current_key, current_op, timeout=0.001)
                    to_submit -= 1
                    current_key = None
                except queue.Full:
                    # Queue is full, retry after short sleep
                    time.sleep(0.001)

            # Use graceful stop for n=X mode (wait for pending ops)
            # Use immediate stop for duration mode (exit fast)
            graceful = not self.config.duration

            # Signal workers to stop and wait for completion
            logger.debug("Stopping workers...")
            manager.stop_workers(graceful=graceful)

            # Get summary from stats collector
            summary = manager.get_summary()
        except Exception:
            # Ensure workers are stopped on error (immediate stop)
            manager.stop_workers(graceful=False)
            raise

        self.print_summary(summary, time.perf_counter() - start_time)

    @staticmethod
    def _get_histogram_stats(histogram):
        """Extract latency stats from histogram as dict (values in ms)."""
        if histogram.get_total_count() > 0:
            return {
                "mean": histogram.get_mean_value() / 1_000_000.0,
                "median": histogram.get_value_at_percentile(50.0) / 1_000_000.0,
                "p95": histogram.get_value_at_percentile(95.0) / 1_000_000.0,
                "p99": histogram.get_value_at_percentile(99.0) / 1_000_000.0,
                "p999": histogram.get_value_at_percentile(99.9) / 1_000_000.0,
                "max": histogram.get_max_value() / 1_000_000.0,
                "count": histogram.get_total_count(),
            }
        return {"mean": 0.0, "median": 0.0, "p95": 0.0, "p99": 0.0, "p999": 0.0, "max": 0.0, "count": 0}

    def print_summary(self, summary, duration):
        """Print final summary using data from stats collector."""
        if summary is None or "error" in summary:
            logger.error("No summary data available")
            return

        # Decode histograms if present
        write_hist = None
        read_hist = None
        if summary.get("write_hist_encoded"):
            write_hist = HdrHistogram.decode(summary["write_hist_encoded"])
        if summary.get("read_hist_encoded"):
            read_hist = HdrHistogram.decode(summary["read_hist_encoded"])

        total_write_ops = summary.get("total_write_ops", 0)
        total_read_ops = summary.get("total_read_ops", 0)

        # Format duration as HH:MM:SS
        hours, remainder = divmod(int(duration), 3600)
        minutes, seconds = divmod(remainder, 60)
        duration_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        print("\nResults:")

        if self.config.command == "write":
            self._print_op_summary("WRITE", write_hist, duration, total_write_ops)
        elif self.config.command == "read":
            self._print_op_summary("READ", read_hist, duration, total_read_ops)
        else:  # mixed
            self._print_mixed_summary(write_hist, read_hist, total_write_ops, total_read_ops, duration)

        print(f"Total operation time      : {duration_str}")

    @staticmethod
    def _print_op_summary(op_type, histogram, duration, ops):
        """Print summary for a single operation type."""
        op_rate = ops / duration if duration > 0 else 0

        if histogram and histogram.get_total_count() > 0:
            latency_mean = histogram.get_mean_value() / 1_000_000.0
            latency_median = histogram.get_value_at_percentile(50.0) / 1_000_000.0
            latency_p95 = histogram.get_value_at_percentile(95.0) / 1_000_000.0
            latency_p99 = histogram.get_value_at_percentile(99.0) / 1_000_000.0
            latency_p999 = histogram.get_value_at_percentile(99.9) / 1_000_000.0
            latency_max = histogram.get_max_value() / 1_000_000.0
        else:
            latency_mean = latency_median = latency_p95 = latency_p99 = latency_p999 = latency_max = 0.0

        print(f"""\
Op rate                   : {op_rate:,.0f} op/s  [{op_type}: {op_rate:,.0f} op/s]
Latency mean              :  {latency_mean:.1f} ms [{op_type}: {latency_mean:.1f} ms]
Latency median            :  {latency_median:.1f} ms [{op_type}: {latency_median:.1f} ms]
Latency 95th percentile   :  {latency_p95:.1f} ms [{op_type}: {latency_p95:.1f} ms]
Latency 99th percentile   :  {latency_p99:.1f} ms [{op_type}: {latency_p99:.1f} ms]
Latency 99.9th percentile :  {latency_p999:.1f} ms [{op_type}: {latency_p999:.1f} ms]
Latency max               :  {latency_max:.1f} ms [{op_type}: {latency_max:.1f} ms]""")

    def _print_mixed_summary(self, write_hist, read_hist, total_write_ops, total_read_ops, duration):
        """Print combined summary for mixed workload matching cassandra-stress format."""
        total_ops = total_write_ops + total_read_ops
        total_rate = total_ops / duration if duration > 0 else 0
        write_rate = total_write_ops / duration if duration > 0 else 0
        read_rate = total_read_ops / duration if duration > 0 else 0

        w = (
            self._get_histogram_stats(write_hist)
            if write_hist
            else {"mean": 0.0, "median": 0.0, "p95": 0.0, "p99": 0.0, "p999": 0.0, "max": 0.0, "count": 0}
        )
        r = (
            self._get_histogram_stats(read_hist)
            if read_hist
            else {"mean": 0.0, "median": 0.0, "p95": 0.0, "p99": 0.0, "p999": 0.0, "max": 0.0, "count": 0}
        )
        total_count = w["count"] + r["count"]

        # Calculate combined latencies (weighted average)
        if total_count > 0:
            combined = {
                "mean": (w["mean"] * w["count"] + r["mean"] * r["count"]) / total_count,
                "median": (w["median"] * w["count"] + r["median"] * r["count"]) / total_count,
                "p95": (w["p95"] * w["count"] + r["p95"] * r["count"]) / total_count,
                "p99": (w["p99"] * w["count"] + r["p99"] * r["count"]) / total_count,
                "p999": (w["p999"] * w["count"] + r["p999"] * r["count"]) / total_count,
                "max": max(w["max"], r["max"]),
            }
        else:
            combined = {"mean": 0.0, "median": 0.0, "p95": 0.0, "p99": 0.0, "p999": 0.0, "max": 0.0}

        print(f"""\
Op rate                   : {total_rate:,.0f} op/s  [READ: {read_rate:,.0f} op/s, WRITE: {write_rate:,.0f} op/s]
Latency mean              :  {combined["mean"]:.1f} ms [READ: {r["mean"]:.1f} ms, WRITE: {w["mean"]:.1f} ms]
Latency median            :  {combined["median"]:.1f} ms [READ: {r["median"]:.1f} ms, WRITE: {w["median"]:.1f} ms]
Latency 95th percentile   :  {combined["p95"]:.1f} ms [READ: {r["p95"]:.1f} ms, WRITE: {w["p95"]:.1f} ms]
Latency 99th percentile   :  {combined["p99"]:.1f} ms [READ: {r["p99"]:.1f} ms, WRITE: {w["p99"]:.1f} ms]
Latency 99.9th percentile :  {combined["p999"]:.1f} ms [READ: {r["p999"]:.1f} ms, WRITE: {w["p999"]:.1f} ms]
Latency max               :  {combined["max"]:,.1f} ms [READ: {r["max"]:,.1f} ms, WRITE: {w["max"]:,.1f} ms]""")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s  [%(name)s] %(asctime)s %(filename)s:%(lineno)s - %(message)s"
    )

    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    config = parse_cli_args(sys.argv[1:])
    tool = CassandraStressPy(config)
    tool.run()

    print("\nEND")
