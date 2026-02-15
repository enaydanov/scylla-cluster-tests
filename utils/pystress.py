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
from typing import TypedDict
from datetime import datetime

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
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
        self.pop_mean = None
        self.pop_stdev = None
        self.output_file = "pystress.hdr"
        self.user = None
        self.password = None
        self.connections_per_host = None
        self.duration = None
        self.throttle = None
        self.log_interval = 10  # Default interval for periodic stats (seconds)
        self.ratio_write = 1  # Write ratio for mixed workload
        self.ratio_read = 1  # Read ratio for mixed workload


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
                    config.col_size_dist = dist
                    if dist["type"] == "FIXED":
                        config.col_size = dist["value"]
                    elif dist["type"] == "UNIFORM":
                        config.col_size = dist["max"]
            if "n=" in part:
                _, val = part.split("n=", 1)
                dist = parse_distribution(val)
                if dist:
                    config.col_count_dist = dist
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
        self.histogram_lock = threading.Lock()

        # Separate histograms for READ and WRITE operations (1 nanosec to 1 hour, 3 sig figs)
        # Summary histograms - accumulated across all intervals for final summary
        self.write_summary_histogram = HdrHistogram(1, 60 * 60 * 1_000_000_000, 3)
        self.read_summary_histogram = HdrHistogram(1, 60 * 60 * 1_000_000_000, 3)

        # Interval histograms - reset after each interval output
        self.write_interval_histogram = HdrHistogram(1, 60 * 60 * 1_000_000_000, 3)
        self.read_interval_histogram = HdrHistogram(1, 60 * 60 * 1_000_000_000, 3)

        # Interval statistics tracking
        self.write_interval_ops = 0
        self.read_interval_ops = 0
        self.interval_errors = 0
        self.total_write_ops = 0
        self.total_read_ops = 0
        self.total_errors = 0
        self.stats_lock = threading.Lock()
        self.last_interval_time = 0
        self.stats_header_printed = False

        # HDR file writer (initialized in run())
        self.hdr_file = None
        self.hdr_writer = None
        self.start_timestamp = None

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

    def _record_latency(self, op_type, latency_nanos):
        """Record latency value to appropriate histograms."""
        with self.histogram_lock:
            if op_type == "write":
                self.write_summary_histogram.record_value(latency_nanos)
                self.write_interval_histogram.record_value(latency_nanos)
            else:
                self.read_summary_histogram.record_value(latency_nanos)
                self.read_interval_histogram.record_value(latency_nanos)
        with self.stats_lock:
            if op_type == "write":
                self.write_interval_ops += 1
                self.total_write_ops += 1
            else:
                self.read_interval_ops += 1
                self.total_read_ops += 1

    def _record_error(self):
        """Record an operation error."""
        with self.stats_lock:
            self.interval_errors += 1
            self.total_errors += 1

    def _run_workload(self, start_time, threads):  # noqa: PLR0914, PLR0915
        """Execute the main workload loop."""
        fixed_payloads = None
        if self.config.col_size_dist["type"] == "FIXED" and self.config.col_count_dist["type"] == "FIXED":
            fixed_payloads = [self._payload_cache for _ in range(self.config.col_count)]

        sessions_count = len(self.sessions)
        session_lock = threading.Lock()
        session_idx = [0]  # Use list for nonlocal mutation
        keys = self._create_key_generator()

        end_time = start_time + self.config.duration if self.config.duration else None
        throttle_delay = 1.0 / self.config.throttle if self.config.throttle else 0
        self.last_interval_time = 0

        def on_done(f, op_type):
            try:
                self._record_latency(op_type, f.result())
            except (OSError, RuntimeError, ValueError) as e:
                logger.error(f"Op failed: {e}")
                self._record_error()

        def pick_session():
            with session_lock:
                idx = session_idx[0]
                session_idx[0] = (idx + 1) % sessions_count
            return idx

        def do_write(key_int, sidx):
            payloads = fixed_payloads if fixed_payloads else self._generate_payloads()
            args = [self._generate_key(key_int)] + payloads
            t0 = time.perf_counter()
            self.sessions[sidx].execute(self.prepared_writes[sidx], args)
            return int((time.perf_counter() - t0) * 1_000_000_000)

        def do_read(key_int, sidx):
            t0 = time.perf_counter()
            self.sessions[sidx].execute(self.prepared_reads[sidx], (self._generate_key(key_int),))
            return int((time.perf_counter() - t0) * 1_000_000_000)

        is_mixed = self.config.command == "mixed"
        default_task = do_write if self.config.command == "write" else do_read
        ratio_write = self.config.ratio_write / (self.config.ratio_write + self.config.ratio_read)

        return self._execute_operations(
            threads,
            keys,
            end_time,
            throttle_delay,
            start_time,
            is_mixed,
            default_task,
            do_write,
            do_read,
            ratio_write,
            pick_session,
            on_done,
        )

    def _execute_operations(
        self,
        threads,
        keys,
        end_time,
        throttle_delay,
        start_time,
        is_mixed,
        default_task,
        do_write,
        do_read,
        ratio_write,
        pick_session,
        on_done,
    ):  # noqa: PLR0913
        """Execute the operation submission loop."""
        next_op_time = start_time
        next_interval_time = start_time + self.config.log_interval
        max_pending = threads * 50

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = []
            submitted = 0

            while True:
                now = time.perf_counter()
                if end_time and now >= end_time:
                    break
                if not self.config.duration and submitted >= self.config.num_operations:
                    break

                if throttle_delay > 0:
                    if now < next_op_time:
                        time.sleep(next_op_time - now)
                    next_op_time = max(next_op_time + throttle_delay, now + throttle_delay)

                try:
                    k = next(keys)
                except StopIteration:
                    break

                idx = pick_session()
                if is_mixed:
                    op_type = "write" if random.random() < ratio_write else "read"
                    task = do_write if op_type == "write" else do_read
                else:
                    op_type = self.config.command
                    task = default_task

                f = executor.submit(task, k, idx)
                f.add_done_callback(lambda fut, ot=op_type: on_done(fut, ot))
                futures.append(f)
                submitted += 1

                if len(futures) >= max_pending:
                    futures = [fut for fut in futures if not fut.done()]
                    while len(futures) >= max_pending:
                        time.sleep(0.005)
                        futures = [fut for fut in futures if not fut.done()]

                if time.perf_counter() >= next_interval_time:
                    elapsed = time.perf_counter() - start_time
                    self._output_interval_histograms(elapsed)
                    self.print_interval_stats(elapsed)
                    next_interval_time = time.perf_counter() + self.config.log_interval

            logger.info("Waiting for pending tasks...")
            for f in futures:
                try:
                    f.result()
                except (OSError, RuntimeError, ValueError):
                    pass

        return submitted

    def run(self):
        self.connect()
        try:
            self.setup_schema()
        except (OSError, RuntimeError) as e:
            logger.error(f"Schema setup failed; aborting run: {e}")
            return

        threads = self.config.threads
        logger.info(
            f"Starting {self.config.command} workload: {self.config.num_operations} ops, {threads} threads, "
            f"CL={self.config.consistency_level}, ColSize={self.config.col_size}, ColCount={self.config.col_count}"
        )

        start_time = time.perf_counter()
        self.start_timestamp = time.time() * 1000  # Epoch milliseconds for HDR file header
        self._init_hdr_writer()

        submitted = self._run_workload(start_time, threads)

        total_duration = time.perf_counter() - start_time
        self._output_interval_histograms(total_duration)
        self.print_interval_stats(total_duration)

        self.save_hdr()
        self.print_summary(total_duration, submitted)
        for cluster in self.clusters:
            cluster.shutdown()

    def _init_hdr_writer(self):
        """Initialize HDR histogram file writer with headers."""
        try:
            self.hdr_file = open(self.config.output_file, "w")
            self.hdr_writer = TaggedHistogramLogWriter(self.hdr_file)

            # Match cassandra-stress header format
            self.hdr_writer.output_comment("Logging op latencies for Cassandra Stress")
            self.hdr_writer.output_log_format_version()  # 1.2 (Python library limitation)
            self.hdr_writer.output_base_time(self.start_timestamp)
            self.hdr_writer.output_start_time(self.start_timestamp)
            self.hdr_writer.output_legend()
        except OSError as e:
            logger.error(f"Failed to initialize HDR file: {e}")
            self.hdr_file = None
            self.hdr_writer = None

    def _output_interval_histograms(self, elapsed_time):
        """Output interval histograms to HDR file."""
        if not self.hdr_writer:
            return

        with self.histogram_lock:
            # Calculate timestamps for this interval (in milliseconds)
            interval_start_ms = int(self.last_interval_time * 1000)
            interval_end_ms = int(elapsed_time * 1000)

            # Output WRITE histogram if has data
            if self.write_interval_histogram.get_total_count() > 0:
                self.write_interval_histogram.set_tag("WRITE-st")
                self.write_interval_histogram.set_start_time_stamp(interval_start_ms)
                self.write_interval_histogram.set_end_time_stamp(interval_end_ms)
                self.hdr_writer.output_interval_histogram(self.write_interval_histogram)

            # Output READ histogram if has data
            if self.read_interval_histogram.get_total_count() > 0:
                self.read_interval_histogram.set_tag("READ-st")
                self.read_interval_histogram.set_start_time_stamp(interval_start_ms)
                self.read_interval_histogram.set_end_time_stamp(interval_end_ms)
                self.hdr_writer.output_interval_histogram(self.read_interval_histogram)

    def save_hdr(self):
        """Close HDR histogram file."""
        if self.hdr_file:
            try:
                self.hdr_file.close()
                logger.info(f"HDR histogram saved to {self.config.output_file}")
            except OSError as e:
                logger.error(f"Failed to close HDR file: {e}")

    def print_interval_stats(self, elapsed_time):
        """Print interval statistics in cassandra-stress format."""
        with self.stats_lock:
            total_interval_ops = self.write_interval_ops + self.read_interval_ops
            if total_interval_ops == 0:
                return

            # Calculate ops/s for this interval
            interval_duration = elapsed_time - self.last_interval_time
            if interval_duration <= 0:
                return

            ops_per_sec = total_interval_ops / interval_duration

            # Combine read and write histograms for console stats
            # We need to get combined stats from both histograms
            with self.histogram_lock:
                write_count = self.write_interval_histogram.get_total_count()
                read_count = self.read_interval_histogram.get_total_count()

                if write_count > 0 and read_count > 0:
                    # Combine stats weighted by count
                    total_count = write_count + read_count
                    mean_ms = (
                        (
                            self.write_interval_histogram.get_mean_value() * write_count
                            + self.read_interval_histogram.get_mean_value() * read_count
                        )
                        / total_count
                        / 1_000_000.0
                    )
                    # For percentiles, use the histogram with more samples (approximate)
                    if write_count >= read_count:
                        hist = self.write_interval_histogram
                    else:
                        hist = self.read_interval_histogram
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
                else:
                    mean_ms = self.read_interval_histogram.get_mean_value() / 1_000_000.0
                    median_ms = self.read_interval_histogram.get_value_at_percentile(50.0) / 1_000_000.0
                    p95_ms = self.read_interval_histogram.get_value_at_percentile(95.0) / 1_000_000.0
                    p99_ms = self.read_interval_histogram.get_value_at_percentile(99.0) / 1_000_000.0
                    p999_ms = self.read_interval_histogram.get_value_at_percentile(99.9) / 1_000_000.0
                    max_ms = self.read_interval_histogram.get_max_value() / 1_000_000.0

                # Reset interval histograms
                self.write_interval_histogram.reset()
                self.read_interval_histogram.reset()

            # Print header once
            if not self.stats_header_printed:
                print(
                    f"{'total ops':>10}, {'op/s':>8}, {'mean':>6}, {'med':>7}, {'.95':>7}, "
                    f"{'.99':>7}, {'.999':>7}, {'max':>7}, {'time':>6}, {'errors':>7}"
                )
                self.stats_header_printed = True

            total_ops = self.total_write_ops + self.total_read_ops

            # Print interval stats
            print(
                f"{total_ops:>10}, {ops_per_sec:>8.0f}, {mean_ms:>6.1f}, {median_ms:>7.1f}, "
                f"{p95_ms:>7.1f}, {p99_ms:>7.1f}, {p999_ms:>7.1f}, {max_ms:>7.1f}, "
                f"{elapsed_time:>6.1f}, {self.interval_errors:>7}"
            )

            # Reset interval counters
            self.write_interval_ops = 0
            self.read_interval_ops = 0
            self.interval_errors = 0
            self.last_interval_time = elapsed_time

    def _print_op_summary(self, op_type, histogram, duration, ops):
        """Print summary for a single operation type."""
        op_rate = ops / duration if duration > 0 else 0

        # Convert nanoseconds to milliseconds for display
        if histogram.get_total_count() > 0:
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

    def _get_histogram_stats(self, histogram):
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

    def _print_mixed_summary(self, duration):
        """Print combined summary for mixed workload matching cassandra-stress format."""
        total_ops = self.total_write_ops + self.total_read_ops
        total_rate = total_ops / duration if duration > 0 else 0
        write_rate = self.total_write_ops / duration if duration > 0 else 0
        read_rate = self.total_read_ops / duration if duration > 0 else 0

        w = self._get_histogram_stats(self.write_summary_histogram)
        r = self._get_histogram_stats(self.read_summary_histogram)
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

        # Print in cassandra-stress format
        print(f"""\
Op rate                   : {total_rate:,.0f} op/s  [READ: {read_rate:,.0f} op/s, WRITE: {write_rate:,.0f} op/s]
Latency mean              :  {combined["mean"]:.1f} ms [READ: {r["mean"]:.1f} ms, WRITE: {w["mean"]:.1f} ms]
Latency median            :  {combined["median"]:.1f} ms [READ: {r["median"]:.1f} ms, WRITE: {w["median"]:.1f} ms]
Latency 95th percentile   :  {combined["p95"]:.1f} ms [READ: {r["p95"]:.1f} ms, WRITE: {w["p95"]:.1f} ms]
Latency 99th percentile   :  {combined["p99"]:.1f} ms [READ: {r["p99"]:.1f} ms, WRITE: {w["p99"]:.1f} ms]
Latency 99.9th percentile :  {combined["p999"]:.1f} ms [READ: {r["p999"]:.1f} ms, WRITE: {w["p999"]:.1f} ms]
Latency max               :  {combined["max"]:,.1f} ms [READ: {r["max"]:,.1f} ms, WRITE: {w["max"]:,.1f} ms]""")

    def print_summary(self, duration, ops):
        # Format duration as HH:MM:SS
        hours, remainder = divmod(int(duration), 3600)
        minutes, seconds = divmod(remainder, 60)
        duration_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        print("\nResults:")

        if self.config.command == "write":
            self._print_op_summary("WRITE", self.write_summary_histogram, duration, self.total_write_ops)
        elif self.config.command == "read":
            self._print_op_summary("READ", self.read_summary_histogram, duration, self.total_read_ops)
        else:  # mixed
            self._print_mixed_summary(duration)

        print(f"Total operation time      : {duration_str}")


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
