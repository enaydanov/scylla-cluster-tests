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

"""Tests for sdcm.sct_config's '!auto_split' YAML tag feature (Splittable, SctYamlLoader)."""

import yaml
import pytest

from sdcm.sct_config import Splittable, SctYamlLoader


# ---------------------------------------------------------------------------
# Golden tests: real (unsplit) commands from
# test-cases/performance/perf-regression-predefined-throughput-steps.yaml, split by 4,
# must exactly match the real manually-split commands already in that file.
# ---------------------------------------------------------------------------


def test_split_prepare_write_cmd_matches_real_example():
    """n=-based command, no $placeholders."""
    unsplit = (
        "cassandra-stress write  cl=ALL n=650000004 -schema 'replication(strategy=NetworkTopologyStrategy,"
        "replication_factor=3)' -mode connectionsPerHost=8 cql3 native -rate threads=50 throttle=37500/s "
        "-col 'size=FIXED(1024) n=FIXED(1)'  -pop seq=1..650000004"
    )
    expected = [
        "cassandra-stress write  cl=ALL n=162500001 -schema 'replication(strategy=NetworkTopologyStrategy,"
        "replication_factor=3)' -mode connectionsPerHost=8 cql3 native -rate threads=50 throttle=37500/s "
        "-col 'size=FIXED(1024) n=FIXED(1)'  -pop seq=1..162500001",
        "cassandra-stress write  cl=ALL n=162500001 -schema 'replication(strategy=NetworkTopologyStrategy,"
        "replication_factor=3)' -mode connectionsPerHost=8 cql3 native -rate threads=50 throttle=37500/s "
        "-col 'size=FIXED(1024) n=FIXED(1)'  -pop seq=162500002..325000002",
        "cassandra-stress write  cl=ALL n=162500001 -schema 'replication(strategy=NetworkTopologyStrategy,"
        "replication_factor=3)' -mode connectionsPerHost=8 cql3 native -rate threads=50 throttle=37500/s "
        "-col 'size=FIXED(1024) n=FIXED(1)'  -pop seq=325000003..487500003",
        "cassandra-stress write  cl=ALL n=162500001 -schema 'replication(strategy=NetworkTopologyStrategy,"
        "replication_factor=3)' -mode connectionsPerHost=8 cql3 native -rate threads=50 throttle=37500/s "
        "-col 'size=FIXED(1024) n=FIXED(1)'  -pop seq=487500004..650000004",
    ]
    assert Splittable(value=unsplit).split_stress_cmd(4) == expected


def test_split_stress_cmd_w_matches_real_example():
    """n=-based command with $placeholders that must be preserved untouched."""
    unsplit = (
        "cassandra-stress write no-warmup cl=QUORUM n=1610612736 -schema 'replication(strategy=NetworkTopologyStrategy,"
        "replication_factor=3)' -mode connectionsPerHost=$connections_per_host cql3 native -rate "
        "'threads=$threads $throttle' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=1..1610612736"
    )
    expected = [
        "cassandra-stress write no-warmup cl=QUORUM n=402653184 -schema 'replication(strategy=NetworkTopologyStrategy,"
        "replication_factor=3)' -mode connectionsPerHost=$connections_per_host cql3 native -rate "
        "'threads=$threads $throttle' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=1..402653184",
        "cassandra-stress write no-warmup cl=QUORUM n=402653184 -schema 'replication(strategy=NetworkTopologyStrategy,"
        "replication_factor=3)' -mode connectionsPerHost=$connections_per_host cql3 native -rate "
        "'threads=$threads $throttle' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=402653185..805306368",
        "cassandra-stress write no-warmup cl=QUORUM n=402653184 -schema 'replication(strategy=NetworkTopologyStrategy,"
        "replication_factor=3)' -mode connectionsPerHost=$connections_per_host cql3 native -rate "
        "'threads=$threads $throttle' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=805306369..1207959552",
        "cassandra-stress write no-warmup cl=QUORUM n=402653184 -schema 'replication(strategy=NetworkTopologyStrategy,"
        "replication_factor=3)' -mode connectionsPerHost=$connections_per_host cql3 native -rate "
        "'threads=$threads $throttle' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=1207959553..1610612736",
    ]
    assert Splittable(value=unsplit).split_stress_cmd(4) == expected


def test_split_stress_cmd_r_matches_real_example():
    """duration=-based command (no top-level n=) - only the -pop seq= range is split."""
    unsplit = (
        "cassandra-stress read no-warmup  cl=QUORUM duration=$duration -mode connectionsPerHost=$connections_per_host "
        "cql3 native -rate 'threads=$threads $throttle' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=1..20000000"
    )
    expected = [
        "cassandra-stress read no-warmup  cl=QUORUM duration=$duration -mode connectionsPerHost=$connections_per_host "
        "cql3 native -rate 'threads=$threads $throttle' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=1..5000000",
        "cassandra-stress read no-warmup  cl=QUORUM duration=$duration -mode connectionsPerHost=$connections_per_host "
        "cql3 native -rate 'threads=$threads $throttle' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=5000001..10000000",
        "cassandra-stress read no-warmup  cl=QUORUM duration=$duration -mode connectionsPerHost=$connections_per_host "
        "cql3 native -rate 'threads=$threads $throttle' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=10000001..15000000",
        "cassandra-stress read no-warmup  cl=QUORUM duration=$duration -mode connectionsPerHost=$connections_per_host "
        "cql3 native -rate 'threads=$threads $throttle' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=15000001..20000000",
    ]
    assert Splittable(value=unsplit).split_stress_cmd(4) == expected


def test_split_stress_cmd_read_disk_matches_real_example():
    """duration=-based command covering the full dataset range (650M, same numbers as prepare_write_cmd)."""
    unsplit = (
        "cassandra-stress read no-warmup  cl=QUORUM duration=$duration -mode connectionsPerHost=$connections_per_host "
        "cql3 native -rate 'threads=$threads $throttle' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=1..650000004"
    )
    expected = [
        "cassandra-stress read no-warmup  cl=QUORUM duration=$duration -mode connectionsPerHost=$connections_per_host "
        "cql3 native -rate 'threads=$threads $throttle' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=1..162500001",
        "cassandra-stress read no-warmup  cl=QUORUM duration=$duration -mode connectionsPerHost=$connections_per_host "
        "cql3 native -rate 'threads=$threads $throttle' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=162500002..325000002",
        "cassandra-stress read no-warmup  cl=QUORUM duration=$duration -mode connectionsPerHost=$connections_per_host "
        "cql3 native -rate 'threads=$threads $throttle' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=325000003..487500003",
        "cassandra-stress read no-warmup  cl=QUORUM duration=$duration -mode connectionsPerHost=$connections_per_host "
        "cql3 native -rate 'threads=$threads $throttle' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=487500004..650000004",
    ]
    assert Splittable(value=unsplit).split_stress_cmd(4) == expected


def test_split_stress_cmd_cache_warmup_matches_real_example():
    """n=-based command with $placeholders."""
    unsplit = (
        "cassandra-stress read  cl=ALL n=20000000 -mode connectionsPerHost=$connections_per_host cql3 native "
        "-rate 'threads=$threads' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=1..20000000"
    )
    expected = [
        "cassandra-stress read  cl=ALL n=5000000 -mode connectionsPerHost=$connections_per_host cql3 native "
        "-rate 'threads=$threads' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=1..5000000",
        "cassandra-stress read  cl=ALL n=5000000 -mode connectionsPerHost=$connections_per_host cql3 native "
        "-rate 'threads=$threads' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=5000001..10000000",
        "cassandra-stress read  cl=ALL n=5000000 -mode connectionsPerHost=$connections_per_host cql3 native "
        "-rate 'threads=$threads' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=10000001..15000000",
        "cassandra-stress read  cl=ALL n=5000000 -mode connectionsPerHost=$connections_per_host cql3 native "
        "-rate 'threads=$threads' -col 'size=FIXED(1024) n=FIXED(1)' -pop seq=15000001..20000000",
    ]
    assert Splittable(value=unsplit).split_stress_cmd(4) == expected


# ---------------------------------------------------------------------------
# Splittable construction validates the '-pop seq=X..Y' range up front.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "stress_cmd,expected_splittable",
    [
        pytest.param("cassandra-stress write cl=ALL n=100 -pop seq=1..100", True, id="seq_range"),
        pytest.param(
            "cassandra-stress write no-warmup cl=QUORUM duration=2850m -pop 'dist=gauss(1..650000000,325000000,9750000)' ",
            False,
            id="dist_gauss_not_splittable",
        ),
        pytest.param("cassandra-stress write cl=ALL n=100 -rate threads=50", False, id="no_pop_at_all"),
        pytest.param("cassandra-stress user profile=/tmp/x.yaml ops'(insert=1)'", False, id="user_profile_no_pop"),
    ],
)
def test_splittable_construction_validates_pop_seq_range(stress_cmd, expected_splittable):
    if expected_splittable:
        Splittable(value=stress_cmd)  # must not raise
    else:
        with pytest.raises(ValueError, match="no splittable"):
            Splittable(value=stress_cmd)


# ---------------------------------------------------------------------------
# Remainder handling: last chunk absorbs any leftover.
# ---------------------------------------------------------------------------


def test_split_stress_cmd_remainder_goes_to_last_chunk():
    """total=10, N=3 -> chunks of size 3,3,4 (not silently dropping the 10th item)."""
    result = Splittable(value="cassandra-stress write cl=ALL n=10 -pop seq=1..10").split_stress_cmd(3)
    assert result == [
        "cassandra-stress write cl=ALL n=3 -pop seq=1..3",
        "cassandra-stress write cl=ALL n=3 -pop seq=4..6",
        "cassandra-stress write cl=ALL n=4 -pop seq=7..10",
    ]


def test_split_stress_cmd_covers_full_range_with_no_gaps_or_overlaps():
    """Regardless of remainder, chunk boundaries must be contiguous and non-overlapping."""
    result = Splittable(value="cassandra-stress write cl=ALL n=17 -pop seq=1..17").split_stress_cmd(5)
    ranges = []
    for cmd in result:
        seq_part = cmd.split("seq=")[1]
        start, end = seq_part.split("..")
        ranges.append((int(start), int(end)))
    # contiguous: each range starts exactly where the previous one ended + 1
    for (_, prev_end), (next_start, _) in zip(ranges, ranges[1:]):
        assert next_start == prev_end + 1
    assert ranges[0][0] == 1
    assert ranges[-1][1] == 17


# ---------------------------------------------------------------------------
# Edge cases / error handling
# ---------------------------------------------------------------------------


def test_split_stress_cmd_no_pop_seq_raises():
    """No '-pop seq=X..Y' range at all - Splittable rejects it at construction time."""
    with pytest.raises(ValueError, match="no splittable"):
        Splittable(value="cassandra-stress write cl=ALL n=100 -rate threads=50")


def test_split_stress_cmd_dist_gauss_raises():
    """dist=gauss(...) isn't a splittable seq= range - rejected at construction time."""
    with pytest.raises(ValueError, match="no splittable"):
        Splittable(value="cassandra-stress write duration=10m -pop 'dist=gauss(1..100,50,10)'")


def test_split_stress_cmd_zero_loaders_raises():
    with pytest.raises(ValueError, match="num_parts"):
        Splittable(value="cassandra-stress write cl=ALL n=100 -pop seq=1..100").split_stress_cmd(0)


def test_split_stress_cmd_single_loader_is_identity():
    """Splitting by 1 loader returns the range/n= unchanged (trivial case)."""
    cmd = "cassandra-stress write cl=ALL n=100 -pop seq=1..100"
    result = Splittable(value=cmd).split_stress_cmd(1)
    assert result == [cmd]


def test_split_stress_cmd_no_top_level_n_leaves_col_n_fixed_untouched():
    """The '-col ... n=FIXED(k)' column-count n= must never be confused with the
    top-level operation-count n= (which may be entirely absent, e.g. duration=-based)."""
    cmd = "cassandra-stress read duration=10m -col 'size=FIXED(1024) n=FIXED(8)' -pop seq=1..8"
    result = Splittable(value=cmd).split_stress_cmd(2)
    for piece in result:
        assert "n=FIXED(8)" in piece


# ---------------------------------------------------------------------------
# SctYamlLoader / Splittable  ('!auto_split' YAML tag machinery)
# ---------------------------------------------------------------------------


def test_sct_yaml_loader_parses_auto_split_tag():
    parsed = yaml.load(
        "prepare_write_cmd: !auto_split 'cassandra-stress write cl=ALL n=100 -pop seq=1..100'",
        Loader=SctYamlLoader,
    )
    assert isinstance(parsed["prepare_write_cmd"], Splittable)
    assert parsed["prepare_write_cmd"].value == "cassandra-stress write cl=ALL n=100 -pop seq=1..100"


def test_sct_yaml_loader_parses_multiple_tagged_and_untagged_keys():
    parsed = yaml.load(
        """
        prepare_write_cmd: !auto_split "cassandra-stress cmd1 -pop seq=1..100"
        stress_cmd_w: !auto_split "cassandra-stress cmd2 -pop seq=1..200"
        stress_cmd_r: "cmd3 -pop seq=1..300"
        n_loaders: 4
        """,
        Loader=SctYamlLoader,
    )
    assert isinstance(parsed["prepare_write_cmd"], Splittable)
    assert parsed["prepare_write_cmd"].value == "cassandra-stress cmd1 -pop seq=1..100"
    assert isinstance(parsed["stress_cmd_w"], Splittable)
    assert parsed["stress_cmd_w"].value == "cassandra-stress cmd2 -pop seq=1..200"
    # untagged values (plain strings, non-strings) are left completely untouched
    assert parsed["stress_cmd_r"] == "cmd3 -pop seq=1..300"
    assert parsed["n_loaders"] == 4


def test_no_tags_present_parses_as_plain_strings():
    parsed = yaml.load("prepare_write_cmd: 'plain string, no tag'", Loader=SctYamlLoader)
    assert parsed["prepare_write_cmd"] == "plain string, no tag"
    assert not isinstance(parsed["prepare_write_cmd"], Splittable)


# ---------------------------------------------------------------------------
# Splittable content validation: raises immediately at YAML-parse time (it can't
# know which field/key it's attached to, so it can only validate the value itself).
# ---------------------------------------------------------------------------


def test_auto_split_tag_on_non_cassandra_stress_value_raises_at_parse_time():
    with pytest.raises(ValueError, match="doesn't look like a cassandra-stress command"):
        yaml.load('test_duration: !auto_split "500"', Loader=SctYamlLoader)


def test_auto_split_tag_on_unsplittable_value_raises_at_parse_time():
    """No '-pop seq=X..Y' range (e.g. 'dist=gauss(...)') - the tag is unconditionally
    strict since it's written directly on the field it targets."""
    with pytest.raises(ValueError, match="no splittable"):
        yaml.load(
            "stress_cmd_w: !auto_split \"cassandra-stress write duration=10m -pop 'dist=gauss(1..100,50,10)'\"",
            Loader=SctYamlLoader,
        )


@pytest.mark.parametrize(
    "tagged_value",
    [
        pytest.param("cassandra-stress write cl=ALL n=100 -pop seq=1..100", id="plain"),
        pytest.param('JVM_OPTS="-Xmx2G" cassandra-stress write n=100 -pop seq=1..100', id="jvm_opts_prefixed"),
        pytest.param("cql-stress-cassandra-stress write cl=ALL n=100 -pop seq=1..100", id="cql_stress_variant"),
    ],
)
def test_auto_split_tag_accepts_valid_cassandra_stress_values(tagged_value):
    # Single-quoted YAML wrapper since some values (e.g. JVM_OPTS-prefixed) contain double quotes.
    parsed = yaml.load(f"prepare_write_cmd: !auto_split '{tagged_value}'", Loader=SctYamlLoader)
    assert isinstance(parsed["prepare_write_cmd"], Splittable)
    assert parsed["prepare_write_cmd"].value == tagged_value


def test_auto_split_tag_does_not_leak_into_global_yaml_safe_load():
    """Registering '!auto_split' on the dedicated SctYamlLoader must never affect the stock
    yaml.SafeLoader / yaml.safe_load() used by ~40 unrelated call sites across the codebase."""
    tagged_yaml = "prepare_write_cmd: !auto_split 'cmd -pop seq=1..100'"

    with pytest.raises(yaml.constructor.ConstructorError, match="auto_split"):
        yaml.safe_load(tagged_yaml)

    with pytest.raises(yaml.constructor.ConstructorError, match="auto_split"):
        yaml.load(tagged_yaml, Loader=yaml.SafeLoader)
