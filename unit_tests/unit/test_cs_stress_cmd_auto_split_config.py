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

"""Tests for the '!auto_split' YAML tag end-to-end through SCTConfiguration.

The split is applied by SCTConfiguration._apply_cs_stress_cmd_auto_split(), called at
the very end of __init__ (not verify_configuration()) - so it's already reflected in
the config immediately after construction, before verify_configuration()/log_config()
are ever called.

The tag can only be expressed via a real YAML file (env vars are plain strings and can't
carry a YAML tag), so these tests write actual temp config files rather than using
config.update()/env vars directly.
"""

from unittest.mock import patch
import typing

import pytest
from pydantic import ValidationError

from sdcm.sct_config import SCTConfiguration, Splittable


@pytest.fixture(autouse=True)
def _set_env(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-1234")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
    with (
        patch("sdcm.sct_config.aws_check_instance_type_supported", return_value=True),
        patch("sdcm.sct_config.convert_name_to_ami_if_needed", return_value="ami-1234"),
    ):
        yield


def _build_config(tmp_path, monkeypatch, yaml_content: str) -> SCTConfiguration:
    """Write `yaml_content` to a temp file, point SCT_CONFIG_FILES at it, and construct."""
    config_path = tmp_path / "test-case.yaml"
    config_path.write_text(yaml_content)
    monkeypatch.setenv("SCT_CONFIG_FILES", str(config_path))
    return SCTConfiguration()


_UNSPLIT_PREPARE_WRITE_CMD = (
    "cassandra-stress write  cl=ALL n=650000004 -schema 'replication(strategy=NetworkTopologyStrategy,"
    "replication_factor=3)' -mode connectionsPerHost=8 cql3 native -rate threads=50 throttle=37500/s "
    "-col 'size=FIXED(1024) n=FIXED(1)'  -pop seq=1..650000004"
)


def test_tagged_field_splits_across_loaders(tmp_path, monkeypatch):
    config = _build_config(
        tmp_path,
        monkeypatch,
        f"""
        n_loaders: 4
        round_robin: true
        prepare_write_cmd: !auto_split "{_UNSPLIT_PREPARE_WRITE_CMD}"
        """,
    )
    assert len(config["prepare_write_cmd"]) == 4
    assert config["prepare_write_cmd"][0].endswith("-pop seq=1..162500001")
    assert config["prepare_write_cmd"][-1].endswith("-pop seq=487500004..650000004")


def test_untagged_field_is_a_complete_no_op(tmp_path, monkeypatch):
    """A plain (untagged) single command is left completely untouched, no round_robin needed."""
    config = _build_config(
        tmp_path,
        monkeypatch,
        f"""
        n_loaders: 4
        prepare_write_cmd: "{_UNSPLIT_PREPARE_WRITE_CMD}"
        """,
    )
    assert config["prepare_write_cmd"] == [_UNSPLIT_PREPARE_WRITE_CMD]


def test_only_tagged_fields_are_split_others_left_alone(tmp_path, monkeypatch):
    config = _build_config(
        tmp_path,
        monkeypatch,
        f"""
        n_loaders: 4
        round_robin: true
        prepare_write_cmd: !auto_split "{_UNSPLIT_PREPARE_WRITE_CMD}"
        stress_cmd_w: "cassandra-stress write no-warmup cl=QUORUM n=100 -pop seq=1..100"
        """,
    )
    assert len(config["prepare_write_cmd"]) == 4
    assert config["stress_cmd_w"] == ["cassandra-stress write no-warmup cl=QUORUM n=100 -pop seq=1..100"]


def test_tag_overridden_untagged_by_later_config_file_is_not_split(tmp_path, monkeypatch):
    """If a field is tagged in one merged source (e.g. backend defaults) but a
    later-merged source (e.g. a user config file) overrides it with a plain, untagged
    value, the override wins completely - auto-split must not apply. This is automatic
    (not tracked bookkeeping): the field-level SplittableStringOrList type simply holds
    whatever the last merge assigned, and a plain override is no longer an Splittable
    at all by the time _apply_cs_stress_cmd_auto_split() runs."""
    first_file = tmp_path / "first.yaml"
    first_file.write_text('prepare_write_cmd: !auto_split "cassandra-stress cmd1 -pop seq=1..10"\n')
    second_file = tmp_path / "second.yaml"
    second_file.write_text(
        'n_loaders: 4\nround_robin: true\nprepare_write_cmd: "cassandra-stress cmd2 -pop seq=1..20"\n'
    )

    monkeypatch.setenv("SCT_CONFIG_FILES", f'["{first_file}", "{second_file}"]')
    config = SCTConfiguration()
    assert config["prepare_write_cmd"] == ["cassandra-stress cmd2 -pop seq=1..20"]


def test_tag_overridden_untagged_by_env_var_is_not_split(tmp_path, monkeypatch):
    """Same as above, but the later override comes from an environment variable
    (env vars are always plain strings - they can never carry the YAML tag)."""
    config_path = tmp_path / "test-case.yaml"
    config_path.write_text('prepare_write_cmd: !auto_split "cassandra-stress cmd1 -pop seq=1..10"\n')
    monkeypatch.setenv("SCT_CONFIG_FILES", str(config_path))
    monkeypatch.setenv("SCT_N_LOADERS", "4")
    monkeypatch.setenv("SCT_ROUND_ROBIN", "true")
    monkeypatch.setenv("SCT_PREPARE_WRITE_CMD", "cassandra-stress cmd2 -pop seq=1..20")

    config = SCTConfiguration()
    assert config["prepare_write_cmd"] == ["cassandra-stress cmd2 -pop seq=1..20"]


def test_tag_on_field_without_splittable_string_or_list_raises_validation_error(tmp_path, monkeypatch):
    """Tagging a field that exists and is even StringOrList-typed, but hasn't opted into
    SplittableStringOrList, is rejected automatically by Pydantic itself (Splittable
    is a BaseModel, structurally incompatible with str/list[str]) - no manual
    bookkeeping of "which fields support this" is needed."""
    with pytest.raises(ValidationError):
        _build_config(
            tmp_path,
            monkeypatch,
            """
            stress_cmd_no_mv: !auto_split "cassandra-stress write cl=ALL n=100 -pop seq=1..100"
            """,
        )


def test_single_loader_is_a_no_op_and_does_not_require_round_robin(tmp_path, monkeypatch):
    config = _build_config(
        tmp_path,
        monkeypatch,
        """
        n_loaders: 1
        prepare_write_cmd: !auto_split "cassandra-stress write cl=ALL n=100 -pop seq=1..100"
        """,
    )
    assert config["prepare_write_cmd"] == ["cassandra-stress write cl=ALL n=100 -pop seq=1..100"]


def test_tagged_field_with_no_splittable_range_raises_at_construction(tmp_path, monkeypatch):
    """Unlike the old best-effort 'true' mode, every tagged field is now unconditionally
    strict: the tag is written directly on the field, so an unsplittable command is a
    config mistake, not something to silently skip. Raised by Splittable itself at
    YAML-parse time (during SCTConfiguration construction), not deferred any further."""
    with pytest.raises(ValidationError, match="no splittable"):
        _build_config(
            tmp_path,
            monkeypatch,
            """
            n_loaders: 4
            round_robin: true
            stress_cmd_w: !auto_split "cassandra-stress write no-warmup cl=QUORUM duration=10m -pop 'dist=gauss(1..100,50,10)'"
            """,
        )


def test_tag_on_non_cassandra_stress_value_raises(tmp_path, monkeypatch):
    """A tagged value that doesn't look like a cassandra-stress command is a config
    mistake (e.g. tagging an unrelated field like test_duration). Raised immediately at
    YAML-parse time (SCTConfiguration construction), by Splittable itself - since a
    YAML tag constructor can validate the value as soon as it's parsed, before it's
    even associated with a field name."""
    with pytest.raises(ValueError, match="doesn't look like a cassandra-stress command"):
        _build_config(
            tmp_path,
            monkeypatch,
            """
            test_duration: !auto_split "500"
            """,
        )


def test_tag_on_jvm_opts_prefixed_command_is_accepted(tmp_path, monkeypatch):
    """A cassandra-stress command prefixed with JVM options (e.g. JVM_OPTS="...") doesn't
    start with the literal 'cassandra-stress' string - must not be rejected. Matches
    sdcm/tester.py's own run_stress_thread dispatch, which also checks substring
    containment rather than a strict prefix, for exactly this reason."""
    config = _build_config(
        tmp_path,
        monkeypatch,
        """
        n_loaders: 4
        round_robin: true
        prepare_write_cmd: !auto_split 'JVM_OPTS="-Xmx2G" cassandra-stress write cl=ALL n=100 -pop seq=1..100'
        """,
    )
    assert len(config["prepare_write_cmd"]) == 4


def test_tag_on_cql_stress_cassandra_stress_command_is_accepted(tmp_path, monkeypatch):
    """The 'cql-stress-cassandra-stress' tool variant doesn't start with the literal
    'cassandra-stress' string either - must not be rejected."""
    config = _build_config(
        tmp_path,
        monkeypatch,
        """
        n_loaders: 4
        round_robin: true
        prepare_write_cmd: !auto_split "cql-stress-cassandra-stress write cl=ALL n=100 -pop seq=1..100"
        """,
    )
    assert len(config["prepare_write_cmd"]) == 4


def test_missing_round_robin_raises_when_split_produces_multiple_commands(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="round_robin"):
        _build_config(
            tmp_path,
            monkeypatch,
            f"""
            n_loaders: 4
            prepare_write_cmd: !auto_split "{_UNSPLIT_PREPARE_WRITE_CMD}"
            """,
        )


def test_multi_dc_n_loaders_list_is_summed(tmp_path, monkeypatch):
    """n_loaders as a per-region list (IntOrList) is summed for the split count."""
    config = _build_config(
        tmp_path,
        monkeypatch,
        f"""
        n_loaders: [2, 2]
        round_robin: true
        prepare_write_cmd: !auto_split "{_UNSPLIT_PREPARE_WRITE_CMD}"
        """,
    )
    assert len(config["prepare_write_cmd"]) == 4


def test_auto_split_multiplier_multiplies_piece_count(tmp_path, monkeypatch):
    """auto_split_multiplier: 2 with n_loaders: 4 produces 8 distinct pieces, so each
    loader ends up running 2 parallel stress processes (via round_robin's cyclic
    loader assignment), each covering a different sub-range."""
    config = _build_config(
        tmp_path,
        monkeypatch,
        f"""
        n_loaders: 4
        round_robin: true
        auto_split_multiplier: 2
        prepare_write_cmd: !auto_split "{_UNSPLIT_PREPARE_WRITE_CMD}"
        """,
    )
    assert len(config["prepare_write_cmd"]) == 8
    assert config["prepare_write_cmd"][0].endswith("-pop seq=1..81250000")
    assert config["prepare_write_cmd"][-1].endswith("-pop seq=568750001..650000004")


def test_auto_split_multiplier_default_is_a_no_op(tmp_path, monkeypatch):
    """When unset, auto_split_multiplier defaults to 1 - no change from n_loaders alone."""
    config = _build_config(
        tmp_path,
        monkeypatch,
        f"""
        n_loaders: 4
        round_robin: true
        prepare_write_cmd: !auto_split "{_UNSPLIT_PREPARE_WRITE_CMD}"
        """,
    )
    assert len(config["prepare_write_cmd"]) == 4


def test_auto_split_multiplier_with_single_loader_still_requires_round_robin(tmp_path, monkeypatch):
    """Even with n_loaders: 1, a multiplier > 1 still produces multiple pieces, which
    still need round_robin: true to be pinned one-per-process instead of running on
    every loader."""
    with pytest.raises(ValueError, match="round_robin"):
        _build_config(
            tmp_path,
            monkeypatch,
            f"""
            n_loaders: 1
            auto_split_multiplier: 2
            prepare_write_cmd: !auto_split "{_UNSPLIT_PREPARE_WRITE_CMD}"
            """,
        )


def test_tags_in_different_merged_files_are_all_applied(tmp_path, monkeypatch):
    """A tag found in one user config file is applied correctly even when n_loaders/
    round_robin come from a different, later-merged file."""
    first_file = tmp_path / "first.yaml"
    first_file.write_text(f'prepare_write_cmd: !auto_split "{_UNSPLIT_PREPARE_WRITE_CMD}"\n')
    second_file = tmp_path / "second.yaml"
    second_file.write_text("n_loaders: 4\nround_robin: true\n")

    monkeypatch.setenv("SCT_CONFIG_FILES", f'["{first_file}", "{second_file}"]')
    config = SCTConfiguration()
    assert len(config["prepare_write_cmd"]) == 4


# ---------------------------------------------------------------------------
# Safety net: every field typed to accept a '!auto_split' tag must actually be
# scanned/resolved by _apply_cs_stress_cmd_auto_split() - see the
# stress_cmd_cache_warmup/stress_cmd_read_disk bug this test guards against
# (both were typed to accept the tag but missing from stress_cmd_params, so a
# tagged value was silently left un-resolved, crashing much later wherever it
# was eventually consumed as a plain string/list).
# ---------------------------------------------------------------------------


def _accepts_splittable(field) -> bool:
    """Return True if a SCTConfiguration field's type accepts a Splittable value
    (i.e. it's SplittableStringOrList, not plain StringOrList)."""
    return Splittable in typing.get_args(field.annotation) or field.annotation is Splittable


def test_every_splittable_field_is_scanned_by_auto_split():
    """_apply_cs_stress_cmd_auto_split() only scans self.stress_cmd_params - any field
    typed to accept a '!auto_split' tag (Splittable) must be included there, or a
    tagged value on it would silently stay unresolved forever."""
    stress_cmd_params = set(SCTConfiguration.model_fields["stress_cmd_params"].default)
    splittable_fields = {name for name, field in SCTConfiguration.model_fields.items() if _accepts_splittable(field)}
    missing = splittable_fields - stress_cmd_params
    assert not missing, (
        f"Field(s) typed SplittableStringOrList but missing from stress_cmd_params, so "
        f"'!auto_split' tags on them would never be resolved: {sorted(missing)}"
    )


def test_tag_on_stress_cmd_cache_warmup_is_resolved(tmp_path, monkeypatch):
    """Regression test for the exact bug this safety net guards against: a tagged
    stress_cmd_cache_warmup value must be split/resolved, not left as a raw
    Splittable object."""
    config = _build_config(
        tmp_path,
        monkeypatch,
        f"""
        n_loaders: 4
        round_robin: true
        stress_cmd_cache_warmup: !auto_split "{_UNSPLIT_PREPARE_WRITE_CMD}"
        """,
    )
    assert len(config["stress_cmd_cache_warmup"]) == 4
    assert all(isinstance(cmd, str) for cmd in config["stress_cmd_cache_warmup"])


def test_tag_on_stress_cmd_read_disk_is_resolved(tmp_path, monkeypatch):
    """Same regression test for stress_cmd_read_disk, the other field missing from
    stress_cmd_params."""
    config = _build_config(
        tmp_path,
        monkeypatch,
        f"""
        n_loaders: 4
        round_robin: true
        stress_cmd_read_disk: !auto_split "{_UNSPLIT_PREPARE_WRITE_CMD}"
        """,
    )
    assert len(config["stress_cmd_read_disk"]) == 4
    assert all(isinstance(cmd, str) for cmd in config["stress_cmd_read_disk"])
