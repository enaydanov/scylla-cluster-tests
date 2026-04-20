"""Tests for MultipleNodesStopWaitStartMonkey."""

import pytest
from unittest.mock import MagicMock, patch

from sdcm.nemesis.monkey import MultipleNodesStopWaitStartMonkey
from unit_tests.unit.nemesis import TestRunner


def make_runner(node_count=6, restart_count=3, sleep_time=10):
    """Create a TestRunner with multiple mock nodes for multi-node nemesis testing."""
    runner = TestRunner(params={
        "nemesis_multi_node_restart_count": restart_count,
        "nemesis_stop_wait_start_sleep_time": sleep_time,
    })
    # Create additional nodes beyond the default target_node
    nodes = [runner.target_node]
    for i in range(1, node_count):
        node = MagicMock()
        node.name = f"node{i + 1}"
        node.running_nemesis = None
        nodes.append(node)
    runner.cluster.nodes = nodes
    runner.current_disruption = "MultipleNodesStopWaitStartMonkey-1"
    runner.node_allocator = MagicMock()
    runner.node_allocator.nodes_running_nemesis.return_value.__enter__ = MagicMock()
    runner.node_allocator.nodes_running_nemesis.return_value.__exit__ = MagicMock(return_value=False)
    return runner


class TestMultipleNodesStopWaitStartMonkey:
    def test_flags(self):
        assert MultipleNodesStopWaitStartMonkey.disruptive is True
        assert MultipleNodesStopWaitStartMonkey.kubernetes is True
        assert MultipleNodesStopWaitStartMonkey.xcloud is True
        assert MultipleNodesStopWaitStartMonkey.limited is True
        assert MultipleNodesStopWaitStartMonkey.zero_node_changes is True

    @patch("sdcm.nemesis.monkey.time.sleep")
    def test_stops_and_starts_correct_number_of_nodes(self, mock_sleep):
        runner = make_runner(node_count=6, restart_count=3, sleep_time=10)
        nemesis = MultipleNodesStopWaitStartMonkey(runner)

        nemesis.disrupt()

        mock_sleep.assert_called_once_with(10)
        # Target node + 2 extra = 3 total
        stopped_nodes = [call.args for call in runner.target_node.stop_scylla_server.call_args_list]
        # target_node.stop_scylla_server is called once (for the target node)
        runner.target_node.stop_scylla_server.assert_called_once()
        runner.target_node.start_scylla_server.assert_called_once()

        # Count total stop calls across all nodes
        all_nodes = runner.cluster.nodes
        total_stopped = sum(1 for n in all_nodes if n.stop_scylla_server.called)
        assert total_stopped == 3

    @patch("sdcm.nemesis.monkey.time.sleep")
    def test_reserves_extra_nodes_via_allocator(self, mock_sleep):
        runner = make_runner(node_count=6, restart_count=3, sleep_time=5)
        nemesis = MultipleNodesStopWaitStartMonkey(runner)

        nemesis.disrupt()

        runner.node_allocator.nodes_running_nemesis.assert_called_once()
        call_args = runner.node_allocator.nodes_running_nemesis.call_args
        extra_nodes = call_args[0][0]
        assert len(extra_nodes) == 2  # 3 total - 1 target = 2 extra
        assert runner.target_node not in extra_nodes

    @patch("sdcm.nemesis.monkey.time.sleep")
    def test_fewer_nodes_available_than_requested(self, mock_sleep):
        """When fewer nodes are available than requested, use all available."""
        runner = make_runner(node_count=2, restart_count=5, sleep_time=5)
        nemesis = MultipleNodesStopWaitStartMonkey(runner)

        nemesis.disrupt()

        # Only 2 nodes total: target + 1 extra
        all_nodes = runner.cluster.nodes
        total_stopped = sum(1 for n in all_nodes if n.stop_scylla_server.called)
        assert total_stopped == 2
        runner.log.warning.assert_called_once()

    @patch("sdcm.nemesis.monkey.time.sleep")
    def test_defaults_when_params_are_none(self, mock_sleep):
        """When params return None, defaults of 3 nodes and 600s sleep are used."""
        runner = make_runner(node_count=6)
        runner.tester.params = {}
        nemesis = MultipleNodesStopWaitStartMonkey(runner)

        nemesis.disrupt()

        mock_sleep.assert_called_once_with(600)
        all_nodes = runner.cluster.nodes
        total_stopped = sum(1 for n in all_nodes if n.stop_scylla_server.called)
        assert total_stopped == 3
