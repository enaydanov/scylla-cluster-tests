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

import logging
import time

from sdcm.sct_events import Severity
from sdcm.sct_events.teardown_validators import ValidatorEvent
from sdcm.teardown_validators.base import TeardownValidator

LOGGER = logging.getLogger(__name__)


class ConnectionsPerShardValidator(TeardownValidator):
    """Fail the run unless every CQL shard held ``min_connections`` client connections at the same time.

    Reads ``scylla_transport_current_connections`` from the monitor's Prometheus over the whole run.
    The checked value is the peak of the per-instant minimum across all (node, shard) series, so a
    single under-filled shard at every point in time fails the check.

    Config::

        teardown_validators:
          connections_per_shard:
            enabled: true
            min_connections: 30000
    """

    validator_name = "connections_per_shard"

    def validate(self):
        if not self.tester.prometheus_db:
            LOGGER.warning("connections_per_shard validation skipped: no Prometheus (monitor was not created).")
            return

        target = int(self.configuration["min_connections"])
        results = self.tester.prometheus_db.query(
            query="min(scylla_transport_current_connections)",
            start=self.tester.start_time,
            end=time.time(),
            scrap_metrics_step=60,
        )
        values = results[0]["values"] if results else []
        reached = max((int(float(value)) for _, value in values), default=0)
        if reached >= target:
            LOGGER.info("connections_per_shard validation passed: every shard reached %d connections", reached)
            return

        ValidatorEvent(
            message=f"connections_per_shard validation failed: the least-connected shard peaked at {reached} "
            f"connections, target is {target}",
            severity=Severity.ERROR,
        ).publish()
        self.tester.get_test_status = lambda: "FAILED"
