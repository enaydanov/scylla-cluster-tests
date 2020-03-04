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
# Copyright (c) 2020 ScyllaDB

from sdcm.tester import ClusterTester


class CoredumpTest(ClusterTester):
    def test_scylla_coredump(self):
        self.db_cluster.nodes[0].remoter.run("sudo pkill -SIGSEGV scylla")
