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

import re

from sdcm.tester import ClusterTester
from sdcm.utils.decorators import cached_property
from sdcm.utils.housekeeping import HousekeepingDB, CHECKVERSION_TABLE


STRESS_CMD: str = "/usr/bin/cassandra-stress"
PRIVATE_REPO_RE = re.compile(r"https://repositories.scylladb.com/scylla/repo/"
                             r"(?P<uuid>[^/]+)/(?P<repoid>[^/]+)/scylladb-(?P<version>[\d.]+)[-.]")


class ArtifactsTest(ClusterTester):
    private_repo_uuid = None
    private_repo_repoid = None
    private_repo_version = None
    housekeeping = None

    def setUp(self):
        super().setUp()

        match = PRIVATE_REPO_RE.match(self.scylla_repo)
        if match:
            self.private_repo_uuid = match.groupdict()["uuid"]
            self.private_repo_repoid = match.groupdict()["repoid"]
            self.private_repo_version = match.groupdict()["version"]
            self.housekeeping = HousekeepingDB.from_keystore_creds()
            self.housekeeping.connect()

    def tearDown(self):
        if self.housekeeping:
            self.housekeeping.close()

        super().tearDown()

    def get_last_id(self) -> int:
        row = self.housekeeping.get_most_recent_record(f"SELECT * FROM {CHECKVERSION_TABLE} "
                                                       f"WHERE ruid = %s "
                                                       f"AND repoid = %s "
                                                       f"AND version LIKE %s "
                                                       f"AND statuscode = 'r'",
                                                       (self.private_repo_uuid,
                                                        self.private_repo_repoid,
                                                        self.private_repo_version + "%"))
        return row[0] if row else 0

    @cached_property
    def scylla_repo(self):
        return self.params.get("scylla_repo")

    @property
    def node(self):
        return self.db_cluster.nodes[0]

    def run_cassandra_stress(self, args: str):
        result = self.node.remoter.run(f"{STRESS_CMD} {args} -node {self.node.ip_address}")
        assert "java.io.IOException" not in result.stdout
        assert "java.io.IOException" not in result.stderr

    def check_scylla(self):
        self.node.run_nodetool("status")
        self.run_cassandra_stress("write n=10000 -mode cql3 native -pop seq=1..10000")
        self.run_cassandra_stress("mixed duration=1m -mode cql3 native -rate threads=10 -pop seq=1..10000")

    def test_scylla_service(self):
        if self.params["cluster_backend"] == "aws":
            with self.subTest("check ENA support"):
                assert self.node.ena_support, "ENA support is not enabled"

        with self.subTest("check Scylla server after installation"):
            self.check_scylla()

        with self.subTest("check Scylla server after stop/start"):
            self.node.stop_scylla(verify_down=True)
            self.node.start_scylla(verify_up=True)
            self.check_scylla()

        with self.subTest("check Scylla server after restart"):
            if self.housekeeping:
                last_id = self.get_last_id()
            self.node.restart_scylla(verify_up_after=True)
            if self.housekeeping:
                assert last_id < self.get_last_id()
            self.check_scylla()

    def get_email_data(self):
        self.log.info("Prepare data for email")

        email_data = self._get_common_email_data()

        # Normalize backend name, e.g., `aws' -> `AWS', `gce' -> `GCE', `docker' -> `Docker'.
        backend = self.params.get("cluster_backend")
        backend = {"aws": "AWS", "gce": "GCE", "docker": "Docker"}.get(backend, backend)

        email_data.update({"backend": backend,
                           "region_name": self.params.get("region_name"),
                           "scylla_instance_type": self.params.get('instance_type_db',
                                                                   self.params.get('gce_instance_type_db')),
                           "scylla_node_image": self.node.image,
                           "scylla_packages_installed": self.node.scylla_packages_installed,
                           "scylla_repo": self.params.get("scylla_repo"),
                           "scylla_version": self.node.scylla_version, })

        return email_data
