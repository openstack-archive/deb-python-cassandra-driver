# Copyright 2013-2017 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import time
import logging

from cassandra import OperationTimedOut
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.policies import RoundRobinPolicy, HostStateListener

from tests.integration.simulacron.utils import start_and_prime_cluster, prime_query, stopt_simulacron, \
    clear_queries, prime_request, PrimeOptions, NO_THEN

import time

class TrackDownListener(HostStateListener):
    hosts_marked_down = []

    def on_down(self, host):
        self.hosts_marked_down.append(host)

class ConnectionTest(unittest.TestCase):

    def test_heart_beat_timeout(self):
        """
        Test to ensure the hosts are marked as down after a OTO is received.
        Also to ensure this happens within the expected timeout
        @since 3.10
        @jira_ticket PYTHON-762
        @expected_result all the hosts have been marked as down at some point

        @test_category metadata
        """
        number_of_dcs = 3
        nodes_per_dc = 200

        query_to_prime = "INSERT INTO test3rf.test (k, v) VALUES (0, 1);"

        idle_heartbeat_timeout = 5
        idle_heartbeat_interval = 1

        start_and_prime_cluster("test_heart_beat_timeout", number_of_dcs, nodes_per_dc)

        listener = TrackDownListener()
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(),
                          idle_heartbeat_interval=idle_heartbeat_interval,
                          idle_heartbeat_timeout=idle_heartbeat_timeout,
                          executor_threads=16)
        session = cluster.connect(wait_for_all_pools=True)
        cluster.register_listener(listener)
        log = logging.getLogger()
        log.setLevel('CRITICAL')

        self.addCleanup(cluster.shutdown)
        self.addCleanup(stopt_simulacron)
        self.addCleanup(log.setLevel, "DEBUG")

        prime_query(query_to_prime, then=NO_THEN)
        prime_request(PrimeOptions(then=NO_THEN))

        futures = []
        for _ in range(600):
            future = session.execute_async(query_to_prime)
            futures.append(future)

        for f in futures:
            f._event.wait()
            assert isinstance(f._final_exception, OperationTimedOut)

        # We allow from some extra time for all the hosts to be to on_down
        time.sleep((idle_heartbeat_timeout + idle_heartbeat_interval)*3)

        for host in cluster.metadata.all_hosts():
            self.assertIn(host, listener.hosts_marked_down)
