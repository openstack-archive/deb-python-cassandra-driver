
# Copyright 2013-2016 DataStax, Inc.
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
# limitations under the License

import requests
import json

from tests.integration import CASSANDRA_VERSION
import subprocess
import time


class SimulacronServer(object):

    def __init__(self, jar_path):
        self.jar_path = jar_path
        self.running = False
        self.proc = None

    def start(self):
        self.proc = subprocess.Popen(['java', '-jar', self.jar_path, "--loglevel", "ERROR"], shell=False)
        self.running = True

    def stop(self):
        if self.proc:
            self.proc.terminate()
        self.running = False

    def is_running(self):
        # We could check self.proc.poll here instead
        return self.running

jar_path = "/home/jaume/workspace/simulacron/standalone/target/standalone-0.1-SNAPSHOT.jar"
server_simulacron = SimulacronServer(jar_path)

def start_simulacron():
    if server_simulacron.is_running():
        server_simulacron.stop()

    server_simulacron.start()

    time.sleep(5)

def stopt_simulacron():
    server_simulacron.stop()


class SimulacronClient(object):

    def __init__(self, admin_addr="127.0.0.1:8187"):
        self.admin_addr = admin_addr

    def submit_request(self, query):
        result = requests.post("http://{0}/{1}{2}".format(
            self.admin_addr, query.path, query.fetch_url_params()),
            json=query.fetch_json())

        return result

    def prime_server_versions(self):
        system_local_row = {}
        system_local_row["cql_version"] = "3.4.4"
        system_local_row["release_version"] = "3.1.1" + "-SNAPSHOT"
        column_types = {"cql_version": "ascii", "release_version": "ascii"}
        system_local = PrimeQuery("SELECT cql_version, release_version FROM system.local",
                                  rows=[system_local_row],
                                  column_types=column_types,
                                  then={"delay_in_ms": 1})

        self.submit_request(system_local)

    def clear_all_queries(self):
        result = requests.delete("http://{0}/{1}".format(
            self.admin_addr, "prime-query-single"))
        return result

NO_THEN = object()


class PrimeOptions(object):
    def __init__(self, then=None):
        self.path = "prime-query-single"
        self.then = then

    def fetch_json(self):
        json_dict = {}
        then = {}
        when = {}

        when['request'] = "options"

        if self.then is not None and self.then is not NO_THEN:
            then.update(self.then)

        json_dict['when'] = when
        if self.then is not NO_THEN:
            json_dict['then'] = then

        return json_dict

    def fetch_url_params(self):
        return ""

class PrimeQuery(object):

    def __init__(self, expected_query, result="success", rows=None, column_types=None, then=None):
        self.expected_query = expected_query
        self.rows = rows
        self.result = result
        self.column_types = column_types
        self.path = "prime-query-single"
        self.then = then

    def fetch_json(self):
        json_dict = {}
        then = {}
        when = {}
        when['query'] = self.expected_query
        then['result'] = self.result
        if self.rows is not None:
            then['rows'] = self.rows

        if self.column_types is not None:
            then['column_types'] = self.column_types

        if self.then is not None and self.then is not NO_THEN:
            then.update(self.then)

        json_dict['when'] = when
        if self.then is not NO_THEN:
            json_dict['then'] = then

        return json_dict

    def set_node(self, cluster_id, datacenter_id, node_id):
        self.cluster_id = cluster_id
        self.datacenter_id = datacenter_id
        self.node_id = node_id

        if self.cluster_id is not None:
            self.path += "/{}".format(self.cluster_id)

        if self.cluster_id is not None:
            self.path += "/{}".format(self.datacenter_id)

        if self.cluster_id is not None:
            self.path += "/{}".format(self.node_id)

    def fetch_url_params(self):
        return ""


class ClusterQuery(object):
    def __init__(self, cluster_name, cassandra_version, data_centers=1, json_dict=None):
        self.cluster_name = cluster_name
        self.cassandra_version = cassandra_version
        self.data_centers = data_centers
        if json_dict is None:
            self.json_dict = {}
        else:
            self.json_dict = json_dict

        self.path = "cluster"

    def fetch_json(self):
        return self.json_dict

    def fetch_url_params(self):
        return "?cluster_name={0}&cassandra_version={1}&data_centers={2}".\
            format(self.cluster_name, self.cassandra_version,  self.data_centers)


def prime_driver_defaults():
    client_simulacron = SimulacronClient()
    client_simulacron.prime_server_versions()


def prime_cluster(cluster_name, data_centers=1, version=None):
    version = version or CASSANDRA_VERSION
    cluster_query = ClusterQuery(cluster_name, version, data_centers)
    client_simulacron = SimulacronClient()
    response = client_simulacron.submit_request(cluster_query)
    return SimulacronCluster(response.text)


def start_and_prime_singledc(cluster_name):
    start_and_prime_cluster(cluster_name, number_of_dc=1)


def start_and_prime_cluster(cluster_name, number_of_dc, nodes_per_dc, version=None):
    start_simulacron()
    data_centers = ",".join([str(nodes_per_dc)] * number_of_dc)
    prime_cluster(cluster_name, data_centers=data_centers, version=version)
    prime_driver_defaults()

default_column_types = {
      "key": "bigint",
      "description": "ascii",
      "dates": "map<ascii,date>"
    }

default_row = {}
default_row["key"] = 2
default_row["description"] = "whatever_description"
default_row["dates"] = {"whatever_text" : "2014-08-01"}

def prime_request(request):
    client_simulacron = SimulacronClient()
    response = client_simulacron.submit_request(request)
    return response

def prime_query(query, rows=[default_row], column_types=default_column_types, then=None):
    client_simulacron = SimulacronClient()
    query = PrimeQuery(query, rows=rows, column_types=column_types, then=then)
    response = client_simulacron.submit_request(query)
    return response

def clear_queries():
    client_simulacron = SimulacronClient()
    client_simulacron.clear_all_queries()


class SimulacronCluster(object):
    def __init__(self, json_text):
        self.json = json_text
        self.o = json.loads(json_text)

    def get_cluster_id(self):
        return self.o["id"]

    def get_cluster_name(self):
        return self.o["name"]

    def get_data_centers_ids(self):
        return [dc["id"] for dc in self.o["data_centers"]]

    def get_data_centers_names(self):
        return [dc["name"] for dc in self.o["data_centers"]]

    def get_node_ids(self, datacenter_id):
        datacenter = filter(lambda x: x["id"] ==  datacenter_id, self.o["data_centers"]).pop()
        return [node["id"] for node in datacenter["nodes"]]
