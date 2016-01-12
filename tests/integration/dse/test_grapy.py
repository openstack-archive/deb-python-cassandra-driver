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
# limitations under the License.


from tests.integration.dse import BasicGraphUnitTestCase


from cassandra.protocol import ServerError


class BasicGraphTest(BasicGraphUnitTestCase):

        def test_basic_query(self):
            """
            Test to validate that basic graph query results can be executed with a sane result set.

            Creates a simple classic tinkerpot graph, and attempts to find all vertices
            related the vertex marco, that have a label of knows.
            See reference graph here
            http://www.tinkerpop.com/docs/3.0.0.M1/

            @since 3.1
            @jira_ticket PYTHON-457
            @expected_result graph should find two vertices related to marco via 'knows' edges.

            @test_category dse graph
            """

            self._generate_classic()
            rs = self.graph_session.execute('''g.V().has('name','marko').out('knows').values('name')''')
            self.assertFalse(rs.has_more_pages)
            results_list = [result.value for result in rs.current_rows]
            self.assertEqual(len(results_list), 2)
            self.assertIn('vadas', results_list)
            self.assertIn('josh', results_list)

        def test_classic_graph(self):
            """
            Test to validate that basic graph generation, and vertex and edges are surfaced correctly

            Creates a simple classic tinkerpot graph, and iterates over the the vertices and edges
            ensureing that each one is correct. See reference graph here
            http://www.tinkerpop.com/docs/3.0.0.M1/

            @since 3.1
            @jira_ticket PYTHON-457
            @expected_result graph should generate and all vertices and edge results should be

            @test_category dse graph
            """
            self._generate_classic()
            rs = self.graph_session.execute('g.V()')
            for vertex in rs:
                self._validate_classic_vertex(vertex)
            rs = self.graph_session.execute('g.E()')
            for edge in rs:
                self._validate_classic_edge(edge)

        def test_large_create_script(self):
            """
            Test to validate that server errors due to large groovy scripts are properly surfaced

            Creates a very large line graph script and executes it. Then proceeds to create a line graph script
            that is to large for the server to handle expects a server error to be returned

            @since 3.1
            @jira_ticket PYTHON-457
            @expected_result graph should generate and all vertices and edge results should be

            @test_category dse graph
            """
            query_to_run = self._generate_line_graph(900)
            self.graph_session.execute(query_to_run)
            query_to_run = self._generate_line_graph(950)
            self.assertRaises(ServerError, self.graph_session.execute, query_to_run)

        def test_range_query(self):
            """
            Test to validate range queries are handled correctly.

            Creates a very large line graph script and executes it. Then proceeds to to a range
            limited query against it, and ensure that the results are formated correctly and that
            the result set is properly sized.

            @since 3.1
            @jira_ticket PYTHON-457
            @expected_result result set should be properly formated and properly sized

            @test_category dse graph
            """
            query_to_run = self._generate_line_graph(900)
            self.graph_session.execute(query_to_run)
            rs = self.graph_session.execute("g.E().range(0,10)")
            self.assertFalse(rs.has_more_pages)
            self.assertEqual(len(rs.current_rows), 10)
            for result in rs:
                self._validate_line_edge(result)

        def test_result_types(self):
            """
            Test to validate that result types are properly cast on return

            Creates a small grap with vertices that contain properties of many different types.
            Queries all vertices and ensure that properties in result set are the correct type

            @since 3.1
            @jira_ticket PYTHON-457
            @expected_result result set types should map correctly to groovy types

            @test_category dse graph
            """
            self._generate_multi_field_graph()

            rs = self.graph_session.execute("g.V()")
            for result in rs:
                self._validate_type(result)

        def test_large_result_set(self):
            """
            Test to validate that large result sets return correctly.

            Creates a very large graph. Ensures that large result sets are handled appropriately.

            @since 3.1
            @jira_ticket PYTHON-457
            @expected_result when limits of result sets are hit errors should be surfaced appropriately

            @test_category dse graph
            """
            self._generate_large_complex_graph(5000)
            rs = self.graph_session.execute("g.V()")
            for result in rs:
                self._validate_generic_vertex_values_exist(result)

        def _validate_type(self, vertex):
            values = vertex.properties.values()
            for value in values:
                type_indicator = value[0].get('id').get('~type')
                print(type_indicator)
                if type_indicator.startswith('int'):
                    actual_value = value[0].get('value')
                    self.assertTrue(isinstance(actual_value, int))
                elif type_indicator.startswith('short'):
                    actual_value = value[0].get('value')
                    self.assertTrue(isinstance(actual_value, int))
                elif type_indicator.startswith('long'):
                    actual_value = value[0].get('value')
                    print(type(actual_value))
                    self.assertTrue(isinstance(actual_value, int))
                elif type_indicator.startswith('float'):
                    actual_value = value[0].get('value')
                    self.assertTrue(isinstance(actual_value, float))
                elif type_indicator.startswith('double'):
                    actual_value = value[0].get('value')
                    self.assertTrue(isinstance(actual_value, float))

        def _validate_classic_vertex(self, vertex):
            vertex_props = vertex.properties.keys()
            self.assertEqual(len(vertex_props), 2)
            self.assertIn('name', vertex_props)
            self.assertTrue('lang' in vertex_props or 'age' in vertex_props)

        def _validate_generic_vertex_values_exist(self, vertex):
            value_map = vertex.value
            self.assertIn('properties', value_map)
            self.assertIn('type', value_map)
            self.assertIn('id', value_map)
            self.assertIn('label', value_map)
            self.assertIn('label', value_map)
            self.assertIn('type', value_map)
            self.assertIn('id', value_map)

        def _validate_classic_edge(self, edge):
            edge_props = edge.properties
            self.assertEqual(len(edge_props.keys()), 1)
            self.assertIn('weight', edge_props)
            self._validate_generic_edge_values_exist(edge)

        def _validate_line_edge(self, edge):
            edge_props = edge.properties
            self.assertEqual(len(edge_props.keys()), 1)
            self.assertIn('distance', edge_props)
            self._validate_generic_edge_values_exist(edge)

        def _validate_generic_edge_values_exist(self, edge):
            value_map = edge.value
            self.assertIn('properties', value_map)
            self.assertIn('outV', value_map)
            self.assertIn('outVLabel', value_map)
            self.assertIn('inV', value_map)
            self.assertIn('inVLabel', value_map)
            self.assertIn('label', value_map)
            self.assertIn('type', value_map)
            self.assertIn('id', value_map)

        def _generate_classic(self):
            rs = self.graph_session.execute('''
                Vertex marko = graph.addVertex("name", "marko", "age", 29);
                Vertex vadas = graph.addVertex("name", "vadas", "age", 27);
                Vertex lop = graph.addVertex("name", "lop", "lang", "java");
                Vertex josh = graph.addVertex("name", "josh", "age", 32);
                Vertex ripple = graph.addVertex("name", "ripple", "lang", "java");
                Vertex peter = graph.addVertex("name", "peter", "age", 35);
                marko.addEdge("knows", vadas, "weight", 0.5f);
                marko.addEdge("knows", josh, "weight", 1.0f);
                marko.addEdge("created", lop, "weight", 0.4f);
                josh.addEdge("created", ripple, "weight", 1.0f);
                josh.addEdge("created", lop, "weight", 0.4f);
                peter.addEdge("created", lop, "weight", 0.2f);''')
            return rs

        def _generate_line_graph(self, length):
            query_parts = []
            for index in range(0, length):
                query_parts.append('''Vertex vertex{0} = graph.addVertex("index", {0}); '''.format(index))
                if index is not 0:
                    query_parts.append('''vertex{0}.addEdge("goesTo", vertex{1}, "distance", 5); '''.format(index-1,index))
            final_graph_generation_statement = "".join(query_parts)
            return final_graph_generation_statement

        def _generate_multi_field_graph(self):
            rs = self.graph_session.execute('''
                short s1 = 5000;
                int i1 = 1000000000;
                Integer i2 = 100000000;
                long l1 = 9223372036854775807;
                Long l2 = 100000000000000000L;
                float f1 = 3.5f;
                double d1 = 3.5e40;
                Double d2 = 3.5e40d;
                graph.addVertex(label, "shortvertex", "shortvalue", s1);
                graph.addVertex(label, "intvertex", "intvalue", i1);
                graph.addVertex(label, "intvertex2", "intvalue2", i2);
                graph.addVertex(label, "longvertex", "longvalue", l1);
                graph.addVertex(label, "longvertex2", "longvalue2", l2);
                graph.addVertex(label, "floatvertex", "floatvalue", f1);
                graph.addVertex(label, "doublevertex", "doublevalue", d1);
                graph.addVertex(label, "doublevertex2", "doublevalue2", d2);''')
            return rs

        def _generate_large_complex_graph(self, size):
            to_run = '''
                int size = 20000;
                List ids = new ArrayList();
                Vertex v = graph.addVertex();
                v.property("ts", 100001);
                v.property("sin", 0);
                v.property("cos", 1);
                v.property("ii", 0);
                ids.add(v.id());
                Random rand = new Random();
                for (int ii = 1; ii < size; ii++) {
                    v = graph.addVertex();
                    v.property("ii", ii);
                    v.property("ts", 100001 + ii);
                    v.property("sin", Math.sin(ii/5.0));
                    v.property("cos", Math.cos(ii/5.0));
                    Vertex u = g.V(ids.get(rand.nextInt(ids.size()))).next();
                    v.addEdge("linked", u);
                    ids.add(u.id());
                    ids.add(v.id());
                }
                g.V().count();'''
            print to_run
            self.graph_session.execute(to_run)
