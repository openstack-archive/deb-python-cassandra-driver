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


from tests.integration import BasicGraphUnitTestCase


class BasicGraphTest(BasicGraphUnitTestCase):

        def test_basic_query(self):

            self.generate_classic()
            rs = self.graph_session.execute('''g.V().has('name','marko').out('knows').values('name')''')
            self.assertFalse(rs.has_more_pages)
            results_list = [result.value for result in rs.current_rows]
            self.assertEqual(len(results_list), 2)
            self.assertIn('vadas', results_list)
            self.assertIn('josh', results_list)

        def test_classic_graph(self):

            self.generate_classic()
            rs = self.graph_session.execute('g.V()')
            for vertex in rs:
                self._validate_classic_vertex(vertex)
            rs = self.graph_session.execute('g.E()')
            for edge in rs:
                self._validate_classic_edge(edge)

        def _validate_classic_vertex(self, vertex):
            vertex_props = vertex.properties.keys()
            self.assertEqual(len(vertex_props), 2)
            self.assertIn('name', vertex_props)
            self.assertTrue('lang' in vertex_props or 'age' in vertex_props)

        def _validate_generic_vertex_values_exist(self, edge):
            value_map = edge.value
            self.assertIn('properties', value_map)
            self.assertIn('type', value_map)
            self.assertIn('id', value_map)
            self.assertIn('label', value_map)
            self.assertIn('inVLabel', value_map)
            self.assertIn('label', value_map)
            self.assertIn('type', value_map)
            self.assertIn('id', value_map)

        def _validate_classic_edge(self, edge):
            edge_props = edge.properties
            self.assertEqual(len(edge_props.keys()), 1)
            self.assertIn('weight', edge_props)
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

        def test_result_properties(self):
            rs = self.graph_session.execute('''
            int size = 5;
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
            g.V().count();
            ''')
            rs = self.graph_session.execute('g.E()')
            for result in rs:

                print("VALUE")
                print type(result.value)
                # convenience attributes for first-level items within the result
                print("ID")
                print result.id
                print("TYPE , LABEL")
                print result.type, result.label
                print("outV")
                print result.outV
                print("outLabel")
                print result.outVLabel
                print("inV")
                print result.inV
                print("inLable")
                print result.inVLabel
                print("ToString")
                print str(result)
                print("DIR")
                print dir(result)

        def test_large_graph_insertion_properties(self):
            rs = self.graph_session.execute('''
            int size = 5;
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
            g.V().count();
            ''')
            rs = self.graph_session.execute('g.E()')
            for result in rs:

                print type(result.value)
                # convenience attributes for first-level items within the result
                print result.id
                print result.type, result.label
                print result.outV
                print result.outVLabel
                print result.inV
                print result.inVLabel

        def test_invalid_graph_syntax(self):
            rs = self.graph_session.execute('''
            int size = 5;
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
            g.V().count();
            ''')
            rs = self.graph_session.execute('g.E()')
            for result in rs:

                print type(result.value)
                # convenience attributes for first-level items within the result
                print result.id
                print result.type, result.label
                print result.outV
                print result.outVLabel
                print result.inV
                print result.inVLabel

        def test_invalid_graph_syntax(self):
            rs = self.graph_session.execute('''
            int size = 5;
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
            g.V().count();
            ''')
            rs = self.graph_session.execute('g.E()')
            for result in rs:

                print type(result.value)
                # convenience attributes for first-level items within the result
                print result.id
                print result.type, result.label
                print result.outV
                print result.outVLabel
                print result.inV
                print result.inVLabel

        def generate_classic(self):
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
