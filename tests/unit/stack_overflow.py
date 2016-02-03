__author__ = 'gregbestland'


from multiprocessing import Process

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


class DataProcess(Process):

    def __init__(self):

        super(DataProcess, self).__init__()

        # Do a few other irrelevant things ...

        # Set up the Cassandra connection
        self.cluster = Cluster(contact_points=["127.0.0.1"])
        self.session = self.cluster.connect('irchelp')
        print "Connected to cassandra."

    def callback(self):

        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        results = self.session.execute("SELECT * FROM system.local")
        print str(results)

cluster = Cluster(contact_points=["127.0.0.1"])
session = cluster.connect('irchelp')
