from pymongo import MongoClient
from cassandra.cluster import Cluster
import pydgraph

def conectar_mongodb(uri="mongodb://localhost:27017", db_name="bdnr_mongo"):
    try:
        cliente = MongoClient(uri, serverSelectionTimeoutMS=3000)
        cliente.server_info()
        db = cliente[db_name]
        print("mongo arriba")
        return db
    except Exception as e:
        print(f"Error MongoDB: {e}")
        return None

def conectar_cassandra(hosts=["127.0.0.1"], keyspace="bdnr_cassandra", port=9042):
    try:
        cluster = Cluster(contact_points=hosts, port=port)
        session = cluster.connect()

        # Crear el keyspace si no existe
        session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)

        session.set_keyspace(keyspace)
        print("cassandra arriba")
        return session
    except Exception as e:
        print(f"Error Cassandra: {e}")
        return None

def conectar_dgraph(host="localhost:9080"):
    try:
        client_stub = pydgraph.DgraphClientStub(host)
        client = pydgraph.DgraphClient(client_stub)
        print("dgraph arriba")
        return client
    except Exception as e:
        print(f"Error Dgraph: {e}")
        return None

if __name__ == "__main__":
    mongo_db = conectar_mongodb()
    cassandra_session = conectar_cassandra()
    dgraph_client = conectar_dgraph()


