
from pymongo import MongoClient
from cassandra.cluster import Cluster

def conectar_mongodb(uri="mongodb://localhost:27017", db_name="bdnr_mongo"):
   
    try:
        cliente = MongoClient(uri, serverSelectionTimeoutMS=3000)
        cliente.server_info() 
        db = cliente[db_name]
        print(" mongo arriba")
        return db
    except Exception as e:
        print(f" Error : {e}")
        return None

def conectar_cassandra(hosts=["localhost"], keyspace="bdnr_cassandra"):
   
    try:
        cluster = Cluster(hosts)
        session = cluster.connect()
        session.set_keyspace(keyspace)
        print("cassandra arriba")
        return session
    except Exception as e:
        return None


if __name__ == "__main__":
    mongo_db = conectar_mongodb()
    cassandra_session = conectar_cassandra()
