#!/usr/bin/env python3
from pymongo import MongoClient
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams

def reset_mongodb(uri="mongodb://localhost:27017/", db_name="crisiscast", coll="unified_posts"):
    client = MongoClient(uri)
    db = client[db_name]
    db.drop_collection(coll)
    print(f"✔ MongoDB: Dropped collection '{coll}' in database '{db_name}'")
    client.close()

def reset_qdrant(host="localhost", port=6333, collection="post_vectors"):
    client = QdrantClient(host=host, port=port)
    client.delete_collection(collection_name=collection)
    print(f"✔ Qdrant: Deleted collection '{collection}'")

if __name__ == "__main__":
    reset_mongodb()
    reset_qdrant()
