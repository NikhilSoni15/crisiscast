# typing imports
from pymongo.synchronous.database import Database
from pymongo.synchronous.mongo_client import MongoClient
from typing import Any

# actual imports
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

ALL_CRISES=["natural_disaster", "terrorist_attack", "cyberattack", "pandemic", "war", "financial_crisis", "none"]

class MongoStorage:
    def __init__(self, host: str, db_name: str, collection_name="unified_post"):
        self.client: MongoClient[Any] = MongoClient(
            host,
            maxPoolSize=50,
            connectTimeoutMS=5000,
            serverSelectionTimeoutMS=5000,
            retryWrites=True
        )
        try:
            _ = self.client.admin.command('ping')
            print("aight we good with mongodb")
        except ConnectionFailure as e:
            print("yo check the mongodb: %s", e)
            raise
        db: Database[Any] = self.client[db_name]
        if collection_name not in db.list_collection_names():
            self.collection = db.create_collection(collection_name, timeseries={
                "timeField": "timestamp",
                "metaField": "crisis_type"
            })
        else:
            self.collection = db[collection_name]

    def __del__(self):
        try:
            self.client.close()
        except Exception:
            pass
        print("see ya mongodb")

    def insert_many(self, documents: list[dict]):
        def date_converter(document):
            document['timestamp'] = datetime.fromisoformat(document['timestamp'])
            return document
        mapper = map(date_converter, documents)
        documents_with_date = list(mapper)
        result = self.collection.insert_many(documents_with_date)
        return str(result.inserted_ids)
    
    # what find functions are needed?