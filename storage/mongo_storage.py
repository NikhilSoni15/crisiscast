"""
    Sample from spark_kafka_consumer.py:

    {
        "_id": {
            "$oid": "68106072bf410b5bad4f791c"
        },
        "id": "1k9zwch",
        "title": "One of first US trade deals may be with India, Treasury's Bessent says",
        "selftext": "",
        "created_utc": {
            "$numberDouble": "1745857881.0"
        },
        "author": "sonicagain",
        "url": "https://www.reuters.com/world/one-first-us-trade-deals-will-be-with-india-treasurys-bessent-says-2025-04-28/",
        "subreddit": "worldnews",
        "crisis_type": "none"
    }

    @Sarasa still needs to give me the appropriate data structure.
    
    Collection is by type of event (point 1 above), not by source. Or so I think.
"""

# typing imports
from pymongo.synchronous.database import Database
from pymongo.synchronous.mongo_client import MongoClient
from typing import Any

# actual imports
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError

mongo_client = MongoClient("mongodb://localhost:27017/")
db: Database[Any] = mongo_client["crisiscast"]
mongo_collection = db["reddit_posts"]

class MongoStorage:
    def __init__(self, host: str, db_name: str):
        self.client: MongoClient[Any] = MongoClient(
            host,
            maxPoolSize=50,
            connectTimeoutMS=5000,
            serverSelectionTimeoutMS=5000,
            retryWrites=True
        )
        self.db: Database[Any] = self.client[db_name]
        try:
            _ = self.client.admin.command('ping')
            print("aight we good with mongodb")
        except ConnectionFailure as e:
            print("yo check the mongodb: %s", e)
            raise

    def __del__(self):
        self.client.close()
        print("see ya mongodb")

    def insert_many(self, collection_name: str, documents: list[dict]):
        result = self.db[collection_name].insert_many(documents)
        return str(result.inserted_ids)
    
    # what find functions are needed?