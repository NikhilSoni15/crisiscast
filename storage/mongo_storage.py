# typing imports
from pymongo.synchronous.database import Database
from pymongo.synchronous.mongo_client import MongoClient
from typing import Any

# actual imports
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
import pandas as pd

ALL_CRISES=["natural_disaster", "terrorist_attack", "cyberattack", "pandemic", "war", "financial_crisis", "none"]

MONGODB_UNIT_TO_PANDAS_FREQ = {
    "year": "YE",
    "quarter": "QE",
    "week": "W",
    "month": "ME",
    "day": "D",
    "hour": "h",
    "minute": "min",
    "second": "s",
}

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
    
    def get_count_by_type_over_time(self, from_date=None, to_date=None, unit="day"):
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "type": "$crisis_type",
                        "date": {
                            "$dateTrunc": {
                                "date": "$timestamp",
                                "unit": unit  # Can be "hour", "week", "month", etc.
                            }
                        }
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "crisis_type": "$_id.type",
                    "date": "$_id.date",
                    "count": 1,
                }
            },
            {
                "$sort": {
                    "date": 1,
                    "type": 1
                }
            }
        ]
        if from_date is not None and to_date is not None:
            pipeline.insert(0, {
                "$match": {
                    "timestamp": {
                        "$gte": from_date,
                        "$lt": to_date
                    }
                }
            })
        results = self.collection.aggregate(pipeline)
        df = pd.DataFrame(list(results))
        # fill in missing timestamps: https://stackoverflow.com/a/49187796
        dates = pd.date_range(
            start=df['date'].min(),
            end=df['date'].max(),
            freq=MONGODB_UNIT_TO_PANDAS_FREQ[unit]
        ).to_pydatetime()
        df = df.set_index('date').reindex(dates).reset_index().reindex(columns=df.columns)
        cols = df.columns.difference(['count'])
        df[cols] = df[cols].ffill()
        df = df.fillna(0)
        print(df)
        return df

