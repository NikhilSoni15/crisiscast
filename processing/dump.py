#!/usr/bin/env python3
import csv
import sys
from pymongo import MongoClient, errors

def export_titles(uri="mongodb://localhost:27017/", db_name="crisiscast",
                  coll_name="unified_posts", out_file="titles.csv"):
    # Connect (with a quick ping to fail fast)
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except errors.PyMongoError as e:
        print(f"Could not connect to MongoDB: {e}")
        return

    db = client[db_name]
    cursor = db[coll_name].find({}, {"_id": 0, "title": 1})

    with open(out_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["title"])
        count = 0
        for doc in cursor:
            writer.writerow([doc.get("title", "")])
            count += 1

    print(f"Exported {count} titles to '{out_file}'")
    client.close()

if __name__ == "__main__":
    # Allow overriding via command-line: python3 export_titles.py <mongo-uri> <out.csv>
    uri   = sys.argv[1] if len(sys.argv) > 1 else "mongodb://localhost:27017/"
    out   = sys.argv[2] if len(sys.argv) > 2 else "titles.csv"
    export_titles(uri, out_file=out)
