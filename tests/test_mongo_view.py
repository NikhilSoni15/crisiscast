from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pprint

# Load environment variables from your config file
load_dotenv("config/.env")

# Connect to MongoDB
client = MongoClient(os.getenv("MONGODB_STRING"))
db = client["crisiscast_nikhil"]  # ← temporarily point to this
collection = db["unified_post"]

# 1. Show most recent documents
print("🔍 Last 5 documents:")
for doc in collection.find().sort("timestamp", -1).limit(5):
    pprint.pprint(doc)

# 2. Show documents that were not 'none'
print("\n✅ Documents with actual predicted crisis_type:")
for doc in collection.find({"crisis_type": {"$ne": "none"}}).sort("timestamp", -1).limit(5):
    pprint.pprint(doc)

# 3. Count documents with none vs non-none
none_count = collection.count_documents({"crisis_type": "none"})
non_none_count = collection.count_documents({"crisis_type": {"$ne": "none"}})

print(f"\n📊 Count of documents:")
print(f"   • 'none':     {none_count}")
print(f"   • non-'none': {non_none_count}")
