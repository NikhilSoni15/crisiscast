from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, window, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType
import os
import requests
import json
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
# due to local import we must run as `python -m processing.spark_kafka_consumer`
from storage.mongo_storage import MongoStorage
import uuid
import time
import threading
import gc
import psutil
from functools import lru_cache
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
import torch
from transformers import BitsAndBytesConfig
from accelerate import infer_auto_device_map
import requests

# 1. Load environment variables
load_dotenv("config/.env")
HF_TOKEN = os.getenv("HF_API_KEY")
HF_API_URL = "https://api-inference.huggingface.co/models/google/flan-t5-base"
HEADERS = {"Authorization": f"Bearer {HF_TOKEN}"}

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# 2. Create Spark Session with improved configurations
spark = SparkSession.builder \
    .appName("KafkaCrisisClassifier") \
    .config("spark.master", "local[*]") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.maxResultSize", "1g") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", "100") \
    .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# Set log level to minimize unnecessary output
spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming JSON
schema = StructType() \
    .add("id", StringType()) \
    .add("title", StringType()) \
    .add("timestamp", StringType(), True) \
    .add("author", StringType()) \
    .add("url", StringType()) \
    .add("source", StringType(), True)

# 4. Read from Kafka with updated configurations
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9095,localhost:9096,localhost:9097") \
    .option("subscribe", "reddit_posts,bluesky_posts,google_news_posts")\
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

# 5. Parse Kafka 'value' field from binary to JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json("json_str", schema)) \
    .select("data.*")

# Add timestamp for window operations and monitoring
df_with_time = df_parsed.withColumn("processing_time", current_timestamp())

def process_partition(iterator):
    from sentence_transformers import SentenceTransformer
    from qdrant_client import QdrantClient
    from qdrant_client.models import PointStruct
    import uuid

    embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
    qdrant = QdrantClient(host="localhost", port=6333)

    points = []
    for row in iterator:
        try:
            text = row.title.strip()
            if not text:
                continue

            vector = embedding_model.encode(text).tolist()
            generated_id = str(uuid.uuid4())

            point = PointStruct(
                id=generated_id,
                vector=vector,
                payload={
                    "title": row.title,
                    "url": row.url,
                    "crisis_type": row.crisis_type,
                    "source": row.source,
                    "id": row.id
                }
            )
            points.append(point)
        except Exception as e:
            print(f"Embedding error: {e}")

    # Batch insert into Qdrant
    try:
        if points:
            qdrant.upsert(collection_name="post_vectors", points=points)
    except Exception as e:
        print(f"Qdrant insert error in partition: {e}")


FASTAPI_URL = "http://localhost:8000/classify"  # or replace with public IP if remote

def classify_crisis(title: str) -> str:
    try:
        response = requests.post(FASTAPI_URL, json={"text": title})
        response.raise_for_status()
        return response.json()["crisis_type"]
    except Exception as e:
        print(f"❌ Classification failed: {e}")
        return "none"
    
def classify_crisis_batch(titles: list[str]) -> list[str]:
    try:
        response = requests.post(FASTAPI_URL + "/batch", json={"texts": titles})
        response.raise_for_status()
        return response.json()["crisis_types"]
    except Exception as e:
        print(f"❌ Batch classification failed: {e}")
        return ["none"] * len(titles)


# 9. Setup MongoDB client with connection pooling and error handling
mongo_client = MongoStorage(os.getenv("MONGODB_STRING", "mongodb://localhost:27017/"), "crisiscast_nikhil", "unified_post")

# 10. Setup Qdrant client and embedding model lazily to avoid driver memory issues
embedding_model = None
qdrant = QdrantClient(host="localhost", port=6333)
COLLECTION_NAME = "post_vectors"

# Check if collection exists before recreating
try:
    collections = qdrant.get_collections()
    collection_names = [c.name for c in collections.collections]
    if COLLECTION_NAME not in collection_names:
        qdrant.recreate_collection(
            collection_name=COLLECTION_NAME,
            vectors_config={"size": 384, "distance": "Cosine"}
        )
except Exception as e:
    print(f"Error checking/creating Qdrant collection: {e}")
    # Create collection anyway as fallback
    qdrant.recreate_collection(
        collection_name=COLLECTION_NAME,
        vectors_config={"size": 384, "distance": "Cosine"}
    )

def write_to_all_outputs(df, epoch_id):
    import time
    start_time = time.time()

    print(f"🔄 Processing batch {epoch_id} with {df.count()} records")

    # Add crisis type using FastAPI (still requires local conversion)
    pandas_df = df.toPandas()
    pandas_df["crisis_type"] = pandas_df["title"].apply(classify_crisis)
    records = pandas_df.to_dict("records")

    # Save to Mongo
    try:
        mongo_client.insert_many(records)
    except Exception as e:
        print(f"❌ MongoDB insert failed: {e}")

    # Create DataFrame with crisis_type to send to Qdrant
    df_with_crisis_type = spark.createDataFrame(pandas_df)

    # Perform distributed vectorization
    df_with_crisis_type.foreachPartition(process_partition)

    duration = time.time() - start_time
    print(f"✅ Batch {epoch_id} processed in {duration:.2f} seconds")

    # Force garbage collection after batch processing
    gc.collect()
    

# Monitor memory usage
def monitor_memory():
    while True:
        # Force garbage collection
        gc.collect()
        
        # Get process memory info
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        
        # Log memory usage
        print(f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")
        
        # Sleep for a minute
        time.sleep(60)

# Start memory monitoring in a separate thread
monitor_thread = threading.Thread(target=monitor_memory, daemon=True)
monitor_thread.start()

# 12. Start the Streaming Query with checkpoint and trigger
query = df_with_time.writeStream \
    .foreachBatch(write_to_all_outputs) \
    .option("checkpointLocation", "/tmp/checkpoints/reddit_stream") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()