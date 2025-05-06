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

# Simple classification function without threading
def classify_crisis_type_simple(title):
    """Simple classification function without threading dependencies"""
    if not title:
        return "none"
    
    # Direct API call
    prompt = f"Classify the type of crisis in the following sentence:\n{title}\nCrisis type:"
    
    try:
        response = requests.post(
            HF_API_URL,
            headers=HEADERS,
            json={"inputs": prompt},
            timeout=10
        )
        
        if response.status_code == 200:
            results = response.json()
            if isinstance(results, list) and len(results) > 0:
                generated_text = results[0].get("generated_text", "").strip().lower()
            else:
                generated_text = results.get("generated_text", "").strip().lower()
            
            # Extract crisis type
            crisis_type = "none"
            allowed = ["natural_disaster", "terrorist_attack", "cyberattack", 
                      "pandemic", "war", "financial_crisis", "none"]
            
            for label in allowed:
                if label in generated_text:
                    crisis_type = label
                    break
                
            return crisis_type
        else:
            print(f"API Error: {response.status_code} {response.text}")
            return "none"
    except Exception as e:
        print(f"Classification error: {e}")
        return "none"

# 7. Register UDF
classify_crisis_udf = udf(classify_crisis_type_simple, StringType())

# 8. Apply UDF to DataFrame
df_with_crisis_type = df_with_time.withColumn("crisis_type", classify_crisis_udf(col("title")))

# 9. Setup MongoDB client with connection pooling and error handling
mongo_client = MongoStorage(os.getenv("MONGODB_STRING", "mongodb://localhost:27017/"), "crisiscast_test")

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
    global embedding_model
    
    # Start timing
    start_time = time.time()
    batch_size = df.count()
    print(f"Processing batch {epoch_id} with {batch_size} records")
    
    # Initialize embedding model lazily
    if embedding_model is None:
        try:
            embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
        except Exception as e:
            print(f"Error initializing embedding model: {e}")
            return
    
    # Convert to pandas
    try:
        pandas_df = df.toPandas()
        data = pandas_df.to_dict("records")
    except Exception as e:
        print(f"Error converting to pandas: {e}")
        return
    
    if not data:
        return
            
    # Insert into the appropriate collection
    try:
        mongo_client.insert_many("unified_posts", data)
    except Exception as e:
        print(f"MongoDB error: {e}")
    
    # Qdrant vector processing
    # Process in smaller chunks to avoid memory issues
    MAX_CHUNK_SIZE = 100
    for i in range(0, len(data), MAX_CHUNK_SIZE):
        chunk = data[i:i+MAX_CHUNK_SIZE]
        
        # Qdrant Insert
        points = []
        for row in chunk:
            try:
                text = row.get('title', '').strip()
                
                if not text:
                    continue
                
                vector = embedding_model.encode(text).tolist()
                generated_id = str(uuid.uuid4())
                
                points.append(PointStruct(
                    id=generated_id,
                    vector=vector,
                    payload={
                        "title": row.get("title", ""),
                        "url": row.get("url", ""),
                        "crisis_type": row.get("crisis_type", "none"),
                        "source": row.get("source", "none"),
                        "id": row.get("id", "")
                    }
                ))
            except Exception as e:
                print(f"Error creating vector embedding: {e}")
        
        # Insert points in smaller batches
        VECTOR_BATCH_SIZE = 50
        for j in range(0, len(points), VECTOR_BATCH_SIZE):
            vector_batch = points[j:j+VECTOR_BATCH_SIZE]
            try:
                if vector_batch:
                    qdrant.upsert(
                        collection_name=COLLECTION_NAME,
                        points=vector_batch
                    )
            except Exception as e:
                print(f"Qdrant insert error: {e}")
    
    # Force garbage collection after batch processing
    gc.collect()
    
    # Log processing time
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"Batch {epoch_id} processed {batch_size} records in {elapsed:.2f} seconds ({batch_size/elapsed:.2f} records/sec)")

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
query = df_with_crisis_type.writeStream \
    .foreachBatch(write_to_all_outputs) \
    .option("checkpointLocation", "/tmp/checkpoints/reddit_stream") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()