from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StringType, DoubleType
import os
import requests
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
# due to local import we must run as `python -m processing.spark_kafka_consumer`
from storage.mongo_storage import MongoStorage
import uuid

# 1. Load environment variables
load_dotenv("config/.env")  # Ensure the path is correct
HF_TOKEN = os.getenv("HF_API_KEY")
HF_API_URL = "https://api-inference.huggingface.co/models/google/flan-t5-base"
HEADERS = {"Authorization": f"Bearer {HF_TOKEN}"}

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# 2. Create Spark Session
spark = SparkSession.builder \
    .appName("RedditKafkaCrisisClassifier") \
    .master("local[*]") \
    .getOrCreate()

# 3. Define schema for incoming JSON
schema = StructType() \
    .add("id", StringType()) \
    .add("title", StringType()) \
    .add("selftext", StringType()) \
    .add("created_utc", DoubleType()) \
    .add("author", StringType()) \
    .add("url", StringType()) \
    .add("subreddit", StringType())

# 4. Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit_posts") \
    .option("startingOffsets", "latest") \
    .load()

# 5. Parse Kafka 'value' field from binary to JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json("json_str", schema)) \
    .select("data.*")

# 6. Crisis Classification Function
def classify_crisis_type(title, selftext):
    if not title:
        return "none"
    prompt = f"Classify the type of crisis in the following sentence:\n{title}\nCrisis type:"
    try:
        response = requests.post(
            HF_API_URL,
            headers=HEADERS,
            json={"inputs": prompt},
            timeout=10
        )
        if response.status_code != 200:
            print(f"Error: {response.status_code} {response.text}")
            return "none"
        result = response.json()[0]["generated_text"].strip().lower()
        allowed = ["natural_disaster", "terrorist_attack", "cyberattack", "pandemic", "war", "financial_crisis", "none"]
        for label in allowed:
            if label in result:
                return label
        return "none"
    except Exception as e:
        print(f"Exception in classify_crisis_type: {e}")
        return "none"

# 7. Register UDF
classify_crisis_udf = udf(classify_crisis_type, StringType())

# 8. Apply UDF to DataFrame
df_with_crisis_type = df_parsed.withColumn("crisis_type", classify_crisis_udf(col("title"), col("selftext")))

# 9. Setup MongoDB client (outside function for efficiency)
mongo_client = MongoStorage(os.getenv("MONGODB_STRING", ""), "crisiscast_test")

# 10. Setup Qdrant client and embedding model (also outside)
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
qdrant = QdrantClient(host="localhost", port=6333)

COLLECTION_NAME = "reddit_vectors"
qdrant.recreate_collection(
    collection_name=COLLECTION_NAME,
    vectors_config={"size": 384, "distance": "Cosine"}
)

# 11. Final batch write function (Mongo + Qdrant together)
def write_to_all_outputs(df, epoch_id):
    data = df.toPandas().to_dict("records")
    if not data:
        return
    
    # MongoDB Insert
    mongo_client.insert_many("reddit_posts", data)
    
    # Qdrant Insert
    points = []
    for row in data:
        text = f"{row.get('title', '')} {row.get('selftext', '')}".strip()
        if not text:
            continue
        vector = embedding_model.encode(text).tolist()
        
        # Generate random UUID instead of using Reddit post ID
        generated_id = str(uuid.uuid4())
        
        points.append(PointStruct(
            id=generated_id,
            vector=vector,
            payload={
                "title": row.get("title", ""),
                "url": row.get("url", ""),
                "crisis_type": row.get("crisis_type", "none"),
                "reddit_id": row.get("id", "")  # (optional: keep original Reddit id inside payload if you want)
            }
        ))
    
    if points:
        qdrant.upsert(
            collection_name=COLLECTION_NAME,
            points=points
        )

# 12. Start the Streaming Query
df_with_crisis_type.writeStream \
    .foreachBatch(write_to_all_outputs) \
    .start() \
    .awaitTermination()
