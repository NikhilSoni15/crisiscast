from transformers import AutoTokenizer, AutoModelForCausalLM
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StringType
import os
import threading
import time
import psutil
import gc
import torch
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from storage.mongo_storage import MongoStorage
from sentence_transformers import SentenceTransformer
import uuid
from dotenv import load_dotenv
import logging

# Logging setup
logging.basicConfig(filename="pipeline.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load env vars
load_dotenv("config/.env")
MONGODB_URI = os.getenv("MONGODB_STRING", "mongodb://localhost:27017/")

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

HF_TOKEN = os.getenv("HF_API_KEY")

# Force CUDA:1 if available
DEVICE = torch.device("cuda:1" if torch.cuda.device_count() > 1 else "cuda:0")

# Load model/tokenizer once
mistral_id = "mistralai/Mistral-7B-Instruct-v0.1"
tokenizer = AutoTokenizer.from_pretrained(mistral_id, token=HF_TOKEN)
mistral_model = AutoModelForCausalLM.from_pretrained(
    mistral_id,
    torch_dtype=torch.float16,
    token=HF_TOKEN
).to(DEVICE)

# Crisis labels
crisis_labels = [
    "natural_disaster", "terrorist_attack", "cyberattack", "pandemic", "war",
    "financial_crisis", "civil_unrest", "infrastructure_failure", "environmental_crisis", "crime", "none"
]

def run_mistral_batch_classification(texts: list[str]) -> list[str]:
    prompts = [
        "<s>[INST] What type of crisis is described below? Choose one of: " +
        ", ".join(crisis_labels) + "\\n\\n" + text.strip().replace("\\n", " ") + " [/INST]"
        for text in texts
    ]
    try:
        inputs = tokenizer(prompts, return_tensors="pt", padding=True, truncation=True).to(DEVICE)
        outputs = mistral_model.generate(**inputs, max_new_tokens=10)
        results = []
        for output in outputs:
            decoded = tokenizer.decode(output, skip_special_tokens=True)
            label = decoded.split("[/INST]")[-1].strip().lower().split()[0].strip(",.:;\n")
            if label not in crisis_labels:
                label = "none"
            results.append(label)
        return results
    except Exception as e:
        logging.error(f"Mistral classification error: {e}")
        return ["none"] * len(texts)

# Create Spark Session
spark = SparkSession.builder \
    .appName("CrisisClassifierDistributed") \
    .config("spark.master", "local[*]") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka Schema
data_schema = StructType() \
    .add("id", StringType()) \
    .add("title", StringType()) \
    .add("timestamp", StringType()) \
    .add("author", StringType()) \
    .add("url", StringType()) \
    .add("source", StringType())

# Read stream from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9095,localhost:9096,localhost:9097") \
    .option("subscribe", "reddit_posts,bluesky_posts,google_news_posts") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .withColumn("data", from_json(col("value"), data_schema)) \
    .select("data.*") \
    .withColumn("processing_time", current_timestamp())

def process_partition(partition):
    try:
        qdrant = QdrantClient(host="localhost", port=6333)
        mongo = MongoStorage(MONGODB_URI, "crisiscast_nikhil", "unified_post")
        embed_model = SentenceTransformer("all-MiniLM-L6-v2")

        rows = list(partition)
        BATCH_SIZE = 5
        qdrant_points = []
        mongo_docs = []

        for i in range(0, len(rows), BATCH_SIZE):
            mini_rows = rows[i:i + BATCH_SIZE]
            mini_titles = [r.title.strip() for r in mini_rows]
            mini_crisis_types = run_mistral_batch_classification(mini_titles)

            for row, crisis_type in zip(mini_rows, mini_crisis_types):
                try:
                    vector = embed_model.encode(row.title.strip()).tolist()
                    qdrant_points.append(PointStruct(
                        id=str(uuid.uuid4()),
                        vector=vector,
                        payload={
                            "title": row.title,
                            "url": row.url,
                            "crisis_type": crisis_type,
                            "source": row.source,
                            "id": row.id
                        }
                    ))
                    row_dict = row.asDict()
                    row_dict["crisis_type"] = crisis_type
                    mongo_docs.append(row_dict)
                except Exception as e:
                    logging.warning(f"Row processing error: {e}")

        if mongo_docs:
            try:
                mongo.insert_many(mongo_docs)
            except Exception as e:
                logging.error(f"Mongo insert error: {e}")

        if qdrant_points:
            try:
                qdrant.upsert(collection_name="post_vectors", points=qdrant_points)
            except Exception as e:
                logging.error(f"Qdrant insert error: {e}")
    except Exception as e:
        logging.critical(f"Partition-level crash: {e}")

def monitor_memory():
    while True:
        gc.collect()
        mem = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        logging.info(f"🧠 Memory usage: {mem:.2f} MB")
        time.sleep(60)

threading.Thread(target=monitor_memory, daemon=True).start()

# Launch query
query = df_parsed.writeStream \
    .foreachBatch(lambda df, epoch_id: df.foreachPartition(process_partition)) \
    .option("checkpointLocation", "/tmp/checkpoints/reddit_stream_final") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
