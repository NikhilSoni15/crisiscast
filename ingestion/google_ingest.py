import os
import time
import json
from pathlib import Path
import requests
import feedparser
import certifi
from kafka import KafkaProducer
from dateutil import parser as date_parser
from datetime import timezone
from dotenv import load_dotenv
from requests.exceptions import RequestException

env_path = Path(__file__).parent.parent.absolute() / ".env"
load_dotenv(env_path)
RSS_URL       = os.getenv("NEWS_RSS_URL")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 60))

def stream_news():
    seen = set()

    # set up Kafka producer
    producer = KafkaProducer(
    bootstrap_servers=['localhost:9095', 'localhost:9096', 'localhost:9097'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',  # Wait for all replicas
    retries=5,  # Retry 5 times if failure
    batch_size=16384,  # Batch size in bytes
    linger_ms=100  # Wait time for batching
)

    print("🚀 Streaming RSS from", RSS_URL)
    while True:
        # --- fetch+parse via requests+certifi with back-off ---
        backoff = 1
        while True:
            try:
                resp = requests.get(RSS_URL,
                                    timeout=10,
                                    verify=certifi.where())
                resp.raise_for_status()
                feed = feedparser.parse(resp.text)
                if feed.bozo:
                    # parsing-level error
                    raise feed.bozo_exception or Exception("feedparser bozo error")
                break
            except Exception as e:
                print(f"⚠️ Fetch/parse error: {e}; retrying in {backoff}s…")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
        # --- end resilient fetch ---

        for e in feed.entries:
            uid = e.get("id") or e.link
            if uid in seen:
                continue
            seen.add(uid)

            # parse published timestamp into UTC ISO
            published_str = e.get("published") or e.get("updated") or ""
            try:
                dt = date_parser.parse(published_str)
                timestamp = dt.astimezone(timezone.utc).isoformat()
            except Exception:
                timestamp = None

            # pull publisher from <source> if author is missing
            publisher = getattr(e, "source", None)
            publisher = publisher.title if publisher else None

            data = {
                "id":        uid,
                "title":     e.title,
                "timestamp": timestamp,
                "author":    e.get("author") or publisher,
                "url":       e.link,
                "source":    "Google News RSS",
            }

            print(f"\n📌 {data['title']}")
            print(data)

            producer.send("google_news_posts", data)

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        stream_news()
    except KeyboardInterrupt:
        print("\n🛑 Stream stopped by user")
