import os
import time
import praw
from dotenv import load_dotenv
import json
from kafka import KafkaProducer

# Load secrets
load_dotenv("config/.env")
print("🔑 Username loaded:", os.getenv("REDDIT_USERNAME"))
print("🔑 Client ID loaded:", os.getenv("REDDIT_CLIENT_ID")[:4], "***")

# Setup Reddit instance
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
)

reddit.read_only = True

try:
    print("✅ Logged in as:", reddit.user.me())
except Exception as e:
    print("❌ Login failed:", e)
    
producer = KafkaProducer(
    bootstrap_servers = 'localhost:9092',
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)


def main():
    print("🚀 Connected to Reddit API. Listening to subreddit...")
    # subreddit = reddit.subreddit("worldnews")
    subreddit = reddit.subreddit("news+worldnews+technology")
    # for submission in subreddit.stream.submissions(skip_existing=True):
    for submission in subreddit.stream.submissions():
        post = {
            "id": submission.id,
            "title": submission.title,
            "selftext": submission.selftext,
            "created_utc": submission.created_utc,
            "author": str(submission.author),
            "url": submission.url,
            "subreddit": str(submission.subreddit),
        }
        print(f"\n📌 {post['title']}")
        print(post)
        
        # Send to Kafka
        producer.send("reddit_posts", post)

if __name__ == "__main__":
    main()
