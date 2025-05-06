from kafka import KafkaConsumer
import json

# topics to subscribe to
TOPICS = ['reddit_posts', 'bluesky_posts', 'google_news_posts']

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=['localhost:9095', 'localhost:9096', 'localhost:9097'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='general-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🧃 General Kafka consumer started. Listening to:", ", ".join(TOPICS), "\n")

for message in consumer:
    post  = message.value
    topic = message.topic

    # derive a human‐friendly “source” label
    if 'source' in post:
        source_label = post['source']
    else:
        source_label = topic

    print(f"\n📥 New post from {source_label}")
    print(f"📝 Title    : {post.get('title')}")
    print(f"👤 Author   : {post.get('author')}")
    print(f"🔗 URL      : {post.get('url')}")
