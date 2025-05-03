from kafka import KafkaConsumer
import json

# Set up Kafka consumer
consumer = KafkaConsumer(
    'reddit_posts',
    bootstrap_servers=['localhost:9095', 'localhost:9096', 'localhost:9097'],
    auto_offset_reset='latest',  # Start at the end of the topic
    enable_auto_commit=True,
    group_id='reddit-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🧃 Kafka consumer started. Listening to 'reddit_posts' topic...\n")

for message in consumer:
    post = message.value
    print(f"\n📥 New Reddit Post from r/{post['subreddit']}")
    print(f"📝 Title: {post['title']}")
    print(f"👤 Author: {post['author']}")
    print(f"🔗 URL: {post['url']}")

