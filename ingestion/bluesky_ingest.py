import os
import time
import json
from atproto import Client
from atproto_client.exceptions import RequestException, BadRequestError
from kafka import KafkaProducer
from dotenv import load_dotenv
from dateutil import parser
from datetime import timezone
from fast_langdetect import detect, detect_multilingual, DetectError

load_dotenv("config/.env")
HANDLE        = os.getenv("BLUESKY_USERNAME")
PASSWORD      = os.getenv("BLUESKY_PASSWORD")
POLL_INTERVAL = 20


def at_uri_to_url(at_uri: str) -> str:
    path = at_uri.replace("at://", "")
    authority, _, rkey = path.split("/", 2)
    return f"https://bsky.app/profile/{authority}/post/{rkey}"

def stream_bluesky_feeds():
    client = Client()
    client.login(HANDLE, PASSWORD)

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9095','localhost:9096','localhost:9097'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Feeds to pull
    feed_streams = {
        "whats_hot":     "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/whats-hot",
        "verified_news": "at://did:plc:kkf4naxqmweop7dv4l2iqqf5/app.bsky.feed.generator/verified-news",
    }
    cursors     = {name: None for name in feed_streams}
    seen_uris   = set()
    # Only apply this filter to the What's Hot feed:
    allowed_topics = {"t-news"}

    print("🚀 Streaming Bluesky feeds (What’s Hot + Verified News)…")
    while True:
        for feed_name, feed_uri in feed_streams.items():
            params = {"feed": feed_uri, "limit": 50}
            if cursors[feed_name]:
                params["cursor"] = cursors[feed_name]

            # backoff + resilience
            backoff = 1
            resp = None
            while True:
                try:
                    resp = client.app.bsky.feed.get_feed(params)
                    break
                except BadRequestError:
                    print(f"⚠️ 400 InvalidRequest on '{feed_name}'; resetting cursor.")
                    cursors[feed_name] = None
                    break
                except RequestException as e:
                    code = e.response.status_code if e.response else None
                    if code in (500,503) or getattr(e, "error","") == "NotEnoughResources":
                        print(f"⚠️ Server {code} on '{feed_name}'; backing off {backoff}s…")
                        time.sleep(backoff)
                        backoff = min(backoff * 2, 60)
                        continue
                    print(f"⚠️ RequestException on '{feed_name}': {e}; backing off {backoff}s")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue
                except Exception as e:
                    print(f"⚠️ Unexpected error on '{feed_name}': {e}; backing off {backoff}s")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue

            if resp is None:
                continue

            posts         = resp.feed
            cursors[feed_name] = resp.cursor

            for item in posts:
                try:
                    # if we're in What's Hot, enforce topic filter:
                    if feed_name == "whats_hot":
                        ctx = getattr(item.feed_context, "type", None) or item.feed_context
                        if ctx not in allowed_topics:
                            continue

                    text = item.post.record.text or ""
                    text = text.replace("\n", "")
                    if not text.strip():
                        continue
                    
                    # language filter via fast-langdetect
                    try:
                        res = detect(text[:100].strip(), low_memory=False)              
                        if res["lang"] != "en":
                            continue                    
                    except DetectError:
                        continue   

                    uri = item.post.uri
                    if uri in seen_uris:
                        continue
                    seen_uris.add(uri)

                    data = {
                        "id":        item.post.cid,
                        "title":     text,
                        "timestamp": parser.isoparse(item.post.indexed_at)
                                                   .astimezone(timezone.utc)
                                                   .isoformat(),
                        "author":    item.post.author.handle,
                        "url":       at_uri_to_url(uri),
                        "source":    f"Bluesky: {feed_name.replace('_',' ').title()}",
                    }

                    # Re-added your original prints:
                    print(f"\n📌 {data['title']}")
                    print(data)

                    producer.send("bluesky_posts", data)

                except Exception as e:
                    print(f"⚠️ Error processing {feed_name} item {uri}: {e}")

        time.sleep(POLL_INTERVAL)

'''
if __name__ == "__main__":
    try:
        stream_bluesky_feeds()
    except KeyboardInterrupt:
        print("\n🛑 Stream stopped by user.")
    except Exception as e:
        print(f"⚠️ Fatal error: {e}; restarting in 5s…")
        time.sleep(5)
        stream_bluesky_feeds()
'''