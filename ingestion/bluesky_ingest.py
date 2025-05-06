import os
import time
import json
from atproto import Client
from atproto_client.exceptions import RequestException, BadRequestError
from kafka import KafkaProducer
from dotenv import load_dotenv
from dateutil import parser
from datetime import datetime, timezone

load_dotenv("config/.env")
HANDLE        = os.getenv("BLUESKY_USERNAME")
PASSWORD      = os.getenv("BLUESKY_PASSWORD")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 30))

def at_uri_to_url(at_uri: str) -> str:
    """
    Convert an AT-URI like:
      at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3k4duaz5vfs2b
    into a web link:
      https://bsky.app/profile/did:plc:z72i7hdynmk6r22z27h6tvur/post/3k4duaz5vfs2b
    """
    path = at_uri.replace("at://", "")
    authority, _, rkey = path.split("/", 2)
    return f"https://bsky.app/profile/{authority}/post/{rkey}"

def stream_whats_hot():
    client = Client()
    client.login(HANDLE, PASSWORD)

    # set up Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9095', 'localhost:9096', 'localhost:9097'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    feed_uri       = (
        "at://did:plc:z72i7hdynmk6r22z27h6tvur/"
        "app.bsky.feed.generator/whats-hot"
    )
    cursor         = None
    seen_uris      = set()
    allowed_topics = {"t-news", "nettop"}

    print("🚀 Streaming Bluesky “What’s Hot”…")
    while True:
        params = {"feed": feed_uri, "limit": 50}
        if cursor:
            params["cursor"] = cursor

        # resilience to 400, 500, 503, and any other errors
        backoff = 1
        resp = None
        while True:
            try:
                resp = client.app.bsky.feed.get_feed(params)
                break
            except BadRequestError:
                # 400 InvalidRequest: bad cursor—reset & skip this round
                print("⚠️ 400 InvalidRequest from Bluesky; resetting cursor and skipping this round.")
                cursor = None
                break
            except RequestException as e:
                code = e.response.status_code if e.response else None
                # retry on server errors
                if code in (500, 503) or getattr(e, "error", "") == "NotEnoughResources":
                    print(f"⚠️ {code or 'ServerError'} from Bluesky; backing off {backoff}s…")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue
                # unknown request error: log and retry
                print(f"⚠️ RequestException: {e}; retrying in {backoff}s…")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue
            except Exception as e:
                # any other error
                print(f"⚠️ Unexpected error: {e}; retrying in {backoff}s…")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

        # if we never got a response (due to BadRequestError), skip processing
        if resp is None:
            time.sleep(POLL_INTERVAL)
            continue

        posts  = resp.feed
        cursor = resp.cursor

        for item in posts:
            try:
                if item.feed_context not in allowed_topics:
                    continue
                text = item.post.record.text or ""
                if not text.strip():
                    continue

                uri = item.post.uri
                if uri in seen_uris:
                    continue
                seen_uris.add(uri)

                post = item.post
                data = {
                    "id":        post.cid,
                    "title":     text,
                    "timestamp": parser.isoparse(post.indexed_at)
                                            .astimezone(timezone.utc)
                                            .isoformat(),
                    "author":    post.author.handle,
                    "url":       at_uri_to_url(post.uri),
                    "source":    "Bluesky: What's Hot",
                }

                print(f"\n📌 {data['title']}")
                print(data)

                producer.send("bluesky_posts", data)
            except Exception as e:
                print(f"⚠️ Error processing item {getattr(item.post, 'cid', '<unknown>')}: {e}; skipping.")

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        stream_whats_hot()
    except KeyboardInterrupt:
        print("\n🛑 Stream stopped by user")
    except Exception as e:
        # catch anything unexpected at top level and keep running
        print(f"⚠️ Fatal error in main loop: {e}; restarting stream…")
        time.sleep(5)
        stream_whats_hot()
