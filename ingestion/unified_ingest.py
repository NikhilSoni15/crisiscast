import threading

from reddit_ingest import main as reddit_main

from bluesky_ingest import stream_bluesky_feeds

from google_ingest import stream_news



# Start each ingestion script in its own thread

threads = [

    threading.Thread(target=reddit_main),

    threading.Thread(target=stream_bluesky_feeds),

    threading.Thread(target=stream_news)

]



for thread in threads:

    thread.start()



for thread in threads:

    thread.join()