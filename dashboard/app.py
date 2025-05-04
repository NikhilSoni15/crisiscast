# dashboard/app.py
import streamlit as st
from pymongo import MongoClient
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer
import pandas as pd
from datetime import datetime, timedelta

# SETUP
st.set_page_config(page_title="CrisisCast Dashboard", layout="wide")

# Mongo
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["crisiscast"]
reddit_collection = db["reddit_posts"]
bluesky_collection = db["bluesky_posts"]
google_collection = db["google_news_posts"]

# Qdrant
qdrant = QdrantClient(host="localhost", port=6333)
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
COLLECTION_NAME = "reddit_vectors"

# UI
st.title("CrisisCast Dashboard")
st.markdown("Monitor real-time global emergencies and explore them using semantic search.")

# TABS
tab1, tab2, tab3, tab4 = st.tabs(["All Sources", "Reddit", "BlueSky", "Google News"])

# Function to format posts
def display_posts(posts, source_name):
    if not posts:
        st.write(f"No recent {source_name} posts found")
        return
        
    for post in posts:
        title = post.get('title', '')
        source = post.get('source', source_name)
        crisis_type = post.get('crisis_type', 'none')
        url = post.get('url', '#')
        
        st.markdown(f"**{title}**")
        st.write(f"Source: `{source}` | Type: `{crisis_type}`")
        st.markdown(f"[🔗 Source Link]({url})")
        st.divider()

# ALL SOURCES TAB
with tab1:
    st.header("Live Feed (All Sources)")
    
    # Get posts from all collections
    reddit_posts = list(reddit_collection.find().sort("timestamp", -1).limit(5))
    bluesky_posts = list(bluesky_collection.find().sort("timestamp", -1).limit(5))
    google_posts = list(google_collection.find().sort("timestamp", -1).limit(5))
    
    # Combine all posts and sort by timestamp
    all_posts = reddit_posts + bluesky_posts + google_posts
    
    # Sort by timestamp (descending)
    all_posts.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
    
    # Display posts
    for post in all_posts[:10]:  # Show most recent 10 posts
        source = post.get('source', 'Unknown')
        title = post.get('title', '')
        crisis_type = post.get('crisis_type', 'none')
        url = post.get('url', '#')
        
        st.markdown(f"**{title}**")
        st.write(f"Source: `{source}` | Type: `{crisis_type}`")
        st.markdown(f"[🔗 Source Link]({url})")
        st.divider()

# REDDIT TAB
with tab2:
    st.header("Reddit Posts")
    reddit_posts = list(reddit_collection.find().sort("timestamp", -1).limit(10))
    display_posts(reddit_posts, "Reddit")

# BLUESKY TAB
with tab3:
    st.header("BlueSky Posts")
    bluesky_posts = list(bluesky_collection.find().sort("timestamp", -1).limit(10))
    display_posts(bluesky_posts, "BlueSky")

# GOOGLE NEWS TAB
with tab4:
    st.header("Google News Posts")
    google_posts = list(google_collection.find().sort("timestamp", -1).limit(10))
    display_posts(google_posts, "Google News")

# SEMANTIC SEARCH (right column)
col1, col2 = st.columns([2, 1])

with col2:
    st.header("🔍 Semantic Search")
    query = st.text_input("Enter search query (e.g., 'india flood', 'terrorist attack')")
    
    if query:
        vector = embedding_model.encode(query).tolist()
        results = qdrant.query_points(collection_name=COLLECTION_NAME, query_vector=vector, limit=5)
        
        if not results or not results.points:
            st.write("No results found.")
        else:
            for i, item in enumerate(results.points):
                payload = item.payload
                st.markdown(f"**Result {i+1} - Score: {round(item.score, 3)}**")
                st.write(f"Type: `{payload.get('crisis_type', 'none')}`")
                st.markdown(f"**{payload.get('title', '')}**")
                st.markdown(f"[🔗 Link]({payload.get('url', '#')})")
                st.divider()