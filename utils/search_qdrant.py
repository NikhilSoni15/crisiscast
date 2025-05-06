from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

# Initialize
client = QdrantClient(host="127.0.0.1", port=6333)
model = SentenceTransformer("all-MiniLM-L6-v2")

def search_reddit_vectors(query_text, top_k=5):
    vector = model.encode(query_text).tolist()
    hits = client.search(
        collection_name="post_vectors",
        query_vector=vector,
        limit=top_k
    )

    print(f"\nTop {top_k} semantic results for: '{query_text}'")
    for i, hit in enumerate(hits, 1):
        print(f"\n🔹 Result {i}")
        print(f"🔍 Score: {hit.score:.4f}")
        print(f"🧠 Title: {hit.payload.get('title')}")
        print(f"🌐 Type: {hit.payload.get('crisis_type')}")
        print(f"🔗 URL: {hit.payload.get('url')}")

# Example run
if __name__ == "__main__":
    from qdrant_client import QdrantClient

    # connect to your local Qdrant
    client = QdrantClient(host="127.0.0.1", port=6333)

    # 1. List your collections, to make sure the name is correct
    print("Collections:", [c.name for c in client.get_collections().collections])

    # 2. Scroll the first N points, with their payloads
    scroll_resp = client.scroll(
        collection_name="post_vectors",
        limit=5,             # pull back 5 records
        with_payload=True    # include the payload dict
    )

    print("\nSample points from 'post_vectors':")
    for pt in scroll_resp:
        #print(f"• id={pt.id}")
        print("  payload:", pt)
        print()

    query = input("Enter your search query: ")
    search_reddit_vectors(query)
