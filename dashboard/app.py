import dash
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta, timezone

import os
from dotenv import load_dotenv

# due to local import we must run as `python -m dashboard.app`
from storage.mongo_storage import MongoStorage
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

load_dotenv("config/.env")

# ─── BACKEND SETUP ───────────────────────────────────────────────────────────────
mongo = MongoStorage(os.getenv("MONGODB_STRING", "mongodb://localhost:27017/"), "crisiscast", "unified_post")
qdrant = QdrantClient(host="127.0.0.1", port=6333)
embed_model = SentenceTransformer("all-MiniLM-L6-v2")
QCOL = "post_vectors"

# draw figure
def draw_initial_time_series():
    now_time = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=1)
    df = mongo.get_count_by_type_over_time(unit="minute", to_date=now_time)
    fig = go.Figure()
    for crisis_type in pd.unique(df['crisis_type']):
        sub_df = df[df['crisis_type'] == crisis_type]
        fig.add_trace(go.Scatter(
            x=sub_df['date'].tolist(),
            y=sub_df['count'].tolist(),
            mode='lines',
            name=crisis_type
        ))
    return fig

# ─── DASH APP LAYOUT ────────────────────────────────────────────────────────────
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
app.layout = dbc.Container([
    dbc.Row([
        # Live Feed column
        dbc.Col([
            html.H2("Live Feed"),
            # This div will get updated every 10 s
            html.Div(id="feed-container"),
            dcc.Interval(id="feed-interval", interval=60*1000, n_intervals=0)
        ], width=6),

        # Semantic Search column
        dbc.Col([
            html.H2("Semantic Search"),
            dcc.Input(id="search-input", placeholder="type query…", type="text", style={"width":"100%"}),
            html.Div(id="search-results", className="mt-3")
        ], width=6),
    ], className="mt-4"),
    dbc.Row([
        # time series chart
        dbc.Col([
            html.H2("Posts by crisis type by time"),
            # This div will get updated every 60 s
            dcc.Graph(id="time-series", figure=draw_initial_time_series()),
            dcc.Interval(id="time-series-interval", interval=60*1000, n_intervals=0),
        ], width=6),
    ], className="mt-4")
], fluid=True, className="p-4")

# ─── CALLBACK: Live Feed ─────────────────────────────────────────────────────────
@app.callback(
    Output("feed-container", "children"),
    Input("feed-interval", "n_intervals")
)
def update_feed(n):
    # 1) Fetch newest posts sorted by your unified timestamp
    print(f"↻ update_feed called (n_intervals={n})")  # debug line
    raw = list(
        mongo.collection.aggregate([
            { "$sort": { "timestamp": -1 } },
            { "$limit": 20 } # grab a few extra so dedupe can trim to 10
        ])
    )
    if raw:
        print("↻  fetched", len(raw), "docs; newest timestamp:", raw[0].get("timestamp"))
    else:
        print("↻  fetched 0 docs")
    # 2) Deduplicate by *title* (normalized), keep first 10 unique titles
    seen_titles = set()
    unique = []
    for doc in raw:
        title = doc.get("title", "").strip().lower()
        if title and title not in seen_titles:
            seen_titles.add(title)
            unique.append(doc)
        if len(unique) >= 10:
            break

    # 3) Build cards
    cards = []
    for d in unique:
        cards.append(
            dbc.Card([
                dbc.CardBody([
                    html.H5(d.get("title","(no title)"), className="card-title"),
                    html.P(
                        f"Time: {d.get('timestamp','')}  |  "
                        f"Type: {d.get('crisis_type','none')}  |  "
                        f"Source: {d.get('source','')}",
                        className="card-text"
                    ),
                    html.A("🔗 Source Link", href=d.get("url","#"), target="_blank")
                ])
            ], className="mb-3 bg-secondary text-white")
        )
    # If no posts, show a placeholder
    if not cards:
        cards = [html.P("No posts available yet.", className="text-muted")]

    return cards

# ─── CALLBACK: Semantic Search ──────────────────────────────────────────────────
@app.callback(
    Output("search-results", "children"),
    Input("search-input", "value")
)
def run_search(q):
    if not q:
        return ""

    # 1) Encode the query to a vector
    vec = embed_model.encode(q).tolist()

    # 2) Fetch top N raw hits from Qdrant
    raw_hits = qdrant.search(collection_name=QCOL, query_vector=vec, limit=20)

    # 3) Deduplicate by *title* (normalized), keep first 5 unique titles
    seen_titles = set()
    unique_hits = []
    for hit in raw_hits:
        title = hit.payload.get("title", "").strip().lower()
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        unique_hits.append(hit)
        if len(unique_hits) >= 5:
            break

    # 4) Build Dash cards from unique_hits
    results = []
    for idx, h in enumerate(unique_hits, start=1):
        p = h.payload
        title = p.get("title", "(no title)")
        url   = p.get("url", "#")
        ctype = p.get("crisis_type", "none")
        results.append(
            dbc.Card([
                dbc.CardBody([
                    html.H6(f"Result {idx} – Score {h.score:.3f}", className="card-subtitle"),
                    html.H5(title, className="card-title"),
                    html.P(f"Type: {ctype}", className="card-text"),
                    html.A("🔗 Link", href=url, target="_blank")
                ])
            ], className="mb-3 bg-secondary text-white")
        )
    return results 


@app.callback(
    Output("time-series", "extendData"),
    Input("time-series", "figure"),
    Input("time-series-interval", component_property="n_intervals")
)
def update_time_series(f, n):
    if n == 0:
        return None
    now_time = min(map(lambda x: datetime.fromisoformat(x['x'][-1]), f['data']))
    print(f"update_time_series: {n} {now_time}")
    df = mongo.get_count_by_type_over_time(from_date=now_time, to_date=datetime.now(), unit="minute")
    if df.empty:
        return (dict(x=[], y=[]), [])
    ts = df['date'].max()
    df = df[df['date'] == ts]
    x = []
    y = []
    traces = []
    for i, trace in enumerate(f['data']):
        traces.append(i)
        x.append(df[df['crisis_type'] == trace['name']]['date'].tolist())
        y.append(df[df['crisis_type'] == trace['name']]['count'].tolist())
    return (dict(x=x, y=y), traces)

# ─── RUN SERVER ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, port=4567)