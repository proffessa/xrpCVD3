import asyncio
import json
import time
import threading
from collections import defaultdict, deque

import websockets
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go

# ================= CONFIG =================
WINDOW_HOURS = 36
BUCKET_MS = 100
WINDOW_SEC = WINDOW_HOURS * 3600

EXCHANGES = {
    "Binance": "wss://stream.binance.com:9443/ws/xrpusdt@trade",
}

PRICE_WS = "wss://stream.binance.com:9443/ws/xrpusdt@trade"

# ================= STATE =================
cvd_data = {}
bucket_data = {}
price_data = deque(maxlen=5000)

lock = threading.Lock()

for ex in EXCHANGES:
    cvd_data[ex] = {
        "cvd": 0.0,
        "buckets": deque()
    }
    bucket_data[ex] = defaultdict(float)

# ================= HELPERS =================
def cleanup(exchange, now):
    cutoff = now - WINDOW_SEC
    data = cvd_data[exchange]

    while data["buckets"] and data["buckets"][0][0] < cutoff:
        ts, delta = data["buckets"].popleft()
        data["cvd"] -= delta

# ================= WEBSOCKETS =================
async def trade_stream(exchange, url):
    async with websockets.connect(url) as ws:
        async for msg in ws:
            data = json.loads(msg)
            qty = float(data["q"])
            is_sell = data["m"]
            delta = -qty if is_sell else qty

            now = time.time()
            bucket_ts = int(now * 1000 // BUCKET_MS * BUCKET_MS) / 1000

            with lock:
                bucket_data[exchange][bucket_ts] += delta

async def price_stream():
    async with websockets.connect(PRICE_WS) as ws:
        async for msg in ws:
            data = json.loads(msg)
            price = float(data["p"])
            ts = time.time()

            with lock:
                price_data.append((ts, price))

# ================= AGGREGATOR =================
def aggregator():
    while True:
        time.sleep(BUCKET_MS / 1000)
        now = time.time()

        with lock:
            for ex in EXCHANGES:
                for ts in list(bucket_data[ex].keys()):
                    delta = bucket_data[ex].pop(ts)
                    cvd_data[ex]["cvd"] += delta
                    cvd_data[ex]["buckets"].append((ts, delta))
                    cleanup(ex, now)

# ================= DASH APP =================
app = dash.Dash(__name__)
server = app.server

app.layout = html.Div(
    style={"backgroundColor": "#111"},
    children=[
        html.H2("XRP – Exchange Based CVD (36h) + Binance Price",
                style={"color": "white"}),

        dcc.Graph(id="graph"),

        dcc.Interval(
            id="interval",
            interval=2000,
            n_intervals=0
        )
    ]
)

@app.callback(
    Output("graph", "figure"),
    Input("interval", "n_intervals")
)
def update_graph(_):
    fig = go.Figure()

    with lock:
        # CVD per exchange
        for ex in cvd_data:
            times = []
            values = []
            running = 0

            for ts, d in cvd_data[ex]["buckets"]:
                running += d
                times.append(ts)
                values.append(running)

            fig.add_trace(go.Scatter(
                x=times,
                y=values,
                name=f"{ex} CVD",
                mode="lines"
            ))

        # Price
        if price_data:
            pt = [t for t, _ in price_data]
            pv = [p for _, p in price_data]

            fig.add_trace(go.Scatter(
                x=pt,
                y=pv,
                name="XRP Price (Binance)",
                yaxis="y2",
                line=dict(color="orange", width=2)
            ))

    fig.update_layout(
        template="plotly_dark",
        height=800,
        yaxis=dict(title="CVD"),
        yaxis2=dict(
            title="Price",
            overlaying="y",
            side="right"
        ),
        legend=dict(font=dict(color="white"))
    )

    return fig

# ================= START =================
def start_async():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for ex, url in EXCHANGES.items():
        loop.create_task(trade_stream(ex, url))

    loop.create_task(price_stream())
    loop.run_forever()

if __name__ == "__main__":
    threading.Thread(target=start_async, daemon=True).start()
    threading.Thread(target=aggregator, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
