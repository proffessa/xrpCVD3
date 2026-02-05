import asyncio
import time
from collections import deque, defaultdict

import websockets
import json
import threading

from flask import Flask, render_template_string
import plotly.graph_objs as go

# ---------------- CONFIG ----------------
WINDOW_HOURS = 36
BUCKET_MS = 100
MAX_WINDOW_SEC = WINDOW_HOURS * 3600

EXCHANGES = {
    "binance": "wss://stream.binance.com:9443/ws/xrpusdt@trade",
}

PRICE_WS = "wss://stream.binance.com:9443/ws/xrpusdt@trade"

# ---------------- STATE ----------------
cvd = 0.0
cvd_buckets = deque()  # (timestamp, delta)
bucket_accumulator = defaultdict(float)

price_points = deque(maxlen=5000)

lock = threading.Lock()

# ---------------- HELPERS ----------------
def cleanup_old_data(now):
    global cvd
    cutoff = now - MAX_WINDOW_SEC
    while cvd_buckets and cvd_buckets[0][0] < cutoff:
        ts, delta = cvd_buckets.popleft()
        cvd -= delta

# ---------------- WEBSOCKET HANDLERS ----------------
async def binance_trades():
    global cvd
    async with websockets.connect(EXCHANGES["binance"]) as ws:
        async for msg in ws:
            data = json.loads(msg)
            qty = float(data["q"])
            is_sell = data["m"]
            delta = -qty if is_sell else qty

            now = time.time()
            bucket_ts = int(now * 1000 // BUCKET_MS * BUCKET_MS) / 1000

            with lock:
                bucket_accumulator[bucket_ts] += delta

async def price_stream():
    async with websockets.connect(PRICE_WS) as ws:
        async for msg in ws:
            data = json.loads(msg)
            price = float(data["p"])
            ts = time.time()

            with lock:
                price_points.append((ts, price))

# ---------------- AGGREGATOR ----------------
def aggregator():
    global cvd
    while True:
        time.sleep(BUCKET_MS / 1000)
        now = time.time()

        with lock:
            keys = list(bucket_accumulator.keys())
            for k in keys:
                delta = bucket_accumulator.pop(k)
                cvd += delta
                cvd_buckets.append((k, delta))

            cleanup_old_data(now)

# ---------------- FLASK APP ----------------
app = Flask(__name__)

@app.route("/")
def index():
    with lock:
        times = [t for t, _ in cvd_buckets]
        cvd_vals = []
        running = 0
        for _, d in cvd_buckets:
            running += d
            cvd_vals.append(running)

        price_t = [t for t, _ in price_points]
        price_v = [p for _, p in price_points]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=times,
        y=cvd_vals,
        name="CVD (36h)",
        yaxis="y1",
        line=dict(color="cyan")
    ))

    fig.add_trace(go.Scatter(
        x=price_t,
        y=price_v,
        name="XRP Price (Binance)",
        yaxis="y2",
        line=dict(color="orange")
    ))

    fig.update_layout(
        template="plotly_dark",
        title="XRP Multi-Exchange CVD (36h) + Binance Price",
        yaxis=dict(title="CVD"),
        yaxis2=dict(
            title="Price",
            overlaying="y",
            side="right"
        ),
        height=800
    )

    return fig.to_html(full_html=True)

# ---------------- START ----------------
def start_async():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(binance_trades())
    loop.create_task(price_stream())
    loop.run_forever()

if __name__ == "__main__":
    threading.Thread(target=start_async, daemon=True).start()
    threading.Thread(target=aggregator, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
