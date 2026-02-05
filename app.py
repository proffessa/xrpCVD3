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

# =====================================================
# CONFIG
# =====================================================
WINDOW_HOURS = 36
WINDOW_SEC = WINDOW_HOURS * 3600
BUCKET_MS = 100

# =====================================================
# EXCHANGES – WS ENDPOINTS
# =====================================================
EXCHANGES = {
    "Binance": {
        "url": "wss://stream.binance.com:9443/ws/xrpusdt@trade",
        "type": "binance"
    },
    "Coinbase": {
        "url": "wss://ws-feed.exchange.coinbase.com",
        "type": "coinbase"
    },
    "OKX": {
        "url": "wss://ws.okx.com:8443/ws/v5/public",
        "type": "okx"
    },
    "Bybit": {
        "url": "wss://stream.bybit.com/v5/public/spot",
        "type": "bybit"
    },
    "Kraken": {
        "url": "wss://ws.kraken.com",
        "type": "kraken"
    },
    "Bitstamp": {
        "url": "wss://ws.bitstamp.net",
        "type": "bitstamp"
    }
}

PRICE_WS = "wss://stream.binance.com:9443/ws/xrpusdt@trade"

# =====================================================
# STATE
# =====================================================
cvd = {}
buckets = {}
price_data = deque(maxlen=5000)
lock = threading.Lock()

for ex in EXCHANGES:
    cvd[ex] = {
        "value": 0.0,
        "data": deque()
    }
    buckets[ex] = defaultdict(float)

# =====================================================
# HELPERS
# =====================================================
def cleanup(exchange, now):
    cutoff = now - WINDOW_SEC
    while cvd[exchange]["data"] and cvd[exchange]["data"][0][0] < cutoff:
        ts, delta = cvd[exchange]["data"].popleft()
        cvd[exchange]["value"] -= delta

def bucket_ts():
    return int(time.time() * 1000 // BUCKET_MS * BUCKET_MS) / 1000

# =====================================================
# PARSERS
# =====================================================
async def binance_ws():
    async with websockets.connect(EXCHANGES["Binance"]["url"]) as ws:
        async for msg in ws:
            d = json.loads(msg)
            qty = float(d["q"])
            delta = -qty if d["m"] else qty
            with lock:
                buckets["Binance"][bucket_ts()] += delta

async def coinbase_ws():
    async with websockets.connect(EXCHANGES["Coinbase"]["url"]) as ws:
        await ws.send(json.dumps({
            "type": "subscribe",
            "channels": [{"name": "matches", "product_ids": ["XRP-USD"]}]
        }))
        async for msg in ws:
            d = json.loads(msg)
            if d.get("type") == "match":
                qty = float(d["size"])
                delta = qty if d["side"] == "buy" else -qty
                with lock:
                    buckets["Coinbase"][bucket_ts()] += delta

async def okx_ws():
    async with websockets.connect(EXCHANGES["OKX"]["url"]) as ws:
        await ws.send(json.dumps({
            "op": "subscribe",
            "args": [{"channel": "trades", "instId": "XRP-USDT"}]
        }))
        async for msg in ws:
            d = json.loads(msg)
            if "data" in d:
                for t in d["data"]:
                    qty = float(t["sz"])
                    delta = qty if t["side"] == "buy" else -qty
                    with lock:
                        buckets["OKX"][bucket_ts()] += delta

async def bybit_ws():
    async with websockets.connect(EXCHANGES["Bybit"]["url"]) as ws:
        await ws.send(json.dumps({
            "op": "subscribe",
            "args": ["publicTrade.XRPUSDT"]
        }))
        async for msg in ws:
            d = json.loads(msg)
            if "data" in d:
                for t in d["data"]:
                    qty = float(t["v"])
                    delta = qty if t["S"] == "Buy" else -qty
                    with lock:
                        buckets["Bybit"][bucket_ts()] += delta

async def kraken_ws():
    async with websockets.connect(EXCHANGES["Kraken"]["url"]) as ws:
        await ws.send(json.dumps({
            "event": "subscribe",
            "pair": ["XRP/USD"],
            "subscription": {"name": "trade"}
        }))
        async for msg in ws:
            d = json.loads(msg)
            if isinstance(d, list):
                for t in d[1]:
                    qty = float(t[1])
                    delta = qty if t[3] == "b" else -qty
                    with lock:
                        buckets["Kraken"][bucket_ts()] += delta

async def bitstamp_ws():
    async with websockets.connect(EXCHANGES["Bitstamp"]["url"]) as ws:
        await ws.send(json.dumps({
            "event": "bts:subscribe",
            "data": {"channel": "live_trades_xrpusd"}
        }))
        async for msg in ws:
            d = json.loads(msg)
            if "data" in d and "amount" in d["data"]:
                qty = float(d["data"]["amount"])
                delta = qty if d["data"]["type"] == 0 else -qty
                with lock:
                    buckets["Bitstamp"][bucket_ts()] += delta

async def price_ws():
    async with websockets.connect(PRICE_WS) as ws:
        async for msg in ws:
            d = json.loads(msg)
            price_data.append((time.time(), float(d["p"])))

# =====================================================
# AGGREGATOR
# =====================================================
def aggregator():
    while True:
        time.sleep(BUCKET_MS / 1000)
        now = time.time()
        with lock:
            for ex in buckets:
                for ts in list(buckets[ex].keys()):
                    delta = buckets[ex].pop(ts)
                    cvd[ex]["value"] += delta
                    cvd[ex]["data"].append((ts, delta))
                    cleanup(ex, now)

# =====================================================
# DASH UI
# =====================================================
app = dash.Dash(__name__)
server = app.server

app.layout = html.Div(
    style={"backgroundColor": "#111"},
    children=[
        html.H2("XRP – Exchange Based CVD (36h) + Binance Price",
                style={"color": "white"}),
        dcc.Graph(id="graph"),
        dcc.Interval(id="interval", interval=2000, n_intervals=0)
    ]
)

@app.callback(Output("graph", "figure"), Input("interval", "n_intervals"))
def update(_):
    fig = go.Figure()

    with lock:
        for ex in cvd:
            t, v, r = [], [], 0
            for ts, d in cvd[ex]["data"]:
                r += d
                from datetime import datetime

t.append(datetime.fromtimestamp(ts / 1000))
                v.append(r)
            fig.add_trace(go.Scatter(x=t, y=v, name=f"{ex} CVD"))

        if price_data:
            pt = [x[0] for x in price_data]
            pv = [x[1] for x in price_data]
            fig.add_trace(go.Scatter(
                x=pt, y=pv, name="XRP Price (Binance)",
                yaxis="y2", line=dict(color="orange", width=2)
            ))

    fig.update_layout(
        template="plotly_dark",
        height=800,
        yaxis=dict(title="CVD"),
        yaxis2=dict(title="Price", overlaying="y", side="right")
    )
    return fig

# =====================================================
# START
# =====================================================
def start_async():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.create_task(binance_ws())
    loop.create_task(coinbase_ws())
    loop.create_task(okx_ws())
    loop.create_task(bybit_ws())
    loop.create_task(kraken_ws())
    loop.create_task(bitstamp_ws())
    loop.create_task(price_ws())

    loop.run_forever()

if __name__ == "__main__":
    threading.Thread(target=start_async, daemon=True).start()
    threading.Thread(target=aggregator, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
