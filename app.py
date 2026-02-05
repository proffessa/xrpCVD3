import asyncio
import json
import threading
from datetime import datetime

import pandas as pd
import websockets
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go

# =========================
# GLOBAL DATA
# =========================
EXCHANGES = ["Binance", "Coinbase", "OKX", "Bybit", "Kraken"]

data = {ex: [] for ex in EXCHANGES}
cvd = {ex: 0.0 for ex in EXCHANGES}

price_data = []  # Binance XRP price

LOCK = threading.Lock()

# =========================
# WEBSOCKETS
# =========================
async def binance_cvd():
    url = "wss://stream.binance.com:9443/ws/xrpusdt@trade"
    async with websockets.connect(url) as ws:
        while True:
            msg = json.loads(await ws.recv())
            vol = float(msg["q"])
            delta = -vol if msg["m"] else vol

            with LOCK:
                cvd["Binance"] += delta
                data["Binance"].append((datetime.utcnow(), cvd["Binance"]))


async def binance_price():
    url = "wss://stream.binance.com:9443/ws/xrpusdt@trade"
    async with websockets.connect(url) as ws:
        while True:
            msg = json.loads(await ws.recv())
            price = float(msg["p"])

            with LOCK:
                price_data.append((datetime.utcnow(), price))


async def coinbase():
    url = "wss://ws-feed.exchange.coinbase.com"
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({
            "type": "subscribe",
            "channels": [{"name": "matches", "product_ids": ["XRP-USD"]}]
        }))

        while True:
            msg = json.loads(await ws.recv())
            if msg.get("type") == "match":
                vol = float(msg["size"])
                delta = vol if msg["side"] == "buy" else -vol

                with LOCK:
                    cvd["Coinbase"] += delta
                    data["Coinbase"].append((datetime.utcnow(), cvd["Coinbase"]))


async def okx():
    url = "wss://ws.okx.com:8443/ws/v5/public"
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({
            "op": "subscribe",
            "args": [{"channel": "trades", "instId": "XRP-USDT"}]
        }))

        while True:
            msg = json.loads(await ws.recv())
            if "data" in msg:
                for t in msg["data"]:
                    vol = float(t["sz"])
                    delta = vol if t["side"] == "buy" else -vol

                    with LOCK:
                        cvd["OKX"] += delta
                        data["OKX"].append((datetime.utcnow(), cvd["OKX"]))


async def bybit():
    url = "wss://stream.bybit.com/v5/public/spot"
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({
            "op": "subscribe",
            "args": ["publicTrade.XRPUSDT"]
        }))

        while True:
            msg = json.loads(await ws.recv())
            if "data" in msg:
                for t in msg["data"]:
                    vol = float(t["v"])
                    delta = vol if t["S"] == "Buy" else -vol

                    with LOCK:
                        cvd["Bybit"] += delta
                        data["Bybit"].append((datetime.utcnow(), cvd["Bybit"]))


async def kraken():
    url = "wss://ws.kraken.com"
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({
            "event": "subscribe",
            "pair": ["XRP/USD"],
            "subscription": {"name": "trade"}
        }))

        while True:
            msg = json.loads(await ws.recv())
            if isinstance(msg, list):
                for t in msg[1]:
                    vol = float(t[1])
                    delta = vol if t[3] == "b" else -vol

                    with LOCK:
                        cvd["Kraken"] += delta
                        data["Kraken"].append((datetime.utcnow(), cvd["Kraken"]))


# =========================
# THREAD RUNNER
# =========================
def run_ws(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(coro)


threading.Thread(target=run_ws, args=(binance_cvd(),), daemon=True).start()
threading.Thread(target=run_ws, args=(binance_price(),), daemon=True).start()
threading.Thread(target=run_ws, args=(coinbase(),), daemon=True).start()
threading.Thread(target=run_ws, args=(okx(),), daemon=True).start()
threading.Thread(target=run_ws, args=(bybit(),), daemon=True).start()
threading.Thread(target=run_ws, args=(kraken(),), daemon=True).start()

# =========================
# DASH APP
# =========================
app = Dash(__name__)

app.layout = html.Div([
    html.H3("XRP – Exchange Based CVD + Binance Price"),
    dcc.Graph(id="cvd-graph"),
    dcc.Interval(id="interval", interval=2000)
])

@app.callback(
    Output("cvd-graph", "figure"),
    Input("interval", "n_intervals")
)
def update(_):
    fig = go.Figure()

    with LOCK:
        for ex, vals in data.items():
            if len(vals) > 0:
                df = pd.DataFrame(vals, columns=["time", "cvd"])
                fig.add_trace(go.Scatter(
                    x=df["time"],
                    y=df["cvd"],
                    mode="lines",
                    name=f"{ex} CVD"
                ))

        if len(price_data) > 0:
            pdf = pd.DataFrame(price_data, columns=["time", "price"])
            fig.add_trace(go.Scatter(
                x=pdf["time"],
                y=pdf["price"],
                mode="lines",
                name="XRP Price (Binance)",
                yaxis="y2",
                line=dict(color="orange", width=2)
            ))

    fig.update_layout(
        template="plotly_dark",
        xaxis=dict(title="UTC Time", type="date"),
        yaxis=dict(title="CVD"),
        yaxis2=dict(
            title="XRP Price",
            overlaying="y",
            side="right",
            showgrid=False
        ),
        legend=dict(orientation="h")
    )

    return fig


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
