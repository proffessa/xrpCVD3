
import asyncio
import json
import threading
from datetime import datetime

import pandas as pd
import websockets
from dash import Dash, dcc, html
from dash.dependencies import Output, Input
import plotly.graph_objects as go

data = {
    "Binance": [],
    "Coinbase": [],
    "OKX": [],
    "Bybit": [],
    "Kraken": []
}

cvd = {k: 0 for k in data.keys()}

async def binance():
    url = "wss://stream.binance.com:9443/ws/xrpusdt@trade"
    async with websockets.connect(url) as ws:
        while True:
            msg = json.loads(await ws.recv())
            vol = float(msg["q"])
            delta = -vol if msg["m"] else vol
            cvd["Binance"] += delta
            data["Binance"].append((datetime.now(), cvd["Binance"]))

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
                cvd["Coinbase"] += delta
                data["Coinbase"].append((datetime.now(), cvd["Coinbase"]))

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
                    cvd["OKX"] += delta
                    data["OKX"].append((datetime.now(), cvd["OKX"]))

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
                    cvd["Bybit"] += delta
                    data["Bybit"].append((datetime.now(), cvd["Bybit"]))

async def kraken():
    url = "wss://ws.kraken.com"
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({
            "event": "subscribe",
            "pair": ["XRP/USD"],
            "subscription": {"name": "trade"}
        }))
        while True:
            msg = await ws.recv()
            msg = json.loads(msg)
            if isinstance(msg, list):
                for trade in msg[1]:
                    vol = float(trade[1])
                    delta = vol if trade[3] == "b" else -vol
                    cvd["Kraken"] += delta
                    data["Kraken"].append((datetime.now(), cvd["Kraken"]))

def start_ws():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.gather(
        binance(), coinbase(), okx(), bybit(), kraken()
    ))

threading.Thread(target=start_ws, daemon=True).start()

app = Dash(__name__)

app.layout = html.Div([
    html.H2("XRP Spot CVD – Multi Exchange"),
    dcc.Graph(id="cvd-graph"),
    dcc.Interval(id="interval", interval=2000)
])

@app.callback(
    Output("cvd-graph", "figure"),
    Input("interval", "n_intervals")
)
def update(_):
    fig = go.Figure()
    for ex, vals in data.items():
        if len(vals) > 10:
            df = pd.DataFrame(vals, columns=["time", "cvd"])
            fig.add_trace(go.Scatter(
                x=df["time"],
                y=df["cvd"],
                mode="lines",
                name=ex
            ))
    fig.update_layout(
        template="plotly_dark",
        xaxis_title="Time",
        yaxis_title="CVD"
    )
    return fig

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050)
