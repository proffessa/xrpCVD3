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
EXCHANGES = [
    "Binance", "Coinbase", "OKX", "Bybit", "Kraken",
    "Bitstamp", "Gate", "MEXC", "HTX", "Upbit"
]

data = {ex: [] for ex in EXCHANGES}
cvd = {ex: 0.0 for ex in EXCHANGES}

price_data = []

LOCK = threading.Lock()

# =========================
# WEBSOCKETS
# =========================
async def binance_cvd():
    async with websockets.connect("wss://stream.binance.com:9443/ws/xrpusdt@trade") as ws:
        while True:
            msg = json.loads(await ws.recv())
            vol = float(msg["q"])
            delta = -vol if msg["m"] else vol
            with LOCK:
                cvd["Binance"] += delta
                data["Binance"].append((datetime.utcnow(), cvd["Binance"]))


async def binance_price():
    async with websockets.connect("wss://stream.binance.com:9443/ws/xrpusdt@trade") as ws:
        while True:
            msg = json.loads(await ws.recv())
            with LOCK:
                price_data.append((datetime.utcnow(), float(msg["p"])))


async def coinbase():
    async with websockets.connect("wss://ws-feed.exchange.coinbase.com") as ws:
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
    async with websockets.connect("wss://ws.okx.com:8443/ws/v5/public") as ws:
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
    async with websockets.connect("wss://stream.bybit.com/v5/public/spot") as ws:
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
    async with websockets.connect("wss://ws.kraken.com") as ws:
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


async def bitstamp():
    async with websockets.connect("wss://ws.bitstamp.net") as ws:
        await ws.send(json.dumps({
            "event": "bts:subscribe",
            "data": {"channel": "live_trades_xrpusd"}
        }))
        while True:
            msg = json.loads(await ws.recv())
            if "data" in msg:
                vol = float(msg["data"]["amount"])
                delta = vol if msg["data"]["type"] == 0 else -vol
                with LOCK:
                    cvd["Bitstamp"] += delta
                    data["Bitstamp"].append((datetime.utcnow(), cvd["Bitstamp"]))


async def gate():
    async with websockets.connect("wss://api.gateio.ws/ws/v4/") as ws:
        await ws.send(json.dumps({
            "time": int(datetime.utcnow().timestamp()),
            "channel": "spot.trades",
            "event": "subscribe",
            "payload": ["XRP_USDT"]
        }))
        while True:
            msg = json.loads(await ws.recv())
            if msg.get("result"):
                for t in msg["result"]:
                    vol = float(t["amount"])
                    delta = vol if t["side"] == "buy" else -vol
                    with LOCK:
                        cvd["Gate"] += delta
                        data["Gate"].append((datetime.utcnow(), cvd["Gate"]))


async def mexc():
    async with websockets.connect("wss://wbs.mexc.com/ws") as ws:
        await ws.send(json.dumps({
            "method": "SUBSCRIPTION",
            "params": ["spot@public.deals.v3.api@XRPUSDT"],
            "id": 1
        }))
        while True:
            msg = json.loads(await ws.recv())
            if "data" in msg:
                for t in msg["data"]:
                    vol = float(t["v"])
                    delta = vol if t["S"] == "BUY" else -vol
                    with LOCK:
                        cvd["MEXC"] += delta
                        data["MEXC"].append((datetime.utcnow(), cvd["MEXC"]))


async def htx():
    async with websockets.connect("wss://api.huobi.pro/ws") as ws:
        await ws.send(json.dumps({
            "sub": "market.xrpusdt.trade.detail",
            "id": "xrp"
        }))
        while True:
            msg = json.loads(await ws.recv())
            if "tick" in msg:
                for t in msg["tick"]["data"]:
                    vol = float(t["amount"])
                    delta = vol if t["direction"] == "buy" else -vol
                    with LOCK:
                        cvd["HTX"] += delta
                        data["HTX"].append((datetime.utcnow(), cvd["HTX"]))


# 🔹 UPBIT (XRP/KRW)
async def upbit():
    async with websockets.connect("wss://api.upbit.com/websocket/v1") as ws:
        await ws.send(json.dumps([
            {"ticket": "xrp"},
            {"type": "trade", "codes": ["KRW-XRP"]}
        ]))
        while True:
            msg = json.loads(await ws.recv())
            vol = float(msg["trade_volume"])
            delta = vol if msg["ask_bid"] == "BID" else -vol
            with LOCK:
                cvd["Upbit"] += delta
                data["Upbit"].append((datetime.utcnow(), cvd["Upbit"]))

# =========================
# THREADS
# =========================
def run_ws(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(coro)

for func in [
    binance_cvd, binance_price, coinbase, okx, bybit,
    kraken, bitstamp, gate, mexc, htx, upbit
]:
    threading.Thread(target=run_ws, args=(func(),), daemon=True).start()

# =========================
# DASH
# =========================
app = Dash(__name__)

app.layout = html.Div([
    html.H3("XRP – 10 Spot Exchange CVD + Binance Price"),
    dcc.Graph(id="graph"),
    dcc.Interval(id="interval", interval=2000)
])

@app.callback(Output("graph", "figure"), Input("interval", "n_intervals"))
def update(_):
    fig = go.Figure()

    with LOCK:
        for ex, vals in data.items():
            if vals:
                df = pd.DataFrame(vals, columns=["time", "cvd"])
                fig.add_trace(go.Scatter(
                    x=df["time"], y=df["cvd"],
                    mode="lines", name=ex
                ))

        if price_data:
            pdf = pd.DataFrame(price_data, columns=["time", "price"])
            fig.add_trace(go.Scatter(
                x=pdf["time"], y=pdf["price"],
                mode="lines",
                name="XRP Price (Binance)",
                yaxis="y2",
                line=dict(color="orange", width=2)
            ))

    fig.update_layout(
        template="plotly_dark",
        xaxis=dict(type="date"),
        yaxis=dict(title="CVD"),
        yaxis2=dict(title="XRP Price", overlaying="y", side="right"),
        legend=dict(orientation="h")
    )
    return fig


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
