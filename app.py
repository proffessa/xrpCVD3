import asyncio
import json
import threading
import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objs as go
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import websockets
import requests

# ===================== CONFIG =====================
SYMBOLS = {
    "binance": "xrpusdt",
    "coinbase": "XRP-USD",
    "okx": "XRP-USDT",
    "bybit": "XRPUSDT",
    "kraken": "XRP/USD",
    "bitstamp": "xrpusd",
    "upbit": "KRW-XRP",
}

WINDOW_HOURS = 36
MAX_AGE = timedelta(hours=WINDOW_HOURS)

# ===================== STORAGE =====================
data = {
    ex: pd.DataFrame(columns=["time", "cvd_raw"])
    for ex in SYMBOLS
}

price_data = pd.DataFrame(columns=["time", "price"])

lock = threading.Lock()

# ===================== HELPERS =====================
def trim_old(df):
    cutoff = datetime.utcnow() - MAX_AGE
    return df[df["time"] >= cutoff]

def indexed(series):
    if len(series) == 0:
        return series
    return series - series.iloc[0]

# ===================== BINANCE PRICE =====================
def fetch_binance_price():
    global price_data
    while True:
        try:
            r = requests.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": "XRPUSDT"},
                timeout=5,
            )
            p = float(r.json()["price"])
            with lock:
                price_data.loc[len(price_data)] = [datetime.utcnow(), p]
                price_data = trim_old(price_data)
        except:
            pass
        time.sleep(5)

# ===================== WEBSOCKET HANDLER =====================
async def trade_stream(exchange):
    uri_map = {
        "binance": "wss://stream.binance.com:9443/ws/xrpusdt@trade",
        "coinbase": "wss://ws-feed.exchange.coinbase.com",
        "okx": "wss://ws.okx.com:8443/ws/v5/public",
        "bybit": "wss://stream.bybit.com/v5/public/spot",
        "kraken": "wss://ws.kraken.com",
        "bitstamp": "wss://ws.bitstamp.net",
        "upbit": "wss://api.upbit.com/websocket/v1",
    }

    async with websockets.connect(uri_map[exchange]) as ws:
        # ---- SUBSCRIPTIONS ----
        if exchange == "coinbase":
            await ws.send(json.dumps({
                "type": "subscribe",
                "channels": ["matches"],
                "product_ids": ["XRP-USD"],
            }))

        elif exchange == "okx":
            await ws.send(json.dumps({
                "op": "subscribe",
                "args": [{"channel": "trades", "instId": "XRP-USDT"}],
            }))

        elif exchange == "bybit":
            await ws.send(json.dumps({
                "op": "subscribe",
                "args": ["publicTrade.XRPUSDT"],
            }))

        elif exchange == "kraken":
            await ws.send(json.dumps({
                "event": "subscribe",
                "pair": ["XRP/USD"],
                "subscription": {"name": "trade"},
            }))

        elif exchange == "bitstamp":
            await ws.send(json.dumps({
                "event": "bts:subscribe",
                "data": {"channel": "live_trades_xrpusd"},
            }))

        elif exchange == "upbit":
            await ws.send(json.dumps([
                {"ticket": "xrp"},
                {"type": "trade", "codes": ["KRW-XRP"]},
            ]))

        # ---- LOOP ----
        async for msg in ws:
            now = datetime.utcnow()
            vol = 0

            try:
                m = json.loads(msg)

                if exchange == "binance":
                    vol = float(m["q"]) if m["m"] else -float(m["q"])

                elif exchange == "coinbase":
                    vol = float(m["size"]) if m["side"] == "buy" else -float(m["size"])

                elif exchange == "okx":
                    t = m["data"][0]
                    vol = float(t["sz"]) if t["side"] == "buy" else -float(t["sz"])

                elif exchange == "bybit":
                    t = m["data"][0]
                    vol = float(t["v"]) if t["S"] == "Buy" else -float(t["v"])

                elif exchange == "kraken":
                    t = m[1][0]
                    vol = float(t[1]) if t[3] == "b" else -float(t[1])

                elif exchange == "bitstamp":
                    t = m["data"]
                    vol = float(t["amount"]) if t["type"] == 0 else -float(t["amount"])

                elif exchange == "upbit":
                    vol = float(m["trade_volume"]) if m["ask_bid"] == "BID" else -float(m["trade_volume"])

                with lock:
                    df = data[exchange]
                    prev = df["cvd_raw"].iloc[-1] if len(df) else 0
                    df.loc[len(df)] = [now, prev + vol]
                    data[exchange] = trim_old(df)

            except:
                pass

# ===================== THREAD STARTER =====================
def start_ws(exchange):
    asyncio.run(trade_stream(exchange))

for ex in SYMBOLS:
    threading.Thread(target=start_ws, args=(ex,), daemon=True).start()

threading.Thread(target=fetch_binance_price, daemon=True).start()

# ===================== DASH APP =====================
app = Dash(__name__)
app.layout = html.Div([
    html.H2("XRP Spot Indexed CVD (36h) + Binance Price"),
    dcc.Graph(id="graph"),
    dcc.Interval(id="interval", interval=3000),
])

@app.callback(Output("graph", "figure"), Input("interval", "n_intervals"))
def update(_):
    fig = go.Figure()

    with lock:
        for ex, df in data.items():
            if len(df) < 2:
                continue
            fig.add_trace(go.Scatter(
                x=df["time"],
                y=indexed(df["cvd_raw"]),
                mode="lines",
                name=f"{ex.capitalize()} CVD",
            ))

        if len(price_data) > 2:
            fig.add_trace(go.Scatter(
                x=price_data["time"],
                y=price_data["price"],
                name="XRP Price (Binance)",
                yaxis="y2",
                line=dict(color="orange"),
            ))

    fig.update_layout(
        xaxis=dict(title="Time"),
        yaxis=dict(title="Indexed CVD"),
        yaxis2=dict(title="Price", overlaying="y", side="right"),
        template="plotly_dark",
        legend=dict(orientation="h"),
    )

    return fig

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050)
