import threading
import time
from collections import deque
from datetime import datetime

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import websocket
import json
import requests

# ---------------- CONFIG ----------------

ROLLING_HOURS = 36
MAX_SECONDS = ROLLING_HOURS * 3600

EXCHANGES = {
    "Binance": {
        "url": "wss://stream.binance.com:9443/ws/xrpusdt@trade",
        "parser": lambda d: (
            d["T"],  # timestamp ms
            float(d["q"]),  # quantity
            1 if not d["m"] else -1
        )
    },
    "Coinbase": {
        "url": "wss://ws-feed.exchange.coinbase.com",
        "subscribe": {
            "type": "subscribe",
            "channels": [{"name": "matches", "product_ids": ["XRP-USD"]}]
        },
        "parser": lambda d: (
            int(datetime.fromisoformat(d["time"].replace("Z", "")).timestamp() * 1000),
            float(d["size"]),
            1 if d["side"] == "buy" else -1
        )
    },
    "Kraken": {
        "url": "wss://ws.kraken.com",
        "subscribe": {
            "event": "subscribe",
            "pair": ["XRP/USD"],
            "subscription": {"name": "trade"}
        },
        "parser": lambda d: (
            int(float(d[1][0][2]) * 1000),
            float(d[1][0][1]),
            1 if d[1][0][3] == "b" else -1
        )
    },
    "OKX": {
        "url": "wss://ws.okx.com:8443/ws/v5/public",
        "subscribe": {
            "op": "subscribe",
            "args": [{"channel": "trades", "instId": "XRP-USDT"}]
        },
        "parser": lambda d: (
            int(d["data"][0]["ts"]),
            float(d["data"][0]["sz"]),
            1 if d["data"][0]["side"] == "buy" else -1
        )
    },
    "Bybit": {
        "url": "wss://stream.bybit.com/v5/public/spot",
        "subscribe": {
            "op": "subscribe",
            "args": ["publicTrade.XRPUSDT"]
        },
        "parser": lambda d: (
            int(d["data"][0]["T"]),
            float(d["data"][0]["v"]),
            1 if d["data"][0]["S"] == "Buy" else -1
        )
    },
    "Bitstamp": {
        "url": "wss://ws.bitstamp.net",
        "subscribe": {
            "event": "bts:subscribe",
            "data": {"channel": "live_trades_xrpusd"}
        },
        "parser": lambda d: (
            int(d["data"]["timestamp"]) * 1000,
            float(d["data"]["amount"]),
            1 if d["data"]["type"] == 0 else -1
        )
    }
}

# ---------------- DATA ----------------

cvd = {
    ex: deque()
    for ex in EXCHANGES
}

price_data = deque()

lock = threading.Lock()

# ---------------- FUNCTIONS ----------------

def cleanup(data):
    now = time.time()
    while data and now - data[0][0] > MAX_SECONDS:
        data.popleft()

def ws_worker(name, cfg):
    def on_open(ws):
        if "subscribe" in cfg:
            ws.send(json.dumps(cfg["subscribe"]))

    def on_message(ws, msg):
        try:
            data = json.loads(msg)
            if isinstance(data, dict) and "event" in data:
                return

            ts, qty, side = cfg["parser"](data)
            delta = qty * side
            with lock:
                cvd[name].append((ts / 1000, delta))
                cleanup(cvd[name])
        except:
            pass

    ws = websocket.WebSocketApp(
        cfg["url"],
        on_open=on_open,
        on_message=on_message
    )
    ws.run_forever()

def price_worker():
    while True:
        try:
            r = requests.get(
                "https://api.binance.com/api/v3/ticker/price?symbol=XRPUSDT",
                timeout=5
            )
            p = float(r.json()["price"])
            with lock:
                price_data.append((time.time(), p))
                cleanup(price_data)
        except:
            pass
        time.sleep(5)

# ---------------- THREADS ----------------

for ex, cfg in EXCHANGES.items():
    threading.Thread(target=ws_worker, args=(ex, cfg), daemon=True).start()

threading.Thread(target=price_worker, daemon=True).start()

# ---------------- DASH ----------------

app = dash.Dash(__name__)
server = app.server

app.layout = html.Div(
    style={"backgroundColor": "black"},
    children=[
        html.H3(
            "XRP – Exchange Based CVD (36h) + Binance Price",
            style={"color": "white"}
        ),
        dcc.Graph(id="graph"),
        dcc.Interval(id="interval", interval=3000)
    ]
)

@app.callback(Output("graph", "figure"), Input("interval", "n_intervals"))
def update(_):
    fig = go.Figure()
    axis_index = 1

    with lock:
        for ex, data in cvd.items():
            t, v = [], []
            running = 0
            for ts, d in data:
                running += d
                t.append(datetime.fromtimestamp(ts))
                v.append(running)

            yaxis = "y" if axis_index == 1 else f"y{axis_index}"

            fig.add_trace(go.Scatter(
                x=t,
                y=v,
                name=f"{ex} CVD",
                yaxis=yaxis
            ))

            fig.update_layout(**{
                yaxis: dict(
                    overlaying="y",
                    side="left" if axis_index % 2 else "right",
                    showgrid=False
                )
            })

            axis_index += 1

        if price_data:
            pt = [datetime.fromtimestamp(x[0]) for x in price_data]
            pv = [x[1] for x in price_data]
            fig.add_trace(go.Scatter(
                x=pt,
                y=pv,
                name="XRP Price (Binance)",
                yaxis="yprice",
                line=dict(color="orange", width=2)
            ))

            fig.update_layout(yaxis_price=dict(
                overlaying="y",
                side="right",
                title="Price"
            ))

    fig.update_layout(
        template="plotly_dark",
        height=900,
        xaxis=dict(title="Date / Time")
    )

    return fig

# ---------------- RUN ----------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050)
