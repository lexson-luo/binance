import logging
import asyncio
import websockets
import json
import time

import pandas as pd
import numpy as np

logging.basicConfig(
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s] %(message)s",
    level=logging.INFO,
)

URL = "wss://fstream.binance.com/ws/btcusdt@aggTrade"

timeseries = {"timestamp": [], "vwap": []}

n_trades = 0
sum_price_size = 0
sum_size = 0


async def subscribe():
    async with websockets.connect(URL) as ws:
        global n_trades, sum_price_size, sum_size
        while ws.open:
            message = await ws.recv()
            trade_data = json.loads(message)
            _traded_price = float(trade_data["p"])
            _traded_size = float(trade_data["q"])
            sum_price_size += _traded_price * _traded_size
            sum_size += _traded_size
            n_trades += 1


async def compute_vwap_vol(interval_secs, window_size):
    global n_trades, sum_price_size, sum_size, timeseries

    while True:
        await asyncio.sleep(interval_secs)
        if sum_price_size == 0:
            continue

        dt = pd.to_datetime(time.time(), unit="s")
        timeseries["timestamp"].append(dt)
        timeseries["vwap"].append((sum_price_size / sum_size))

        df = pd.DataFrame(timeseries)
        df.set_index("timestamp", inplace=True)
        df["log_returns"] = np.log(df["vwap"] / df["vwap"].shift())
        vwap_vol = df["log_returns"].std()
        vol_scale = np.sqrt(24 * 60 * 60 / interval_secs)
        vwap_vol_scaled = vol_scale * vwap_vol

        logging.info(
            f"VWAP Vol: scaled_to_daily= {vwap_vol_scaled:.4f}, n_trades= {n_trades}, vwap={df['vwap'][-1]:.4f}, volume= {sum_size:.3f}, window_size= {df.shape[0]}"
        )

        n_trades = 0
        sum_price_size = 0
        sum_size = 0

        if df.shape[0] == window_size:
            timeseries["timestamp"].pop(0)
            timeseries["vwap"].pop(0)


async def start():
    logging.info("Start computing 5min vwap vol...")
    await asyncio.gather(subscribe(), compute_vwap_vol(5, 60))


if __name__ == "__main__":
    asyncio.run(start())