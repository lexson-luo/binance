import websockets
import logging
import asyncio
import time
import json
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
close = 0


async def subscribe():
    async with websockets.connect(URL) as ws:
        while ws.open:
            message = await ws.recv()
            trade_data = json.loads(message)
            global n_trades, sum_price_size, sum_size, close
            _trade_price = float(trade_data["p"])
            _trade_size = float(trade_data["q"])
            sum_price_size += _trade_price * _trade_size
            sum_size += _trade_size
            close = _trade_price
            n_trades += 1


async def compute_vwap_vol(interval_secs, window_size):
    global n_trades, sum_price_size, sum_size, timeseries

    while True:
        await asyncio.sleep(interval_secs)

        logging.info("Compute VWAP volatility")
        if (
            sum_price_size == 0
        ):  # skip-div-error/continue, in case no trading (ie. 0 volume) or 0 price.
            continue

        dt = pd.to_datetime(time.time(), unit="s")
        vwap = sum_price_size / sum_size

        timeseries["timestamp"].append(dt)
        timeseries["vwap"].append(vwap)

        df = pd.DataFrame(timeseries)
        df.set_index("timestamp", inplace=True)
        df["log_returns"] = np.log(df["vwap"] / df["vwap"].shift())
        df["vol"] = df["log_returns"].std()

        vol_scale = np.sqrt(24 * 60 * 60 / interval_secs)
        vol_daily = df["vol"][-1] * vol_scale

        logging.info(
            f"5-min VWAP vol: scaled-to-daily-vwap-vol= {vol_daily:.4f}, vwap={vwap:.4f}, n={n_trades}, volume={sum_size:.3f}"
        )  # 5-min because 5secs * 60 (window_size)

        n_trades = 0
        sum_price_size = 0
        sum_size = 0

        if df.shape[0] == window_size:
            timeseries["timestamp"].pop(0)
            timeseries["vwap"].pop(0)


async def start():
    logging.info("Starting...")

    await asyncio.gather(subscribe(), compute_vwap_vol(5, 60))


if __name__ == "__main__":
    asyncio.run(start())
