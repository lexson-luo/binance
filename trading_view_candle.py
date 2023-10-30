import logging
import time
import json
import pandas as pd
import asyncio
import websockets
import matplotlib.pyplot as plt
import matplotlib as mpl
import mplfinance as mpf
import matplotlib.animation as animation
import matplotlib.ticker as ticker

from threading import Thread

timeseries = {
    "date": [],
    "open": [],
    "high": [],
    "low": [],
    "close": [],
    "volume": [],
}


class RealTimeCandlestickGraph:
    logging.basicConfig(
        format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s] %(message)s",
        level=logging.INFO,
    )

    def __init__(self, symbol: str, interval: str, width=360):
        self.symbol = symbol
        self.interval = interval
        self.width = width
        self.socket = "wss://fstream.binance.com/ws/"

        self.fig, (self.ax, self.volume_ax) = plt.subplots(
            2, 1, gridspec_kw={"height_ratios": [4, 1]}, sharex=True, figsize=(10, 8)
        )

        # plt.style.use("ggplot")

        self.ani = animation.FuncAnimation(
            self.fig, self._animate, interval=1000, blit=False
        )

    async def _listen_kline_forever(self):
        global timeseries
        kline_url = self.socket + self.symbol + "@kline_" + self.interval
        print(kline_url)
        logging.info("Starting websocket connection..")
        async with websockets.connect(kline_url) as ws:
            logging.info("Websocket connected..")
            while ws.open:
                _message = await ws.recv()
                # print(_message)
                _data = json.loads(_message)
                _event = _data["e"]
                _kdata = _data["k"]
                if _event == "kline":
                    if (
                        not timeseries["date"]
                        or int(_kdata["t"]) != timeseries["date"][-1]
                    ):
                        timeseries["date"].append(int(_kdata["t"]))

                        timeseries["open"].append(float(_kdata["o"]))

                        timeseries["high"].append(float(_kdata["h"]))

                        timeseries["low"].append(float(_kdata["l"]))

                        timeseries["close"].append(float(_kdata["c"]))

                        timeseries["volume"].append(float(_kdata["v"]))
                    else:
                        timeseries["open"][-1] = float(_kdata["o"])

                        timeseries["high"][-1] = float(_kdata["h"])

                        timeseries["low"][-1] = float(_kdata["l"])

                        timeseries["close"][-1] = float(_kdata["c"])

                        timeseries["volume"][-1] = float(_kdata["v"])

                if len(timeseries["date"]) > self.width:
                    timeseries["date"] = timeseries["date"][-self.width :]
                    timeseries["open"] = timeseries["open"][-self.width :]
                    timeseries["high"] = timeseries["high"][-self.width :]
                    timeseries["low"] = timeseries["low"][-self.width :]
                    timeseries["close"] = timeseries["close"][-self.width :]
                    timeseries["volume"] = timeseries["volume"][-self.width :]
                # print(timeseries)
                # print(len(timeseries["date"]))

    def run_task(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(self._listen_kline_forever())
        loop.run_forever()

    def start(self):
        loop_thread = Thread(target=self.run_task, daemon=True, name="Async Thread")
        loop_thread.start()

    def _animate(self, _):
        self.ax.clear()
        self.volume_ax.clear()

        mpl.rcParams["mathtext.default"] = "regular"
        plt.style.use("ggplot")

        self.ax.set_title("BTCUSDT Candlestick Chart")
        self.ax.yaxis.set_major_formatter(ticker.ScalarFormatter(useMathText=False))
        self.ax.ticklabel_format(style="plain", axis="y")
        self.ax.grid(color="gray", linestyle="--", linewidth=0.5)
        self.ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())

        self.volume_ax.grid(color="gray", linestyle="--", linewidth=0.5)

        df = pd.DataFrame(timeseries)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"], unit="ms")
            df.set_index("date", inplace=True)

            mpf.plot(
                df,
                ax=self.ax,
                type="candle",
                style="yahoo",
                volume=self.volume_ax,
                show_nontrading=False,
            )

            self.fig.canvas.draw()
            self.fig.canvas.flush_events()


if __name__ == "__main__":
    quant = RealTimeCandlestickGraph(symbol="btcusdt", interval="1m")
    quant.start()
    plt.pause(0.01)
    plt.show()

    while True:
        time.sleep(1)
