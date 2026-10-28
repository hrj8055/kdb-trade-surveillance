# python/binance_feed.py
# REAL market data: Binance bookTicker -> quotes, trade -> mktTrades, published to the TP.
#   live:            python python/binance_feed.py
#   live + record:   python python/binance_feed.py --record
#   replay:          python python/binance_feed.py --replay data/recordings/FILE.jsonl --speed 10
#   (--speed 0 = as fast as possible, for load testing)

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import pykx as kx
import websockets

SYMS = ["btcusdt", "ethusdt"]
STREAMS = "/".join(f"{s}@{kind}" for s in SYMS for kind in ("bookTicker", "trade"))
URL = f"wss://data-stream.binance.vision/stream?streams={STREAMS}"
REC_DIR = Path("data/recordings")


class Publisher:
    """Parses one raw Binance message and publishes it to the TP with explicit q types."""

    def __init__(self, tp_port):
        try:
            self.tp = kx.SyncQConnection(host="localhost", port=tp_port)
        except kx.QError:
            sys.exit(f"Cannot reach the tickerplant on port {tp_port}. Start it first:\n"
                     f"  rlwrap q tick.q sym data/tplog -p {tp_port}")
        self.counts = {"quotes": 0, "mktTrades": 0}

    def handle(self, raw):
        msg = json.loads(raw)
        name, d = msg["stream"], msg["data"]
        if name.endswith("@bookTicker"):
            row = [kx.SymbolAtom(d["s"]),
                   kx.FloatAtom(float(d["b"])), kx.FloatAtom(float(d["a"])),
                   kx.FloatAtom(float(d["B"])), kx.FloatAtom(float(d["A"]))]
            self.tp(".u.upd", kx.SymbolAtom("quotes"), row, wait=False)
            self.counts["quotes"] += 1
        elif name.endswith("@trade"):
            row = [kx.SymbolAtom(d["s"]),
                   kx.FloatAtom(float(d["p"])), kx.FloatAtom(float(d["q"])),
                   kx.BooleanAtom(bool(d["m"]))]
            self.tp(".u.upd", kx.SymbolAtom("mktTrades"), row, wait=False)
            self.counts["mktTrades"] += 1
        total = self.counts["quotes"] + self.counts["mktTrades"]
        if total % 1000 == 0:
            print("published:", self.counts)


async def live(pub, record):
    rec = None
    if record:
        REC_DIR.mkdir(parents=True, exist_ok=True)
        path = REC_DIR / f"binance_{datetime.now():%Y%m%d_%H%M%S}.jsonl"
        rec = open(path, "a", buffering=1)          # line-buffered: safe if we crash
        print("Recording to", path)
    delay = 1
    try:
        while True:                                  # reconnect loop
            try:
                async with websockets.connect(URL, ping_interval=20) as ws:
                    print("Connected to Binance:", STREAMS)
                    delay = 1
                    async for raw in ws:
                        if rec:
                            rec.write(json.dumps({"t": time.time_ns(), "m": raw}) + "\n")
                        pub.handle(raw)
            except (websockets.ConnectionClosed, OSError) as e:
                print(f"Disconnected ({e}); reconnecting in {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
    finally:
        if rec:
            rec.close()
            print("Recording saved.")


def replay(pub, path, speed):
    print(f"Replaying {path} at speed {speed}x")
    start_wall = time.perf_counter()
    t0 = None
    with open(path) as f:
        for line in f:
            r = json.loads(line)
            if t0 is None:
                t0 = r["t"]
            if speed > 0:
                due = start_wall + (r["t"] - t0) / 1e9 / speed   # when this msg should go out
                wait = due - time.perf_counter()
                if wait > 0:
                    time.sleep(wait)
            pub.handle(r["m"])
    print("Replay finished:", pub.counts)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tp", type=int, default=5010)
    ap.add_argument("--record", action="store_true", help="save raw messages while live")
    ap.add_argument("--replay", metavar="FILE", help="replay a recording instead of going live")
    ap.add_argument("--speed", type=float, default=1.0, help="replay speed multiplier (0 = max)")
    args = ap.parse_args()

    pub = Publisher(args.tp)
    try:
        if args.replay:
            replay(pub, args.replay, args.speed)
        else:
            asyncio.run(live(pub, args.record))
    except KeyboardInterrupt:
        print("Feed stopped:", pub.counts)