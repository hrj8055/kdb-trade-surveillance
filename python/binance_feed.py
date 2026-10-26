# python/binance_feed.py
# REAL market data: Binance bookTicker -> quotes, trade -> mktTrades, published to the TP.
# usage: python python/binance_feed.py [--tp 5010]

import argparse
import asyncio
import json

import pykx as kx
import websockets

SYMS = ["btcusdt", "ethusdt"]
STREAMS = "/".join(f"{s}@{kind}" for s in SYMS for kind in ("bookTicker", "trade"))
# Binance's market-data-only endpoint (public, no API key needed)
URL = f"wss://data-stream.binance.vision/stream?streams={STREAMS}"


def to_quote(d):
    # bookTicker fields: s=symbol, b=best bid, B=bid qty, a=best ask, A=ask qty (all strings)
    return [kx.SymbolAtom(d["s"]),
            kx.FloatAtom(float(d["b"])), kx.FloatAtom(float(d["a"])),
            kx.FloatAtom(float(d["B"])), kx.FloatAtom(float(d["A"]))]


def to_trade(d):
    # trade fields: s=symbol, p=price, q=qty, m=buyer is maker (bool)
    return [kx.SymbolAtom(d["s"]),
            kx.FloatAtom(float(d["p"])), kx.FloatAtom(float(d["q"])),
            kx.BooleanAtom(bool(d["m"]))]


async def stream(tp, counts):
    async with websockets.connect(URL, ping_interval=20) as ws:
        print("Connected to Binance:", STREAMS)
        async for raw in ws:
            msg = json.loads(raw)
            name, d = msg["stream"], msg["data"]
            if name.endswith("@bookTicker"):
                tp(".u.upd", kx.SymbolAtom("quotes"), to_quote(d), wait=False)
                counts["quotes"] += 1
            elif name.endswith("@trade"):
                tp(".u.upd", kx.SymbolAtom("mktTrades"), to_trade(d), wait=False)
                counts["mktTrades"] += 1
            total = counts["quotes"] + counts["mktTrades"]
            if total % 1000 == 0:
                print("published:", counts)


async def main(tp_port):
    tp = kx.SyncQConnection(host="localhost", port=tp_port)
    counts = {"quotes": 0, "mktTrades": 0}
    delay = 1
    while True:                      # reconnect loop: Binance drops connections periodically
        try:
            await stream(tp, counts)
            delay = 1
        except (websockets.ConnectionClosed, OSError) as e:
            print(f"Disconnected ({e}); reconnecting in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)   # exponential backoff, max 30s


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tp", type=int, default=5010)
    args = ap.parse_args()
    try:
        asyncio.run(main(args.tp))
    except KeyboardInterrupt:
        print("Feed stopped.")