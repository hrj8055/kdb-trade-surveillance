# python/order_gen.py
# SIMULATED trader activity around the LIVE market. Normal flow only (abuse comes in Step 3).
# Market data is PUSHED: we subscribe to quotes on the TP (RawQConnection + poll_recv),
# following the official PyKX subscriber pattern, instead of polling the RDB.
# usage: python python/order_gen.py [--rate N] [--seed N]

import argparse
import asyncio
import random
import time

import pykx as kx

from common import CFG, connect, get_logger

log = get_logger("order_gen")
G = CFG["generator"]
SYMS = CFG["syms"]
TICK = CFG["tickSize"]
QTY = G["qtyRange"]
TRADERS = [f"T{i:02d}" for i in range(1, G["traders"] + 1)]


class Sim:
    def __init__(self, tp, rdb, seed):
        self.tp = tp
        self.rng = random.Random(seed)
        self.mid = {}
        self.open = {}   # orderId -> {sym, trader, side, price, qty}
        # continue IDs from the RDB (0| turns an empty max, which is -0W, into 0)
        self.next_oid = int(rdb("0|exec max orderId from orders").py()) + 1
        self.next_tid = int(rdb("0|exec max tradeId from trades").py()) + 1
        self.stats = {"new": 0, "amend": 0, "cancel": 0, "fill": 0, "trades": 0, "quotesIn": 0}

    # ---------- market data, pushed by the TP ----------
    def drain_quotes(self, sub):
        """Apply every quote the TP has pushed since we last looked (non-blocking)."""
        while (msg := sub.poll_recv()) is not None:
            if msg[0] == "upd" and msg[1] == "quotes":
                d = msg[2]
                for s, b, a in zip(d["sym"].py(), d["bid"].py(), d["ask"].py()):
                    self.mid[s] = 0.5 * (b + a)
                    self.stats["quotesIn"] += 1

    # ---------- publishing (explicit q types) ----------
    def pub_order(self, oid, o, action, price=None, qty=None):
        row = [kx.SymbolAtom(o["sym"]), kx.LongAtom(oid), kx.SymbolAtom(o["trader"]),
               kx.SymbolAtom(o["side"]),
               kx.FloatAtom(o["price"] if price is None else price),
               kx.FloatAtom(o["qty"] if qty is None else qty),
               kx.SymbolAtom(action)]
        self.tp(".u.upd", kx.SymbolAtom("orders"), row, wait=False)
        self.stats[action] += 1

    def pub_trade(self, sym, px, qty, b_oid, b, s_oid, s):
        row = [kx.SymbolAtom(sym), kx.LongAtom(self.next_tid),
               kx.FloatAtom(px), kx.FloatAtom(qty),
               kx.LongAtom(b_oid), kx.LongAtom(s_oid),
               kx.SymbolAtom(b["trader"]), kx.SymbolAtom(s["trader"])]
        self.tp(".u.upd", kx.SymbolAtom("trades"), row, wait=False)
        self.next_tid += 1
        self.stats["trades"] += 1

    # ---------- the four actions ----------
    def new_order(self):
        sym = self.rng.choice(SYMS)
        side = self.rng.choice(["buy", "sell"])
        ticks = self.rng.randint(1, G["maxTicksFromMid"])
        px = self.mid[sym] + (-ticks if side == "buy" else ticks) * TICK[sym]
        lo, hi = QTY[sym]
        o = {"sym": sym, "trader": self.rng.choice(TRADERS), "side": side,
             "price": round(px, 2), "qty": round(self.rng.uniform(lo, hi), 4)}
        oid = self.next_oid
        self.next_oid += 1
        self.open[oid] = o
        self.pub_order(oid, o, "new")

    def cancel(self):
        oid = self.rng.choice(list(self.open))
        self.pub_order(oid, self.open.pop(oid), "cancel")

    def amend(self):
        oid = self.rng.choice(list(self.open))
        o = self.open[oid]
        o["price"] = round(o["price"] + self.rng.choice([-3, -2, -1, 1, 2, 3]) * TICK[o["sym"]], 2)
        self.pub_order(oid, o, "amend")

    def fill(self):
        sym = self.rng.choice(SYMS)
        buys = [k for k, o in self.open.items() if o["sym"] == sym and o["side"] == "buy"]
        sells = [k for k, o in self.open.items() if o["sym"] == sym and o["side"] == "sell"]
        if not buys or not sells:
            return
        b_oid = self.rng.choice(buys)
        b = self.open[b_oid]
        sells = [k for k in sells if self.open[k]["trader"] != b["trader"]]   # never self-trade
        if not sells:
            return
        s_oid = self.rng.choice(sells)
        s = self.open[s_oid]
        px, qty = round(self.mid[sym], 2), min(b["qty"], s["qty"])
        self.pub_order(b_oid, b, "fill", price=px, qty=qty)
        self.pub_order(s_oid, s, "fill", price=px, qty=qty)
        self.pub_trade(sym, px, qty, b_oid, b, s_oid, s)
        del self.open[b_oid], self.open[s_oid]

    def step(self):
        r = self.rng.random()
        if len(self.open) < 20 or r < 0.45:
            self.new_order()
        elif r < 0.75:
            self.cancel()
        elif r < 0.85:
            self.amend()
        else:
            self.fill()


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=G["rate"], help="events per second")
    ap.add_argument("--seed", type=int, default=G["seed"])
    args = ap.parse_args()

    tp, rdb = connect("tp", log), connect("rdb", log)       # publish + one-off ID lookup
    sim = Sim(tp, rdb, args.seed)

    # separate subscription connection: the TP pushes quotes to it
    async with kx.RawQConnection(host=CFG["host"], port=CFG["ports"]["tp"]) as sub:
        await sub(".u.sub", kx.SymbolAtom("quotes"), kx.SymbolVector(SYMS))
        log.info("Subscribed to quotes for %s; waiting for first prices", SYMS)

        while len(sim.mid) < len(SYMS):
            sim.drain_quotes(sub)
            await asyncio.sleep(0.05)
        log.info("Mid prices: %s", sim.mid)

        last_print = time.time()
        try:
            while True:
                sim.drain_quotes(sub)     # bring our market view up to date...
                sim.step()                # ...then act on it
                now = time.time()
                if now - last_print >= 10:
                    log.info("open=%d %s", len(sim.open), sim.stats)
                    last_print = now
                await asyncio.sleep(1 / args.rate)
        except RuntimeError as e:
            log.error("Lost connection to tickerplant (%s). Exiting so a supervisor can restart us.", e)
            raise SystemExit(1)
        finally:
            log.info("Generator stopped: %s", sim.stats)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
