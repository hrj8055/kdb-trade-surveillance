# python/order_gen.py
# SIMULATED trader activity around the LIVE mid price. Normal flow only (abuse comes in Step 3).
# usage: python python/order_gen.py [--rate 20] [--seed 42]

import argparse
import random
import sys
import time

import pykx as kx

SYMS = ["BTCUSDT", "ETHUSDT"]
TICK = 0.01
QTY = {"BTCUSDT": (0.001, 0.05), "ETHUSDT": (0.01, 1.0)}   # normal order size range
TRADERS = [f"T{i:02d}" for i in range(1, 21)]              # T01 .. T20


def connect(port, name):
    try:
        return kx.SyncQConnection(host="localhost", port=port)
    except kx.QError:
        sys.exit(f"Cannot reach the {name} on port {port}. Start it first.")


class Sim:
    def __init__(self, tp, rdb, seed):
        self.tp, self.rdb = tp, rdb
        self.rng = random.Random(seed)
        self.mid = {}
        self.open = {}   # orderId -> {sym, trader, side, price, qty}
        # continue IDs from what the RDB already has (0^ turns an empty max into 0)
        self.next_oid = int(rdb("0|exec max orderId from orders").py()) + 1
        self.next_tid = int(rdb("0|exec max tradeId from trades").py()) + 1
        self.stats = {"new": 0, "amend": 0, "cancel": 0, "fill": 0, "trades": 0}

    # ---------- market data from kdb ----------
    def refresh_mid(self):
        q = "exec sym!0.5*bid+ask from 0!select last bid, last ask by sym from quotes"
        self.mid.update({k: v for k, v in self.rdb(q).py().items() if k in SYMS})

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
        ticks = self.rng.randint(1, 20)                      # passive: 1-20 ticks from mid
        px = self.mid[sym] + (-ticks if side == "buy" else ticks) * TICK
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
        o["price"] = round(o["price"] + self.rng.choice([-3, -2, -1, 1, 2, 3]) * TICK, 2)
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tp", type=int, default=5010)
    ap.add_argument("--rdb", type=int, default=5011)
    ap.add_argument("--rate", type=float, default=20, help="events per second")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--mid-every", type=float, default=0.1, help="seconds between mid refreshes")
    args = ap.parse_args()

    sim = Sim(connect(args.tp, "tickerplant"), connect(args.rdb, "RDB"), args.seed)

    print("Waiting for live quotes in the RDB...")
    while len(sim.mid) < len(SYMS):
        sim.refresh_mid()
        time.sleep(1)
    print("Mid prices:", sim.mid)

    last_mid = last_print = time.time()
    try:
        while True:
            now = time.time()
            if now - last_mid >= args.mid_every:          # refresh the live mid once a second
                sim.refresh_mid()
                last_mid = now
            if now - last_print >= 10:
                print(f"open={len(sim.open)}", sim.stats)
                last_print = now
            sim.step()
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        print("Generator stopped:", sim.stats)


if __name__ == "__main__":
    main()
