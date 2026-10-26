# python/test_tp.py
# Checks Python can reach the tickerplant and publish one row with correct types.
import pykx as kx

with kx.SyncQConnection(host="localhost", port=5010) as tp:
    print("Tables on TP:", tp(".u.t"))

    # One mktTrades row WITHOUT time: the TP stamps it.
    # Explicit types match tick/sym.q: symbol, float, float, boolean.
    row = [kx.SymbolAtom("BTCUSDT"),
           kx.FloatAtom(67000.5),
           kx.FloatAtom(0.01),
           kx.BooleanAtom(False)]
    tp(".u.upd", kx.SymbolAtom("mktTrades"), row)
    print("Published one mktTrades row")