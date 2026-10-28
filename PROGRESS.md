# Progress log

- [x] Step 1   Tickerplant + schemas
- [x] Step 2.1 RDB with tplog replay
- [x] Step 2.2 Live Binance feed
- [x] Step 2.3 Record / replay (183,760 msgs, 0 lost, ~1,850 msgs/s Python max)
- [x] Step 2.4 order_gen.py simulated traders (20 traders, ~20 events/s, 0 self-trades, mid polled at 10 Hz, ~5% marketable)
- [x] Step 2.5a config.json, UTC logging, push-based quotes in generator (BTC 10.3 ticks from mid, 0% crossed)
- [x] Step 2.5b q reads config.json (lib/common.q), same UTC log format in q and Python, fail-fast on TP loss (all processes exit 1; RDB recovers via replay)
- [ ] Step 3   Inject abuse + truth table
