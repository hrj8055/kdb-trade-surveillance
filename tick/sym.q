/ tick/sym.q
/ Schemas for every table that flows through the tickerplant.
/ tick.q rule: first two columns MUST be `time` (timespan) and `sym` (symbol).

/ ================= REAL market data (Binance) =================
quotes:([]
  time:`timespan$(); sym:`symbol$();
  bid:`float$(); ask:`float$(); bsize:`float$(); asize:`float$())

mktTrades:([]
  time:`timespan$(); sym:`symbol$();
  price:`float$(); qty:`float$(); isBuyerMaker:`boolean$())

/ ================= SIMULATED trader activity =================
/ action: `new`amend`cancel`fill   side: `buy`sell
orders:([]
  time:`timespan$(); sym:`symbol$();
  orderId:`long$(); traderId:`symbol$(); side:`symbol$();
  price:`float$(); qty:`float$(); action:`symbol$())

trades:([]
  time:`timespan$(); sym:`symbol$();
  tradeId:`long$(); price:`float$(); qty:`float$();
  buyOrderId:`long$(); sellOrderId:`long$();
  buyTrader:`symbol$(); sellTrader:`symbol$())

chats:([]
  time:`timespan$(); sym:`symbol$();
  chatId:`long$(); traderId:`symbol$(); counterparty:`symbol$();
  channel:`symbol$(); text:())

/ ground truth of injected abuse: used ONLY by eval.q, never by the rules
truth:([]
  time:`timespan$(); sym:`symbol$();
  eventId:`long$(); abuseType:`symbol$(); traderId:`symbol$())

/ ================= DERIVED outputs (published back to the TP) =================
alerts:([]
  time:`timespan$(); sym:`symbol$();
  alertId:`long$(); alertType:`symbol$(); traderId:`symbol$();
  score:`float$(); details:())

chatRisk:([]
  time:`timespan$(); sym:`symbol$();
  chatId:`long$(); traderId:`symbol$();
  riskScore:`float$(); label:`symbol$(); reason:())

mlScores:([]
  time:`timespan$(); sym:`symbol$();
  alertId:`long$(); traderId:`symbol$();
  mlScore:`float$(); topReasons:())

/ verdict: `real`false
feedback:([]
  time:`timespan$(); sym:`symbol$();
  alertId:`long$(); verdict:`symbol$(); analyst:`symbol$())

alertNotes:([]
  time:`timespan$(); sym:`symbol$();
  alertId:`long$(); summary:(); verified:`boolean$())