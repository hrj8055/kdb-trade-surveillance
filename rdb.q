/ rdb.q: Real-time database. Holds today's data in memory.
/ usage: q rdb.q [tpPort] -p 5011

tpPort:$[count .z.x; "J"$first .z.x; 5010];

/ 1. What to do with each message from the TP: append it to the table.
/    The TP calls upd on us; replay (-11!) calls it too, once per logged message.
upd:{[t;x] t insert x};

/ 2. End-of-day hook. The TP calls .u.end on every subscriber at UTC midnight.
/    For now we only log it; saving to the HDB comes in Step 5.
.u.end:{[d] -1 string[.z.p]," end of day for ",string[d]," (HDB save comes in Step 5)";};

/ 3. Rebuild state: create each table from its schema, then replay today's log.
.u.rep:{[schemas;logInfo]
  (.[;();:;].) each schemas;      / each item is (tableName;emptyTable): sets it globally
  if[null first logInfo; :()];    / no log file: nothing to replay
  -1 "Replaying ",string[logInfo 0]," message(s) from ",string logInfo 1;
  -11!logInfo;                    / replay the first n messages, calling upd for each
  };

/ 4. Connect, subscribe to all tables, and get the log position in ONE call.
h:hopen tpPort;
.u.rep . h"(.u.sub[`;`];`.u `i`L)";
-1 "RDB ready. Tables: ",", " sv string tables[];