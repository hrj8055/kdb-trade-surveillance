/ rdb.q: Real-time database. Holds today's data in memory.
/ usage: q rdb.q      (its port and the TP's address come from config/config.json)

\l lib/common.q
.log.proc:"rdb";
if[0=system"p"; system"p ",string .conf.port`rdb];   / -p on the command line still wins

/ 1. Each message from the TP (and each replayed log message): append to the table.
upd:{[t;x] t insert x};

/ 2. End-of-day hook, called by the TP at UTC midnight. HDB save comes in Step 5.
.u.end:{[d] .log.info "end of day for ",string[d]," (HDB save comes in Step 5)";};

/ 3. Rebuild state: create tables from schemas, then replay today's log.
.u.rep:{[schemas;logInfo]
  (.[;();:;].) each schemas;
  if[null first logInfo; :()];
  .log.info "Replaying ",string[logInfo 0]," message(s) from ",string logInfo 1;
  -11!logInfo;
  };

/ 4. Losing the TP means our data is going stale. A stale RDB that looks healthy is
/    dangerous, so we exit; on restart after the TP, the log replay restores everything.
.z.pc:{[h] if[h=.rdb.tp;
  .log.error "Lost connection to tickerplant. Exiting; restart after the TP to replay the log.";
  exit 1]};

/ 5. Connect (with a clear error if the TP is down), subscribe and replay in one call.
.rdb.addr:hsym `$":" sv (string .conf.host; string .conf.port`tp);
.rdb.tp:@[hopen; .rdb.addr; {.log.error "Cannot reach tickerplant at ",string[.rdb.addr],": ",x; exit 1}];
.u.rep . .rdb.tp"(.u.sub[`;`];`.u `i`L)";
.log.info "RDB ready on port ",string[system"p"],". Tables: ",", " sv string tables[];
