/ lib/common.q
/ Shared by every q process: reads config/config.json and provides UTC logging
/ in the same format as the Python processes. Load with: \l lib/common.q

/ ---------- config ----------
.conf.raw:.j.k raze read0 hsym `$"config/config.json";
.conf.host:`$.conf.raw`host;
.conf.port:{[p] "j"$.conf.raw[`ports;p]};      / JSON numbers arrive as floats: cast to long
.conf.path:{[p] .conf.raw[`paths;p]};          / returns a string

/ ---------- logging (UTC, same format as Python) ----------
.log.proc:"q";
.log.ts:{s:string .z.p; ssr[10#s;".";"-"],"T",(8#11_s),"Z"};
.log.info:{[msg] -1 .log.ts[]," INFO [",.log.proc,"] ",msg;};
.log.warn:{[msg] -1 .log.ts[]," WARN [",.log.proc,"] ",msg;};
.log.error:{[msg] -2 .log.ts[]," ERROR [",.log.proc,"] ",msg;};
