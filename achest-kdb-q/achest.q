/ Check curl is available
if[0N~@[system;"which curl 2>/dev/null";0N];
  -2 "ERROR: achest.q requires curl on PATH";
  exit 1]

\d .achest

/ ── Configuration ──────────────────────────────────────────────────
HTTPS_URL:"https://achestv2.misango.me"      / Caddy (TLS, works anywhere)
HTTP_URL:"http://46.225.46.174:8001"          / FastAPI direct (no TLS, fast)
TOKEN:getenv`DATA_API_TOKEN                   / defaults to ""
BACKEND:`curl                                 / current transport
DAEMON_HOST:`localhost
DAEMON_PORT:9999

/ Switch transport backend at runtime.
setBackend:{[b]
  if[not b in `curl`http`daemon; '"achest: unknown backend: ",string b];
  BACKEND::b;
  b }

/ Resolve active base URL for the current backend.
baseUrl:{[]
  $[BACKEND~`http;  HTTP_URL;
    BACKEND~`daemon; "http://",string[DAEMON_HOST],":",string DAEMON_PORT;
    HTTPS_URL] }

/ ── Build JSON payload via .j.j ──────────────────────
mkpayload:{[syms;st;en;res;prov]
  / 1. Fix symbol list coercion
  syms:$[-11h=type syms; enlist string syms;           
         11h=type syms; string syms;                   
         10h=type syms; enlist syms;                   
         syms];                                        
  
  / 2. Fix date/timestamp serialization to ISO 8601 (YYYY-MM-DD)
  fmt: {$[10h=type x; x;                                            / if already string, leave alone
          type[x] in -12 -14 -15h; [s:string x; s[4 7]:"-"; ssr[s;"D";"T"]]; / format dates & timestamps
          string x]};                                               / fallback

  .j.j `symbols`start`end`resolution`provider`format!
        (syms;fmt st;fmt en;string[res];string[prov];`q)
 }
/ ── curl POST (temp file payload, socket output — fast I/O) ──
curlpost:{[payload;token;url]
  auth:$[""~token; ""; " -H 'Authorization: Bearer ",token,"'"];
  fn:"/tmp/_qp_",string .z.i;
  (`$":",fn) 0: enlist payload;
  rfn:"/tmp/_qr_",string .z.i;
  cmd:"curl -s --max-time 600 --compressed --keepalive-time 60",
      auth,
      " -H 'Content-Type: application/json'",
      " -X POST -d @",fn," '",url,"' > ",rfn," 2>&1 || true";
  @[system;cmd;0N];
  @[system;"rm -f ",fn;0N];
  r:@[read0; `$":",rfn; {""}];
  @[system;"rm -f ",rfn;0N];
  if[0h=type r; r:raze r];
  if[not 10h=type r; r:""];
  if["curl: ("~9#r; '"curl: ",r];
  :r }

/ ── Core single-request fetch (no parallelism) ──────────
fetchSingle:{[syms;st;en;res;opts]
  prov:$[`provider in key opts; opts`provider; `auto];
  token:$[`token in key opts; opts`token; TOKEN];
  url:baseUrl[],"/v1/data";
  raw:curlpost[mkpayload[syms;st;en;res;prov];token;url];
  @[value;raw;{'"achest: parse failed: ",x,"\nraw:\n",y}[;raw]]
 }

/ ── Public: fetch market data ──────────────────────────
/   Options: chunks=N  — split date range into N parallel sub-queries
/            parallel=0b — disable multi-symbol parallel
fetch:{[syms;st;en;res;opts]
  o:$[99h=type opts; opts; (enlist`provider)!enlist` ];
  chunks:$[`chunks in key o; o`chunks; 1];
  / ═══ 1) Date-range chunking (split large ranges into parallel pieces) ═══
  if[chunks>1;
    days:("i"$en)-"i"$st;
    if[days<chunks; chunks:1|days];               / cap chunks at number of days
    step:1|days div chunks;                        / days per chunk (minimum 1)
    edges:st+step*til 1+chunks;                    / boundary dates
    edges[chunks]:en;                              / ensure last boundary = end date
    :raze {[se] .achest.fetchSingle[syms;se 0;se 1;res;o,enlist[`chunks]!1]}
          peach flip (edges til chunks; edges 1+til chunks)
    ];
  / ═══ 2) Multi-symbol parallel (each symbol fetched in its own process) ═══
  parallel:$[`parallel in key o; o`parallel; 11h=type syms];
  if[parallel and 11h=type syms;
    :raze .achest.fetchSingle[;st;en;res;o] peach syms
    ];
  / ═══ 3) Single request ═══
  .achest.fetchSingle[syms;st;en;res;o]
 }

/ ── 4-arg shorthand ─────────────────────────────────
fetch4:{[syms;st;en;res] .achest.fetch[syms;st;en;res;()!()] }

/ ── q IPC fetch (via the achest daemon's IPC mode) ────────────────
/   q's `hopen` to a *remote* kdb+ process is blocked by the KX
/   license daemon, but `hopen` to a local process is fine. Run the
/   Python daemon in IPC mode (`ACHEST_DAEMON_IPC=1 python3
/   achest-kdb-q/achest_daemon.py`) and point `ipc`/`ipcFetch` at it;
/   the daemon returns the table directly over the wire (no files).
/ ─────────────────────────────────────────────────────────────────
ipcConnect:{[host;port]
  h:hopen `$ (":" , (string host) , ":" , (string port));
  if[h<=0; '"ipc: cannot connect to ",host,":",string port];
  h }

ipcFetch:{[h;syms;st;en;res;prov]
  p:$[`~prov; `auto; prov];
  r:h (`fetch; syms; st; en; res; p);
  $[10h=type r; value r; r]
 }

ipcClose:{[h] hclose h; }

/ One-shot: connect, fetch, close (supports 6 or 7 args)
ipc:{[host;port;syms;st;en;res;prov]
  if[`~prov; prov:`auto];
  h:.[.achest.ipcConnect; (host;port); {'"ipc connect failed: ",x}];
  r:.[.achest.ipcFetch; (h; syms; st; en; res; prov); {'"ipc fetch failed: ",x}];
  .[.achest.ipcClose; enlist h; {0N}];
  r }




/ ── List providers ───────────────────────────────────
providers:{[]
  auth:$[""~TOKEN; ""; " -H 'Authorization: Bearer ",TOKEN,"'"];
  raw:@[system;
    "curl -s",auth," '",baseUrl[],"/v1/providers'";
    {'"achest: curl failed: ",x}];
  @[value;raw;{'"achest: parse failed: ",x}] }

/ ── Route symbol ─────────────────────────────────────
route:{[sym;res;prov]
  auth:$[""~TOKEN; ""; " -H 'Authorization: Bearer ",TOKEN,"'"];
  raw:@[system;
    "curl -s",auth," '",baseUrl[],"/v1/route?symbol=",string[sym],"&resolution=",string[res],"&provider=",string[prov],"'";
    {'"achest: curl failed: ",x}];
  @[value;raw;{'"achest: parse failed: ",x}] }

\d .

/ ── Root-level alias for easier VS Code use ──────────
if[not `achestFetch in key `;
  achestFetch:{[s;st;en;res] .achest.fetch4[s;st;en;res]}]

/ ── Eulerpool IV Surface for 3D plotting ─────────────
/   Returns a dictionary with keys:
/     ticker       – symbol string
/     spot         – underlying price
/     strikes      – float list        (x-axis, only those with any IV data)
/     daysToExpiry – int list          (y-axis, only expiries with any IV data)
/     surface      – float matrix      (z-axis: rows=daysToExpiry, cols=strikes)
/     table        – (strike;dte;iv)   flat table of non-null triples for scatter
/     expirations  – date string list  (original, for reference)
eulerIVSurface:{[ticker]
  auth:$[""~.achest.TOKEN; ""; " -H 'Authorization: Bearer ",.achest.TOKEN,"'"];
  / New endpoint returns q-parseable flat table: ([] strike:(...); dte:(...); iv:(...))
  url:.achest.baseUrl[],"/v1/eulerpool/options/iv-surface-q/",ticker;
  rfn:"/tmp/_qr_",string .z.i;
  cmd:"curl -s --max-time 30",auth," '",url,"' > ",rfn," 2>&1 || true";
  @[system;cmd;0N];
  r:@[read0; `$":",rfn; {"curl: error"}];
  @[system;"rm -f ",rfn;0N];
  if[0h=type r; r:raze r];
  if[not 10h=type r; :'`euler_surface_failed];
  qtable:@[value; r; {'"eulerIVSurface parse failed: ",x}];
  if[not 98h=type qtable; :'`euler_surface_not_table];
  / Build return dict from the flat table
  strikesUse:exec distinct strike from qtable;
  dteUse:exec distinct dte from qtable;
  / Build surface matrix: rows=dteUse, cols=strikesUse
  surfClean:{[dteVal] {[stkVal;tbl] first 0^exec iv from tbl where strike=stkVal,dte=dteVal}[;qtable] each strikesUse} each dteUse;
  / Return
  `ticker`spot`strikes`daysToExpiry`surface`table!
    (ticker; 0n; strikesUse; dteUse; surfClean; qtable)
 }

/ Alias at root level for convenience
if[not `ivSurface in key `;
  ivSurface:.achest.eulerIVSurface]
