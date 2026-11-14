/ qserver_proxy.q — q IPC proxy for FastAPI
/ Returns the q table directly over IPC; no temp files, no local file reads.
fmtDate:{ssr[string x;".";"-"]};

.z.pg:{[x]
  cmd:first x;
  if[not cmd in (`fetch;`text); '`unknown];

  syms:enlist $[10h=type x 1; x 1; string x 1];
  st:fmtDate x 2;
  en:fmtDate x 3;
  res:$[10h=type x 4; x 4; string x 4];
  prov:$[10h=type x 5; x 5; "auto"];
  token:getenv`DATA_API_TOKEN;
  payload:.j.j `symbols`start`end`resolution`provider`format!(syms;st;en;res;prov;`q);

  auth:$[""~token; ""; " -H 'Authorization: Bearer ",token,"'"];
  / Inline the JSON payload directly (`.j.j` emits double quotes, no single
  / quotes), and capture curl stdout via `system` — no /tmp files, no read0.
  curlcmd:"curl -s --max-time 600 --compressed",auth," -H 'Content-Type: application/json' -X POST -d '",payload,"' 'http://localhost:8001/v1/data' 2>&1";
  r:@[system;curlcmd;0N];
  if[0N~r; '"qproxy: curl failed"];
  if[0=count r; '"qproxy: empty response from FastAPI"];
  resStr:$[0h=type r; 10h$"\n" sv r; string r];
  if["curl: ("~9#resStr; '"qproxy: ",resStr];
  / Try 1: value as q literal (format = "q" response)
  @[value; resStr; {
    / Try 2: .j.k as JSON (error responses from FastAPI)
    @[.j.k; resStr; {`nodata}]
   }]
 };