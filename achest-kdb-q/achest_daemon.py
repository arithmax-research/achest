#!/usr/bin/env python3
"""
achest_daemon.py — Fastest keepalive daemon for achest.q

Clients:   q -> system(curl) -> daemon HTTP -> httpx pool -> FastAPI
IPC mode:  q -> hopen          -> daemon IPC -> httpx pool -> FastAPI

USAGE: python3 achest-kdb-q/achest_daemon.py [--port 9999]
       ACHEST_DAEMON_IPC=1 for q IPC mode (q `hopen` -> daemon).
       ACHEST_DAEMON_BASE_URL env overrides backend endpoint.
"""
from __future__ import annotations
import argparse, json, os, socket, struct, sys, threading
import httpx
from datetime import date, timedelta

BASE_URL_ENV = "ACHEST_DAEMON_BASE_URL"
DEFAULT_BASE_URL = "http://46.225.46.174:8001"
_client = httpx.Client(timeout=httpx.Timeout(600, connect=10),
    limits=httpx.Limits(max_keepalive_connections=16, max_connections=32))
IPC_MODE = os.environ.get("ACHEST_DAEMON_IPC") == "1"

# ── HTTP handler (reliable) ────────────────────────────────────────

def handle_http(conn):
    try:
        buf = b""
        while True:
            c = conn.recv(1)
            if not c: raise ConnectionError("disconnected")
            buf += c
            if buf.endswith(b"\r\n\r\n"): break
        lines = buf.decode("utf-8",errors="replace").split("\r\n")
        ep = lines[0].split(" ",2)[1] if len(lines[0].split(" ",2))>1 else "/v1/data"
        cl, tk = 0, ""
        for l in lines[1:]:
            if ":" not in l: continue
            k,v = l.split(":",1)
            lk = k.strip().lower()
            if lk=="authorization": tk=v.strip().replace("Bearer ","",1)
            if lk=="content-length": cl=int(v.strip())
        body = b""
        while len(body)<cl:
            c = conn.recv(cl-len(body))
            if not c: break
            body += c
    except (ConnectionError,ValueError): conn.close(); return
    hdrs = {"Content-Type":"application/json","Accept-Encoding":"gzip"}
    if tk: hdrs["Authorization"]=f"Bearer {tk}"
    try:
        base = os.environ.get(BASE_URL_ENV,DEFAULT_BASE_URL).rstrip("/")
        r = _client.post(base+ep, data=body, headers=hdrs)
        conn.sendall(f"HTTP/1.1 {r.status_code} OK\r\nContent-Type:text/plain\r\n"
            f"Content-Length:{len(r.text)}\r\nConnection:close\r\n\r\n{r.text}".encode())
    except Exception as e:
        err = json.dumps({"error":str(e)})
        conn.sendall(f"HTTP/1.1 502 Bad Gateway\r\nContent-Type:application/json\r\n"
            f"Content-Length:{len(err)}\r\nConnection:close\r\n\r\n{err}".encode())
    finally: conn.close()

# ── q IPC handler ──────────────────────────────────────────────────

def _rn(conn, n):
    b = b""
    while len(b) < n:
        x = conn.recv(n - len(b))
        if not x:
            raise ConnectionError("connection closed")
        b += x
    return b

def _sb(byte):
    """Interpret a raw byte as a signed q IPC type code."""
    return byte if byte < 128 else byte - 256


def _dq(endian, data, off):
    """Decode one q IPC value from ``data`` at ``off``; returns (value, next_off)."""
    if off >= len(data):
        return None, off
    t = _sb(data[off])
    if t == 0:               # general list
        cnt = struct.unpack(endian + "i", data[off+1:off+5])[0]
        off += 5
        items = []
        for _ in range(cnt):
            v, off = _dq(endian, data, off)
            items.append(v)
        return items, off
    if t == -11:             # symbol atom (null-terminated)
        end = data.index(b"\x00", off + 1)
        return data[off+1:end].decode("utf-8", errors="replace"), end + 1
    if t == 11:              # symbol list (null-terminated entries)
        cnt = struct.unpack(endian + "i", data[off+1:off+5])[0]
        off += 5
        syms = []
        for _ in range(cnt):
            end = data.index(b"\x00", off)
            syms.append(data[off:end].decode("utf-8", errors="replace"))
            off = end + 1
        return syms, off
    if t == 10:              # char vector (q string)
        n = struct.unpack(endian + "i", data[off+1:off+5])[0]
        return data[off+5:off+5+n].decode("utf-8", errors="replace"), off+5+n
    if t == -14:             # date (days since 2000.01.01)
        return struct.unpack(endian + "i", data[off+1:off+5])[0], off+5
    if t == -6:              # 32-bit int
        return struct.unpack(endian + "i", data[off+1:off+5])[0], off+5
    if t == -7:              # 64-bit long
        return struct.unpack(endian + "q", data[off+1:off+9])[0], off+9
    if t == -8:              # 64-bit float
        return struct.unpack(endian + "d", data[off+1:off+9])[0], off+9
    return None, off + 1


def _days_to_iso(days, default):
    if days is None:
        return default
    return (date(2000, 1, 1) + timedelta(days=int(days))).isoformat()


def _q_string(endian, s):
    """Serialize a Python string as a q char vector (type 10)."""
    raw = s.encode("utf-8")
    return bytes([10]) + struct.pack(endian + "i", len(raw)) + raw


def handle_ipc(conn):
    endian = "<"
    e = 1
    try:
        # ── 1) q IPC handshake ──
        #   q's `hopen` sends a null-terminated "user:pass" login followed by a
        #   capability byte, and expects a single-byte capability response.
        login = b""
        while not login.endswith(b"\x00"):
            c = conn.recv(1)
            if not c:
                raise ConnectionError("disconnected during handshake")
            login += c
        conn.sendall(b"\x03")          # accept: protocol/capability 3

        # ── 2) answer sync fetch requests until the client disconnects ──
        while True:
            hdr = _rn(conn, 8)
            e = hdr[0]                 # 1 = little-endian, 0 = big-endian
            endian = "<" if e else ">"
            size = struct.unpack(endian + "i", hdr[4:8])[0]
            py = _rn(conn, size - 8) if size > 8 else b""

            args, _ = _dq(endian, py, 0)
            if not isinstance(args, list) or len(args) < 6:
                continue

            syms = args[1]
            if isinstance(syms, list):
                syms = ",".join(str(s) for s in syms if s is not None)
            syms = str(syms) if syms else "BTCUSDT"
            st = _days_to_iso(args[2], "2026-10-01")
            en = _days_to_iso(args[3], "2026-10-06")
            res = str(args[4]) if args[4] else "daily"
            prov = str(args[5]) if args[5] else "auto"

            body = json.dumps({"symbols": [syms], "start": st, "end": en,
                               "resolution": res, "provider": prov, "format": "q"})
            hdrs = {"Content-Type": "application/json", "Accept-Encoding": "gzip"}
            try:
                base = os.environ.get(BASE_URL_ENV, DEFAULT_BASE_URL).rstrip("/")
                r = _client.post(base + "/v1/data", data=body, headers=hdrs)
                payload = _q_string(endian, r.text)
            except Exception as exc:
                payload = _q_string(endian, f"daemon error: {exc}")

            # ── 3) send the sync response (type 2) ──
            header = bytes([e, 2, 0, 0]) + struct.pack(endian + "i", 8 + len(payload))
            conn.sendall(header + payload)
    except (ConnectionError, struct.error, OSError, ValueError):
        pass
    finally:
        conn.close()

# ── Server ─────────────────────────────────────────────────────────

def run(port):
    srv=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    srv.bind(("",port)); srv.listen(64)
    mode="IPC" if IPC_MODE else "HTTP"
    base=os.environ.get(BASE_URL_ENV,DEFAULT_BASE_URL)
    print(f"achest-daemon: {mode} port {port} -> {base}",file=sys.stderr,flush=True)
    while True:
        conn,_=srv.accept()
        tgt=handle_ipc if IPC_MODE else handle_http
        threading.Thread(target=tgt,args=(conn,),daemon=True).start()

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--port",type=int,default=9999)
    run(p.parse_args().port)

if __name__=="__main__": main()