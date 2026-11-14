"""Focused tests for the q IPC protocol handling in achest_daemon.py.

These exercise the wire protocol without requiring a q process, using a
socket pair and a mocked upstream HTTP client.
"""
import importlib.util
import json
import socket
import struct
import threading
from datetime import date, timedelta
from pathlib import Path

import pytest


DAEMON_PATH = Path(__file__).resolve().parent.parent / "achest-kdb-q" / "achest_daemon.py"


@pytest.fixture(scope="module")
def daemon():
    spec = importlib.util.spec_from_file_location("achest_daemon", DAEMON_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def q_sym(s):
    return bytes([0xF5]) + s.encode() + b"\x00"  # -11 symbol atom (no attributes byte)


def q_date(d):
    return bytes([0xF2]) + struct.pack("<i", (d - date(2000, 1, 1)).days)  # -14 date atom


def q_general(items):
    # general list: type byte (0) + attributes byte (0) + 4-byte count
    return bytes([0x00, 0x00]) + struct.pack("<i", len(items)) + b"".join(items)


def q_message(payload, e=1, mtype=1):
    endian = "<" if e else ">"
    return bytes([e, mtype, 0, 0]) + struct.pack(endian + "i", 8 + len(payload)) + payload


class _Chunked:
    """Fake socket that returns data in small chunks to stress _rn."""

    def __init__(self, data):
        self.data = data
        self.pos = 0

    def recv(self, n):
        chunk = self.data[self.pos:self.pos + n]
        self.pos += len(chunk)
        return chunk


def test_rn_accumulates(daemon):
    # The original _rn never appended to the buffer (the `b += x` fell inside
    # the `if not x:` suite), so it raised ConnectionError instead of reading.
    assert daemon._rn(_Chunked(b"hello world"), 11) == b"hello world"


def test_dq_parses_fetch_list(daemon):
    payload = q_general([
        q_sym("fetch"),
        q_sym("BTCUSDT"),
        q_date(date(2026, 9, 1)),
        q_date(date(2026, 10, 6)),
        q_sym("daily"),
        q_sym("auto"),
    ])
    args, off = daemon._dq("<", payload, 0)
    assert off == len(payload)
    assert args[0] == "fetch"
    assert args[1] == "BTCUSDT"
    assert daemon._days_to_iso(args[2], None) == "2026-09-01"
    assert daemon._days_to_iso(args[3], None) == "2026-10-06"
    assert args[4] == "daily"
    assert args[5] == "auto"


def test_dq_parses_symbol_list(daemon):
    payload = q_general([
        q_sym("fetch"),
        bytes([0x0B, 0x00]) + struct.pack("<i", 2) + q_sym("AAPL")[1:] + q_sym("MSFT")[1:],
        q_date(date(2026, 1, 1)),
        q_date(date(2026, 1, 2)),
        q_sym("daily"),
        q_sym("auto"),
    ])
    args, _ = daemon._dq("<", payload, 0)
    assert args[1] == ["AAPL", "MSFT"]


def test_handle_ipc_roundtrip(daemon, monkeypatch):
    class Response:
        text = "([] time:(2026.09.01;); close:1.5f)"

    posted = []
    monkeypatch.setattr(
        daemon._client,
        "post",
        lambda url, data=None, headers=None: posted.append((url, json.loads(data))) or Response(),
    )

    parent, child = socket.socketpair()
    parent.settimeout(5)
    child.settimeout(5)
    server = threading.Thread(target=daemon.handle_ipc, args=(child,), daemon=True)
    server.start()

    payload = q_general([
        q_sym("fetch"),
        q_sym("BTCUSDT"),
        q_date(date(2026, 9, 1)),
        q_date(date(2026, 10, 6)),
        q_sym("daily"),
        q_sym("auto"),
    ])

    # 1) handshake
    parent.sendall(b":\x03\x00")
    assert parent.recv(1) == b"\x03"

    # 2) sync request
    parent.sendall(q_message(payload))

    # 3) response: 8-byte header + payload
    header = b""
    while len(header) < 8:
        header += parent.recv(8 - len(header))
    e, mtype = header[0], header[1]
    endian = "<" if e else ">"
    size = struct.unpack(endian + "i", header[4:8])[0]
    body = b""
    while len(body) < size - 8:
        body += parent.recv(size - 8 - len(body))

    assert mtype == 2  # response
    value, off = daemon._dq(endian, body, 0)
    assert value == Response.text

    # 4) the daemon posted the expected JSON upstream
    url, body_json = posted[0]
    assert url == "http://46.225.46.174:8001/v1/data"
    assert body_json["symbols"] == ["BTCUSDT"]
    assert body_json["start"] == "2026-09-01"
    assert body_json["end"] == "2026-10-06"
    assert body_json["resolution"] == "daily"
    assert body_json["provider"] == "auto"
    assert body_json["format"] == "q"

    parent.close()
    server.join(timeout=2)
    assert not server.is_alive()
