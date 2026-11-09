"""LEAN archive format tests.

LEAN data files must not contain a header row — the first line inside
``{symbol}.zip`` has to be a valid data row.  A ``Time,Open,...`` header makes
LEAN reject every bar, which shows up as
``failed-data-requests-percentage: 100%`` in the backtest report.

Run with pytest, or directly::

    python tests/test_lean_zip_format.py
"""

from io import BytesIO
from pathlib import Path
from zipfile import ZipFile
import sys

import pandas as pd

# Make sure the repo copy of ``achest`` wins over any installed version.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from achest.client import _read_lean_zip
from achest.service import to_lean_zip

_HEADER = "Time,Open,High,Low,Close,Volume"


def _daily_frame() -> pd.DataFrame:
    index = pd.DatetimeIndex(
        [pd.Timestamp("2020-01-02 15:30"), pd.Timestamp("2020-01-03 15:30")],
        name="timestamp",
    )
    return pd.DataFrame(
        {
            "symbol": ["SOXX", "SOXX"],
            "open": [219.5, 220.7],
            "high": [220.1, 221.4],
            "low": [217.8, 219.9],
            "close": [219.9, 220.2],
            "volume": [123456, 234567],
        },
        index=index,
    )


def _read_csv_inside(zip_bytes: bytes) -> tuple[str, str]:
    """Return (csv_name, csv_text) for a single-CSV LEAN zip."""
    with ZipFile(BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        assert len(names) == 1, f"expected one CSV per zip, got {names}"
        return names[0], zf.read(names[0]).decode()


def test_to_lean_zip_has_no_header():
    zips = to_lean_zip(_daily_frame(), "daily")

    assert list(zips) == ["soxx.zip"]
    csv_name, csv_text = _read_csv_inside(zips["soxx.zip"])
    assert csv_name == "soxx_daily_trade.csv"
    assert not csv_text.startswith(_HEADER)

    lines = csv_text.strip().splitlines()
    assert lines[0] == "20200102 00:00,2195000,2201000,2178000,2199000,123456"
    assert lines[1] == "20200103 00:00,2207000,2214000,2199000,2202000,234567"
    # Every row, including the last, is newline-terminated.
    assert csv_text.endswith("\n")


def test_to_lean_zip_intraday_uses_milliseconds():
    frame = _daily_frame()
    zips = to_lean_zip(frame, "minute")

    _, csv_text = _read_csv_inside(zips["soxx.zip"])
    first_line = csv_text.splitlines()[0]
    # 15:30 == 55 800 000 ms since midnight, prices still scaled x10 000.
    assert first_line == "55800000,2195000,2201000,2178000,2199000,123456"


def test_client_round_trip_reads_headerless_archive():
    inner = to_lean_zip(_daily_frame(), "daily")
    outer = BytesIO()
    with ZipFile(outer, "w") as outer_zf:
        for name, data in inner.items():
            outer_zf.writestr(name, data)

    frame = _read_lean_zip(outer.getvalue()).set_index("timestamp")

    assert list(frame.columns) == ["open", "high", "low", "close", "volume", "symbol"]
    assert frame.iloc[0]["symbol"] == "SOXX"
    assert frame.iloc[0]["close"] == 2199000
    assert frame.index[0] == pd.Timestamp("2020-01-02")


def test_client_tolerates_legacy_header():
    legacy_csv = f"{_HEADER}\n20200102 00:00,2195000,2201000,2178000,2199000,123456\n"
    inner = BytesIO()
    with ZipFile(inner, "w") as inner_zf:
        inner_zf.writestr("soxx_daily_trade.csv", legacy_csv)

    outer = BytesIO()
    with ZipFile(outer, "w") as outer_zf:
        outer_zf.writestr("soxx.zip", inner.getvalue())

    frame = _read_lean_zip(outer.getvalue())

    assert len(frame) == 1
    assert frame.iloc[0]["close"] == 2199000
    assert frame.iloc[0]["timestamp"] == pd.Timestamp("2020-01-02")


if __name__ == "__main__":
    for name, test in sorted(globals().items()):
        if name.startswith("test_") and callable(test):
            test()
            print(f"PASS {name}")
