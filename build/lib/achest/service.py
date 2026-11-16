"""Provider routing and normalized market-data retrieval."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, time
import os
import threading
from typing import Iterable

import pandas as pd
import requests


@dataclass(frozen=True)
class DataRequest:
    symbols: list[str]
    start: date
    end: date
    resolution: str = "daily"
    provider: str = "auto"


PROVIDER_CAPABILITIES = {
    "yahoo": {
        "assets": {"equity", "etf", "index", "crypto", "forex", "futures"},
        "resolutions": {"minute", "hour", "daily", "weekly", "monthly"},
    },
    "binance": {
        "assets": {"crypto"},
        "resolutions": {"tick", "second", "minute", "hour", "daily", "weekly", "monthly"},
    },
    "massive": {
        "assets": {"equity", "etf", "index", "crypto", "forex", "futures", "options"},
        "resolutions": {"tick", "second", "minute", "hour", "daily", "weekly", "monthly"},
    },
    "databento": {
        "assets": {"equity", "etf", "futures", "options", "forex"},
        "resolutions": {"tick", "second", "minute", "hour", "daily"},
    },
    "alpaca": {
        "assets": {"equity", "etf", "crypto"},
        "resolutions": {"tick", "minute", "hour", "daily"},
    },
    "tiingo": {
        "assets": {"equity", "etf", "index", "crypto", "forex"},
        "resolutions": {"tick", "second", "minute", "hour", "daily"},
    },
    "alpha_vantage": {
        "assets": {"equity", "etf", "index", "crypto", "forex", "economic"},
        "resolutions": {"minute", "hour", "daily", "weekly", "monthly"},
    },
    "fred": {
        "assets": {"economic"},
        "resolutions": {"daily", "weekly", "monthly", "quarterly", "annual"},
    },
    "quandl": {
        "assets": {"equity", "etf", "index", "crypto", "economic", "futures"},
        "resolutions": {"daily", "weekly", "monthly", "quarterly", "annual"},
    },
    "eulerpool": {
        "assets": {"equity", "etf", "index", "crypto", "forex", "futures", "economic", "commodity", "options"},
        "resolutions": {"daily", "weekly", "monthly"},
    },
}
FUTURES_ROOTS = {"ES", "NQ", "YM", "RTY", "CL", "GC", "SI", "ZB", "ZN", "NG", "ZS", "ZC", "ZW"}
ECONOMIC_SYMBOLS = {"GDP", "UNRATE", "CPIAUCSL", "FEDFUNDS", "DGS10"}


class UnsupportedRequest(ValueError):
    pass


def classify_symbol(symbol: str) -> str:
    clean = symbol.upper().strip()
    # Commodities first: many roots (NG, ZC, ZW, ...) collide with futures roots
    # and bare metals pairs (XAUUSD) would otherwise fall through to equity.
    if _commodity_symbol(clean):
        return "commodity"
    if _forex_pair(clean):
        return "forex"
    if clean.endswith(("USDT", "USDC", "-USD")):
        return "crypto"
    if clean.endswith(".FUT") or ".c." in clean or clean in FUTURES_ROOTS:
        return "futures"
    if clean.startswith("^"):
        return "index"
    if clean in ECONOMIC_SYMBOLS:
        return "economic"
    if clean.isalpha() and len(clean) <= 5:
        return "equity"
    return "equity"


# Commodity symbols accepted by Eulerpool's /commodity endpoints (from
# /commodity/list), plus common Yahoo-style futures and metal-pair aliases.
_COMMODITY_SYMBOLS = {
    # energy
    "CL1", "CL", "BZ", "BRENT", "NG", "RB", "HO", "COAL", "EU_GAS", "UK_GAS",
    "ETHANOL", "NAPHTHA", "PROPANE", "URANIUM", "METHANOL", "COKING_COAL",
    "LNG_JKM", "DE_GAS", "URALS", "WTI", "WTI_CRUDE",
    # metals
    "XAU", "XAG", "HG", "STEEL", "LITHIUM", "IRON_CNY", "PL", "SCRAP_ALU",
    "IRON", "SILICON", "SCRAP_STEEL", "TITANIUM", "HRC_STEEL", "ALU_ALLOY",
    "COBALT_OH", "PA", "ALI", "ALUMINUM",
    # agriculture
    "ZS", "ZW", "LUMBER", "PALM_OIL", "CHEESE", "MILK", "RUBBER", "OJ", "KC",
    "CT", "RICE", "CANOLA", "OAT", "WOOL", "SB", "CC", "TEA", "SUNFLOWER",
    "RAPESEED", "BARLEY", "BUTTER", "ZC", "ZL", "ZM", "SOYBEAN",
    # industrial
    "BITUMEN", "COBALT", "LEAD", "TIN", "ZINC", "NICKEL", "MOLYBDENUM",
    "RHODIUM", "PHOSPHORUS", "POLYETHYLENE", "POLYVINYL", "POLYPROPYLENE",
    "SYN_RUBBER", "SODA_ASH", "NEODYMIUM", "STYRENE", "SULFUR", "TELLURIUM",
    "UREA", "DAP", "MAGNESIUM", "GALLIUM", "GERMANIUM", "MANGANESE", "INDIUM",
    "KRAFT_PULP", "COPPER",
    # livestock
    "FEEDER", "LE", "HE", "BEEF", "POULTRY", "EGGS_US", "EGGS_CH", "SALMON",
    # commodity indexes / electricity
    "CRB", "SSE_COMM", "GSCI", "CF_INDEX", "TE_CRACK", "TE_GRAINS", "TE_METALS",
    "TE_SOFT", "CARBON", "WIND", "NUCLEAR", "SOLAR", "ELEC_UK", "ELEC_DE",
    "ELEC_FR", "ELEC_IT", "ELEC_ES",
}

# Precious/industrial metal codes that are quoted against a fiat currency
# (e.g. XAUUSD) — these are commodities, not FX pairs.
_METAL_CODES = {"XAU", "XAG", "XPT", "XPD", "HG"}

# Yahoo-style futures roots (and short aliases) mapped to Eulerpool commodity
# tickers, so ``GC=F`` / ``CL=F`` / ``SI=F`` resolve to a servable symbol.
_COMMODITY_ALIASES = {
    "GC": "XAU", "SI": "XAG", "PL": "PL", "PA": "PA", "HG": "HG",
    "CL": "CL1", "BZ": "BZ", "NG": "NG", "RB": "RB", "HO": "HO",
    "ZC": "ZC", "ZW": "ZW", "ZS": "ZS", "ZL": "ZL", "ZM": "ZM",
    "KC": "KC", "CC": "CC", "SB": "SB", "CT": "CT", "OJ": "OJ",
    "LE": "LE", "HE": "HE", "GF": "FEEDER",
}


def _commodity_symbol(symbol: str) -> str | None:
    """Return the Eulerpool commodity ticker if *symbol* names a commodity.

    Accepts bare commodity codes (``XAU``, ``NG``, ``CL1``), Yahoo-style
    futures (``GC=F``, ``CL=F``), and metal pairs quoted in a fiat currency
    (``XAUUSD`` → ``XAU``, ``XAU/USD`` → ``XAU``).
    """
    clean = symbol.upper().strip()

    # Yahoo futures suffix: GC=F, CL=F, SI=F ...
    if clean.endswith("=F"):
        base = clean[:-2]
        if base in _COMMODITY_ALIASES:
            return _COMMODITY_ALIASES[base]
        if base in _COMMODITY_SYMBOLS:
            return base

    # Metal pairs like XAUUSD / XAU-USD / XAU/USD -> XAU
    if clean[:3] in _METAL_CODES:
        rest = clean[3:]
        if not rest:
            return clean
        if rest[:1] in ("/", "-", "_", ":"):
            return clean[:3]
        if len(rest) == 3 and rest.isalpha():
            return clean[:3]

    if clean in _COMMODITY_ALIASES:
        return _COMMODITY_ALIASES[clean]
    if clean in _COMMODITY_SYMBOLS:
        return clean
    return None


def _crypto_base(symbol: str) -> str:
    """Strip a quote-currency suffix from a crypto pair (``BTCUSDT`` → ``BTC``).

    Eulerpool's crypto endpoints key on the bare base symbol, so pairs must be
    reduced before the request is made.
    """
    clean = symbol.upper().strip()
    for suffix in ("USDT", "USDC", "-USD", "USD"):
        if clean.endswith(suffix) and len(clean) > len(suffix):
            return clean[: -len(suffix)]
    return clean


# Common fiat/currency codes used to recognise a bare ``EURUSD``-style pair.
_CURRENCY_CODES = {
    "USD", "EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD", "CNY", "CNH",
    "HKD", "SGD", "SEK", "NOK", "DKK", "TRY", "MXN", "ZAR", "INR", "BRL",
    "RUB", "PLN", "CZK", "HUF", "KRW", "TWD", "THB", "ILS", "SAR", "AED",
}


def _forex_pair(symbol: str) -> tuple[str, str] | None:
    """Return ``(base, quote)`` if *symbol* names a currency pair.

    Accepts ``EUR/USD``, ``EUR-USD``, ``EUR_USD``, ``EURUSD`` and the
    Yahoo-style ``EURUSD=X`` suffix.  Returns ``None`` when the symbol is
    not an unambiguous FX pair.
    """
    clean = symbol.upper().strip()
    if clean.endswith("=X"):
        clean = clean[:-2]

    for sep in ("/", "-", "_", ":"):
        if sep in clean:
            base, _, quote = clean.partition(sep)
            if base and quote:
                return base, quote
            return None

    if len(clean) == 6 and clean.isalpha():
        base, quote = clean[:3], clean[3:]
        if base in _CURRENCY_CODES and quote in _CURRENCY_CODES:
            return base, quote
    return None



# Map provider -> env var name for required API key (empty = no key needed)
_PROVIDER_KEY_MAP = {
    "databento": "DATA_BENTO_API_KEY",
    "massive": "MASSIVE_API_KEY",
    "alpaca": "ALPACA_API_KEY",
    "tiingo": "TIINGO_API_KEY",
    "alpha_vantage": "ALPHA_VANTAGE_API_KEY",
    "fred": "FRED_API_KEY",
    "quandl": "QUANDL_API_KEY",
    "binance": "BINANCE_API_KEY",   # requires API key
    "yahoo": "",                     # public API, no key required
    "eulerpool": "EULERPOOL_API_KEY",
}


def _provider_key_configured(provider: str) -> bool:
    """Return True if the provider can be used (key configured or not needed)."""
    env_var = _PROVIDER_KEY_MAP.get(provider, "")
    if not env_var:
        return True
    # Eulerpool supports both EULERPOOL_API_KEY and EULER_TOKEN
    if provider == "eulerpool":
        return bool(os.getenv("EULERPOOL_API_KEY") or os.getenv("EULER_TOKEN"))
    return bool(os.getenv(env_var))


def select_provider(symbol: str, requested: str, resolution: str) -> str:
    asset = classify_symbol(symbol)
    if requested != "auto":
        capabilities = PROVIDER_CAPABILITIES.get(requested)
        if not capabilities or asset not in capabilities["assets"] or resolution not in capabilities["resolutions"]:
            raise UnsupportedRequest(f"Provider {requested!r} does not support {asset} at {resolution} resolution")
        if not _provider_key_configured(requested):
            raise UnsupportedRequest(f"Provider {requested!r} is not configured on this server (missing API key)")
        return requested
    preferences = {
        "futures": ["databento", "massive", "eulerpool"],
        "crypto": ["binance", "yahoo", "tiingo", "alpha_vantage", "eulerpool"],
        "equity": ["yahoo", "alpaca", "tiingo", "alpha_vantage", "eulerpool"],
        "etf": ["yahoo", "alpaca", "tiingo", "eulerpool"],
        "index": ["yahoo", "alpaca", "tiingo", "eulerpool"],
        "economic": ["fred", "quandl", "eulerpool"],
        "options": ["eulerpool"],
        "commodity": ["eulerpool"],
    }.get(asset, ["yahoo", "eulerpool"])
    for provider in preferences:
        if resolution in PROVIDER_CAPABILITIES[provider]["resolutions"] and _provider_key_configured(provider):
            return provider
    raise UnsupportedRequest(f"No provider supports {asset} at {resolution} resolution")


def _as_datetime(value: date, end_of_day: bool = False) -> datetime:
    return datetime.combine(value, time.max if end_of_day else time.min)


def _frame_from_bars(bars: Iterable[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(list(bars))
    if frame.empty:
        return frame
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    return frame.set_index("timestamp").sort_index()[["open", "high", "low", "close", "volume"]]


def _yahoo(symbol: str, request: DataRequest) -> pd.DataFrame:
    import yfinance as yf

    # yfinance treats its ``end`` parameter as *exclusive* (like SQL ``<``),
    # not inclusive.  Bump by one day so the API's inclusive contract is
    # honoured: ``request.end`` itself is included in the result.
    from datetime import timedelta
    adjusted_end = (request.end + timedelta(days=1)).isoformat()

    interval = {"minute": "1m", "hour": "1h", "daily": "1d"}[request.resolution]
    history = yf.Ticker(symbol).history(
        start=request.start.isoformat(), end=adjusted_end, interval=interval, auto_adjust=False
    )
    if history.empty:
        return pd.DataFrame()
    history.index.name = "timestamp"
    return history.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})[
        ["open", "high", "low", "close", "volume"]
    ]


def _binance(symbol: str, request: DataRequest) -> pd.DataFrame:
    """Fetch OHLCV from Binance with concurrent pagination and API-key auth.

    The total date range is split into day-sized chunks, each fetched in a
    separate thread with proper sequential pagination (handles data gaps).
    Results are merged and sorted at the end.
    """
    interval = {"second": "1s", "minute": "1m", "hour": "1h", "daily": "1d"}[request.resolution]
    interval_ms = {"second": 1000, "minute": 60_000, "hour": 3_600_000, "daily": 86_400_000}[request.resolution]

    base_url = os.getenv("BINANCE_BASE_URL", "https://api.binance.com").rstrip("/")
    api_key = os.getenv("BINANCE_API_KEY")
    proxy_url = os.getenv("BINANCE_PROXY") or os.getenv("HTTPS_PROXY")
    # Use a shared session for connection pooling across all threads
    session = requests.Session()
    if api_key:
        session.headers.update({"X-MBX-APIKEY": api_key})
    if proxy_url:
        session.proxies.update({"https": proxy_url, "http": proxy_url})

    start_ms = int(_as_datetime(request.start).timestamp() * 1000)
    end_ms = int(_as_datetime(request.end, True).timestamp() * 1000)

    # Size of each parallel chunk: 1 day in milliseconds
    _CHUNK_MS = 86_400_000

    chunk_starts = list(range(start_ms, end_ms, _CHUNK_MS))
    all_rows: list = []
    lock = threading.Lock()

    def _fetch_chunk(chunk_start_ms: int) -> None:
        """Sequentially paginate through one day-sized chunk."""
        chunk_end_ms = min(chunk_start_ms + _CHUNK_MS, end_ms)
        local_rows: list = []
        t = chunk_start_ms
        while t < chunk_end_ms:
            try:
                response = session.get(
                    f"{base_url}/api/v3/klines",
                    params={
                        "symbol": symbol.upper(),
                        "interval": interval,
                        "startTime": t,
                        "endTime": chunk_end_ms,
                        "limit": 1000,
                    },
                    timeout=60,
                )
                response.raise_for_status()
                rows = response.json()
                if not rows:
                    break
                local_rows.extend(rows)
                # Advance past the last candle's open time to avoid duplicates
                t = rows[-1][0] + interval_ms
            except requests.RequestException:
                break

        if local_rows:
            with lock:
                all_rows.extend(local_rows)

    # Determine concurrency — cap at 20 threads (Binance IP rate limit)
    max_workers = min(20, len(chunk_starts) or 1)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_fetch_chunk, cs) for cs in chunk_starts]
        for future in as_completed(futures):
            future.result()  # re-raise any exception from the thread

    # Sort all candles by open-time to restore chronological order
    all_rows.sort(key=lambda r: r[0])

    return _frame_from_bars({
        "timestamp": datetime.fromtimestamp(row[0] / 1000), "open": row[1], "high": row[2],
        "low": row[3], "close": row[4], "volume": row[5],
    } for row in all_rows)


def _massive(symbol: str, request: DataRequest) -> pd.DataFrame:
    """Fetch OHLCV data from Massive (replaces deprecated Polygon)."""
    key = os.getenv("MASSIVE_API_KEY")
    if not key:
        raise RuntimeError("MASSIVE_API_KEY is not configured on the server")
    access_key = os.getenv("MASSIVE_ACCESS_KEY_ID", "")
    secret_key = os.getenv("MASSIVE_SECRET_ACCESS_KEY", "")
    endpoint = os.getenv("MASSIVE_S3_ENDPOINT", "https://files.massive.com")
    bucket = os.getenv("MASSIVE_S3_BUCKET", "flatfiles")
    timespan = {"minute": "minute", "hour": "hour", "daily": "day"}[request.resolution]
    response = requests.get(
        f"{endpoint}/v1/{bucket}/aggs/ticker/{symbol}/range/1/{timespan}/{request.start}/{request.end}",
        params={"apiKey": key, "accessKey": access_key, "secretKey": secret_key, "limit": 50000},
        timeout=120,
    )
    response.raise_for_status()
    return _frame_from_bars({
        "timestamp": datetime.fromtimestamp(row["t"] / 1000), "open": row["o"], "high": row["h"],
        "low": row["l"], "close": row["c"], "volume": row.get("v", 0),
    } for row in response.json().get("results", []))


def _databento(symbol: str, request: DataRequest) -> pd.DataFrame:
    import databento as db
    from databento import Schema, SType

    key = os.getenv("DATA_BENTO_API_KEY")
    if not key:
        raise RuntimeError("DATA_BENTO_API_KEY is not configured on the server")
    continuous = symbol.replace(".FUT", ".c.0")
    schema = {"tick": Schema.MBP_1, "second": Schema.OHLCV_1S, "minute": Schema.OHLCV_1M, "hour": Schema.OHLCV_1H, "daily": Schema.OHLCV_1D}[request.resolution]
    data = db.Historical(key=key).timeseries.get_range(
        dataset="GLBX.MDP3", symbols=continuous, schema=schema,
        start=request.start.isoformat(), end=request.end.isoformat(),
        stype_in=SType.CONTINUOUS if ".c." in continuous else SType.RAW_SYMBOL,
    ).to_df()
    if data.empty:
        return data
    # Databento OHLCV schemas return timestamp as the index named "ts_event"
    if "ts_event" in data.columns:
        data = data.rename(columns={"ts_event": "timestamp"}).set_index("timestamp")
    elif data.index.name == "ts_event":
        data.index.name = "timestamp"
    return data[["open", "high", "low", "close", "volume"]]


def _fred(symbol: str, request: DataRequest) -> pd.DataFrame:
    """Fetch economic indicators from FRED."""
    key = os.getenv("FRED_API_KEY")
    if not key:
        raise RuntimeError("FRED_API_KEY is not configured on the server")
    response = requests.get(
        "https://api.stlouisfed.org/fred/series/observations",
        params={"series_id": symbol, "api_key": key, "file_type": "json",
                "observation_start": request.start.isoformat(), "observation_end": request.end.isoformat()},
        timeout=60,
    )
    response.raise_for_status()
    rows = response.json().get("observations", [])
    bars = []
    for row in rows:
        value = row.get("value", ".")
        if value == ".":
            continue
        bars.append({
            "timestamp": row["date"],
            "open": float(value),
            "high": float(value),
            "low": float(value),
            "close": float(value),
            "volume": 0,
        })
    return _frame_from_bars(bars)


def _alpaca(symbol: str, request: DataRequest) -> pd.DataFrame:
    key = os.getenv("ALPACA_API_KEY")
    secret = os.getenv("ALPACA_SECRET_KEY")
    if not key or not secret:
        raise RuntimeError("ALPACA_API_KEY or ALPACA_SECRET_KEY is not configured")
    timespan = {"tick": "tick", "minute": "1Min", "hour": "1Hour", "daily": "1Day"}[request.resolution]
    url = f"https://data.alpaca.markets/v2/stocks/{symbol}/bars"
    response = requests.get(url, params={
        "timeframe": timespan, "start": request.start.isoformat(), "end": request.end.isoformat(), "limit": 10000,
    }, headers={"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}, timeout=60)
    response.raise_for_status()
    return _frame_from_bars({
        "timestamp": bar["t"], "open": bar["o"], "high": bar["h"],
        "low": bar["l"], "close": bar["c"], "volume": bar["v"],
    } for bar in response.json().get("bars", []))


def _tiingo(symbol: str, request: DataRequest) -> pd.DataFrame:
    key = os.getenv("TIINGO_API_KEY")
    if not key:
        raise RuntimeError("TIINGO_API_KEY is not configured")
    freq = {"tick": "tick", "second": "1sec", "minute": "1min", "hour": "1hour", "daily": "daily"}[request.resolution]
    # Tiingo crypto API expects lowercase tickers
    ticker = symbol.lower() if "USDT" in symbol or "USD" in symbol else symbol
    url = f"https://api.tiingo.com/tiingo/{'crypto' if 'USDT' in symbol or 'USD' in symbol else 'daily'}/{ticker}/prices"
    response = requests.get(url, params={
        "startDate": request.start.isoformat(), "endDate": request.end.isoformat(), "resampleFreq": freq,
    }, headers={"Authorization": f"Token {key}"}, timeout=60)
    response.raise_for_status()
    return _frame_from_bars({
        "timestamp": row["date"], "open": row["open"], "high": row["high"],
        "low": row["low"], "close": row["close"], "volume": row.get("volume", 0),
    } for row in response.json())


def _alpha_vantage(symbol: str, request: DataRequest) -> pd.DataFrame:
    key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not key:
        raise RuntimeError("ALPHA_VANTAGE_API_KEY is not configured")
    func = {"minute": "TIME_SERIES_INTRADAY", "hour": "TIME_SERIES_INTRADAY", "daily": "TIME_SERIES_DAILY"}[request.resolution]
    params = {"function": func, "symbol": symbol, "apikey": key, "outputsize": "full"}
    if func == "TIME_SERIES_INTRADAY":
        params["interval"] = {"minute": "1min", "hour": "60min"}[request.resolution]
    response = requests.get("https://www.alphavantage.co/query", params=params, timeout=60)
    response.raise_for_status()
    data = response.json()
    key_map = {"1. open": "open", "2. high": "high", "3. low": "low", "4. close": "close", "5. volume": "volume"}
    time_key = next((k for k in data if "Time Series" in k), None)
    if not time_key:
        return pd.DataFrame()
    return _frame_from_bars(
        {"timestamp": ts, **{key_map[k]: float(v) if k != "5. volume" else int(float(v)) for k, v in row.items() if k in key_map}}
        for ts, row in data[time_key].items()
    )


def _quandl(symbol: str, request: DataRequest) -> pd.DataFrame:
    key = os.getenv("QUANDL_API_KEY")
    if not key:
        raise RuntimeError("QUANDL_API_KEY is not configured")
    response = requests.get(f"https://www.quandl.com/api/v3/datasets/{symbol}/data.json", params={
        "api_key": key, "start_date": request.start.isoformat(), "end_date": request.end.isoformat(),
    }, timeout=60)
    response.raise_for_status()
    data = response.json().get("dataset_data", {})
    cols = {c.lower(): i for i, c in enumerate(data.get("column_names", []))}
    return _frame_from_bars({
        "timestamp": row[0], "open": row[cols.get("open", 1)], "high": row[cols.get("high", 2)],
        "low": row[cols.get("low", 3)], "close": row[cols.get("close", 4)], "volume": row[cols.get("volume", 5)] if "volume" in cols else 0,
    } for row in data.get("data", []))


def _eulerpool(symbol: str, request: DataRequest) -> pd.DataFrame:
    """Fetch OHLCV data from Eulerpool API as a fallback provider.

    Eulerpool does not return true OHLCV via its simple quotes endpoint
    (it returns ``{timestamp, price}`` only), but can serve as a fallback
    when other providers have gaps.  The *price* field is mapped to the
    ``close`` column; ``open`` / ``high`` / ``low`` are copied from it.
    """
    from .eulerpool_provider import EulerpoolProvider

    ep = EulerpoolProvider()
    asset = classify_symbol(symbol)
    if asset == "crypto":
        # Eulerpool uses bare base symbols (BTC, ETH), not pairs (BTCUSDT).
        return ep.crypto_quotes(_crypto_base(symbol), request.start, request.end)
    if asset == "commodity":
        # Normalize aliases/pairs (XAUUSD -> XAU, GC=F -> XAU, CL=F -> CL1).
        ticker = _commodity_symbol(symbol) or symbol
        return ep.commodity_quotes(ticker, request.start, request.end)
    if asset == "forex":
        pair = _forex_pair(symbol)
        if pair:
            # Eulerpool has no /forex/quotes endpoint; FX lives at /market/fx/{from}/{to}.
            return ep.forex_quotes(*pair, start=request.start, end=request.end)
    # equity, etf, index — all use the same generic path
    return ep.equity_quotes(symbol, request.start, request.end)


def fetch_symbol(request: DataRequest, symbol: str) -> tuple[str, pd.DataFrame]:
    provider = select_provider(symbol, request.provider, request.resolution)
    fetchers = {"yahoo": _yahoo, "binance": _binance, "massive": _massive,
                "databento": _databento, "fred": _fred,
                "alpaca": _alpaca, "tiingo": _tiingo,
                "alpha_vantage": _alpha_vantage, "quandl": _quandl,
                "eulerpool": _eulerpool}
    frame = fetchers[provider](symbol, request)
    if not frame.empty:
        frame.index = pd.to_datetime(frame.index, utc=True)
        frame.insert(0, "symbol", symbol)
        frame.insert(1, "provider", provider)
    return provider, frame


def fetch(request: DataRequest) -> pd.DataFrame:
    if request.start > request.end:
        raise UnsupportedRequest("start must be before or equal to end")
    if not request.symbols:
        raise UnsupportedRequest("at least one symbol is required")
    frames = [frame for symbol in request.symbols if not (frame := fetch_symbol(request, symbol)[1]).empty]
    if not frames:
        return pd.DataFrame(columns=["symbol", "provider", "open", "high", "low", "close", "volume"])
    return pd.concat(frames).sort_index()


# ── Lean format helpers ──────────────────────────────────────────────
_LEAN_PRICE_MULTIPLIER = 10000  # deci-cents for equity/futures


def _milliseconds_since_midnight(dt: datetime) -> int:
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int((dt - midnight).total_seconds() * 1000)


def _lean_symbol(symbol: str, asset_type: str) -> str:
    """Normalize a symbol for QuantConnect Lean compatibility.

    Returns the uppercased, stripped symbol — no asset-type suffix
    appended, matching the convention used by the reference data files
    under ``Data/`` (e.g. ``spy.zip`` → ``spy_daily_trade.csv``).
    """
    return symbol.strip().upper()


def to_lean_zip(frame: pd.DataFrame, resolution: str) -> dict[str, bytes]:
    """Convert a DataFrame to Lean-format per-symbol zip files.

    Returns a dict keyed by zip filename (e.g. ``spy.zip``) with the zip
    bytes as the value — matching the Lean directory convention::

        {symbol}.zip  ─→  {symbol}_{resolution}_trade.csv

    The *data type* is hard-coded as ``trade`` (the API always returns trade
    bars).  Timestamps for daily-resolution data are normalized to midnight
    (``00:00``) to match the reference files under ``Data/``.  If the
    DataFrame contains quote columns in the future, a separate ``quote``
    zip can be generated.

    The CSV inside the zip carries **no header row**: LEAN requires the very
    first line of a data file to be a data row, so a ``Time,Open,...`` header
    makes every bar unreadable (100% failed data requests).  Each row is
    newline-terminated, matching the reference archives under ``Data/``.
    """
    from io import BytesIO
    import zipfile

    if frame.empty:
        return {}

    result: dict[str, bytes] = {}
    for symbol, group in frame.groupby("symbol"):
        asset_type = classify_symbol(symbol)
        lean_sym = _lean_symbol(symbol, asset_type)

        # Price multiplier: 10 000 for equity / index / futures
        multiplier = _LEAN_PRICE_MULTIPLIER if asset_type in ("equity", "index", "futures") else 1

        group_sorted = group.sort_index()
        # LEAN data files must NOT contain a header row — the first line has to
        # be a valid data row.  Build data rows only.
        lines: list[str] = []

        for ts, row in group_sorted.iterrows():
            ts_dt = ts.to_pydatetime()

            # Normalize to midnight for daily data (matches reference format)
            if resolution == "daily":
                ts_dt = ts_dt.replace(hour=0, minute=0, second=0, microsecond=0)

            # Time column format
            if asset_type == "crypto" or resolution == "daily":
                time_str = ts_dt.strftime("%Y%m%d %H:%M")
            else:
                time_str = str(_milliseconds_since_midnight(ts_dt))

            o, h, l, c = (
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
            )
            v = int(float(row["volume"]))

            if multiplier == 1:
                lines.append(f"{time_str},{o},{h},{l},{c},{v}")
            else:
                lines.append(
                    f"{time_str},"
                    f"{int(round(o * multiplier))},"
                    f"{int(round(h * multiplier))},"
                    f"{int(round(l * multiplier))},"
                    f"{int(round(c * multiplier))},"
                    f"{v}"
                )

        # Build the individual zip for this symbol
        #   zip name :  spy.zip
        #   csv inside: spy_daily_trade.csv
        clean_sym = lean_sym.lower()
        csv_name = f"{clean_sym}_{resolution}_trade.csv"
        zip_name = f"{clean_sym}.zip"

        # Every row (including the last) is newline-terminated, headerless.
        csv_text = "\n".join(lines)
        if csv_text:
            csv_text += "\n"

        buf = BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(csv_name, csv_text)
        result[zip_name] = buf.getvalue()

    return result


def _is_valid_q_symbol(s: str) -> bool:
    """Return True if *s* is a valid q symbol literal (letters, digits, ., _ only).

    In kdb+/q, symbol names may contain:
      - a-z, A-Z
      - 0-9
      - .  (namespace separator)
      - _  (underscore)

    The ``^`` (ascending-sort / fill operator), ``-``, and most other special
    characters are **reserved operators** and cannot appear inside a symbol
    literal.  When a ticker contains such characters (e.g. ``^VIX``, ``BTC-USD``,
    ``ES.FUT`` — the dot is actually valid in q symbols) we fall back to a
    string literal so the output can be parsed by ``value`` in the q client.
    """
    if not s:
        return False
    return all(c.isalnum() or c in (".", "_") for c in s)


def _q_literal(value, column: str = ""):
    if pd.isna(value):
        return "0n" if column != "symbol" else "`"
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        return value.strftime("%Y.%m.%dD%H:%M:%S.%f")
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        if column == "symbol":
            # Use a string literal when the ticker contains characters
            # that are not valid inside a q symbol (e.g. ^VIX, BTC-USD).
            # This keeps the q-table literal parseable by `value`.
            return f"`{value}" if _is_valid_q_symbol(value) else f'"{value}"'
        return f'"{value}"'
    return str(value)


def to_q_table(frame: pd.DataFrame, include_metadata: bool = False) -> str:
    """Render a pandas DataFrame as a KDB/q+ table literal for internal tooling."""
    if frame is None or frame.empty:
        return "([])"

    table = frame.copy()
    if "timestamp" in table.columns:
        table = table.rename(columns={"timestamp": "time"})
    elif table.index.name and table.index.name not in {None, "index"}:
        table = table.reset_index(names=table.index.name)
        table = table.rename(columns={table.index.name: "time"})
    elif table.index.nlevels == 1 and isinstance(table.index, pd.DatetimeIndex):
        table = table.reset_index()
        table = table.rename(columns={"index": "time"})

    if not include_metadata:
        for column in ["symbol", "provider"]:
            if column in table.columns:
                table = table.drop(columns=[column])

    keep = ["time", "open", "high", "low", "close", "volume"]
    if include_metadata:
        keep.extend(["symbol", "provider"])
    for column in list(table.columns):
        if column not in keep and column not in {"time", "open", "high", "low", "close", "volume"}:
            table = table.drop(columns=[column])
    if "time" not in table.columns:
        table["time"] = pd.to_datetime(table.index, utc=True)

    columns = [column for column in ["time", "open", "high", "low", "close", "volume"] + (["symbol", "provider"] if include_metadata else []) if column in table.columns]
    if not columns:
        return "([])"

    lists = []
    for column in columns:
        values = [
            _q_literal(value, column)
            for value in table[column].tolist()
        ]
        lists.append(f"{column}:({'; '.join(values)})")
    return "([] " + "; ".join(lists) + ")"
