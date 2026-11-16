"""Configured symbols for the short-lived in-memory market-data cache."""

HOT_GROUPS: dict[str, tuple[str, ...]] = {
    "sp500": (
        "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "BRK.B", "LLY", "AVGO", "TSLA",
    ),
    "hk": (
        "0700.HK", "9988.HK", "3690.HK", "0939.HK", "1299.HK", "1398.HK", "0005.HK", "9888.HK", "1810.HK", "2318.HK",
    ),
    "korea": (
        "005930.KS", "000660.KS", "373220.KS", "207940.KS", "005380.KS", "000270.KS", "035420.KS", "005490.KS", "035720.KS", "068270.KS",
    ),
    "uk": (
        "HSBA.L", "SHEL.L", "AZN.L", "RR.L", "ULVR.L", "RIO.L", "BP.L", "BATS.L", "GSK.L", "GLEN.L",
    ),
    "commodities": (
        "XAUUSD", "XAGUSD", "USO", "BNO", "UNG", "CPER", "DBA", "UGL",
    ),
    "leveraged_etfs": (
        "TQQQ", "UPRO", "SQQQ", "SPXU", "SOXL", "SOXS", "DIG", "UGL",
    ),
    "crypto": (
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
    ),
}

# Futures and options are intentionally on-demand and are not in this set.
HOT_SYMBOLS = frozenset(symbol for symbols in HOT_GROUPS.values() for symbol in symbols)


def is_hot(symbols: list[str]) -> bool:
    """Return whether every requested symbol belongs to the hot universe."""
    return bool(symbols) and all(symbol.upper() in HOT_SYMBOLS for symbol in symbols)
