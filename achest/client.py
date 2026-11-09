"""Python client for the centralized market-data API."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable
import time
import zipfile

import httpx
import pandas as pd

from .service import classify_symbol, to_q_table

_DEFAULT_BASE_URL = "https://achestv2.misango.me"

#: Transport-level exceptions that are safe to retry (transient network/SSL failures).
_RETRYABLE_EXCEPTIONS = (
    httpx.ConnectError,
    httpx.TimeoutException,
    httpx.RemoteProtocolError,
)

# Chunk thresholds per resolution (days).  0 = no auto-chunking (single request).
# High-res data over long ranges is split into smaller parallel requests.
_CHUNK_DAYS: dict[str, int] = {
    "tick": 1,
    "second": 1,
    "minute": 7,
    "hour": 30,
}


def _chunk_date_range(
    start: date,
    end: date,
    resolution: str,
    chunk_days: int | None = None,
) -> list[tuple[date, date]]:
    """Split (start, end) into non-overlapping sub-ranges for parallel fetching.

    The API treats *end* as exclusive (the day *after* the last trading day),
    so consecutive chunks tile perfectly: chunk₂.start == chunk₁.end.

    Returns ``[(start, end)]`` (no split) for low-res data or when the range
    is already smaller than one chunk.
    """
    size = _CHUNK_DAYS.get(resolution, 0) if chunk_days is None else chunk_days
    if size <= 0:
        return [(start, end)]

    chunks: list[tuple[date, date]] = []
    cursor = start
    while cursor < end:
        chunk_end = cursor + timedelta(days=size)
        if chunk_end > end:
            chunk_end = end
        chunks.append((cursor, chunk_end))
        cursor = chunk_end  # end is exclusive, so this tiles cleanly
    return chunks


def _default_output_path(symbols: list[str], resolution: str) -> Path:
    """Derive a sensible output-directory path from symbol types and resolution.

    Uses the same convention as the repo's ``Data/`` directory layout::

        Data/equity/usa/{resolution}/   for equities
        Data/crypto/{resolution}/       for crypto
        Data/futures/{resolution}/      for futures
        Data/forex/{resolution}/        for forex
        Data/other/{resolution}/        for everything else
    """
    if not symbols:
        return Path.cwd()
    # Classify the first symbol to pick the directory
    asset_type = classify_symbol(symbols[0])
    market = {
        "equity": "equity/usa",
        "etf": "equity/usa",
        "index": "equity/usa",
        "crypto": "crypto",
        "futures": "futures",
        "forex": "forex",
        "economic": "other",
    }.get(asset_type, "other")
    return Path("Data") / market / resolution
def _read_lean_zip(zip_bytes: bytes) -> pd.DataFrame:
    """Parse a Lean-format zip-of-zips back into a pandas DataFrame.

    The server returns an outer zip containing individual per-symbol zips
    (e.g. ``spy.zip``).  Each inner zip holds a single merged CSV with the
    positional columns ``Time,Open,High,Low,Close,Volume``.  The CSV is
    headerless (LEAN requirement), so the columns are assigned by position;
    legacy archives that still start with a header row are tolerated.

    Returns a DataFrame with the same lowercase column schema as the
    JSON / CSV formats: ``timestamp, symbol, open, high, low, close, volume``.
    """
    _lean_columns = ["Time", "Open", "High", "Low", "Close", "Volume"]
    frames = []
    with zipfile.ZipFile(BytesIO(zip_bytes)) as outer_zf:
        for inner_name in outer_zf.namelist():
            if not inner_name.endswith(".zip"):
                continue
            inner_bytes = outer_zf.read(inner_name)
            with zipfile.ZipFile(BytesIO(inner_bytes)) as inner_zf:
                for csv_name in inner_zf.namelist():
                    if not csv_name.endswith(".csv"):
                        continue
                    # Extract symbol from the CSV filename: "spy_daily_trade.csv" → "SPY"
                    tokens = csv_name.replace(".csv", "").split("_")
                    symbol_from_path = tokens[0].upper() if tokens else "UNKNOWN"

                    # Read positionally — Lean archives carry no header row.
                    df = pd.read_csv(inner_zf.open(csv_name), header=None)
                    if df.empty:
                        continue
                    # Tolerate legacy archives whose first line is a header.
                    if not str(df.iloc[0, 0]).strip()[:1].isdigit():
                        df = df.iloc[1:].reset_index(drop=True)
                    df = df.iloc[:, : len(_lean_columns)]
                    df.columns = _lean_columns
                    for col in _lean_columns[1:]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    df["symbol"] = symbol_from_path
                    # Parse the Lean time column
                    raw = df["Time"].astype(str)
                    parsed = pd.to_datetime(raw, format="%Y%m%d %H:%M", errors="coerce")
                    if parsed.isna().all():
                        parsed = pd.to_numeric(raw, errors="coerce")
                        parsed = pd.to_datetime(parsed, unit="ms", origin="unix", errors="coerce")
                    df["timestamp"] = parsed
                    df = df.drop(columns=["Time"])
                    # Normalise column names to lowercase (Open → open, Volume → volume)
                    df.columns = [col.lower() for col in df.columns]
                    frames.append(df)
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame(columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"])


class MarketDataClient:
    def __init__(
        self,
        base_url: str | None = None,
        token: str | None = None,
        timeout: float = 300.0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        max_workers: int = 4,
        chunk_days: int | None = None,
        eulerpool_direct: bool | None = None,
    ):
        """Market-data client.

        Parameters
        ----------
        eulerpool_direct : bool or None, optional
            * ``True``  — Eulerpool methods talk **directly** to api.eulerpool.com
                         (no server needed; works in local scripts).
            * ``False`` — Go through the chest server at ``/v1/eulerpool/...``.
            * ``None`` (default) — Auto-detect: try the server first; if it
                         responds, use it; otherwise fall back to direct.
        """
        final_base_url = (base_url or _DEFAULT_BASE_URL).rstrip("/")
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self.client = httpx.Client(base_url=final_base_url, headers=headers, timeout=timeout)
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._max_workers = max_workers
        # None = use per-resolution defaults from _CHUNK_DAYS
        self._chunk_days = chunk_days
        self._eulerpool_direct = eulerpool_direct
        self._state: dict[str, bool] = {}  # caches resolved direct/server mode

    def _euler(self) -> Any:
        """Lazy-import and return an EulerpoolProvider for direct access."""
        from .eulerpool_provider import EulerpoolProvider
        return EulerpoolProvider()

    def _euler_request(self, path: str, **params) -> Any:
        """Fetch from Eulerpool via server, or direct if configured/server-unavailable.

        Auto-converts JSON responses to pandas DataFrames when possible:
        - list-of-dicts → multi-row DataFrame (even if empty list)
        - single flat dict → 1-row DataFrame
        - nested dict / scalar / str → unchanged
        """
        if self._eulerpool_direct is True:
            return self._euler_direct(path, **params)
        # Try server
        try:
            resp = self._request("get", f"/v1/eulerpool/{path}", params=params)
            resp.raise_for_status()
            if self._eulerpool_direct is None:
                self._state.setdefault("euler_server_ok", True)
            data = resp.json()
            import pandas as pd
            # Empty list → empty DataFrame
            if isinstance(data, list) and len(data) == 0:
                return pd.DataFrame()
            # List of dicts → multi-row DataFrame
            if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                return pd.DataFrame(data)
            # Single flat dict → 1-row DataFrame
            if isinstance(data, dict) and not any(
                isinstance(v, (dict, list)) for v in data.values()
            ):
                return pd.DataFrame([data])
            return data
        except Exception as exc:
            if self._eulerpool_direct is False:
                raise  # user explicitly asked for server
            if self._state.get("euler_server_ok"):
                raise  # was working before — real error
            return self._euler_direct(path, **params)

    # ── Internal helpers ────────────────────────────────────────────────
    def _euler_direct(self, path: str, **params) -> Any:
        """Execute an Eulerpool data request directly against api.eulerpool.com."""
        from .eulerpool_provider import EulerpoolProvider

        ep = EulerpoolProvider()
        parts = path.split("/")

        # fundamentals
        if parts[0] == "fundamentals" and len(parts) >= 3:
            dt, identifier = parts[1], parts[2]
            m = {
                "profile": lambda: ep.company_profile(identifier),
                "overview": lambda: ep.company_overview(identifier),
                "income": lambda: ep.income_statement(identifier),
                "balance": lambda: ep.balance_sheet(identifier),
                "cashflow": lambda: ep.cash_flow_statement(identifier),
                "metrics": lambda: ep.financial_metrics(identifier),
                "key-figures": lambda: ep.key_figures(identifier),
                "growth": lambda: ep.growth_metrics(identifier),
                "margins": lambda: ep.margins(identifier),
                "esg": lambda: ep.esg_rating(identifier),
                "aaqs": lambda: ep.aaqs_score(identifier),
                "fair-value": lambda: ep.fair_value(identifier),
            }
            if dt in m:
                return m[dt]()
            raise ValueError(f"Unknown fundamentals data_type={dt!r}")

        # sentiment
        if parts[0] == "sentiment" and len(parts) >= 3:
            dt, identifier = parts[1], parts[2]
            m = {
                "news": lambda: ep.news_sentiment(identifier),
                "social": lambda: ep.social_sentiment(identifier),
                "insider": lambda: ep.insider_sentiment(identifier),
                "swot": lambda: ep.swot_analysis(identifier),
            }
            if dt in m:
                return m[dt]()
            raise ValueError(f"Unknown sentiment data_type={dt!r}")

        # analyst
        if parts[0] == "analyst" and len(parts) >= 3:
            dt, identifier = parts[1], parts[2]
            m = {
                "estimates": lambda: ep.analyst_estimates(identifier),
                "price-target": lambda: ep.price_target_consensus(identifier),
                "upgrades": lambda: ep.analyst_upgrades(identifier),
                "recommendations": lambda: ep.analyst_recommendations(identifier),
            }
            if dt in m:
                return m[dt]()
            raise ValueError(f"Unknown analyst data_type={dt!r}")

        # ownership
        if parts[0] == "ownership" and len(parts) >= 3:
            dt, identifier = parts[1], parts[2]
            m = {
                "institutional": lambda: ep.institutional_ownership(identifier),
                "fund": lambda: ep.fund_ownership(identifier),
                "insider": lambda: ep.insider_trades(identifier),
                "etf-exposure": lambda: ep.etf_exposure(identifier),
            }
            if dt in m:
                return m[dt]()
            raise ValueError(f"Unknown ownership data_type={dt!r}")

        # dividends / short
        if parts[0] == "dividends" and len(parts) >= 3:
            dt, identifier = parts[1], parts[2]
            if dt == "history":
                return ep.dividends(identifier)
            if dt == "quality":
                return ep.dividend_quality(identifier)
            raise ValueError(f"Unknown dividends data_type={dt!r}")
        if parts[0] == "short" and len(parts) >= 3:
            dt, identifier = parts[1], parts[2]
            if dt == "volume":
                return ep.short_volume(identifier)
            if dt == "interest":
                return ep.short_interest(identifier)
            raise ValueError(f"Unknown short data_type={dt!r}")

        # macro
        if parts[0] == "macro" and len(parts) >= 2:
            sub = parts[1]
            m = {
                "country-risk": lambda: ep.country_risk(**params),
                "fred": lambda: ep.fred_observations(parts[2], **params),
                "fred-latest": lambda: ep.fred_latest(),
                "credit-spreads": lambda: ep.credit_spreads(**params),
                "calendar": lambda: ep.macro_calendar(**params),
            }
            if sub in m:
                return m[sub]()
            raise ValueError(f"Unknown macro sub={sub!r}")

        # index / bonds / forex
        if parts[0] == "index" and len(parts) >= 2:
            return ep.index_constituents(parts[1], **params)
        if parts[0] == "bonds" and parts[1] == "yield-curve":
            return ep.yield_curve(**params)
        if parts[0] == "forex" and parts[1] == "rates":
            return ep.forex_rates(parts[2])
# crypto
        if parts[0] == "crypto" and len(parts) >= 2:
            sub = parts[1]
            m = {
                "top": lambda: ep.top_cryptocurrencies(),
                "market-overview": lambda: ep.crypto_market_overview(),
                "analysis": lambda: ep.crypto_analysis(parts[2]),
                "fear-greed": lambda: ep.crypto_fear_greed(**params),
                "funding-rates": lambda: ep.crypto_funding_rates(parts[2], **params),
                "open-interest": lambda: ep.crypto_open_interest(parts[2], **params),
                "defi-protocols": lambda: ep.defi_protocols(**params),
                "onchain": lambda: ep.onchain_metrics(parts[2]),
            }
            if sub in m:
                return m[sub]()
            raise ValueError(f"Unknown crypto sub={sub!r}")

        # options
        if parts[0] == "options" and len(parts) >= 2:
            sub, rest = parts[1], "/".join(parts[2:])
            m = {
                "chain": lambda: ep.options_chain(rest),
                "greeks": lambda: ep.options_greeks(rest),
                "iv-surface": lambda: ep.iv_surface(rest),
                "unusual-activity": lambda: ep.unusual_options_activity(**params),
                "vix-term-structure": lambda: ep.vix_term_structure(**params),
            }
            if sub in m:
                return m[sub]()
            raise ValueError(f"Unknown options sub={sub!r}")

        # alternative
        if parts[0] == "alternative" and len(parts) >= 2:
            sub = parts[1]
            m = {
                "fear-greed": lambda: ep.fear_greed_index(**params),
                "superinvestors": lambda: ep.superinvestors_list(),
                "congress-trading": lambda: ep.congress_trading(**params),
                "patents": lambda: ep.patents(parts[2], **params),
                "gov-contracts": lambda: ep.government_contracts(parts[2], **params),
                "google-trends": lambda: ep.google_trends(parts[2], **params),
            }
            if sub in m:
                return m[sub]()
            raise ValueError(f"Unknown alternative sub={sub!r}")

        # news
        if parts[0] == "news" and len(parts) >= 2:
            if len(parts) == 2:
                return ep.company_news(parts[1])
            sub = parts[1]
            m = {
                "market": lambda: ep.market_news(**params),
                "transcripts": lambda: ep.earnings_call_list(parts[2], **params),
            }
            if sub in m:
                return m[sub]()
            raise ValueError(f"Unknown news sub={sub!r}")

        # etf
        if parts[0] == "etf" and len(parts) >= 3:
            dt, identifier = parts[1], parts[2]
            m = {
                "profile": lambda: ep.etf_profile(identifier),
                "holdings": lambda: ep.etf_holdings(identifier),
                "flows": lambda: ep.etf_flows(identifier, **params),
            }
            if dt in m:
                return m[dt]()
            raise ValueError(f"Unknown etf data_type={dt!r}")

        # market
        if parts[0] == "market" and len(parts) >= 2:
            sub = parts[1]
            m = {
                "latest-quotes": lambda: ep.latest_quotes(**params),
                "top-movers": lambda: ep.top_movers(**params),
                "status": lambda: ep.market_status(),
                "breadth": lambda: ep.market_breadth(**params),
                "indicators": lambda: ep.market_indicators(**params),
                "holidays": lambda: ep.market_holidays(),
                "sector-performance": lambda: ep.sector_performance(**params),
            }
            if sub in m:
                return m[sub]()
            raise ValueError(f"Unknown market sub={sub!r}")

        raise ValueError(f"Unsupported Eulerpool path: {path}")

    def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        """Send an HTTP request, retrying on transient transport errors.

        Retries with exponential backoff for ``ConnectError``,
        ``TimeoutException``, and ``RemoteProtocolError``.  Non-2xx
        HTTP statuses are **not** retried — they raise immediately.
        """
        last_exc: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                return getattr(self.client, method)(path, **kwargs)
            except _RETRYABLE_EXCEPTIONS as exc:
                last_exc = exc
                if attempt < self._max_retries - 1:
                    time.sleep(self._retry_delay * (2**attempt))
                    continue
                raise
        # Should never reach here, but keeps type-checkers happy
        raise RuntimeError("unreachable") from last_exc

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def route(self, symbol: str, resolution: str = "daily", provider: str = "auto") -> dict:
        response = self._request("get", "/v1/route", params={"symbol": symbol, "resolution": resolution, "provider": provider})
        response.raise_for_status()
        return response.json()

    def get(
        self,
        symbols: str | Iterable[str],
        start: date | str,
        end: date | str,
        resolution: str = "daily",
        provider: str = "auto",
        format: str = "json",
    ) -> pd.DataFrame:
        if isinstance(symbols, str):
            symbols = [symbols]
        symbols = list(symbols)

        start_date = start if isinstance(start, date) else date.fromisoformat(str(start))
        end_date = end if isinstance(end, date) else date.fromisoformat(str(end))

        chunks = _chunk_date_range(start_date, end_date, resolution, self._chunk_days)

        if len(chunks) <= 1:
            return self._fetch_chunk(symbols, start, end, resolution, provider, format)

        # Parallel fetch across chunks
        frames: list[pd.DataFrame] = []
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = {
                pool.submit(
                    self._fetch_chunk, symbols, cs, ce, resolution, provider, format
                ): (cs, ce)
                for cs, ce in chunks
            }
            for future in as_completed(futures):
                frames.append(future.result())

        result = pd.concat(frames, ignore_index=True)
        if "timestamp" in result.columns:
            result = result.sort_values("timestamp").reset_index(drop=True)
        return result

    def _fetch_chunk(
        self,
        symbols: list[str],
        start: date | str,
        end: date | str,
        resolution: str,
        provider: str,
        format: str,
    ) -> pd.DataFrame:
        """Execute a single date-range request and return a DataFrame."""
        body = {
            "symbols": symbols,
            "start": str(start),
            "end": str(end),
            "resolution": resolution,
            "provider": provider,
            "format": format,
        }
        response = self._request("post", "/v1/data", json=body)
        response.raise_for_status()

        if format == "lean":
            return _read_lean_zip(response.content)

        return pd.DataFrame(response.json())

    def download(
        self,
        symbols: str | Iterable[str],
        start: date | str,
        end: date | str,
        resolution: str = "daily",
        provider: str = "auto",
        format: str = "lean",
        output: str | Path | None = None,
    ) -> Path:
        if isinstance(symbols, str):
            symbols = [symbols]
        symbols = list(symbols)
        if output is None:
            if format == "lean":
                output = _default_output_path(symbols, resolution)
            else:
                ext = {"parquet": ".parquet", "csv": ".csv", "json": ".json"}.get(format, "")
                output = Path("Custom_Downloads") / f"{symbols[0].lower()}_{resolution}{ext}"
        destination = Path(output)

        start_date = start if isinstance(start, date) else date.fromisoformat(str(start))
        end_date = end if isinstance(end, date) else date.fromisoformat(str(end))

        chunks = _chunk_date_range(start_date, end_date, resolution, self._chunk_days)

        if len(chunks) <= 1:
            return self._download_single(
                symbols, start, end, resolution, provider, format, destination,
            )

        # Parallel chunks
        if format == "lean":
            destination.mkdir(parents=True, exist_ok=True)
            with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
                futures = {
                    pool.submit(
                        self._download_single,
                        symbols, cs, ce, resolution, provider, format, destination,
                    ): (cs, ce)
                    for cs, ce in chunks
                }
                for future in as_completed(futures):
                    future.result()  # re-raises if the chunk failed
            return destination

        # Non-lean: fetch chunks as DataFrames in parallel, concat, write final file
        frames: list[pd.DataFrame] = []
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = {
                pool.submit(
                    self._fetch_chunk, symbols, cs, ce, resolution, provider, format,
                ): (cs, ce)
                for cs, ce in chunks
            }
            for future in as_completed(futures):
                frames.append(future.result())

        result = pd.concat(frames, ignore_index=True)
        if "timestamp" in result.columns:
            result = result.sort_values("timestamp").reset_index(drop=True)

        destination.parent.mkdir(parents=True, exist_ok=True)
        if format == "csv":
            result.to_csv(destination, index=False)
        elif format == "json":
            destination.write_text(result.to_json(orient="records", date_format="iso"))
        else:  # parquet
            result.to_parquet(destination, index=False)
        return destination

    def _download_single(
        self,
        symbols: list[str],
        start: date | str,
        end: date | str,
        resolution: str,
        provider: str,
        format: str,
        destination: Path,
    ) -> Path:
        """Execute a single date-range download and write to *destination*."""
        body = {
            "symbols": symbols,
            "start": str(start),
            "end": str(end),
            "resolution": resolution,
            "provider": provider,
            "format": format,
        }
        response = self._request("post", "/v1/data", json=body)
        response.raise_for_status()

        if format == "lean":
            destination.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(BytesIO(response.content)) as outer_zf:
                for zip_name in outer_zf.namelist():
                    if not zip_name.endswith(".zip"):
                        continue
                    (destination / zip_name).write_bytes(outer_zf.read(zip_name))
            return destination

        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(response.content)
        return destination

    # ── Eulerpool Extended Data Methods ─────────────────────────────────────

    def fundamentals(self, identifier: str, data_type: str = "overview") -> dict | list:
        return self._euler_request(f"fundamentals/{data_type}/{identifier}")

    def analyst(self, identifier: str, data_type: str = "estimates") -> dict | list:
        return self._euler_request(f"analyst/{data_type}/{identifier}")

    def ownership(self, identifier: str, data_type: str = "institutional") -> list:
        return self._euler_request(f"ownership/{data_type}/{identifier}")

    def dividends_data(self, identifier: str, data_type: str = "history") -> dict | list:
        return self._euler_request(f"dividends/{data_type}/{identifier}")

    def short_data(self, identifier: str, data_type: str = "volume") -> list:
        return self._euler_request(f"short/{data_type}/{identifier}")

    def macro(self, endpoint: str, **params) -> dict | list:
        return self._euler_request(f"macro/{endpoint}", **params)

    def index_constituents(self, index_id: str = "sp500", start: int = 0, end: int = 500) -> list:
        return self._euler_request(f"index/{index_id}", start=start, end=end)

    def yield_curve(self, country: str , days: int = 90) -> list:
        return self._euler_request("bonds/yield-curve", country=country, days=days)

    def forex_rates(self, base: str = "USD") -> list:
        return self._euler_request(f"forex/rates/{base}")

    def logo(self, symbol: str, size: int = 128) -> bytes:
        if self._eulerpool_direct:
            return self._euler().logo_by_ticker(symbol, size)
        resp = self._request("get", f"/v1/eulerpool/logo/{symbol}", params={"size": size})
        resp.raise_for_status()
        return resp.content

    def sentiment(self, identifier: str, data_type: str = "news") -> dict | list:
        return self._euler_request(f"sentiment/{data_type}/{identifier}")
    sentiment_data = sentiment

    def crypto(self, endpoint: str, **params) -> dict | list:
        """Eulerpool crypto extended (top, analysis/BTC, fear-greed, ...)."""
        return self._euler_request(f"crypto/{endpoint}", **params)
    crypto_data = crypto  # alias

    def options(self, endpoint: str, **params) -> dict | list:
        """Eulerpool options & derivatives (chain/AAPL, greeks, iv-surface, ...)."""
        return self._euler_request(f"options/{endpoint}", **params)
    options_data = options  # alias

    def alternative(self, endpoint: str, **params) -> dict | list:
        """Eulerpool alternative data (fear-greed, superinvestors, congress, ...)."""
        return self._euler_request(f"alternative/{endpoint}", **params)
    alternative_data = alternative  # alias

    def news(self, endpoint: str, **params) -> list:
        """Eulerpool news & research (ticker, market, transcripts/search, ...)."""
        return self._euler_request(f"news/{endpoint}", **params)
    news_research = news  # alias

    def etf(self, identifier: str, data_type: str = "profile") -> dict | list:
        """Eulerpool ETF data (profile, holdings, flows)."""
        return self._euler_request(f"etf/{data_type}/{identifier}")
    etf_data = etf  # alias

    def market(self, endpoint: str, **params) -> dict | list:
        """Eulerpool market-wide data (latest-quotes, status, breadth, ...)."""
        return self._euler_request(f"market/{endpoint}", **params)
    market_data_ext = market  # alias

    def shipping_vessels(
        self, vessel_type: str | None = None, vessel_class: str | None = None,
        flag: str | None = None, search: str | None = None,
        limit: int = 100, offset: int = 0,
    ) -> dict | list:
        """Search the global tanker, LNG, and LPG vessel registry."""
        return self._euler_request(
            "shipping/vessels", vessel_type=vessel_type, vessel_class=vessel_class,
            flag=flag, search=search, limit=limit, offset=offset,
        )

    def shipping_vessel(self, imo: int) -> dict | list:
        """Get vessel details and current position by IMO number."""
        return self._euler_request(f"shipping/vessels/{imo}")

    def shipping_vessel_track(self, imo: int, days: int = 7, limit: int = 1000) -> dict | list:
        """Get historical AIS positions for a vessel."""
        return self._euler_request(f"shipping/vessels/{imo}/track", days=days, limit=limit)

    def shipping_positions(
        self, bbox: str | None = None, vessel_type: str | None = None,
        min_speed: float | None = None,
    ) -> dict | list:
        """Get current vessel positions, optionally filtered by bounding box."""
        return self._euler_request(
            "shipping/positions", bbox=bbox, vessel_type=vessel_type, min_speed=min_speed,
        )

    def shipping_voyages(
        self, status: str | None = None, cargo_type: str | None = None,
        imo: int | None = None, origin_port: int | None = None,
        destination_port: int | None = None, limit: int = 100, offset: int = 0,
    ) -> dict | list:
        """List active and recent shipping voyages with filters."""
        return self._euler_request(
            "shipping/voyages", status=status, cargo_type=cargo_type, imo=imo,
            origin_port=origin_port, destination_port=destination_port,
            limit=limit, offset=offset,
        )

    def shipping_cargoes(
        self, product: str | None = None, origin_country: str | None = None,
        destination_country: str | None = None, start_date: str | None = None,
        end_date: str | None = None, limit: int = 100, offset: int = 0,
    ) -> dict | list:
        """Track cargo movements by product, origin, destination, and date."""
        return self._euler_request(
            "shipping/cargoes", product=product, origin_country=origin_country,
            destination_country=destination_country, start_date=start_date,
            end_date=end_date, limit=limit, offset=offset,
        )

    def shipping_ports(
        self, country_code: str | None = None, port_type: str | None = None,
        search: str | None = None, limit: int = 100,
    ) -> dict | list:
        """Search global oil, LNG, and shipping ports and terminals."""
        return self._euler_request(
            "shipping/ports", country_code=country_code, port_type=port_type,
            search=search, limit=limit,
        )

    def shipping_port_activity(self, port_id: int, days: int = 30, limit: int = 100) -> dict | list:
        """Get recent voyages arriving at or departing from a port."""
        return self._euler_request(
            f"shipping/ports/{port_id}/activity", days=days, limit=limit,
        )

    def q_table(self, symbols: Iterable[str], start: date | str, end: date | str, resolution: str = "daily", provider: str = "auto", include_metadata: bool = False) -> str:
        frame = self.get(symbols, start, end, resolution=resolution, provider=provider)
        return to_q_table(frame, include_metadata=include_metadata)

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
