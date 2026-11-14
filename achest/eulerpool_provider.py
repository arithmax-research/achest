"""Eulerpool Financial Data API — comprehensive provider for Arithmax Chest.

Wraps 200+ Eulerpool REST endpoints into Python methods that return
normalized pandas DataFrames or plain dicts.  Serves as:
  1. A fallback OHLCV provider when primaries (yahoo, binance, ...) fail
  2. A source for data types the chest cannot currently fetch
     (fundamentals, ESG, sentiment, macro, crypto on-chain, options, ...)

Base URL: https://api.eulerpool.com/api/1
Auth:     ?token=...  (EULERPOOL_API_KEY env var)
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from enum import Enum
from typing import Any, Iterable

import os
import pandas as pd
import requests

__all__ = [
    "EulerpoolProvider",
    "AssetClass",
    "EulerpoolError",
]

# ── Configuration ───────────────────────────────────────────────────────────

DEFAULT_BASE_URL = "https://api.eulerpool.com/api/1"
_ENV_KEY = "EULERPOOL_API_KEY"
_ENV_TOKEN = "EULER_TOKEN"  # fallback env-var name


class EulerpoolError(RuntimeError):
    """Raised when an Eulerpool API call fails."""


class AssetClass(str, Enum):
    EQUITY = "equity"
    ETF = "etf"
    CRYPTO = "crypto"
    FOREX = "forex"
    COMMODITY = "commodity"
    MACRO = "macro"
    BOND = "bond"
    OPTIONS = "options"
    FUTURES = "futures"
    MUTUAL_FUND = "mutual_fund"
    CERTIFICATE = "certificate"
    NFT = "nft"


# ═══════════════════════════════════════════════════════════════════════════════
#  EulerpoolProvider  (main class)
# ═══════════════════════════════════════════════════════════════════════════════

class EulerpoolProvider:
    """A client for the Eulerpool Financial Data REST API.

    Usage::

        e = EulerpoolProvider()
        df = e.equity_quotes("AAPL", date(2025,1,1), date(2025,1,31))
        profile = e.company_profile("US0378331005")
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        session: requests.Session | None = None,
        timeout: int = 60,
    ):
        self.api_key = api_key or os.getenv(_ENV_KEY) or os.getenv(_ENV_TOKEN)
        if not self.api_key:
            raise EulerpoolError(
                f"Eulerpool API key missing -- set {_ENV_KEY} or {_ENV_TOKEN}"
            )
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.session.params["token"] = self.api_key
        self.timeout = timeout

    # ── internal helpers ───────────────────────────────────────────────────

    def _get(self, path: str, **params: Any) -> Any:
        url = f"{self.base_url}{path}"
        resp = self.session.get(url, params=params, timeout=self.timeout)
        if not resp.ok:
            raise EulerpoolError(
                f"Eulerpool {resp.status_code} on GET {path}: {resp.text[:300]}"
            )
        ctype = resp.headers.get("content-type", "")
        if "xml" in ctype:
            return resp.text
        try:
            return resp.json()
        except ValueError:
            return resp.text

    # ═══════════════════════════════════════════════════════════════════════
    #  OHLCV / QUOTES  (used by the fallback provider in service.py)
    # ═══════════════════════════════════════════════════════════════════════

    def equity_quotes(
        self,
        identifier: str,
        start: date | None = None,
        end: date | None = None,
    ) -> pd.DataFrame:
        """Historical daily quotes for an equity.

        Eulerpool returns ``[{timestamp, price}]`` -- single price per day.
        Maps *price* to ``close`` (copied to ``open``, ``high``, ``low``).
        """
        params = {}
        if start:
            params["startdate"] = int(
                datetime.combine(start, datetime.min.time()).timestamp() * 1000
            )
        if end:
            params["enddate"] = int(
                datetime.combine(end, datetime.max.time()).timestamp() * 1000
            )
        data = self._get(f"/equity/quotes/{identifier}", **params)
        if not data or not isinstance(data, list):
            return pd.DataFrame()
        rows = []
        for row in data:
            ts = self._maybe_ms(row.get("timestamp"))
            price = float(row.get("price", 0))
            rows.append({
                "timestamp": ts, "open": price, "high": price,
                "low": price, "close": price, "volume": 0,
            })
        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        return frame.set_index("timestamp").sort_index()

    def crypto_quotes(
        self, identifier: str, start: date | None = None, end: date | None = None,
    ) -> pd.DataFrame:
        """Historical cryptocurrency price (same schema as equity_quotes)."""
        params = {}
        if start:
            params["startdate"] = int(
                datetime.combine(start, datetime.min.time()).timestamp() * 1000
            )
        if end:
            params["enddate"] = int(
                datetime.combine(end, datetime.max.time()).timestamp() * 1000
            )
        data = self._get(f"/crypto/quotes/{identifier}", **params)
        if not data or not isinstance(data, list):
            return pd.DataFrame()
        rows = []
        for row in data:
            ts = self._maybe_ms(row.get("timestamp"))
            price = float(row.get("price", 0))
            rows.append({
                "timestamp": ts, "open": price, "high": price,
                "low": price, "close": price, "volume": 0,
            })
        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        return frame.set_index("timestamp").sort_index()

    def etf_quotes(
        self, identifier: str, start: date | None = None, end: date | None = None,
    ) -> pd.DataFrame:
        """Historical NAV quotes for an ETF."""
        params = {}
        if start:
            params["startdate"] = int(
                datetime.combine(start, datetime.min.time()).timestamp() * 1000
            )
        if end:
            params["enddate"] = int(
                datetime.combine(end, datetime.max.time()).timestamp() * 1000
            )
        data = self._get(f"/etf/quotes/{identifier}", **params)
        if not isinstance(data, list):
            return pd.DataFrame()
        rows = []
        for entry in data:
            if isinstance(entry, dict) and "timestamp" in entry:
                ts = self._maybe_ms(entry.get("timestamp"))
                price = float(entry.get("price", entry.get("close", 0)))
                rows.append({
                    "timestamp": ts, "open": price, "high": price,
                    "low": price, "close": price, "volume": 0,
                })
        frame = pd.DataFrame(rows)
        if frame.empty:
            return pd.DataFrame()
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        return frame.set_index("timestamp").sort_index()

    def commodity_quotes(
        self, ticker: str, start: date | None = None, end: date | None = None,
    ) -> pd.DataFrame:
        """Historical commodity prices."""
        params = {}
        if start:
            params["startdate"] = int(
                datetime.combine(start, datetime.min.time()).timestamp() * 1000
            )
        if end:
            params["enddate"] = int(
                datetime.combine(end, datetime.max.time()).timestamp() * 1000
            )
        data = self._get(f"/commodity/quotes/{ticker}", **params)
        if not data or not isinstance(data, list):
            return pd.DataFrame()
        rows = []
        for row in data:
            ts = self._maybe_ms(row.get("timestamp"))
            price = float(row.get("price", 0))
            rows.append({
                "timestamp": ts, "open": price, "high": price,
                "low": price, "close": price, "volume": 0,
            })
        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        return frame.set_index("timestamp").sort_index()

    def forex_rates(self, base_currency: str = "USD") -> pd.DataFrame:
        """Current exchange rates for a base currency as a DataFrame."""
        data = self._get(f"/forex/rates/{base_currency}")
        if not isinstance(data, dict):
            return pd.DataFrame()
        rates = data.get("rates", {})
        rows = [{"target": target, "rate": rate} for target, rate in rates.items()]
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def fx_series(
        self, from_curr: str, to_curr: str, range_: str = "1y",
    ) -> pd.DataFrame:
        """Historical exchange-rate time series."""
        data = self._get(f"/market/fx/{from_curr}/{to_curr}", range=range_)
        if not isinstance(data, dict):
            return pd.DataFrame()
        rates = data.get("rates", [])
        rows = []
        for entry in rates:
            if isinstance(entry, dict):
                rows.append({
                    "timestamp": entry.get("date"),
                    "rate": entry.get("rate", entry.get("close", 0)),
                })
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def forex_quotes(
        self, from_curr: str, to_curr: str,
        start: date | None = None, end: date | None = None,
    ) -> pd.DataFrame:
        """Historical daily quotes for a currency pair.

        Eulerpool exposes FX only through ``/market/fx/{from}/{to}``, which
        returns ``{rates: [{date, rate}]}`` and is windowed by a ``range``
        string (no date parameters).  We request the smallest standard range
        that covers ``[start, end]`` and then clip the returned rates to the
        requested window, emitting the standard OHLCV schema (``rate`` is
        mapped to ``close`` and copied to ``open`` / ``high`` / ``low``).
        """
        range_ = self._range_for(start, end)

        # Some Eulerpool tiers return no rows for the short ranges (e.g. ``1m``
        # / ``3m``); escalate until a range yields data, then clip to the window.
        attempted: list[str] = []
        rates: list[Any] = []
        for candidate in self._range_escalation(range_):
            data = self._get(f"/market/fx/{from_curr}/{to_curr}", range=candidate)
            if not isinstance(data, dict):
                continue
            attempted.append(candidate)
            rates = data.get("rates", []) or []
            if rates:
                break

        rows = []
        for entry in rates:
            ts, price = self._parse_fx_entry(entry)
            if ts is None:
                continue
            rows.append({
                "timestamp": ts, "open": price, "high": price,
                "low": price, "close": price, "volume": 0,
            })
        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        frame = frame.set_index("timestamp").sort_index()
        return self._clip_to_range(frame, start, end)

    @staticmethod
    def _parse_fx_entry(entry: Any) -> tuple[datetime | None, float]:
        """Parse one ``/market/fx`` rate entry into ``(timestamp, price)``.

        The live API returns entries as ``[timestamp_ms, rate]`` pairs, but the
        published docs describe ``{"date": ..., "rate": ...}`` dicts — accept both.
        """
        if isinstance(entry, dict):
            ts_raw = entry.get("timestamp", entry.get("date"))
            price = entry.get("rate", entry.get("close", 0)) or 0
        elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
            ts_raw, price = entry[0], entry[1]
        else:
            return None, 0.0
        return EulerpoolProvider._to_utc(ts_raw), float(price or 0)

    @staticmethod
    def _to_utc(value: Any) -> datetime | None:
        """Coerce a millisecond epoch, ISO string, or datetime to naive UTC."""
        if isinstance(value, (int, float)):
            return EulerpoolProvider._ts_ms(int(value))
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            parsed = pd.to_datetime(value, utc=True, errors="coerce")
            return None if pd.isna(parsed) else parsed.to_pydatetime().replace(tzinfo=None)
        return None



    @staticmethod
    def _range_for(start: date | None, end: date | None) -> str:
        """Pick the smallest Eulerpool ``range`` token covering ``[start, end]``."""
        if not start or not end:
            return "max"
        span_days = (end - start).days
        for token, days in (
            ("1m", 31), ("3m", 93), ("6m", 186),
            ("1y", 366), ("2y", 731), ("5y", 1827),
        ):
            if span_days <= days:
                return token
        return "max"

    @staticmethod
    def _range_escalation(chosen: str) -> list[str]:
        """Return the chosen range followed by progressively larger ranges."""
        order = ["1m", "3m", "6m", "1y", "2y", "5y", "max"]
        if chosen not in order:
            return ["max"]
        return order[order.index(chosen):]


    @staticmethod
    def _clip_to_range(frame: pd.DataFrame, start: date | None, end: date | None) -> pd.DataFrame:
        """Clip a timestamp-indexed frame to the inclusive ``[start, end]`` window."""
        if frame.empty or not start or not end:
            return frame
        lo = pd.Timestamp(datetime.combine(start, datetime.min.time()), tz="UTC")
        hi = pd.Timestamp(datetime.combine(end, datetime.max.time()), tz="UTC")
        return frame.loc[(frame.index >= lo) & (frame.index <= hi)]

    # ═══════════════════════════════════════════════════════════════════════
    #  EQUITY FUNDAMENTALS
    # ═══════════════════════════════════════════════════════════════════════

    def company_profile(self, identifier: str, language: str = "en") -> dict:
        """Full company profile (description, sector, employees, website, ...)."""
        return self._get(f"/equity/profile/{identifier}", language=language)

    def company_overview(self, identifier: str) -> dict:
        """Compact overview: P/E, P/S, div yld, 52w range, fair value, ..."""
        return self._get(f"/equity/overview/{identifier}")

    def income_statement(self, identifier: str) -> pd.DataFrame:
        """Annual income statements (revenue, net income, EPS, ...)."""
        data = self._get(f"/equity/incomestatement/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def quarterly_income_statement(self, identifier: str) -> pd.DataFrame:
        """Quarterly income statements."""
        data = self._get(f"/equity/income-statement-quarterly/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def balance_sheet(self, identifier: str) -> pd.DataFrame:
        """Annual balance sheets (assets, liabilities, equity, ...)."""
        data = self._get(f"/equity/balancesheet/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def cash_flow_statement(self, identifier: str) -> pd.DataFrame:
        """Annual cash-flow statements (operating, investing, financing)."""
        data = self._get(f"/equity/cashflowstatement/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def quarterly_cash_flow(self, identifier: str) -> pd.DataFrame:
        """Quarterly cash-flow statements."""
        data = self._get(f"/equity/cashflow-statement-quarterly/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def quarterly_fundamentals(self, identifier: str) -> pd.DataFrame:
        """Quarterly fundamental data (revenue, earnings, EPS, margins)."""
        data = self._get(f"/equity/fundamentals-quarterly/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def financial_metrics(self, identifier: str) -> dict:
        """40+ computed financial ratios (P/E, ROE, margins, growth, ...)."""
        return self._get(f"/equity/metrics/{identifier}")

    def key_figures(self, identifier: str) -> dict:
        """Condensed key-figure panel (valuation, profitability, leverage)."""
        return self._get(f"/equity/key-figures/{identifier}")

    def growth_metrics(self, identifier: str) -> dict:
        """CAGR for revenue, net income, EBIT, EPS (3Y / 5Y / 10Y)."""
        return self._get(f"/equity/growth/{identifier}")

    def margins(self, identifier: str) -> dict:
        """Gross / operating / net / FCF margins."""
        return self._get(f"/equity/margins/{identifier}")

    def revenue_by_region(self, identifier: str) -> pd.DataFrame:
        """Geographic revenue breakdown."""
        data = self._get(f"/equity/regions/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def business_segments(self, identifier: str) -> pd.DataFrame:
        """Revenue by product/service segment."""
        data = self._get(f"/equity/segments/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def segment_history(self, identifier: str) -> pd.DataFrame:
        """Annual revenue segmentation history (product + geography)."""
        data = self._get(f"/equity/segments-history/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def valuation_history(self, identifier: str) -> dict:
        """Historical valuation multiples (P/E, P/S, EV/EBIT) time-series."""
        return self._get(f"/equity/valuation-history/{identifier}")

    def stock_returns(self, identifier: str, years: int = 10) -> dict:
        """Annual price returns (absolute + relative, dividend-adjusted)."""
        return self._get(f"/equity/returns/{identifier}", years=years)

    def shares_outstanding_history(self, identifier: str) -> pd.DataFrame:
        """Diluted average shares outstanding per fiscal year."""
        data = self._get(f"/equity/shares-outstanding/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def market_cap_history(self, identifier: str, range_: str = "1y") -> dict:
        """Daily historical market cap (price x shares outstanding)."""
        return self._get(f"/equity/market-cap/{identifier}", range=range_)

    def employee_count_history(self, identifier: str) -> pd.DataFrame:
        """Historical employee counts from SEC filings."""
        data = self._get(f"/equity/employees/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def shares_float(self, identifier: str) -> dict:
        """Free-float percentage, float shares, outstanding shares."""
        return self._get(f"/equity/shares-float/{identifier}")

    # ── ESG ───────────────────────────────────────────────────────────────

    def esg_rating(self, identifier: str) -> dict:
        """ESG scores (total, environment, social, governance) + controversy flags."""
        return self._get(f"/equity/esg-rating/{identifier}")

    # ── AAQS (proprietary quality score) ──────────────────────────────────

    def aaqs_score(self, identifier: str) -> dict:
        """AlleAktien Quality Score (0-10) -- unique to Eulerpool."""
        return self._get(f"/equity/aaqs/{identifier}")

    # ── Analyst estimates & price targets ─────────────────────────────────

    def analyst_estimates(self, identifier: str) -> pd.DataFrame:
        """Revenue / EPS / EBIT estimates for future periods."""
        data = self._get(f"/equity/estimates/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def analyst_forecast(self, identifier: str) -> dict:
        """Consensus forecast: revenue, EPS, EBITDA per fiscal year."""
        return self._get(f"/equity/forecast/{identifier}")

    def price_target_consensus(self, identifier: str) -> dict:
        """Current price target (high, low, mean, median, analyst count)."""
        return self._get(f"/equity/price-target/{identifier}")

    def price_target_history(self, identifier: str) -> pd.DataFrame:
        """Weekly price-target snapshots up to 5 years."""
        data = self._get(f"/equity-extended/price-target-history/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def analyst_upgrades(self, identifier: str) -> pd.DataFrame:
        """Upgrade / downgrade events with analyst firm and grades."""
        data = self._get(f"/equity/upgrades/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def analyst_grades(self, identifier: str, limit: int = 50) -> pd.DataFrame:
        """Dated analyst rating actions (upgrade, downgrade, maintain)."""
        data = self._get(f"/equity/analyst-grades/{identifier}", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def analyst_recommendations(self, ticker: str) -> pd.DataFrame:
        """Buy / hold / sell consensus + price targets per period."""
        data = self._get(f"/research/recommendations/{ticker}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ── Executives ───────────────────────────────────────────────────────

    def executives(self, identifier: str) -> pd.DataFrame:
        """Company executives with compensation, age, position, tenure."""
        data = self._get(f"/equity/executives/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ── Peers & supply chain ─────────────────────────────────────────────

    def peers(self, identifier: str) -> pd.DataFrame:
        """Peer / comparable companies."""
        data = self._get(f"/equity/peers/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def supply_chain(self, identifier: str) -> pd.DataFrame:
        """Customers & suppliers with return correlations."""
        data = self._get(f"/equity/supply-chain/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ── Dividends ────────────────────────────────────────────────────────

    def dividends(self, identifier: str) -> pd.DataFrame:
        """Individual dividend payments."""
        data = self._get(f"/equity/dividends/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def dividends_by_fiscal_year(self, identifier: str) -> pd.DataFrame:
        """Dividends grouped by fiscal year."""
        data = self._get(f"/equity/dividends-by-fy/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def dividend_quality(self, identifier: str) -> dict:
        """Years paid, not decreased, increased, frequency, ex/pay dates."""
        return self._get(f"/equity/dividend-quality/{identifier}")

    def dividend_safety(self, ticker: str) -> dict:
        """Dividend safety & sustainability scores."""
        return self._get(f"/equity/dividend-safety/{ticker}")

    # ── Stock splits ─────────────────────────────────────────────────────

    def stock_splits(self, identifier: str) -> pd.DataFrame:
        """Historical stock splits."""
        data = self._get(f"/equity/splits/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ── Short data ───────────────────────────────────────────────────────

    def short_volume(self, identifier: str, limit: int = 90) -> pd.DataFrame:
        """FINRA daily short volume."""
        data = self._get(f"/equity/short-volume/{identifier}", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def short_interest(self, identifier: str, limit: int = 24) -> pd.DataFrame:
        """FINRA bi-monthly short interest (outstanding, days-to-cover)."""
        data = self._get(f"/equity/short-interest-positions/{identifier}", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def sec_fail_to_deliver(self, ticker: str, days: int = 90) -> pd.DataFrame:
        """SEC fail-to-deliver data."""
        data = self._get(f"/equity/sec-ftd/{ticker}", days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ── Ownership & Insider ──────────────────────────────────────────────

    def stock_ownership(self, identifier: str) -> pd.DataFrame:
        """Notable individual owners."""
        data = self._get(f"/equity/ownership/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def institutional_ownership(self, identifier: str) -> pd.DataFrame:
        """13-F institutional holders."""
        data = self._get(f"/sentiment/institutional-ownership/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def fund_ownership(self, identifier: str) -> pd.DataFrame:
        """Mutual fund holders."""
        data = self._get(f"/sentiment/fund-ownership/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def beneficial_ownership(self, identifier: str) -> pd.DataFrame:
        """SEC 13D/13G beneficial ownership (5%+ positions)."""
        data = self._get(f"/equity/beneficial-ownership/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def insider_trades(self, identifier: str) -> pd.DataFrame:
        """Insider trading activity (open-market)."""
        data = self._get(f"/equity/insider-trades/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def insider_trades_derivatives(self, identifier: str) -> pd.DataFrame:
        """Options/derivatives insider trades from SEC Form 4."""
        data = self._get(f"/equity/insider-trades-derivatives/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def insider_trades_eu(self, identifier: str) -> pd.DataFrame:
        """European insider trading (BaFin, AMF, FCA, ...)."""
        data = self._get(f"/equity/insider-trades-eu/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def sec_form4(self, identifier: str, limit: int = 50, days: int = 365) -> pd.DataFrame:
        """SEC Form 4 insider filings parsed from EDGAR (detailed)."""
        data = self._get(f"/equity/sec-form4/{identifier}", limit=limit, days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def etf_exposure(self, identifier: str) -> pd.DataFrame:
        """Every ETF that holds this stock with weight percentage."""
        data = self._get(f"/equity/etf-exposure/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def etf_list(self, start: int = 0, limit: int = 10) -> pd.DataFrame:
        """Paginated list of all ETF ISINs available."""
        data = self._get(f"/etf/list/{start}/{limit}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ── Sentiment ────────────────────────────────────────────────────────

    def insider_sentiment(self, identifier: str) -> pd.DataFrame:
        """Monthly MSPR (monthly share purchase ratio) -- -100 to +100."""
        data = self._get(f"/sentiment/insider-sentiment/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def news_sentiment(self, identifier: str) -> dict:
        """Aggregated news sentiment (buzz, bullish/bearish percentages)."""
        return self._get(f"/sentiment/news-sentiment/{identifier}")

    def social_sentiment(self, identifier: str) -> pd.DataFrame:
        """Social media sentiment (Reddit) daily mentions & scores."""
        data = self._get(f"/sentiment/social-sentiment/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

# ── ETF Data (profiles, holdings, sectors, flows) ────────────────────

    def etf_profile(self, identifier: str) -> dict:
        """Comprehensive ETF profile (AUM, NAV, expense ratio, holdings, ...)."""
        return self._get(f"/etf/profile/{identifier}")

    def etf_holdings(self, identifier: str) -> pd.DataFrame:
        """Top ETF holdings with weight, shares, value."""
        data = self._get(f"/etf/holdings/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def etf_sectors(self, identifier: str) -> pd.DataFrame:
        """ETF sector allocation."""
        data = self._get(f"/etf/sectors/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def etf_countries(self, identifier: str) -> pd.DataFrame:
        """ETF country allocation."""
        data = self._get(f"/etf/countries/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def etf_description(self, identifier: str, language: str = "en") -> dict:
        """Short ETF description in requested language."""
        return self._get(f"/etf/description/{identifier}", language=language)

    def etf_flows(self, ticker: str, days: int = 90) -> pd.DataFrame:
        """Daily fund flow data (inflows/outflows)."""
        data = self._get(f"/etf/flows/{ticker}", days=days)
# ── Crypto Extended ──────────────────────────────────────────────────

    def crypto_profile(self, symbol: str) -> dict:
        """Cryptocurrency profile / metadata."""
        return self._get(f"/crypto/profile/{symbol}")

    def top_cryptocurrencies(self) -> pd.DataFrame:
        """Top 100 cryptocurrencies ranked by market cap."""
        data = self._get("/crypto-extended/top-coins")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def crypto_market_overview(self) -> dict:
        """Fear & Greed, top gainers/losers, DeFi TVL, stablecoins."""
        return self._get("/crypto-extended/market-overview")

    def crypto_ohlcv(self, symbol: str, interval: str = "1d", limit: int = 100) -> pd.DataFrame:
        """Binance OHLCV candles (1h, 4h, 1d, 1w)."""
        data = self._get(f"/crypto-extended/candles/{symbol}", interval=interval, limit=limit)
        if not isinstance(data, list):
            return pd.DataFrame()
        rows = []
        for row in data:
            rows.append({
                "timestamp": self._maybe_ms(row.get("open_time", 0)),
                "open": row.get("open"), "high": row.get("high"),
                "low": row.get("low"), "close": row.get("close"),
                "volume": row.get("volume", 0),
            })
        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        return frame.set_index("timestamp").sort_index()

    def crypto_analysis(self, symbol: str) -> dict:
        """Technical indicators (RSI, MACD, SMA, ...), BTC correlation, derivatives."""
        return self._get(f"/crypto-extended/analysis/{symbol}")

    def crypto_derivatives(self, symbol: str) -> dict:
        """Funding rates, open interest, long/short ratios, taker volumes."""
        return self._get(f"/crypto-extended/derivatives/{symbol}")

    def onchain_metrics(self, symbol: str = "BTC") -> dict:
        """Hash rate, active addresses, tx count, fees, NVT ratio."""
        return self._get(f"/crypto-extended/onchain/{symbol}")

    def defi_protocols(self, limit: int = 50) -> pd.DataFrame:
        """Top DeFi protocols ranked by TVL."""
        data = self._get("/crypto-extended/defi-protocols", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def defi_yields(self, chain: str | None = None, limit: int = 50) -> pd.DataFrame:
        """Top DeFi yield opportunities ranked by APY."""
        params: dict[str, Any] = {"limit": limit}
        if chain:
            params["chain"] = chain
        data = self._get("/crypto-extended/defi-yields", **params)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def dex_volumes(self) -> pd.DataFrame:
        """Daily DEX trading volumes by protocol (last 30 days)."""
        data = self._get("/crypto-extended/dex-volumes")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def stablecoin_market_caps(self, days: int = 30) -> dict:
        """Current & historical stablecoin market caps (USDT, USDC, DAI)."""
        return self._get("/crypto-extended/stablecoins", days=days)

    def crypto_fear_greed(self, days: int = 90) -> pd.DataFrame:
        """Daily Crypto Fear & Greed Index values."""
        data = self._get("/crypto-extended/fear-greed-history", days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def global_crypto_market(self, days: int = 90) -> dict:
        """Global crypto market snapshots (mcap, volume, dominance)."""
        return self._get("/crypto-extended/global", days=days)

    def crypto_funding_rates(self, symbol: str, days: int = 30) -> pd.DataFrame:
        """Binance perpetual futures funding rate time series."""
        data = self._get(f"/crypto-extended/funding-rates/{symbol}", days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

# ── Macro / Economic Data ───────────────────────────────────────────

    def country_risk(self, region: str | None = None) -> pd.DataFrame:
        """Equity risk premiums, credit ratings for 249 countries."""
        params = {} if region is None else {"region": region}
        data = self._get("/macro/country-risk", **params)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def macro_calendar(
        self, start: str | None = None, end: str | None = None,
        countries: str | None = None,
    ) -> pd.DataFrame:
        """Economic events calendar (FOMC, CPI, NFP, ...)."""
        params: dict[str, Any] = {}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if countries:
            params["countries"] = countries
        data = self._get("/macro/calendar", **params)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def country_indicators(self, country: str = "US") -> dict:
        """All macro indicators for a country (GDP, unemployment, inflation)."""
        return self._get(f"/macro/country/{country}")

    # ── FRED ─────────────────────────────────────────────────────────

    def fred_series_list(self, category: str | None = None) -> pd.DataFrame:
        """All available FRED series with categories."""
        params = {} if category is None else {"category": category}
        data = self._get("/macro/fred/series", **params)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def fred_observations(self, series_id: str, limit: int = 500) -> dict:
        """Time series for a FRED series (GDP, UNRATE, CPIAUCSL, ...)."""
        return self._get(f"/macro/fred/observations/{series_id}", limit=limit)

    def fred_latest(self) -> pd.DataFrame:
        """Latest observations for ALL FRED series in one call."""
        data = self._get("/macro/latest/fred")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ── ECB ──────────────────────────────────────────────────────────

    def ecb_series_list(self, category: str | None = None) -> pd.DataFrame:
        """All available ECB data series."""
        params = {} if category is None else {"category": category}
        data = self._get("/macro/ecb/series", **params)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def ecb_observations(self, series_key: str, limit: int = 500) -> dict:
        """Time series for an ECB series (rates, EURIBOR, ...)."""
        return self._get(f"/macro/ecb/observations/{series_key}", limit=limit)

    # ── Options & Derivatives ───────────────────────────────────────────

    def options_chain(self, ticker: str, expiration: str | None = None) -> dict:
        """Options chain from CBOE (calls & puts) grouped by expiration."""
        params = {} if expiration is None else {"expiration": expiration}
        return self._get(f"/market/options/{ticker}", **params)

    def options_greeks(
        self, identifier: str, expiration: str | None = None,
    ) -> pd.DataFrame:
        """Compute Greeks (delta, gamma, theta, vega, rho) for the chain."""
        params: dict[str, Any] = {}
        if expiration:
            params["expiration"] = expiration
        data = self._get(f"/derivatives/options/greeks/{identifier}", **params)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def iv_surface(self, identifier: str) -> dict:
        """Implied volatility surface (matrix by expiration x strike)."""
        return self._get(f"/derivatives/options/iv-surface/{identifier}")

    def unusual_options_activity(
        self, min_volume: int = 1000, limit: int = 50,
    ) -> pd.DataFrame:
        """Unusual options activity (high volume/OI ratio, block trades)."""
        data = self._get(
            "/derivatives/options/unusual-activity",
            minVolume=min_volume, limit=limit,
        )
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def options_flow(
        self, identifier: str, days: int = 7, limit: int = 50,
    ) -> pd.DataFrame:
        """Recent large options trades sorted by premium."""
        data = self._get(f"/derivatives/options/flow/{identifier}", days=days, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def options_chain_historical(
        self,
        ticker: str,
        start: date | str | None = None,
        end: date | str | None = None,
        expiration: str | None = None,
    ) -> pd.DataFrame:
        """Historical options chain over a date range.

        Fetches option chains for each day in ``[start, end]`` and normalizes
        into a time-series DataFrame with columns:

            timestamp, ticker, expiration, strike, option_type,
            bid, ask, last, volume, open_interest, implied_volatility,
            in_the_money

        If Eulerpool's ``/market/options/{ticker}`` supports ``startdate`` /
        ``enddate`` query parameters (millisecond epochs, same convention as
        ``equity_quotes``), a single request covers the whole window.
        Otherwise the method falls back to iterating day-by-day and merging
        per-day snapshots.

        Parameters
        ----------
        ticker : str
            Underlying symbol (e.g. ``"AAPL"``).
        start, end : date | str | None
            Inclusive date window.  ISO-format strings accepted.  If omitted,
            fetches the latest chain (delegates to :meth:`options_chain`).
        expiration : str | None
            Optional ISO expiry filter (``"2025-12-20"``).

        Returns
        -------
        pd.DataFrame
            Empty DataFrame if no data available.
        """
        # Normalize string dates -> date objects
        if isinstance(start, str):
            start = date.fromisoformat(start)
        if isinstance(end, str):
            end = date.fromisoformat(end)

        if not start and not end:
            raw = self.options_chain(ticker, expiration=expiration)
            return self._normalize_options_chain(raw, ticker, None)

        params: dict[str, Any] = {}
        if expiration:
            params["expiration"] = expiration
        if start:
            params["startdate"] = int(
                datetime.combine(start, datetime.min.time()).timestamp() * 1000
            )
        if end:
            params["enddate"] = int(
                datetime.combine(end, datetime.max.time()).timestamp() * 1000
            )

        raw = self._get(f"/market/options/{ticker}", **params)

        if isinstance(raw, list) and raw:
            return self._normalize_options_chain_list(raw, ticker)

        if isinstance(raw, dict):
            return self._options_chain_date_range(
                ticker, start, end, expiration=expiration
            )

        return pd.DataFrame()

    def _options_chain_date_range(
        self,
        ticker: str,
        start: date | None,
        end: date | None,
        expiration: str | None = None,
    ) -> pd.DataFrame:
        """Iterate day-by-day over ``[start, end]`` and merge option chains."""
        if not start:
            start = date.today()
        if not end:
            end = date.today()

        frames: list[pd.DataFrame] = []
        cursor = start
        while cursor <= end:
            raw = self.options_chain(ticker, expiration=expiration)
            frame = self._normalize_options_chain(raw, ticker, cursor)
            if not frame.empty:
                frames.append(frame)
            cursor += timedelta(days=1)

        if not frames:
            return pd.DataFrame()
        result = pd.concat(frames, ignore_index=True)
        result["timestamp"] = pd.to_datetime(result["timestamp"], utc=True)
        return result.set_index("timestamp").sort_index()

    @staticmethod
    def _normalize_options_chain(
        raw: dict | None, ticker: str, snapshot_date: date | None
    ) -> pd.DataFrame:
        """Normalize a single options-chain snapshot into a DataFrame."""
        if not raw or not isinstance(raw, dict):
            return pd.DataFrame()

        rows: list[dict] = []
        snap_ts = (
            pd.Timestamp(datetime.combine(snapshot_date, datetime.min.time()), tz="UTC")
            if snapshot_date
            else pd.Timestamp.utcnow()
        )

        expirations: list[str] = []
        if isinstance(raw.get("expirations"), list):
            expirations = raw["expirations"]
        elif isinstance(raw.get("chain"), dict):
            expirations = list(raw["chain"].keys())

        for exp in expirations:
            exp_data = (
                raw.get("chain", {}).get(exp, {})
                if isinstance(raw.get("chain"), dict)
                else raw
            )
            for opt_type in ("calls", "puts"):
                contracts = exp_data.get(opt_type, []) if isinstance(exp_data, dict) else []
                if isinstance(contracts, dict):
                    contracts = [contracts]
                for c in contracts:
                    if not isinstance(c, dict):
                        continue
                    rows.append({
                        "timestamp": snap_ts,
                        "ticker": ticker,
                        "expiration": exp,
                        "option_type": opt_type.rstrip("s"),
                        "strike": float(c.get("strike", 0) or 0),
                        "bid": float(c.get("bid", 0) or 0),
                        "ask": float(c.get("ask", 0) or 0),
                        "last": float(c.get("last", c.get("lastPrice", 0)) or 0),
                        "volume": int(c.get("volume", 0) or 0),
                        "open_interest": int(c.get("openInterest", 0) or 0),
                        "implied_volatility": float(c.get("impliedVolatility", 0) or 0),
                        "in_the_money": bool(c.get("inTheMoney", False)),
                        "contract_symbol": c.get("contractSymbol", c.get("symbol", "")),
                    })

        for opt_type in ("calls", "puts"):
            contracts = raw.get(opt_type, [])
            if isinstance(contracts, list):
                for c in contracts:
                    if not isinstance(c, dict):
                        continue
                    rows.append({
                        "timestamp": snap_ts,
                        "ticker": ticker,
                        "expiration": c.get("expiration", ""),
                        "option_type": opt_type.rstrip("s"),
                        "strike": float(c.get("strike", 0) or 0),
                        "bid": float(c.get("bid", 0) or 0),
                        "ask": float(c.get("ask", 0) or 0),
                        "last": float(c.get("last", c.get("lastPrice", 0)) or 0),
                        "volume": int(c.get("volume", 0) or 0),
                        "open_interest": int(c.get("openInterest", 0) or 0),
                        "implied_volatility": float(c.get("impliedVolatility", 0) or 0),
                        "in_the_money": bool(c.get("inTheMoney", False)),
                        "contract_symbol": c.get("contractSymbol", c.get("symbol", "")),
                    })

        return pd.DataFrame(rows) if rows else pd.DataFrame()

    @staticmethod
    def _normalize_options_chain_list(raw: list, ticker: str) -> pd.DataFrame:
        """Normalize a list of dated chain snapshots into a DataFrame."""
        frames: list[pd.DataFrame] = []
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            snap_raw = entry.get("date", entry.get("timestamp", entry.get("snapshot_date")))
            snap_date = None
            if snap_raw:
                try:
                    snap_date = pd.to_datetime(snap_raw, utc=True).date()
                except Exception:
                    snap_date = None
            chain_data = entry.get("chain", entry)
            frame = EulerpoolProvider._normalize_options_chain(chain_data, ticker, snap_date)
            if not frame.empty:
                frames.append(frame)

        if not frames:
            return pd.DataFrame()
        result = pd.concat(frames, ignore_index=True)
        result["timestamp"] = pd.to_datetime(result["timestamp"], utc=True)
        return result.set_index("timestamp").sort_index()

    def cboe_indices(
        self, symbol: str | None = None, days: int = 30,
    ) -> pd.DataFrame:
        """CBOE index values (VIX, SKEW, VIX9D, VIX3M, VIX6M)."""
# ── Alternative Data (superinvestors, congress, social, patents) ────

    def fear_greed_index(self, days: int = 30) -> dict:
        """Market Fear & Greed Index with sub-indicator history."""
        return self._get("/alternative/fear-and-greed", days=days)

    def superinvestors_list(self) -> pd.DataFrame:
        """All tracked superinvestors (Buffett, Dalio, ...)."""
        data = self._get("/alternative/superinvestors/list")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def superinvestor_holdings(self, slug: str) -> dict:
        """Portfolio holdings of a specific superinvestor."""
        return self._get(f"/alternative/superinvestors/holdings/{slug}")

    def superinvestor_top_holdings(self, limit: int = 30) -> pd.DataFrame:
        """Most popular holdings across all superinvestors."""
        data = self._get("/alternative/superinvestors/top-holdings", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def superinvestor_recent_activity(self, limit: int = 100) -> pd.DataFrame:
        """Recent buy/sell activity from superinvestors."""
        data = self._get("/alternative/superinvestors/recent-activity", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def congress_trading(
        self, symbol: str | None = None, limit: int = 100,
    ) -> pd.DataFrame:
        """US Congress stock trades (STOCK Act disclosures)."""
        params: dict[str, Any] = {"limit": limit}
        if symbol:
            params["symbol"] = symbol
        data = self._get("/alternative/congress-trading", **params)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def google_trends(self, ticker: str, limit: int = 90) -> pd.DataFrame:
        """Google search interest over time for a company."""
        data = self._get(f"/alternative/google-trends/{ticker}", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def wikipedia_pageviews(self, ticker: str, limit: int = 90) -> pd.DataFrame:
        """Daily Wikipedia pageviews for a company article."""
        data = self._get(f"/alternative/wikipedia-pageviews/{ticker}", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def reddit_mentions(self, ticker: str, limit: int = 30) -> pd.DataFrame:
        """Daily Reddit mention counts for a stock."""
        data = self._get(f"/alternative/reddit-mentions/{ticker}", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def stocktwits_sentiment(self, ticker: str) -> dict:
        """Real-time StockTwits sentiment and message volume."""
        return self._get(f"/alternative/stocktwits/{ticker}")

    def patents(self, ticker: str, limit: int = 100) -> pd.DataFrame:
        """Patent filings for a company."""
        data = self._get(f"/patents/list/{ticker}", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def patent_statistics(self, ticker: str) -> pd.DataFrame:
        """Annual patent statistics for a company."""
        data = self._get(f"/patents/stats/{ticker}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def government_contracts(self, ticker: str, limit: int = 100) -> pd.DataFrame:
        """Federal government contracts awarded to a company."""
        data = self._get(f"/government/contracts/{ticker}", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()
# ── News, Research & Transcripts ─────────────────────────────────────

    def company_news(self, ticker: str) -> pd.DataFrame:
        """Latest news articles mentioning a company."""
        data = self._get(f"/research/news/{ticker}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def press_releases(self, ticker: str) -> pd.DataFrame:
        """Official company press releases."""
        data = self._get(f"/research/press-releases/{ticker}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def market_news(self, limit: int = 50) -> pd.DataFrame:
        """Latest general market news."""
        data = self._get("/equity-extended/market-news", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def earnings_call_list(self, identifier: str, limit: int = 20) -> pd.DataFrame:
        """List of earnings call transcripts for a security."""
        data = self._get(f"/transcripts/calls/{identifier}", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def earnings_call_transcript(self, identifier: str, call_id: int) -> dict:
        """Full earnings call transcript with parsed content."""
        return self._get(f"/transcripts/calls/{identifier}/{call_id}")

    def earnings_call_nlp(self, identifier: str, call_id: int) -> dict:
        """NLP analysis: sentiment, topics, management tone, guidance, metrics."""
        return self._get(f"/transcripts/calls/{identifier}/{call_id}/nlp")

    def search_transcripts(
        self, q: str, ticker: str | None = None, limit: int = 20,
    ) -> pd.DataFrame:
        """Full-text search across earnings call transcripts."""
        params: dict[str, Any] = {"q": q, "limit": limit}
        if ticker:
            params["ticker"] = ticker
        data = self._get("/transcripts/search", **params)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ── Mutual Funds ─────────────────────────────────────────────────

    def mutual_fund_profile(self, identifier: str) -> dict:
        """Mutual fund profile / overview."""
        return self._get(f"/mutual-fund/profile/{identifier}")

    def mutual_fund_holdings(self, symbol: str) -> pd.DataFrame:
        """Top mutual fund holdings."""
        data = self._get(f"/mutual-fund/holdings/{symbol}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def mutual_fund_sectors(self, symbol: str) -> pd.DataFrame:
        """Mutual fund sector allocation."""
        data = self._get(f"/mutual-fund/sectors/{symbol}")
# ── Institutional (13-F, fund holders) ──────────────────────────────

    def institutional_profile(self, cik: str) -> dict:
        """Institutional investor profile (hedge fund / asset manager)."""
        return self._get(f"/institutional/profile/{cik}")

    def institutional_portfolio(self, cik: str) -> pd.DataFrame:
        """Full 13-F portfolio holdings of an institutional investor."""
        data = self._get(f"/institutional/portfolio/{cik}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def sec_13f_holders(self, ticker: str) -> pd.DataFrame:
        """All institutional holders of a stock from latest 13F filings."""
        data = self._get(f"/institutional/13f-holders/{ticker}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def top_13f_filers(self, limit: int = 50) -> pd.DataFrame:
        """Largest institutional investors by 13F AUM."""
        data = self._get("/institutional/13f-filers", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ── SEC / XBRL ───────────────────────────────────────────────────

    def sec_filings(self, identifier: str) -> pd.DataFrame:
        """Recent SEC filings (10-K, 10-Q, 8-K)."""
        data = self._get(f"/equity-extended/sec-filings/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def sec_company_info(self, ticker: str) -> dict:
        """SEC EDGAR metadata (CIK, SIC code, fiscal year end)."""
        return self._get(f"/fundamentals/company/{ticker}")

    def xbrl_facts(
        self, ticker: str, tag: str | None = None, limit: int = 500,
    ) -> pd.DataFrame:
        """SEC XBRL financial facts from EDGAR."""
        params: dict[str, Any] = {"limit": limit}
        if tag:
            params["tag"] = tag
        data = self._get(f"/equity-extended/xbrl/facts/{ticker}", **params)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def xbrl_fact_series(self, ticker: str, tag: str) -> pd.DataFrame:
        """Historical time series for a specific XBRL fact."""
# ── Government Bonds & Yield Curve ───────────────────────────────────

    def yield_curve(self, country: str = "US", days: int = 90) -> pd.DataFrame:
        """Government bond yield curve by tenor."""
        data = self._get("/bonds/yield-curve", country=country, days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def treasury_auctions(
        self, type_: str | None = None, limit: int = 100,
    ) -> pd.DataFrame:
        """US Treasury auction results."""
        params: dict[str, Any] = {"limit": limit}
        if type_:
            params["type"] = type_
        data = self._get("/government/treasury/auctions", **params)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ── Market Data (live quotes, movers, status) ─────────────────────

    def latest_quotes(self, stocks: str | None = None) -> pd.DataFrame:
        """Latest price for one or more tickers."""
        params = {} if stocks is None else {"stocks": stocks}
        data = self._get("/market/quotes/latest", **params)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def intraday_quotes(self, identifier: str, exchange: str | None = None) -> pd.DataFrame:
        """Intraday price data for current/previous trading day."""
        params = {} if exchange is None else {"exchange": exchange}
        data = self._get(f"/market/quotes/intraday/{identifier}", **params)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def top_movers(self, country: str = "US", limit: int = 20) -> dict:
        """Top gainers & losers for the day."""
        return self._get("/market/top-movers", country=country, limit=limit)

    def market_status(self) -> dict:
        """Whether major stock exchanges are currently open/closed."""
        return self._get("/market/market-status")

    def market_breadth(self, days: int = 30) -> pd.DataFrame:
        """Advancing/declining stocks, A/D ratio."""
        data = self._get("/market/breadth", days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def exchanges(self) -> pd.DataFrame:
        """Full list of covered stock exchanges with MIC codes, hours."""
        data = self._get("/market/exchanges")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def market_holidays(self) -> pd.DataFrame:
        """Exchange holiday schedule for 17 global exchanges."""
        data = self._get("/market/holidays")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def sector_performance(self, period: str = "1d", days: int = 30) -> pd.DataFrame:
        """Sector returns for a given period."""
        data = self._get("/market/sector-performance", period=period, days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ── Index Data ───────────────────────────────────────────────────

    def index_constituents(
        self, index_id: str, start: int = 0, end: int = 500,
    ) -> pd.DataFrame:
        """Constituents of a market index (S&P 500, DAX, NASDAQ 100, ...)."""
        data = self._get(f"/index/constituents/{index_id}", start=start, end=end)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ── Calendar ────────────────────────────────────────────────────────

    def ipo_calendar(self) -> pd.DataFrame:
        """Upcoming and recent IPOs."""
        data = self._get("/calendar/ipo")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def dividend_calendar(
        self, year: str = "initial", country: str = "US", limit: int = 500,
    ) -> pd.DataFrame:
        """Ex-dividend dates and amounts."""
        data = self._get(f"/calendar/dividends/{year}", country=country, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def forward_dividend_calendar(self, days: int = 60, limit: int = 100) -> pd.DataFrame:
        """Upcoming ex-dividend dates."""
        data = self._get("/calendar/dividends-forward", days=days, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def earnings_calendar_weekly(self, date_str: str) -> pd.DataFrame:
        """Earnings reports for the week containing a date."""
        data = self._get(f"/calendar/earnings/{date_str}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def earnings_by_symbol(self, symbol: str) -> pd.DataFrame:
        """Upcoming and past earnings dates for a company."""
        data = self._get(f"/calendar/earnings-by-symbol/{symbol}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def earnings_surprises(self, symbol: str) -> pd.DataFrame:
        """Historical earnings surprises (actual vs estimate)."""
        data = self._get(f"/calendar/earnings-surprises/{symbol}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def ma_deals(
        self, status: str = "all", limit: int = 100, days: int = 90,
    ) -> pd.DataFrame:
        """M&A deal announcements from SEC filings."""
        data = self._get("/calendar/ma-deals", status=status, limit=limit, days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

# ── Ticker Trends ───────────────────────────────────────────────────

    def ticker_trends(self, symbol: str = "all") -> dict:
        """Current ticker trend values (prev day, last, current quotes, ...)."""
        return self._get(f"/trends/ticker-trends/{symbol}")

    # ── Commodities (extended) ────────────────────────────────────────

    def commodity_list(self) -> pd.DataFrame:
        """All available commodities with prices and metadata."""
        data = self._get("/commodity/list")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def commodity_profile(self, ticker: str) -> dict:
        """Commodity profile information."""
        return self._get(f"/commodity/profile/{ticker}")

    def futures_curve(self, product: str) -> pd.DataFrame:
        """Futures term structure (contango/backwardation)."""
        data = self._get(f"/commodity/futures-curve/{product}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def futures_curve_history(self, product: str, days: int = 90) -> pd.DataFrame:
        """Historical contango/backwardation analytics."""
        data = self._get(f"/commodity/futures-curve/{product}/history", days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def crack_spreads(self, days: int = 90) -> pd.DataFrame:
        """3-2-1 crack spread (refinery margin) from WTI, RBOB, HO futures."""
        data = self._get("/commodity/crack-spreads", days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ── Quality / Risk Scores ────────────────────────────────────────

    def quality_scores(self, ticker: str) -> dict:
        """Piotroski F-Score & Altman Z-Score."""
        return self._get(f"/equity/quality-scores/{ticker}")

    def risk_return_analytics(
        self, identifier: str, range_: str = "1y", risk_free_rate: float = 4.5,
    ) -> dict:
        """Volatility, max drawdown, Sharpe, Sortino, return stats."""
        return self._get(
            f"/market/analytics/risk/{identifier}",
            range=range_, riskFreeRate=risk_free_rate,
        )

    def correlation(self, isin1: str, isin2: str, range_: str = "1y") -> dict:
        """Correlation & beta between two stocks."""
        return self._get(
            "/market/analytics/correlation",
            isin1=isin1, isin2=isin2, range=range_,
        )

    def fair_value(self, identifier: str) -> dict:
        """Computed fair value with upside/downside from income, revenue, DDM."""
        return self._get(f"/fair-value/by-isin/{identifier}")

    # ── ICE SWAP ─────────────────────────────────────────────────────

    def ice_swap(self, code: str) -> pd.DataFrame:
        """ICE-SWAP data for a given currency code."""
        data = self._get(f"/ice-swap/{code}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ── Screener & Search ────────────────────────────────────────────

    def search_symbols(self, query: str) -> dict:
        """Search stocks, ETFs, crypto, bonds by name/ticker/ISIN."""
        return self._get(f"/equity/search", q=query)

    def screen_stocks(self, filters: list[dict]) -> pd.DataFrame:
        """Filter stocks by fundamental criteria."""
        data = self._post("/screener/screen", json=filters)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def screener_metadata(self) -> dict:
        """Available filter dimensions (countries, sectors, industries)."""
        return self._get("/screener/metadata")

    # ── Logo (returns bytes) ─────────────────────────────────────────

    def logo_by_ticker(self, symbol: str, size: int = 128) -> bytes:
        """Company logo image by ticker symbol."""
        resp = self.session.get(
            f"{self.base_url}/logo/ticker/{symbol}",
            params={"size": size, "token": self.api_key},
            timeout=self.timeout,
        )
        return resp.content if resp.ok else b""

    def logo_by_isin(self, code: str, size: int = 128) -> bytes:
        """Company logo image by ISIN."""
        resp = self.session.get(
            f"{self.base_url}/logo/isin/{code}",
            params={"size": size, "token": self.api_key},
            timeout=self.timeout,
        )
        return resp.content if resp.ok else b""

    # ── Peer Comparison ──────────────────────────────────────────────

    def auto_peers(self, identifier: str) -> dict:
        """Find up to 20 comparable companies (GICS sector + market cap)."""
        return self._get(f"/peer-comparison/peers/{identifier}")

    def relative_valuation(self, identifier: str) -> dict:
        """Premium/discount vs sector median for each valuation metric."""
        return self._get(f"/peer-comparison/relative-valuation/{identifier}")

    def financial_benchmarking(self, identifier: str) -> dict:
        """Benchmark vs sector/industry medians, quartiles, percentiles."""
        return self._get(f"/peer-comparison/financial-benchmarking/{identifier}")
        """Upcoming economic events (FOMC, CPI, NFP)."""
        data = self._get("/calendar/economic-calendar", days=days, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def ipo_pipeline(self, status: str | None = None, limit: int = 200) -> pd.DataFrame:
        """Upcoming and recent IPOs from Nasdaq + SEC filings."""
        params: dict[str, Any] = {"limit": limit}
        if status:
            params["status"] = status
        data = self._get("/calendar/ipo-pipeline", **params)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def corporate_events(self, ticker: str, limit: int = 50) -> pd.DataFrame:
        """SEC 8-K corporate events (M&A, leadership changes, ...)."""
        data = self._get(f"/equity-extended/corporate-events/{ticker}", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def fund_disclosure(self, symbol: str) -> pd.DataFrame:
        """Quarterly SEC N-PORT disclosure for a fund or ETF."""
        data = self._get(f"/mutual-fund/disclosure/{symbol}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def government_contract_stats(self, ticker: str) -> pd.DataFrame:
        """Annual government contract statistics."""
        data = self._get(f"/government/stats/{ticker}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def investment_themes(self) -> pd.DataFrame:
        """Curated portfolios grouped by investment themes (AI, Clean Energy, ...)."""
        data = self._get("/alternative/investment-themes")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()
        params: dict[str, Any] = {"days": days}
        if symbol:
            params["symbol"] = symbol
        data = self._get("/market/cboe/indices", **params)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()
        """Time series for an ECB series (rates, EURIBOR, ...)."""
        return self._get(f"/macro/ecb/observations/{series_key}", limit=limit)

    def ecb_exchange_rates(self, currency: str | None = None) -> dict:
        """ECB euro foreign-exchange reference rates."""
        params = {} if currency is None else {"currency": currency}
        return self._get("/ecb/exchange-rates", **params)

    def ecb_key_rates(self) -> dict:
        """ECB main refinancing, deposit facility, marginal lending rates."""
        return self._get("/ecb/key-rates")

    # ── Credit spreads & market indicators ───────────────────────────

    def credit_spreads(self, days: int = 365) -> pd.DataFrame:
        """Credit spread indices (OAS, Baa/Aaa, TED, yield curve)."""
        data = self._get("/macro/credit-spreads", days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def market_indicators(self, days: int = 30) -> pd.DataFrame:
        """VIX, put/call ratio, market-wide indicators."""
        data = self._get("/market/indicators", days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()
    def crypto_open_interest(self, symbol: str, days: int = 30) -> pd.DataFrame:
        """Binance perpetual futures open interest time series."""
        data = self._get(f"/crypto-extended/open-interest/{symbol}", days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ── On-chain DEX (GeckoTerminal) ──────────────────────────────────

    def dex_trending_pools(self) -> pd.DataFrame:
        """Trending liquidity pools across all networks."""
        data = self._get("/dex/trending-pools")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def dex_new_pools(self) -> pd.DataFrame:
        """Newly created liquidity pools (near real-time)."""
        data = self._get("/dex/new-pools")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def dex_network_pools(self, network: str = "eth") -> pd.DataFrame:
        """Top pools on a specific blockchain network."""
        data = self._get(f"/dex/pools/{network}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def swot_analysis(self, identifier: str, language: str = "en") -> dict:
        """AI-generated SWOT analysis (Strengths, Weaknesses, ...)."""
        return self._get(f"/equity/swot/{identifier}", language=language)

    def sec_fail_to_deliver(self, ticker: str, days: int = 90) -> pd.DataFrame:
        """SEC fail-to-deliver data."""
        data = self._get(f"/equity/sec-ftd/{ticker}", days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()
# ═══════════════════════════════════════════════════════════════════════
    #  SHIPPING  (vessels, positions, voyages, cargoes, ports)
    # ═══════════════════════════════════════════════════════════════════════

    def shipping_vessels(
        self,
        vessel_type: str | None = None,
        vessel_class: str | None = None,
        flag: str | None = None,
        search: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> pd.DataFrame:
        """Search global tanker/LNG/LPG vessel registry."""
        data = self._get("/shipping/vessels", vessel_type=vessel_type, vessel_class=vessel_class, flag=flag, search=search, limit=limit, offset=offset)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def shipping_vessel(self, imo: int) -> dict:
        """Get vessel details by IMO number including current position."""
        return self._get(f"/shipping/vessels/{imo}")

    def shipping_vessel_track(self, imo: int, days: int = 7, limit: int = 1000) -> pd.DataFrame:
        """Historical AIS positions for a vessel."""
        data = self._get(f"/shipping/vessels/{imo}/track", days=days, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def shipping_positions(self, bbox: str | None = None, vessel_type: str | None = None, min_speed: float | None = None) -> pd.DataFrame:
        """All current vessel positions within a bounding box."""
        data = self._get("/shipping/positions", bbox=bbox, vessel_type=vessel_type, min_speed=min_speed)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def shipping_voyages(self, status: str | None = None, cargo_type: str | None = None, imo: int | None = None, origin_port: int | None = None, destination_port: int | None = None, limit: int = 100, offset: int = 0) -> pd.DataFrame:
        """List active and recent voyages with filtering."""
        data = self._get("/shipping/voyages", status=status, cargo_type=cargo_type, imo=imo, origin_port=origin_port, destination_port=destination_port, limit=limit, offset=offset)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def shipping_cargoes(self, product: str | None = None, origin_country: str | None = None, destination_country: str | None = None, start_date: str | None = None, end_date: str | None = None, limit: int = 100, offset: int = 0) -> pd.DataFrame:
        """Track cargo movements by product, origin, destination, and date."""
        data = self._get("/shipping/cargoes", product=product, origin_country=origin_country, destination_country=destination_country, start_date=start_date, end_date=end_date, limit=limit, offset=offset)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def shipping_ports(self, country_code: str | None = None, port_type: str | None = None, search: str | None = None, limit: int = 100) -> pd.DataFrame:
        """List global oil/LNG ports and terminals."""
        data = self._get("/shipping/ports", country_code=country_code, port_type=port_type, search=search, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def shipping_port_activity(self, port_id: int, days: int = 30, limit: int = 100) -> pd.DataFrame:
        """Recent voyages arriving at or departing from a port (e.g. Port of LA)."""
        data = self._get(f"/shipping/ports/{port_id}/activity", days=days, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    @staticmethod
    def _ts_ms(ts: int | float) -> datetime:
        """Convert milliseconds-since-epoch to UTC datetime."""
        return datetime.utcfromtimestamp(ts / 1000)

    @staticmethod
    def _maybe_ms(value: Any) -> datetime | None:
        if isinstance(value, (int, float)):
            return EulerpoolProvider._ts_ms(int(value))
        return None
# ═══════════════════════════════════════════════════════════════════════
    #  ENERGY  (pipelines, storage, natural gas, petroleum, coal, JODI)
    # ═══════════════════════════════════════════════════════════════════════

    def energy_pipelines(self, commodity: str | None = None, country: str | None = None, status: str | None = None, limit: int = 200) -> pd.DataFrame:
        """List global oil and gas pipelines with GeoJSON routes."""
        data = self._get("/energy/pipelines/", commodity=commodity, country=country, status=status, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def energy_pipeline(self, pipeline_id: int) -> dict:
        """Get pipeline details including latest flow reading."""
        return self._get(f"/energy/pipelines/{pipeline_id}")

    def energy_pipeline_flows(self, pipeline_id: int, start_date: str | None = None, end_date: str | None = None, limit: int = 500) -> pd.DataFrame:
        """Historical flow time-series for a pipeline."""
        data = self._get(f"/energy/pipelines/{pipeline_id}/flows", start_date=start_date, end_date=end_date, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def energy_latest(self) -> dict:
        """Latest energy data snapshot (US, EU, global)."""
        return self._get("/energy/energy/latest")

    def energy_natural_gas(self, start_date: str | None = None, end_date: str | None = None, limit: int = 100) -> pd.DataFrame:
        """Weekly natural gas storage and production data (EIA)."""
        data = self._get("/energy/natural-gas/weekly", start_date=start_date, end_date=end_date, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def energy_petroleum(self, start_date: str | None = None, end_date: str | None = None, limit: int = 100) -> pd.DataFrame:
        """Weekly petroleum status report (EIA)."""
        data = self._get("/energy/petroleum/weekly", start_date=start_date, end_date=end_date, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def energy_coal(self, start_date: str | None = None, end_date: str | None = None, limit: int = 100) -> pd.DataFrame:
        """Quarterly coal data."""
        data = self._get("/energy/coal/quarterly", start_date=start_date, end_date=end_date, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def energy_electricity(self, start_date: str | None = None, end_date: str | None = None, limit: int = 100) -> pd.DataFrame:
        """Monthly electricity data."""
        data = self._get("/energy/electricity/monthly", start_date=start_date, end_date=end_date, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def energy_jodi(self, commodity: str = "oil", country_code: str | None = None, limit: int = 100) -> pd.DataFrame:
        """JODI oil & gas flows data."""
        data = self._get("/energy/jodi", commodity=commodity, country_code=country_code, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def energy_storage(self, limit: int = 100) -> pd.DataFrame:
        """List storage facilities."""
        data = self._get("/energy/storage/facilities", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def energy_storage_levels(self, facility_id: int, start_date: str | None = None, end_date: str | None = None, limit: int = 100) -> pd.DataFrame:
        """Historical storage levels for a facility."""
        data = self._get(f"/energy/storage/facilities/{facility_id}/levels", start_date=start_date, end_date=end_date, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def energy_storage_summary(self) -> dict:
        """Storage summary across all facilities."""
        return self._get("/energy/storage/summary")
# ═══════════════════════════════════════════════════════════════════════
    #  GOVERNMENT  (treasury auctions, debt, yields)
    # ═══════════════════════════════════════════════════════════════════════

    def government_treasury_auctions(self, limit: int = 100) -> pd.DataFrame:
        """US Treasury auction results."""
        data = self._get("/government/treasury/auctions", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def government_treasury_debt(self, limit: int = 100) -> pd.DataFrame:
        """US Treasury debt outstanding."""
        data = self._get("/government/treasury/debt", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def government_treasury_yields(self, limit: int = 100) -> pd.DataFrame:
        """US Treasury yield history."""
        data = self._get("/government/treasury/yields", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ═══════════════════════════════════════════════════════════════════════
    #  FIXED INCOME  (spot curve, forward curve, analytics, default prob)
    # ═══════════════════════════════════════════════════════════════════════

    def fixed_income_analytics(self) -> dict:
        """Fixed income analytics dashboard."""
        return self._get("/fixed-income/analytics")

    def fixed_income_spot_curve(self, country: str = "US") -> pd.DataFrame:
        """Government bond spot curve."""
        data = self._get("/fixed-income/curve/spot", country=country)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def fixed_income_forward_curve(self, country: str = "US", term: str = "1y") -> pd.DataFrame:
        """Government bond forward curve."""
        data = self._get("/fixed-income/curve/forward", country=country, term=term)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def fixed_income_default_probabilities(self, recovery: float = 0.4) -> pd.DataFrame:
        """Implied default probabilities from CDS spreads."""
        data = self._get("/fixed-income/default-probabilities", recovery=recovery)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()
# ═══════════════════════════════════════════════════════════════════════
    #  SINGAPORE  (ACRA, MAS, REITs, economic stats, corporate actions)
    # ═══════════════════════════════════════════════════════════════════════

    def singapore_acra(self, isin: str | None = None, limit: int = 100) -> pd.DataFrame:
        """Singapore ACRA (Accounting & Corporate Regulatory Authority) data."""
        data = self._get("/singapore/acra", isin=isin, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def singapore_acra_uen(self, uen: str) -> dict:
        """Singapore ACRA data by UEN."""
        return self._get(f"/singapore/acra/{uen}")

    def singapore_announcements(self, isin: str | None = None, limit: int = 100) -> pd.DataFrame:
        """Singapore corporate announcements."""
        data = self._get("/singapore/announcements", isin=isin, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def singapore_corporate_actions(self, limit: int = 100) -> pd.DataFrame:
        """Singapore corporate actions."""
        data = self._get("/singapore/corporate-actions", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def singapore_economic_stats(self, limit: int = 100) -> pd.DataFrame:
        """Singapore economic statistics."""
        data = self._get("/singapore/economic-stats", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def singapore_insider_trades(self, limit: int = 100) -> pd.DataFrame:
        """Singapore insider trading disclosures."""
        data = self._get("/singapore/insider-trades", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def singapore_mas_exchange_rates(self, currency: str = "USD", limit: int = 100) -> pd.DataFrame:
        """MAS (Monetary Authority of Singapore) exchange rates."""
        data = self._get("/singapore/mas/exchange-rates", currency=currency, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def singapore_mas_interest_rates(self, limit: int = 100) -> pd.DataFrame:
        """MAS interest rates."""
        data = self._get("/singapore/mas/interest-rates", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def singapore_mas_money_supply(self, limit: int = 100) -> pd.DataFrame:
        """MAS money supply data."""
        data = self._get("/singapore/mas/money-supply", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def singapore_reits(self, isin: str | None = None, limit: int = 100) -> pd.DataFrame:
        """Singapore REITs data."""
        data = self._get("/singapore/reits", isin=isin, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def singapore_reits_list(self) -> pd.DataFrame:
        """List of all Singapore REITs."""
        data = self._get("/singapore/reits/list")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ═══════════════════════════════════════════════════════════════════════
    #  NFT  (collections, profile, search, price history, markets)
    # ═══════════════════════════════════════════════════════════════════════

    def nft_collections(self, limit: int = 100, offset: int = 0) -> pd.DataFrame:
        """List NFT collections."""
        data = self._get("/nft/list", limit=limit, offset=offset)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def nft_collection_profile(self, slug: str) -> dict:
        """NFT collection profile."""
        return self._get(f"/nft/profile/{slug}")

    def nft_search(self, query: str, limit: int = 100) -> pd.DataFrame:
        """Search NFT collections."""
        data = self._get("/nft/search", q=query, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def nft_price_history(self, slug: str) -> pd.DataFrame:
        """NFT collection price history."""
        data = self._get(f"/nft/price-history/{slug}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def nft_markets(self, limit: int = 100) -> pd.DataFrame:
        """NFT marketplace ranking."""
        data = self._get("/nft/markets", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()
# ═══════════════════════════════════════════════════════════════════════
    #  ANALYTICS  (CFTC TFF, corporate events, Fama-French, options volume)
    # ═══════════════════════════════════════════════════════════════════════

    def analytics_cftc_tff(self) -> pd.DataFrame:
        """CFTC TFF (Treasury Futures & Options) report."""
        data = self._get("/analytics/cftc/tff")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def analytics_cftc_tff_exchange(self, exchange: str = "CME", days: int = 365) -> pd.DataFrame:
        """CFTC TFF report for a specific exchange."""
        data = self._get(f"/analytics/cftc/tff/{exchange}", days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def analytics_corporate_events(self, days: int = 7, limit: int = 100) -> pd.DataFrame:
        """Market-wide corporate events."""
        data = self._get("/analytics/corporate-events", days=days, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def analytics_fama_french(self, start_date: str | None = None, end_date: str | None = None, days: int = 252) -> pd.DataFrame:
        """Fama-French factor returns."""
        data = self._get("/analytics/fama-french", start_date=start_date, end_date=end_date, days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def analytics_options_volume(self, days: int = 30) -> pd.DataFrame:
        """Total options volume across the market."""
        data = self._get("/analytics/options-volume", days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ═══════════════════════════════════════════════════════════════════════
    #  CHARTING  (OHLCV, indicators, patterns, overlays, compare)
    # ═══════════════════════════════════════════════════════════════════════

    def charting_ohlcv(self, identifier: str, resolution: str = "D") -> pd.DataFrame:
        """OHLCV time series for chart rendering."""
        data = self._get(f"/charting/ohlcv/{identifier}", resolution=resolution)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def charting_indicators(self, identifier: str, indicator: str = "sma", period: int = 14, resolution: str = "D") -> pd.DataFrame:
        """Technical indicator values for chart overlay."""
        data = self._get(f"/charting/indicators/{identifier}", indicator=indicator, period=period, resolution=resolution)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def charting_patterns(self, identifier: str, resolution: str = "D") -> pd.DataFrame:
        """Chart pattern recognition results."""
        data = self._get(f"/charting/patterns/{identifier}", resolution=resolution)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def charting_overlay(self, identifier: str, overlay_type: str = "fibonacci", pivot_method: str = "standard", resolution: str = "D") -> dict:
        """Chart overlay data (fibonacci, trends, etc.)."""
        return self._get(f"/charting/overlay/{identifier}", type=overlay_type, pivot_method=pivot_method, resolution=resolution)

    def charting_compare(self, symbols: list[str], normalize: bool = False) -> pd.DataFrame:
        """Compare multiple symbols on a normalized chart."""
        data = self._get("/charting/compare", symbols=",".join(symbols), normalize=str(normalize).lower())
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()
# ═══════════════════════════════════════════════════════════════════════
    #  RISK MODELS  (covariance, exposure, factor returns, factors)
    # ═══════════════════════════════════════════════════════════════════════

    def risk_covariance(self) -> dict:
        """Risk model covariance matrix."""
        return self._get("/risk-models/covariance")

    def risk_exposure(self, identifier: str) -> dict:
        """Risk model factor exposure for a security."""
        return self._get(f"/risk-models/exposure/{identifier}")

    def risk_factor_returns(self, from_date: str | None = None, to_date: str | None = None, frequency: str = "D") -> pd.DataFrame:
        """Risk model factor return time series."""
        data = self._get("/risk-models/factor-returns", from_date=from_date, to_date=to_date, frequency=frequency)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def risk_factors(self) -> pd.DataFrame:
        """List of all risk model factors."""
        data = self._get("/risk-models/factors")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def risk_portfolio_risk(self) -> dict:
        """Aggregate portfolio risk decomposition."""
        return self._get("/risk-models/portfolio-risk")

    # ═══════════════════════════════════════════════════════════════════════
    #  MARKET EXTENDED  (52-week, multi-exchange, dark pool, L2, bulk)
    # ═══════════════════════════════════════════════════════════════════════

    def market_52week(self, identifier: str) -> dict:
        """52-week price analytics (high, low, change, percentile)."""
        return self._get(f"/market/analytics/52week/{identifier}")

    def market_fx_returns(self, identifier: str) -> dict:
        """Currency-adjusted returns."""
        return self._get(f"/market/analytics/fx-returns/{identifier}")

    def market_multi_exchange(self, identifier: str) -> pd.DataFrame:
        """Quotes from multiple exchanges."""
        data = self._get(f"/market/multi-exchange/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def market_bulk_quotes(self, identifiers: str) -> pd.DataFrame:
        """Bulk quotes for many identifiers."""
        data = self._get("/market/bulk", identifiers=identifiers)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def market_dark_pool(self, identifier: str, days: int = 30) -> pd.DataFrame:
        """Dark pool trading volume."""
        data = self._get(f"/market/darkpool/{identifier}", days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def market_level2(self, identifier: str) -> dict:
        """Level 2 order book snapshot."""
        return self._get(f"/market/l2/{identifier}")

    def market_last_trade(self, identifier: str) -> dict:
        """Last trade for a security."""
        return self._get(f"/market/last-trade/{identifier}")

    def market_last_quote(self, identifier: str) -> dict:
        """Last quote for a security."""
        return self._get(f"/market/last-quote/{identifier}")

    def market_precomputed_risk(self, identifier: str) -> dict:
        """Precomputed risk metrics (beta, vol, Sharpe, etc.)."""
        return self._get(f"/market/risk/{identifier}")

    def market_unusual_moves(self, days: int = 5, limit: int = 50) -> pd.DataFrame:
        """Unusual price moves detected across the market."""
# ═══════════════════════════════════════════════════════════════════════
    #  SENTIMENT EXTENDED  (price-metrics, sector-metrics, social feed)
    # ═══════════════════════════════════════════════════════════════════════

    def sentiment_price_metrics(self, identifier: str) -> dict:
        """Price-derived sentiment metrics."""
        return self._get(f"/sentiment/price-metrics/{identifier}")

    def sentiment_sector_metrics(self) -> pd.DataFrame:
        """Sector-level sentiment metrics."""
        data = self._get("/sentiment/sector-metrics")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def sentiment_social_feed(self, identifier: str) -> pd.DataFrame:
        """Raw social media feed for a security."""
        data = self._get(f"/sentiment/social/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ═══════════════════════════════════════════════════════════════════════
    #  ALTERNATIVE EXTENDED  (COT, datasets)
    # ═══════════════════════════════════════════════════════════════════════

    def alternative_cot(self, product: str = "CRUDE", limit: int = 10) -> pd.DataFrame:
        """CFTC Commitment of Traders report."""
        data = self._get(f"/alternative/cot/{product}", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def alternative_datasets(self) -> pd.DataFrame:
        """List available alternative datasets."""
        data = self._get("/alternative/datasets")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def alternative_dataset(self, dataset_id: str, ticker: str, days: int = 365) -> pd.DataFrame:
        """Time series data from an alternative dataset."""
        data = self._get(f"/alternative/datasets/{dataset_id}", ticker=ticker, days=days)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ═══════════════════════════════════════════════════════════════════════
    #  SCREENER EXTENDED / VENDOR WAREHOUSE
    # ═══════════════════════════════════════════════════════════════════════

    def screener_universe(self) -> pd.DataFrame:
        """Available screener universes."""
        data = self._get("/screener/universe")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def vendor_catalog(self) -> pd.DataFrame:
        """Every dataset in the vendor warehouse."""
        data = self._get("/vendor/catalog")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def vendor_dataset(self, vendor: str, dataset: str, as_of: str | None = None) -> pd.DataFrame:
        """Latest market-wide snapshot for a vendor dataset."""
        data = self._get(f"/vendor/{vendor}/{dataset}", as_of=as_of)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def vendor_dataset_key(self, vendor: str, dataset: str, key: str, as_of: str | None = None) -> dict:
        """Latest snapshot for one key inside a vendor dataset."""
        return self._get(f"/vendor/{vendor}/{dataset}/{key}", as_of=as_of)
        data = self._get("/market/unusual-moves", days=days, limit=limit)
# ═══════════════════════════════════════════════════════════════════════
    #  FMP  (dcf, key-metrics, ratios, eod, scores, rating, gainers, news)
    # ═══════════════════════════════════════════════════════════════════════

    def fmp_catalog(self) -> pd.DataFrame:
        """FMP dataset catalog with slugs."""
        data = self._get("/fmp/catalog")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def fmp_dcf(self, identifier: str) -> dict:
        """Discounted cash flow valuation."""
        return self._get(f"/fmp/dcf/{identifier}")

    def fmp_key_metrics(self, identifier: str) -> dict:
        """Key metrics (TTM)."""
        return self._get(f"/fmp/key-metrics/{identifier}")

    def fmp_ratios(self, identifier: str) -> dict:
        """Financial ratios (TTM)."""
        return self._get(f"/fmp/ratios/{identifier}")

    def fmp_eod(self, identifier: str) -> dict:
        """Latest end-of-day OHLCV snapshot."""
        return self._get(f"/fmp/eod/{identifier}")

    def fmp_scores(self, identifier: str) -> dict:
        """Altman Z-score, Piotroski score, financial health."""
        return self._get(f"/fmp/scores/{identifier}")

    def fmp_rating(self, identifier: str) -> dict:
        """Composite rating with per-factor scores."""
        return self._get(f"/fmp/rating/{identifier}")

    def fmp_dataset(self, dataset: str, limit: int = 100, offset: int = 0, as_of: str | None = None) -> pd.DataFrame:
        """Market-wide FMP dataset snapshot."""
        data = self._get(f"/fmp/{dataset}", limit=limit, offset=offset, as_of=as_of)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def fmp_company_dataset(self, dataset: str, identifier: str, as_of: str | None = None) -> dict:
        """FMP dataset snapshot for one security."""
        return self._get(f"/fmp/{dataset}/{identifier}", as_of=as_of)

    def fmp_gainers(self, limit: int = 10) -> pd.DataFrame:
        """Top gainers."""
        data = self._get("/fmp/gainers", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def fmp_losers(self, limit: int = 10) -> pd.DataFrame:
        """Top losers."""
        data = self._get("/fmp/losers", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def fmp_most_active(self, limit: int = 10) -> pd.DataFrame:
        """Most active stocks by volume."""
        data = self._get("/fmp/most-active", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def fmp_news(self, limit: int = 10) -> pd.DataFrame:
        """Market news feed from FMP."""
        data = self._get("/fmp/news", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def fmp_sector_pe(self, exchange: str = "NASDAQ") -> pd.DataFrame:
        """Sector PE snapshots."""
        data = self._get("/fmp/sector-pe", exchange=exchange)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def fmp_industry_pe(self, exchange: str = "NASDAQ") -> pd.DataFrame:
        """Industry PE snapshots."""
        data = self._get("/fmp/industry-pe", exchange=exchange)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()
# ═══════════════════════════════════════════════════════════════════════
    #  FUNDS  (N-PORT, Form D)
    # ═══════════════════════════════════════════════════════════════════════

    def funds_nport(self, cik: int | None = None, cusip: str | None = None, isin: str | None = None, limit: int = 500) -> pd.DataFrame:
        """SEC Form N-PORT fund holdings."""
        data = self._get("/funds/nport", cik=cik, cusip=cusip, isin=isin, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def funds_nport_by_cik(self, cik: int, limit: int = 2000) -> pd.DataFrame:
        """N-PORT holdings by fund CIK."""
        data = self._get(f"/funds/nport/{cik}", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def funds_form_d(self, cik: int | None = None, state: str | None = None, is_fund: bool | None = None, limit: int = 500) -> pd.DataFrame:
        """SEC Form D exempt offerings."""
        data = self._get("/funds/form-d", cik=cik, state=state, is_fund=is_fund, limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def funds_form_d_by_cik(self, cik: int) -> pd.DataFrame:
        """Form D filings by issuer CIK."""
        data = self._get(f"/funds/form-d/{cik}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    # ═══════════════════════════════════════════════════════════════════════
    #  ADDITIONAL EQUITY / MARKET METHODS
    # ═══════════════════════════════════════════════════════════════════════

    def price_change(self, identifier: str) -> dict:
        """Total return over standard windows (1D to max)."""
        return self._get(f"/equity/price-change/{identifier}")

    def grade_news(self, limit: int = 100) -> pd.DataFrame:
        """Latest analyst upgrade/downgrade news market-wide."""
        data = self._get("/equity/grade-news", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def price_target_news(self, identifier: str) -> pd.DataFrame:
        """Recent analyst price-target changes for a security."""
        data = self._get(f"/equity/price-target-news/{identifier}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def price_target_news_latest(self, limit: int = 100) -> pd.DataFrame:
        """Latest analyst price-target changes market-wide."""
        data = self._get("/equity/price-target-news-latest", limit=limit)
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def market_multiples(self, multiple_type: str = "pe") -> pd.DataFrame:
        """Market-wide valuation multiple time series (pe, pb, ps, pc, liab)."""
        data = self._get(f"/equity/market-multiples/{multiple_type}")
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()

    def relative_move(self, identifier: str) -> dict:
        """Today's price move relative to its exchange/benchmark."""
        return self._get(f"/equity/relative-move/{identifier}")

    def vendor_ratings(self, identifier: str) -> dict:
        """Third-party analyst rating scores including European coverage."""
        return self._get(f"/equity/vendor-ratings/{identifier}")

    def news_feed_xml(self, language: str | None = None, type_: str | None = None) -> str:
        """RSS Feed XML stream of all news."""
        return self._get("/news/feed.xml", language=language, type=type_)

    def partner_alleaktien(self, isins: list[str]) -> pd.DataFrame:
        """Batch fundamental metrics from AlleAktien by ISINs."""
        data = self._get("/partner/alleaktien/fundamentals", isins=",".join(isins))
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()
        return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()