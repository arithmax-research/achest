<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="Treasure Chest DataGraph Logo.png">
    <img src="customisations/logo.png" alt="Arithmax Chest" width="120" height="120">
  </picture>
  <h1 align="center">achest</h1>
  <p align="center">
    <em>Normalized market data. One API. Every provider.</em>
    <br>
    Equities &middot; Crypto &middot; Futures &middot; Macro &middot; Fundamentals &middot; Sentiment &middot; Options &middot; Alternatives
  </p>
  <p align="center">
    <a href="https://pypi.org/project/arithmaxchest/"><img src="https://img.shields.io/pypi/v/arithmaxchest?color=4B8BBE&label=PyPI" alt="PyPI"></a>
    <a href="https://pypi.org/project/arithmaxchest/"><img src="https://img.shields.io/pypi/pyversions/arithmaxchest?color=4B8BBE" alt="Python Versions"></a>
    <a href="https://achestv2.misango.me/health"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fachestv2.misango.me%2Fhealth&query=%24.status&label=system&success_message=ok&color=green&failed_color=red" alt="API Status"></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
  </p>
</p>

---

**Arithmax Chest** provides one Python client and FastAPI service for normalized market data and extended financial research data with atleast 200+ variations"

## Install

```bash
pip install arithmaxchest
```


Python 3.10 or newer is supported.

## Market Data

```python
from achest import MarketDataClient

with MarketDataClient() as client:
  data = client.get(
    ["AAPL", "BTCUSDT", "ES.FUT", "^GSPC"],
    "2025-01-01",
    "2025-01-31",
    resolution="daily",
    provider="auto",
  )
  print(data.head())
```

`symbols` may be a string or an iterable of symbols. `start` and `end` accept ISO date strings, `datetime.date`, or `datetime.datetime` values. The client automatically splits long high-resolution requests into parallel chunks and merges the results in timestamp order.

### Supported symbols

| Asset class | Examples | Classification |
|-------------|----------|----------------|
| Equities and ETFs | `AAPL`, `MSFT`, `SPY` | Alphabetic symbols up to five characters |
| Crypto | `BTCUSDT`, `ETHUSDT`, `BTC-USD` | Ends in `USDT`, `USDC`, or `-USD` |
| Futures | `ES.FUT`, `NQ.FUT`, `CL.FUT` | `.FUT`, continuous-contract notation, or supported root |
| Indices | `^GSPC`, `^VIX` | Starts with `^` |
| Economic series | `GDP`, `UNRATE`, `CPIAUCSL`, `FEDFUNDS`, `DGS10` | Recognized economic symbols |

Asset classes can be mixed in one request. Use `client.route(symbol, resolution, provider)` to inspect the selected route without fetching data.

### Resolutions and providers

Supported resolutions depend on the selected provider:

| Resolution | Typical use |
|------------|-------------|
| `tick` | Tick or trade data |
| `second` | One-second bars |
| `minute` | One-minute bars |
| `hour` | One-hour bars |
| `daily` | Daily bars, the default |
| `weekly`, `monthly` | Provider-supported low-frequency data |
| `quarterly`, `annual` | Economic and fundamental series where supported |

With `provider="auto"`, routing considers asset class, resolution, and configured credentials. Providers implemented by the service are `yahoo`, `binance`, `massive`, `databento`, `alpaca`, `tiingo`, `alpha_vantage`, `fred`, `quandl`, and `eulerpool`. A manually selected provider is validated against its capabilities before the request is sent.

```python
with MarketDataClient() as client:
  minute_data = client.get("BTCUSDT", "2025-01-15", "2025-01-16", resolution="minute")
  futures = client.get(["ES.FUT", "NQ.FUT"], "2025-01-01", "2025-01-31", provider="databento")
```

### Normalized response

Market-data responses are pandas DataFrames with the columns `timestamp`, `symbol`, `provider`, `open`, `high`, `low`, `close`, and `volume`. Timestamps are normalized to UTC. Economic series are represented in the same shape, with the observation value copied into the OHLC fields and volume set to zero.

## Extended Financial Data

`MarketDataClient` also exposes Eulerpool-backed data. By default, these methods try the hosted Chest endpoint and fall back to direct Eulerpool access when the server is unavailable. Set `eulerpool_direct=True` to always use Eulerpool directly, or `False` to require the Chest server.

```python
with MarketDataClient() as client:
  overview = client.fundamentals("AAPL", "overview")
  income = client.fundamentals("AAPL", "income")
  estimates = client.analyst("AAPL", "estimates")
  holders = client.ownership("AAPL", "institutional")
  sentiment = client.sentiment("AAPL", "news")
  dividends = client.dividends_data("AAPL", "history")
  short_interest = client.short_data("AAPL", "interest")
```

The grouped methods cover:

| Method | Available data |
|--------|----------------|
| `fundamentals()` | Profiles, overview, income, balance sheet, cash flow, metrics, ESG, AAQS, fair value, growth, margins, and key figures |
| `analyst()` | Estimates, price targets, upgrades, and recommendations |
| `ownership()` | Institutional, fund, insider, and ETF exposure |
| `dividends_data()` | Dividend history and quality |
| `short_data()` | Short volume and short interest |
| `sentiment()` | News, social, insider sentiment, and SWOT |
| `macro()` | Country risk, calendars, FRED, credit spreads, and latest observations |
| `crypto()` | Market overview, analysis, fear and greed, funding, open interest, DeFi, and on-chain data |
| `options()` | Chains, Greeks, implied-volatility surfaces, unusual activity, and VIX term structure |
| `alternative()` | Fear and greed, superinvestors, congressional trading, patents, government contracts, and Google Trends |
| `etf()` | ETF profiles, holdings, and flows |
| `market()` | Quotes, movers, status, breadth, and market indicators |
| `news()` | Company and market news, transcripts, and transcript search |

Additional helpers include `index_constituents()`, `yield_curve()`, `forex_rates()`, and `logo()`. These methods return dictionaries, lists, pandas DataFrames, or image bytes according to the endpoint.

## Saving and Q Tables

Use `download()` for files. Supported formats are `csv`, `parquet`, `json`, and `lean`; the default is `lean`. It returns a `pathlib.Path`.

```python
with MarketDataClient() as client:
  client.download(
    ["SPY", "QQQ"],
    "2025-01-01",
    "2025-01-31",
    format="parquet",
    output="downloads/etfs.parquet",
  )
  client.download(
    ["AAPL", "MSFT"],
    "2025-01-01",
    "2025-01-31",
    format="lean",
    output="Data/equity/usa/daily",
  )
```

When `output` is omitted, Lean files are placed under `Data/{asset}/{resolution}/`; non-Lean files use `Custom_Downloads/{symbol}_{resolution}.{extension}`. Lean output contains one headerless zip archive per symbol (e.g. `spy.zip` → `spy_daily_trade.csv`) and is compatible with [QuantConnect Lean](https://github.com/QuantConnect/Lean). Lean data files must not contain a `Time,Open,High,Low,Close,Volume` header row — the first line is always a data row, otherwise Lean rejects every bar.

For compact text suitable for an LLM or research prompt, use `q_table()`:

```python
with MarketDataClient() as client:
  table = client.q_table(["AAPL"], "2025-01-01", "2025-01-10", include_metadata=True)
```

## kdb+/q Native Client

Fetch market data directly into a q table — no Python intermediary needed.

```q
/ 1. Load the client
\l achest-kdb-q/achest.q

/ 2. Fetch OHLCV into a q table
tbl: .achest.fetch[`BTCUSDT; 2026.09.01; 2026.09.23; `minute; ()!()]

/ 3. Widen console and display
\c 200 1000
tbl

/ 4. Schema and q-sql
meta tbl
select avg close by time from tbl
select time, close, mavg[5; close] from tbl
```

```q
/ Quick one-liner for a loaded session
achestFetch[`BTCUSDT; 2026.09.01; 2026.09.23; `minute]

/ With explicit provider
.achest.fetch[`BTCUSDT; 2026.09.01; 2026.09.23; `minute; (`provider)!`massive]

/ List providers and route symbols
.achest.providers[]
.achest.route[`AAPL; `daily; `auto]
```

### Using with PyKX (embedded Python in q)

If you already use PyKX, the Python `MarketDataClient` works inside q too:

```q
/ Python code inside q via PyKX
.pykx.pyexec"
import pandas as pd
from achest import MarketDataClient

client = MarketDataClient('https://achestv2.misango.me')
raw = client.get(['BTCUSDT'], '2026-09-01', '2026-09-23', 'minute')
client.close()

df = pd.DataFrame(raw)
df['time'] = pd.to_datetime(df['timestamp'])
for col in ['open', 'high', 'low', 'close', 'volume']:
    df[col] = df[col].astype(float)
df = df.drop(columns=['timestamp'])
";

/ Convert to q table
tbl: .pykx.toq .pykx.get `df
\c 200 1000
tbl
```

### Pure q vs PyKX — comparison

| Aspect | Pure q (`achest.q`) | PyKX approach |
|--------|---------------------|---------------|
| Dependencies | `curl` only | Python, pykx, pandas |
| Latency | One HTTP call | Spawns Python, converts types |
| Result type | Native q table | Native q table |
| Auth | `DATA_API_TOKEN` env var | Python `MarketDataClient(token=)` |

### Available functions

```q
/ Fetch market data (5 args — use ()!() for empty options)
.achest.fetch[`BTCUSDT; 2026.09.01; 2026.09.23; `minute; ()!()]

/ 4-arg shorthand (no options)
.achest.fetch4[`BTCUSDT; 2026.09.01; 2026.09.23; `minute]

/ Root-level alias
achestFetch[`BTCUSDT; 2026.09.01; 2026.09.23; `minute]

/ With provider override
.achest.fetch[`BTCUSDT; 2026.09.01; 2026.09.23; `minute; (`provider)!`massive]

/ With custom API token
.achest.fetch[`BTCUSDT; 2026.09.01; 2026.09.23; `minute; (`token)!("your-token";)]

/ List providers and route symbols
.achest.providers[]
.achest.route[`AAPL; `daily; `auto]
```

The q client requires `curl` and works with any q version. No token is needed when the server has auth disabled.

## Authentication and Configuration

The hosted Chest API is `https://achestv2.misango.me`. Pass a server token with `token=` when the deployment requires one:

```python
client = MarketDataClient(
  base_url="https://your-server.example.com",
  token="your-data-api-token",
)
```

Provider credentials belong on the server, not in application code. Common variables are `DATA_API_TOKEN`, `DATA_BENTO_API_KEY`, `MASSIVE_API_KEY`, `ALPACA_API_KEY`, `ALPACA_SECRET_KEY`, `BINANCE_API_KEY`, `TIINGO_API_KEY`, `ALPHA_VANTAGE_API_KEY`, `FRED_API_KEY`, `QUANDL_API_KEY`, and `EULERPOOL_API_KEY` (or `EULER_TOKEN`).

## Data Pipeline

`Data_Pipeline/` contains provider-specific downloaders, validation utilities, configuration, and the interactive pipeline entry point. It supports bulk historical collection for equities, crypto, futures, options, forex, macro data, and alternative sources. See [documentation.md](documentation.md) for provider routing, deployment, environment setup, and API details.

## Self-Hosting

```bash
pip install arithmaxchest[all]
uvicorn achest.server:app --host 0.0.0.0 --port 8000
```

The data endpoint keeps a bounded in-process cache for repeated IPC or HTTP
requests. Configure it with `DATA_CACHE_TTL_SECONDS` (default `60`),
`HOT_CACHE_TTL_SECONDS` (default `30`), and `DATA_CACHE_MAX_ENTRIES` (default
`256`). The configured hot universe covers selected US, Hong Kong, Korean, UK,
commodity, leveraged ETF, and crypto symbols. Futures and options remain
on-demand and are not prefetched. Each server worker has its own cache.

The FastAPI service exposes `/health`, `/v1/providers`, `/v1/route`, `/v1/data`, and the extended `/v1/eulerpool/...` endpoints. Docker and EC2 deployment files are included in the repository.

## Why Arithmax Chest?

- **One interface** for routed OHLCV and extended financial research data
- **Normalized DataFrames** across providers and asset classes
- **Automatic fallback and chunking** for resilient historical retrieval
- **Research-ready exports** including Parquet, JSON, CSV, and Lean archives
- **Usable as a library, hosted API, self-hosted service, or ingestion pipeline**

## License

MIT

---

*Built by [Arithmax Research](https://achestv2.misango.me). Market data for the next generation of analysts and algorithms.*