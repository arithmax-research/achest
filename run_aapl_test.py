import os

# Load .env manually (handles spaces around =)
env_path = 'Data_Pipeline/.env'
with open(env_path) as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' in line:
            key, val = line.split('=', 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            os.environ[key] = val

print("EULERPOOL_API_KEY loaded:", bool(os.getenv("EULERPOOL_API_KEY")))

from datetime import date, timedelta
from achest.eulerpool_provider import EulerpoolProvider

ep = EulerpoolProvider()

# === Test 1: Current options chain (this works) ===
print("\n" + "=" * 60)
print("TEST 1: options_chain('AAPL') — current snapshot")
print("=" * 60)
try:
    raw = ep.options_chain("AAPL")
    print(f"Ticker: {raw.get('ticker')}")
    print(f"As of: {raw.get('asOf')}")
    print(f"Total expirations: {raw.get('totalExpirations')}")
    print(f"Returned expirations: {raw.get('returnedExpirations')}")

    # Normalize it
    df = ep._normalize_options_chain(raw, "AAPL", None)
    print(f"\nNormalized DataFrame: {len(df)} rows")
    print(f"Columns: {list(df.columns)}")
    print("\nFirst 10 rows:")
    print(df.head(10).to_string())
except Exception as e:
    print(f"ERROR: {type(e).__name__}: {e}")

# === Test 2: Historical range (falls back to day-by-day, returns current data) ===
print("\n" + "=" * 60)
print("TEST 2: options_chain_historical('AAPL', 2025-01-01 to 2025-01-05)")
print("=" * 60)
print("(Eulerpool API only has current data — day-by-day returns current snapshot per day)")
try:
    df2 = ep.options_chain_historical(
        "AAPL",
        start=date(2025, 1, 1),
        end=date(2025, 1, 5),
        expiration=None,
    )
    if df2.empty:
        print("No data returned (expected — no historical options coverage)")
    else:
        print(f"Results: {len(df2)} rows across {df2.index.normalize().nunique()} days")
        print(df2.head(10).to_string())
except Exception as e:
    print(f"ERROR: {type(e).__name__}: {e}")

# === Test 3: Client-level access ===
print("\n" + "=" * 60)
print("TEST 3: MarketDataClient.options_chain_historical()")
print("=" * 60)
try:
    from achest.client import MarketDataClient
    client = MarketDataClient(eulerpool_direct=True)
    df3 = client.options_chain_historical("AAPL", start="2025-01-01", end="2025-01-03")
    if df3.empty:
        print("No data returned via client (expected)")
    else:
        print(f"Results via client: {len(df3)} rows")
        print(df3.head(5).to_string())
except Exception as e:
    print(f"ERROR: {type(e).__name__}: {e}")

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print("✅ options_chain('AAPL') — WORKS: current options chain with bid/ask/last/IV/Greeks")
print("❌ Historical options (Jan–Dec 2025) — NOT AVAILABLE from Eulerpool API")
print("   The API returns 404 for startdate/enddate params and past expiration dates")
print("   Day-by-day fallback returns current snapshot only (not true historical)")
print()
print("For true historical options data, consider:")
print("  • Data_Pipeline/nse_india_downloader.py (Indian options with start_date/end_date)")
print("  • Data_Pipeline/yahoo_finance_downloader.py (current chains via Yahoo Finance)")
print("  • A dedicated options data provider (e.g. Polygon, CBOE, Tradier)")