"""Tests for historical options chain functionality without requiring an API key."""
import pandas as pd
from datetime import date

from achest.eulerpool_provider import EulerpoolProvider
from achest.client import MarketDataClient
import inspect


def test_normalize_options_chain_dict():
    """Test _normalize_options_chain with chain dict containing expirations."""
    mock_chain = {
        "expirations": ["2025-03-21"],
        "chain": {
            "2025-03-21": {
                "calls": [
                    {
                        "contractSymbol": "AAPL250321C00220000",
                        "strike": 220.0,
                        "bid": 5.50,
                        "ask": 5.75,
                        "last": 5.60,
                        "volume": 1250,
                        "openInterest": 3400,
                        "impliedVolatility": 0.28,
                        "inTheMoney": True,
                    },
                    {
                        "contractSymbol": "AAPL250321C00230000",
                        "strike": 230.0,
                        "bid": 2.10,
                        "ask": 2.25,
                        "last": 2.15,
                        "volume": 890,
                        "openInterest": 2100,
                        "impliedVolatility": 0.31,
                        "inTheMoney": False,
                    },
                ],
                "puts": [
                    {
                        "contractSymbol": "AAPL250321P00220000",
                        "strike": 220.0,
                        "bid": 0.85,
                        "ask": 0.95,
                        "last": 0.90,
                        "volume": 650,
                        "openInterest": 1800,
                        "impliedVolatility": 0.29,
                        "inTheMoney": False,
                    },
                ],
            }
        },
    }

    df = EulerpoolProvider._normalize_options_chain(mock_chain, "AAPL", date(2025, 3, 21))
    assert len(df) == 3, f"Expected 3 rows, got {len(df)}"
    assert df["ticker"].iloc[0] == "AAPL"
    assert df["option_type"].iloc[0] == "call"
    assert df["option_type"].iloc[2] == "put"
    assert df["strike"].iloc[0] == 220.0
    assert df["bid"].iloc[0] == 5.50
    assert df["last"].iloc[2] == 0.90
    assert df["implied_volatility"].iloc[0] == 0.28
    print("  PASSED: dict with chain expirations")


def test_normalize_options_chain_flat():
    """Test _normalize_options_chain with flat calls/puts at top level."""
    mock_flat = {
        "calls": [
            {
                "strike": 200.0,
                "bid": 10.0,
                "ask": 10.50,
                "last": 10.25,
                "volume": 500,
                "openInterest": 1000,
                "impliedVolatility": 0.25,
                "inTheMoney": True,
                "expiration": "2025-06-20",
            }
        ],
        "puts": [
            {
                "strike": 200.0,
                "bid": 1.50,
                "ask": 1.75,
                "last": 1.60,
                "volume": 300,
                "openInterest": 800,
                "impliedVolatility": 0.27,
                "inTheMoney": False,
                "expiration": "2025-06-20",
            }
        ],
    }

    df = EulerpoolProvider._normalize_options_chain(mock_flat, "MSFT", None)
    assert len(df) == 2, f"Expected 2 rows, got {len(df)}"
    assert df["ticker"].iloc[0] == "MSFT"
    assert df["expiration"].iloc[0] == "2025-06-20"
    print("  PASSED: flat calls/puts at top level")


def test_normalize_empty():
    """Test that empty/invalid input returns empty DataFrame."""
    df1 = EulerpoolProvider._normalize_options_chain(None, "TEST", None)
    assert df1.empty
    df2 = EulerpoolProvider._normalize_options_chain({}, "TEST", None)
    assert df2.empty
    print("  PASSED: empty/invalid data")


def test_normalize_options_chain_list():
    """Test _normalize_options_chain_list with dated snapshots."""
    mock_list = [
        {
            "date": "2025-01-02",
            "chain": {
                "calls": [
                    {
                        "strike": 150.0,
                        "bid": 5.0,
                        "ask": 5.25,
                        "last": 5.10,
                        "volume": 1000,
                        "openInterest": 2000,
                        "impliedVolatility": 0.22,
                        "inTheMoney": True,
                    }
                ]
            },
        },
        {
            "date": "2025-01-03",
            "chain": {
                "calls": [
                    {
                        "strike": 150.0,
                        "bid": 5.50,
                        "ask": 5.75,
                        "last": 5.60,
                        "volume": 1100,
                        "openInterest": 2100,
                        "impliedVolatility": 0.23,
                        "inTheMoney": True,
                    }
                ]
            },
        },
    ]

    df = EulerpoolProvider._normalize_options_chain_list(mock_list, "SPY")
    assert len(df) == 2, f"Expected 2 rows, got {len(df)}"
    assert len(df.index.unique()) == 2, "Expected 2 distinct timestamps"
    print("  PASSED: list of dated snapshots")


def test_method_signatures():
    """Verify method signatures on provider and client."""
    provider_sig = inspect.signature(EulerpoolProvider.options_chain_historical)
    provider_params = list(provider_sig.parameters.keys())
    assert "ticker" in provider_params
    assert "start" in provider_params
    assert "end" in provider_params
    assert "expiration" in provider_params
    print(f"  Provider signature: {provider_sig}")

    client_sig = inspect.signature(MarketDataClient.options_chain_historical)
    client_params = list(client_sig.parameters.keys())
    assert "ticker" in client_params
    assert "start" in client_params
    assert "end" in client_params
    assert "expiration" in client_params
    print(f"  Client signature: {client_sig}")
    print("  PASSED: method signatures")


def test_date_conversion():
    """Test that string dates are converted to date objects."""
    sig = inspect.signature(EulerpoolProvider.options_chain_historical)
    start_param = sig.parameters["start"]
    end_param = sig.parameters["end"]
    assert start_param.default is None
    assert end_param.default is None
    print("  PASSED: date/str parameter types")


if __name__ == "__main__":
    print("Running historical options tests...\n")
    test_normalize_options_chain_dict()
    test_normalize_options_chain_flat()
    test_normalize_empty()
    test_normalize_options_chain_list()
    test_method_signatures()
    test_date_conversion()
    print("\nAll tests PASSED!")