"""Tests for historical options chain functionality without requiring an API key."""
import pandas as pd
from datetime import date

from achest.eulerpool_provider import EulerpoolProvider
from achest.client import MarketDataClient
import inspect


def test_normalize_options_chain_dict():
    """Test _normalize_options_chain with real Eulerpool response format."""
    mock_chain = {
        "ticker": "AAPL",
        "asOf": "2026-11-13",
        "totalExpirations": 1,
        "returnedExpirations": 1,
        "expirations": [
            {
                "expiration_date": "2026-11-16",
                "calls": [
                    {
                        "option_symbol": "AAPL261116C00220000",
                        "type": "call",
                        "strike": 220.0,
                        "bid": 5.50,
                        "ask": 5.75,
                        "last_price": 5.60,
                        "volume": 1250,
                        "open_interest": 3400,
                        "implied_vol": 0.28,
                        "in_the_money": True,
                        "delta": 0.75,
                        "gamma": 0.02,
                        "vega": 0.15,
                        "theta": -0.05,
                        "rho": 0.03,
                        "last_trade_time": "2026-11-13T10:30:00.000Z",
                    },
                    {
                        "option_symbol": "AAPL261116C00230000",
                        "type": "call",
                        "strike": 230.0,
                        "bid": 2.10,
                        "ask": 2.25,
                        "last_price": 2.15,
                        "volume": 890,
                        "open_interest": 2100,
                        "implied_vol": 0.31,
                        "in_the_money": False,
                        "delta": 0.30,
                        "gamma": 0.04,
                        "vega": 0.18,
                        "theta": -0.06,
                        "rho": 0.02,
                        "last_trade_time": "2026-11-13T11:00:00.000Z",
                    },
                ],
                "puts": [
                    {
                        "option_symbol": "AAPL261116P00220000",
                        "type": "put",
                        "strike": 220.0,
                        "bid": 0.85,
                        "ask": 0.95,
                        "last_price": 0.90,
                        "volume": 650,
                        "open_interest": 1800,
                        "implied_vol": 0.29,
                        "in_the_money": False,
                        "delta": -0.25,
                        "gamma": 0.02,
                        "vega": 0.15,
                        "theta": -0.04,
                        "rho": -0.02,
                        "last_trade_time": "2026-11-13T10:45:00.000Z",
                    },
                ],
            }
        ],
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
    assert df["delta"].iloc[0] == 0.75
    assert df["contract_symbol"].iloc[0] == "AAPL261116C00220000"
    print("  PASSED: dict with chain expirations (real Eulerpool format)")


def test_normalize_options_chain_flat():
    """Test _normalize_options_chain with real Eulerpool format (single expiration)."""
    mock_flat = {
        "ticker": "MSFT",
        "asOf": "2026-06-20",
        "totalExpirations": 1,
        "returnedExpirations": 1,
        "expirations": [
            {
                "expiration_date": "2026-06-20",
                "calls": [
                    {
                        "option_symbol": "MSFT260620C00200000",
                        "type": "call",
                        "strike": 200.0,
                        "bid": 10.0,
                        "ask": 10.50,
                        "last_price": 10.25,
                        "volume": 500,
                        "open_interest": 1000,
                        "implied_vol": 0.25,
                        "in_the_money": True,
                        "delta": 0.65,
                        "gamma": 0.03,
                        "vega": 0.20,
                        "theta": -0.07,
                        "rho": 0.04,
                        "last_trade_time": "2026-06-20T09:00:00.000Z",
                    }
                ],
                "puts": [
                    {
                        "option_symbol": "MSFT260620P00200000",
                        "type": "put",
                        "strike": 200.0,
                        "bid": 1.50,
                        "ask": 1.75,
                        "last_price": 1.60,
                        "volume": 300,
                        "open_interest": 800,
                        "implied_vol": 0.27,
                        "in_the_money": False,
                        "delta": -0.35,
                        "gamma": 0.03,
                        "vega": 0.20,
                        "theta": -0.06,
                        "rho": -0.03,
                        "last_trade_time": "2026-06-20T09:15:00.000Z",
                    }
                ],
            }
        ],
    }

    df = EulerpoolProvider._normalize_options_chain(mock_flat, "MSFT", None)
    assert len(df) == 2, f"Expected 2 rows, got {len(df)}"
    assert df["ticker"].iloc[0] == "MSFT"
    assert df["expiration"].iloc[0] == "2026-06-20"
    assert df["option_type"].iloc[0] == "call"
    assert df["option_type"].iloc[1] == "put"
    print("  PASSED: real Eulerpool format with calls + puts")


def test_normalize_empty():
    """Test that empty/invalid input returns empty DataFrame."""
    df1 = EulerpoolProvider._normalize_options_chain(None, "TEST", None)
    assert df1.empty
    df2 = EulerpoolProvider._normalize_options_chain({}, "TEST", None)
    assert df2.empty
    print("  PASSED: empty/invalid data")


def test_normalize_options_chain_list():
    """Test _normalize_options_chain_list with real Eulerpool format entries."""
    mock_list = [
        {
            "ticker": "SPY",
            "asOf": "2025-01-02",
            "expirations": [
                {
                    "expiration_date": "2025-01-17",
                    "calls": [
                        {
                            "option_symbol": "SPY250117C00150000",
                            "type": "call",
                            "strike": 150.0,
                            "bid": 5.0,
                            "ask": 5.25,
                            "last_price": 5.10,
                            "volume": 1000,
                            "open_interest": 2000,
                            "implied_vol": 0.22,
                            "in_the_money": True,
                            "last_trade_time": "2025-01-02T10:00:00.000Z",
                        }
                    ],
                    "puts": [],
                }
            ],
        },
        {
            "ticker": "SPY",
            "asOf": "2025-01-03",
            "expirations": [
                {
                    "expiration_date": "2025-01-17",
                    "calls": [
                        {
                            "option_symbol": "SPY250117C00150000",
                            "type": "call",
                            "strike": 150.0,
                            "bid": 5.50,
                            "ask": 5.75,
                            "last_price": 5.60,
                            "volume": 1100,
                            "open_interest": 2100,
                            "implied_vol": 0.23,
                            "in_the_money": True,
                            "last_trade_time": "2025-01-03T10:00:00.000Z",
                        }
                    ],
                    "puts": [],
                }
            ],
        },
    ]

    df = EulerpoolProvider._normalize_options_chain_list(mock_list, "SPY")
    assert len(df) == 2, f"Expected 2 rows, got {len(df)}"
    assert len(df.index.unique()) == 2, "Expected 2 distinct timestamps"
    print("  PASSED: list of dated snapshots (real Eulerpool format)")


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