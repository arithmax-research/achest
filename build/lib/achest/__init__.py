"""Arithmax Chest market-data client and provider service."""
from __future__ import annotations

from .client import MarketDataClient
from .eulerpool_provider import EulerpoolProvider
from .service import to_lean_zip, to_q_table

__version__ = "0.5.0"


def beautify(data):
    """Convert any Eulerpool response into the most readable form.

    - Already a DataFrame -> return as-is
    - List of flat dicts -> multi-row DataFrame
    - Single flat dict -> 1-row DataFrame
    - Nested dict -> flattened via ``pd.json_normalize``
    - Everything else -> unchanged
    """
    import pandas as pd
    if isinstance(data, pd.DataFrame):
        return data
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return pd.DataFrame(data)
    if isinstance(data, dict):
        if any(isinstance(v, (dict, list)) for v in data.values()):
            try:
                return pd.json_normalize(data, sep="_")
            except Exception:
                return pd.DataFrame([data])
        return pd.DataFrame([data])
    return data


__all__ = [
    "MarketDataClient",
    "EulerpoolProvider",
    "to_lean_zip",
    "to_q_table",
    "beautify",
    "__version__",
]
