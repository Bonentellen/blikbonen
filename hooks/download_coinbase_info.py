"""
Hook that downloads coinbase currency info.
"""

from __future__ import annotations

import json
from typing import Any

import requests
from hatchling.builders.hooks.plugin.interface import BuildHookInterface

# ---- 1. Constants  ------------------------------------------------
# URL to fetch
_CRYPTO_CURRENCIES_URL = "https://api.coinbase.com/v2/currencies/crypto"
_CURRENCIES_URL = "https://api.coinbase.com/v2/currencies"
_TARGET_LOCATION = "blikbonen/importers/coinbase/currency_data.py"


class CustomBuildHook(BuildHookInterface):
    PLUGIN_NAME = "Import Coinbase Info"

    def initialize(self, _version: str, _build_data: dict[str, Any]) -> None:
        combined_data = []

        for url, key in [[_CRYPTO_CURRENCIES_URL, "code"], [_CURRENCIES_URL, "id"]]:
            params = {}
            resp = requests.get(url=url, params=params, timeout=30)

            data = resp.json()["data"]

            combined_data.extend([[row[key], row[key], row["name"]] for row in data])

        with open(_TARGET_LOCATION, "w") as f:
            f.write("CURRENCIES = ")
            json.dump(combined_data, f, indent=2)
