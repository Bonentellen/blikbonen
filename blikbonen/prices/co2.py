from __future__ import annotations

import datetime as dt
import decimal
from typing import override

from beanprice import source

from blikbonen.prices import hicp


class Source(source.Source):
    """
    Beanprice source for the carbon price, according to the high estimate from
    https://ce.nl/method/milieuprijzen/, corrected for inflation using the
    EU harmonized CPI.

    Symbol supported:
        KGCO2

    Example usage:
    ```shell
    bean-price -e "EUR:blikbonen.prices.co2/KGCO2"
    ```
    """

    @staticmethod
    def _convert_price(value: decimal.Decimal) -> decimal.Decimal:
        return (value * decimal.Decimal("0.0019750648")).quantize(decimal.Decimal("0.00001"))

    @override
    def get_latest_price(self, ticker: str) -> source.SourcePrice | None:
        return self.get_historical_price(ticker, dt.datetime.now(tz=dt.timezone.utc))

    @override
    def get_historical_price(self, ticker: str, time: dt.datetime) -> source.SourcePrice | None:
        if ticker != "KGCO2":
            return None
        cpi = hicp.Source().get_historical_price("CPI", time)
        if cpi is None:
            return None
        return cpi._replace(price=self._convert_price(cpi.price))
