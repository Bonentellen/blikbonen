# my_sources/cbs_hicp.py
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

import datetime as dt
from decimal import Decimal
from typing import override

import cbsodata
import pytz
from beanprice import source

_TABLE_ID = "86144NED"
_MONTHS: Sequence[str] = [
    "januari",
    "februari",
    "maart",
    "april",
    "mei",
    "juni",
    "juli",
    "augustus",
    "september",
    "oktober",
    "november",
    "december",
]


class Source(source.Source):
    """
    Beanprice source for CBS dataset 86144NED:
    HICP constante belastingen (category T001112)

    Symbol supported:
        CPI

    Example usage:
    ```shell
    bean-price -e "EUR:blikbonen.prices.hicp/CPI"
    ```
    """

    def __init__(self):
        super().__init__()

    def _periods_definitive(self) -> dict[str, bool]:
        is_period_definite = {}
        periods = cbsodata.get_meta(_TABLE_ID, "Perioden")
        for period in periods:
            is_period_definite[period["Title"]] = period["Status"] == "Definitief"
        return is_period_definite

    def _fetch_all(self) -> Generator[source.SourcePrice]:
        periods_definitive = self._periods_definitive()
        data = cbsodata.get_data(_TABLE_ID)
        for row in data:
            period = row.get("Perioden")
            value = row.get("HICPConstanteBelastingen_2")
            if not periods_definitive[period] or value is None:
                continue
            date = self._period_to_date(period)
            if date is not None:
                yield source.SourcePrice(
                    price=Decimal(str(value)),
                    # For the time, use 9:00 as a reasonable default.
                    time=dt.datetime.combine(date, dt.time(9), tzinfo=pytz.timezone("Europe/Amsterdam")),
                    quote_currency="EUR",
                )

    @staticmethod
    def _period_to_date(period: str) -> dt.date | None:
        year = int(period[:4])
        # Format like 2024MM01
        for month in range(len(_MONTHS)):
            if period.endswith(_MONTHS[month]):
                return dt.date(year, month + 1, 1)
        return None

    # ------------------------------------------------------------------
    # beanprice API
    # ------------------------------------------------------------------
    @override
    def get_latest_price(self, ticker: str) -> source.SourcePrice | None:
        return self.get_historical_price(ticker, dt.datetime.now(tz=dt.timezone.utc))

    @override
    def get_historical_price(self, ticker: str, time: dt.datetime) -> source.SourcePrice | None:
        if ticker != "CPI":
            return None

        latest_value = None
        latest_date = None

        for price in self._fetch_all():
            # We have created this price, so we know that the time is not None.
            assert price.time is not None  # noqa: S101
            if latest_date is None or (price.time > latest_date and price.time <= time):
                latest_date = price.time
                latest_value = price
        return latest_value
