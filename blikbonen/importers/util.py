"""Generic utility functions that may be useful for multiple importers."""

from __future__ import annotations

from typing import TypedDict

import petl as etl
from beancount.core import data


class FundInfo(TypedDict):
    fund_data: list[tuple[str, str, str]]
    money_market: list[tuple[str, str, str]]


def switch_payee_narration(new_entries: data.Entries) -> data.Entries:
    updated_entries: data.Entries = []
    for entry in new_entries:
        if isinstance(entry, data.Transaction):
            payee = entry.payee
            narration = entry.narration
            updated_entries.append(entry._replace(payee=narration, narration=payee))
        else:
            updated_entries.append(entry)
    return updated_entries


_REVERSE_ROW_ORDER_KEY: str = "rn_to_reverse"


def reverse_row_order(rdr: etl.Table) -> etl.Table:
    """
    Takes a table, and reverses the row ordering.
    """
    rdr = etl.addrownumbers(rdr, field=_REVERSE_ROW_ORDER_KEY)
    rdr = etl.sort(rdr, _REVERSE_ROW_ORDER_KEY, reverse=True)
    return etl.cutout(rdr, _REVERSE_ROW_ORDER_KEY)
