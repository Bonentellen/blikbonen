"""
An importer used for Coinbase activity `.csv` files.

# Example
The following example configures this importer:

```python
ACCOUNT = "Assets:Coinbase"
return coinbase.Importer(
    {
        "account_number": "897414yb-147214yu-87243b5r-3248yb",
        "main_account": f"{ACCOUNT}:{{ticker}}",
        "cash_account": f"{ACCOUNT}:{{currency}}",
        "transfer": "Assets:Zero-Sum-Accounts:Coinbase",
        "learning_reward": "Income:CoinbaseEarn",
        "staking": "Income:CoinbaseRewards",
        "cg": "Income:CapitalGains",
        "fees": "Expenses:TradingFees",
    }
)
```
"""

from __future__ import annotations

import decimal
import re
from typing import Any, TypedDict, cast, override

import petl as etl
from beancount.core import data
from beancount.core.position import CostSpec
from beancount_reds_importers.libreader import csvreader
from beancount_reds_importers.libtransactionbuilder import common, investments

from blikbonen.importers.coinbase.currency_data import CURRENCIES
from blikbonen.importers.util import switch_payee_narration


def _advanced_regex(a: str):
    return re.match(
        r"^(Bought|Sold) (\d+\.?\d*) ([A-Z0-9]+) for (\d+\.?\d*) ([A-Z0-9]+) on ([A-Z0-9]+)-([A-Z0-9]+) at (\d+\.?\d*) ([A-Z0-9]+\/[A-Z0-9]+)$",
        a,
    )


def _advanced_buy_sell_add_amount(a) -> str | None:
    m = _advanced_regex(a["Notes"])
    if m is not None:
        if m.group(1) == "Bought":
            return m.group(2)
        return m.group(4)
    return None


def _advanced_buy_sell_subtract_amount(a) -> str | None:
    m = _advanced_regex(a["Notes"])
    if m is not None:
        if m.group(1) == "Bought":
            return m.group(4)
        return m.group(2)
    return None


def _advanced_buy_sell_add_currency(a) -> str | None:
    m = _advanced_regex(a["Notes"])
    if m is not None:
        if m.group(1) == "Bought":
            return m.group(3)
        return m.group(5)
    return None


def _advanced_buy_sell_subtract_currency(a) -> str | None:
    m = _advanced_regex(a["Notes"])
    if m is not None:
        if m.group(1) == "Bought":
            return m.group(5)
        return m.group(3)
    return None


class Config(TypedDict):
    """
    The configuration used to initialize the importer.
    """

    account_number: str
    """
    This is the random-looking string appearing in downloaded files before the first underscore.
    """

    main_account: str
    """
    The beancount account used for all assets, except for cash. You can use `{ticker}` to differentiate between assets.
    """

    cash_account: str
    """
    The beancount account used for cash. Use `{currency}` to include the currency.
    """

    transfer: str
    """
    The account used for transfers, for example a `zerosum` account.
    """

    learning_reward: str
    """
    The beancount account used for learning rewards income.
    """

    staking: str
    """
    The beancount account used for staking income.
    """

    cg: str
    """
    The beancount account used for capital gains income.
    """

    fees: str
    """
    The beancount account for trading fees.
    """


class Importer(csvreader.Importer, investments.Importer):
    IMPORTER_NAME = "Coinbase Importer"

    @override
    def __init__(self, config: Config):
        cfg = cast("dict[str, Any]", config)
        cfg["capgainsd_lt"] = "Income:UNUSED"
        cfg["capgainsd_st"] = "Income:UNUSED"
        cfg["dividends"] = "Income:UNUSED"
        cfg["interest"] = "Income:UNUSED"

        # Load fund data from json. This file is downloaded as part of the build process.
        currencies = CURRENCIES
        currencies.append(["ETH2", "ETH2", "Ethereum"])
        cfg["fund_info"] = {"fund_data": CURRENCIES, "money_market": []}

        super().__init__(cfg)

    def _advanced_buy_sell_market(self, a) -> str | None:
        m = _advanced_regex(a["Notes"])
        if m is not None:
            cur1_symb = m.group(6)
            cur2_symb = m.group(7)
            _symb, cur1_name = self.get_ticker_info(cur1_symb)
            _symb, cur2_name = self.get_ticker_info(cur2_symb)
            return f"[{cur1_symb}/{cur2_symb}] {cur1_name} / {cur2_name}"
        return None

    @override
    def prepare_table(self, rdr: etl.Table):
        rdr = etl.addfield(rdr, "tradeDate", lambda r: r["Timestamp"])

        # Parse the advanced trade options
        rdr = etl.addfield(rdr, "advanced_add_amount", _advanced_buy_sell_add_amount)
        rdr = etl.addfield(rdr, "advanced_add_currency", _advanced_buy_sell_add_currency)
        rdr = etl.addfield(rdr, "advanced_subtract_amount", _advanced_buy_sell_subtract_amount)
        rdr = etl.addfield(rdr, "advanced_subtract_currency", _advanced_buy_sell_subtract_currency)
        rdr = etl.addfield(rdr, "advanced_market", self._advanced_buy_sell_market)

        # Some advanced trade options can be interpreted as normal buy/sell
        rdr = etl.convert(
            rdr,
            "Transaction Type",
            lambda _r: "sellstock",
            where=lambda r: r["advanced_add_currency"] == self.currency,
        )
        rdr = etl.convert(
            rdr,
            "Transaction Type",
            lambda _r: "buystock",
            where=lambda r: r["advanced_subtract_currency"] == self.currency,
        )
        # The remaining entries have two different currencies, neither `self.currency`

        # Fix sell postings: if `units` is negative, `total` will be used for
        # the other posting and should be positive
        return etl.convert(
            rdr,
            "Total (inclusive of fees and/or spread)",
            lambda v: v[1:],
            where=lambda r: (
                r["Total (inclusive of fees and/or spread)"].startswith("-")
                and r["Quantity Transacted"].startswith("-")
            ),
        )

    @override
    def custom_init(self):
        self.currency = "EUR"
        self.max_rounding_error = 0.0000000000000001
        self.filename_pattern_def = f"{self.config['account_number']}_.*\\.csv"
        self.column_labels_line = "ID,Timestamp,Transaction Type,Asset,Quantity Transacted,Price Currency,Price at Transaction,Subtotal,Total (inclusive of fees and/or spread),Fees and/or Spread,Notes"
        self.header_identifier = ""
        self.date_format = "%Y-%m-%d %H:%M:%S UTC"
        self.header_map = {
            "Timestamp": "date",
            "Transaction Type": "type",
            "Subtotal": "amount",
            "Total (inclusive of fees and/or spread)": "total",
            "Quantity Transacted": "units",
            "Asset": "security",
            "Price Currency": "currency",
            "Notes": "memo",
            "Fees and/or Spread": "fees",
            "Price at Transaction": "unit_price",
        }
        self.transaction_type_map = {
            "Staking Income": "staking",
            "Send": "transfer",
            "Receive": "transfer",
            "Deposit": "cash",
            "Withdrawal": "cash",
            "Advanced Trade Sell": "advanced",
            "Advanced Trade Buy": "advanced",
            "Learning Reward": "learning_reward",
        }
        self.skip_transaction_types = []
        self.get_ticker_info = self.get_ticker_info_from_id

    def _generate_advanced_trade_entry(self, ot: etl.Record, file: str, counter):
        metadata = data.new_metadata(file, next(counter))
        metadata.update(self.build_metadata(file, metatype="transaction_transfer", data={"transaction": ot}))
        ticker_add, _ticker_add_long_name = self.get_ticker_info(ot.advanced_add_currency)
        ticker_sub, _ticker_sub_long_name = self.get_ticker_info(ot.advanced_subtract_currency)
        units_add = decimal.Decimal(ot.advanced_add_amount)
        units_sub = -abs(decimal.Decimal(ot.advanced_subtract_amount))
        add_account = self.get_acct("main_account", ot, ticker_add)
        sub_account = self.get_acct("main_account", ot, ticker_sub)
        narration = ot.advanced_market
        date = ot.date.date()
        payee = self.get_payee(ot)

        entry = data.Transaction(
            metadata,
            date,
            self.FLAG,
            payee,
            narration,
            self.get_tags(ot),
            self.get_links(ot),
            [],
        )
        # Since the price is given in CURR1/CURR2, we need to compute it manually
        unit_sub_price = abs(ot.amount / units_sub)
        unit_add_price = abs(ot.total / units_add)
        common.create_simple_posting_with_cost_or_price(
            entry,
            sub_account,
            units_sub,
            ticker_sub,
            price_number=unit_sub_price,
            price_currency=self.currency,
            costspec=CostSpec(None, None, None, None, None, None),
            price_cost_both_zero_handler=self.price_cost_both_zero_handler,
            ot=ot,
        )
        # Profit/Loss
        cg_acct = self.get_acct("cg", ot, ticker_sub)
        data.create_simple_posting(entry, cg_acct, None, None)
        common.create_simple_posting_with_cost(
            entry,
            add_account,
            units_add,
            ticker_add,
            unit_add_price,
            self.currency,
            self.price_cost_both_zero_handler,
            ot=ot,
        )
        return entry

    def _generate_staking_entry(self, ot, file, counter):
        metadata = data.new_metadata(file, next(counter))
        metadata.update(self.build_metadata(file, metatype="transaction_transfer", data={"transaction": ot}))
        ticker, _ticker_long_name = self.get_ticker_info(ot.security)
        units = ot.units
        main_acct = self.get_acct("main_account", ot, ticker)
        narration = self.security_narration(ot)
        date = ot.date.date()

        payee = self.get_payee(ot)
        if ot.type == "staking":
            payee = "Staking Income"

        entry = data.Transaction(
            metadata,
            date,
            self.FLAG,
            payee,
            narration,
            self.get_tags(ot),
            self.get_links(ot),
            [],
        )

        if ot.type == "staking":
            target_account = self.config["staking"]
        elif ot.type == "learning_reward":
            target_account = self.config["learning_reward"]
        else:
            msg = f"transaction type {ot.type} unknown"
            raise ValueError(msg)

        common.create_simple_posting_with_cost(entry, main_acct, units, ticker, ot.unit_price, ot.currency, ot=ot)
        common.create_simple_posting_with_cost(
            entry, target_account, -1 * units, ticker, ot.unit_price, ot.currency, ot=ot
        )

        return entry

    @override
    def extract_transactions(self, file: str, counter):
        new_entries = []
        self.read_file(file)
        for ot in self.get_transactions():
            if self.skip_transaction(ot):
                continue
            if ot.type in [
                "buymf",
                "sellmf",
                "buystock",
                "buydebt",
                "sellstock",
                "buyother",
                "sellother",
                "reinvest",
            ]:
                entry = self.generate_trade_entry(ot, file, counter)
            elif ot.type in [
                "other",
                "credit",
                "debit",
                "transfer",
                "xfer",
                "dep",
                "income",
                "fee",
                "dividends",
                "capgainsd_st",
                "capgainsd_lt",
                "cash",
                "payment",
                "check",
                "invexpense",
            ]:
                entry = self.generate_transfer_entry(ot, file, counter)
            elif ot.type in ["staking", "learning_reward"]:
                entry = self._generate_staking_entry(ot, file, counter)
            elif ot.type == "advanced":
                entry = self._generate_advanced_trade_entry(ot, file, counter)
            else:
                msg = f"unknown entry type {ot.type}"
                raise ValueError(msg)
            self.add_fee_postings(entry, ot)
            self.add_custom_postings(entry, ot)
            new_entries.append(entry)
        return new_entries

    @override
    def custom_entry_mods(self, new_entries: list[data.Directive]):
        return switch_payee_narration(new_entries)
