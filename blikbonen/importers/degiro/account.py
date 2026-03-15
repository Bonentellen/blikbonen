"""
A DeGiro account `.csv` Importer. This imports transactions, including buys and sells, but cannot generate balance statements. For this, use the `blikbonen.importers.degiro.portfolio` importer.

# Example
The following example shows how you can initialize this importer:
```python
from blikbonen.importers.degiro import account

ACCOUNT = "Assets:DeGiro"
FUND_INFO = {
    "fund_data": [
        (
            "VUSD",
            "IE00B3XXRP09",
            "Vanguard S&P 500 UCITS ETF (USD) Distributing",
        ),
    ],
    "money_market": [],
}
importer = account.Importer(
    {
        "account_number": "NL12345678901234567890",
        "main_account": f"{ACCOUNT}:{{ticker}}",
        "cash_account": f"{ACCOUNT}:{{currency}}",
        "transfer": "Assets:Zero-Sum-Accounts:DeGiro",
        "interest": "Income:Interest",
        "dividends": "Income:Dividend:{ticker}",
        "cg": "Income:CG",
        "rounding_error": "Equity:RoundingError",
        "tax": "Expenses:Tax",
        "service_fee": "Expenses:ServiceFee",
        "fund_info": FUND_INFO,
        "fees": "Expenses:TradingFees",
    }
)
```
"""

from __future__ import annotations

import decimal
import re
from typing import TypedDict, override

import petl as etl
from beancount.core import data
from beancount_reds_importers.libreader import csvreader
from beancount_reds_importers.libtransactionbuilder import investments

from blikbonen.importers.util import FundInfo, switch_payee_narration


def _is_fee_for_curr(nxt: etl.Record, cur: etl.Record) -> bool:
    return (
        cur is not None
        and nxt is not None
        and nxt["Omschrijving"] == "DEGIRO Transactiekosten en/of kosten van derden"
        and cur["Datum"] == nxt["Datum"]
        and cur["Tijd"] == nxt["Tijd"]
        and cur["Product"] == nxt["Product"]
    )


def _get_fee_field_from_context(_prv: etl.Record, cur: etl.Record, nxt: etl.Record) -> decimal.Decimal | None:
    if _is_fee_for_curr(nxt, cur):
        return -decimal.Decimal(nxt[8])
    return None


def _remove_fee_transaction_from_context(prv: etl.Record, cur: etl.Record, _nxt: etl.Record) -> bool:
    # Return false if current is fee for previous
    return not _is_fee_for_curr(cur, prv)


_BUY_REGEX = r"Koop (\d+(?:,\d+)?) @ (\d+(?:,\d+)?) ([A-Z]+)"
_SELL_REGEX = r"Verkoop (\d+(?:,\d+)?) @ (\d+(?:,\d+)?) ([A-Z]+)"

_TRANSFER_FROM_REGEX = r"^Overboeking van uw geldrekening bij flatexDEGIRO Bank ((?:\d+\.)*\d+,\d+) ([A-Z]+)$"
_TRANSFER_TO_REGEX = r"^Overboeking naar uw geldrekening bij flatexDEGIRO Bank: ((?:\d+\.)*\d+,\d+) ([A-Z]+)$"

_CURRENCY_CREDIT = "Valuta Creditering"
_CURRENCY_DEBET = "Valuta Debitering"


class Config(TypedDict):
    """
    The configuration used to initialize the importer.
    """

    account_numer: str
    """
    The IBAN of your account.
    """

    main_account: str
    """
    The beancount account to use for all assets. You may use `{ticker}` in this string to
    differentiate between currencies.
    """

    cash_account: str
    """
    The beancount account to store cash in. You can use `{currency}` to specify the currency.
    """

    transfer: str
    """
    The beancount account to use for transfers, i.e. the source of money coming into
    `cash_account`. This could for example be a `zerosum` account.
    """

    interest: str
    """
    Any interest income comes from this beancount account.
    """

    dividends: str
    """
    Dividend income comes from this beancount account.
    """

    cg: str
    """
    Capital gains income comes from this beancount account.
    """

    rounding_error: str
    """
    In case an asset buy or sell has a rounding error, this is the beancount account the
    result will be posted to.
    """

    tax: str
    """
    Taxes are posted to this beancount account.
    """

    service_fee: str
    """
    Service fees are posted to this beancount account.
    """

    fund_info: FundInfo
    """
    Info about the funds that are included.
    """

    fees: str
    """
    Trading fees are posted to this beancount account.
    """


class Importer(csvreader.Importer, investments.Importer):
    IMPORTER_NAME = "DeGiro Transactions Importer"

    @override
    def __init__(self, config: Config):
        super().__init__(config)

    def _get_units(self, row) -> str | None:
        """Get the units for this transaction. We fetch it from the description
        in case of a buy/sell or use the amount in case of a currency
        conversion.
        """
        m = re.match(_BUY_REGEX, row["Omschrijving"])
        if m is not None:
            return m.group(1)
        m = re.match(_SELL_REGEX, row["Omschrijving"])
        if m is not None:
            return "-" + m.group(1)
        if row["Omschrijving"] in [_CURRENCY_CREDIT, _CURRENCY_DEBET] and row["Mutatie"] != self.currency:
            return row["mutatie_units"]
        return None

    def _get_security(self, row) -> str | None:
        if row["Omschrijving"] in [_CURRENCY_CREDIT, _CURRENCY_DEBET] and row["Mutatie"] != self.currency:
            return row["Mutatie"]
        return row["ISIN"]

    def _get_field_type(self, row) -> str:
        if (
            row["Omschrijving"].startswith("Overboeking van uw geldrekening bij flatexDEGIRO Bank")
            or row["Omschrijving"].startswith("Overboeking naar uw geldrekening bij flatexDEGIRO Bank")
            or row["Omschrijving"] == "SEPA Instant Terugstorting"
            or row["Omschrijving"] == "flatex Storting"
            or row["Omschrijving"] == "iDEAL Deposit"
            or row["Omschrijving"] == "flatex terugstorting"
        ):
            return "cash"
        if row["Omschrijving"].startswith("Koop "):
            return "buystock"
        if row["Omschrijving"].startswith("Verkoop "):
            return "sellstock"
        if row["Omschrijving"] in ["B.T.W.", "Service-fee", "Dividendbelasting"]:
            return "fee"
        if row["Omschrijving"].startswith("DEGIRO Aansluitingskosten"):
            return "fee"
        if row["Omschrijving"] == "Flatex Interest Income":
            return "income"
        if row["Omschrijving"] == "Dividend":
            return "dividends"
        if row["Omschrijving"] in [_CURRENCY_CREDIT, _CURRENCY_DEBET] and row["Mutatie"] == self.currency:
            return "cash"
        if row["Omschrijving"] == _CURRENCY_CREDIT and row["Mutatie"] != self.currency:
            return "buyother"
        if row["Omschrijving"] == _CURRENCY_DEBET and row["Mutatie"] != self.currency:
            return "sellother"
        if row["Omschrijving"] == "Terugstorting in behandeling":
            return "cash"
        msg = f"transaction type not determined: {row['Omschrijving']}"
        raise ValueError(msg)

    def _get_amount(self, cur):
        m = re.match(_TRANSFER_FROM_REGEX, cur["Omschrijving"])
        if m is not None:
            return f"-{m.group(1)}".replace(".", "").replace(",", ".")
        m = re.match(_TRANSFER_TO_REGEX, cur["Omschrijving"])
        if m is not None:
            return f"{m.group(1)}".replace(".", "").replace(",", ".")
        return cur["mutatie_units"]

    @override
    def build_metadata(self, file, metatype=None, data=None):
        if data is None:
            data = {}
        metadata = super().build_metadata(file, metatype, data)
        metadata["time"] = data["transaction"].Tijd
        metadata["currency"] = data["transaction"].currency
        return metadata

    def _get_total_from_context(self, prv, row, nxt):
        if row["Omschrijving"].startswith("Koop ") or row["Omschrijving"].startswith("Verkoop "):
            return row["mutatie_units"]
        # For currency conversions, there is one debit and one credit
        # transaction. We use the other to get the total
        if (
            nxt is not None
            and row["Omschrijving"] == _CURRENCY_DEBET
            and nxt["Omschrijving"] == _CURRENCY_CREDIT
            and row["Mutatie"] != self.currency
            and nxt["Mutatie"] == self.currency
        ):
            return nxt["mutatie_units"]
        if (
            prv is not None
            and row["Omschrijving"] == _CURRENCY_CREDIT
            and prv["Omschrijving"] == _CURRENCY_DEBET
            and row["Mutatie"] != self.currency
            and prv["Mutatie"] == self.currency
        ):
            return prv["mutatie_units"]
        return row["amount"]

    def _get_unit_price(self, row):
        m = re.match(_BUY_REGEX, row["Omschrijving"])
        if m is not None:
            return m.group(2).replace(",", ".")
        m = re.match(_SELL_REGEX, row["Omschrijving"])
        if m is not None:
            return m.group(2).replace(",", ".")
        if row["Omschrijving"] in [_CURRENCY_CREDIT, _CURRENCY_DEBET] and row["Mutatie"] != self.currency:
            return 1 / decimal.Decimal(row["FX"].replace(",", "."))
        return None

    def _get_currency(self, row):
        for reg in [_BUY_REGEX, _SELL_REGEX]:
            m = re.match(reg, row["Omschrijving"])
            if m is not None:
                return m.group(3)
        for reg in [_TRANSFER_FROM_REGEX, _TRANSFER_TO_REGEX]:
            m = re.match(reg, row["Omschrijving"])
            if m is not None:
                return m.group(2)
        return row["Mutatie"]

    def _remove_useless_debit_credit(self, row):
        """
        We have moved these postings to the other row, so we can remove them.
        """
        return not (row["Omschrijving"] in [_CURRENCY_CREDIT, _CURRENCY_DEBET] and row["Mutatie"] == self.currency)

    @override
    def prepare_table(self, rdr):
        # Ignore these internal transactions
        rdr = etl.selectne(rdr, "Omschrijving", "Degiro Cash Sweep Transfer")
        rdr = etl.selectne(rdr, "Omschrijving", "Processed Flatex Withdrawal")
        rdr = etl.selectne(rdr, "Omschrijving", "Reservation iDEAL")
        rdr = etl.select(
            rdr,
            lambda r: (not r["Omschrijving"].startswith("Overboeking van uw geldrekening bij flatexDEGIRO Bank ")),
        )

        # Reverse rows (keeping header first)
        header = etl.header(rdr)
        rows = list(etl.data(rdr))
        rdr = etl.wrap([header, *list(reversed(rows))])

        # Add names to unnamed columns
        rdr = etl.rename(rdr, 8, "mutatie_units")
        rdr = etl.rename(rdr, 10, "saldo_units")

        rdr = etl.convert(rdr, "mutatie_units", lambda v: v.replace(",", "."))
        rdr = etl.convert(rdr, "saldo_units", lambda v: v.replace(",", "."))

        # Add the fee to the previous transaction
        rdr = etl.addfieldusingcontext(rdr, "fees", _get_fee_field_from_context)
        rdr = etl.selectusingcontext(rdr, _remove_fee_transaction_from_context)

        rdr = etl.addfield(rdr, "type", self._get_field_type)
        rdr = etl.addfield(rdr, "units", self._get_units)
        rdr = etl.addfield(rdr, "amount", self._get_amount)
        rdr = etl.addfield(rdr, "security", self._get_security)
        rdr = etl.addfieldusingcontext(rdr, "total", self._get_total_from_context)
        rdr = etl.addfield(rdr, "unit_price", self._get_unit_price)
        rdr = etl.addfield(rdr, "currency", self._get_currency)
        return etl.select(rdr, self._remove_useless_debit_credit)

    @override
    def custom_init(self):
        self.config["currency"] = "EUR"
        self.config["capgainsd_lt"] = self.config["cg"]
        self.config["capgainsd_st"] = self.config["cg"]
        self.max_rounding_error = 0.004
        self.currency = self.config["currency"]
        self.filename_pattern_def = "Account.*\\.csv"
        self.column_labels_line = "Datum,Tijd,Valutadatum,Product,ISIN,Omschrijving,FX,Mutatie,,Saldo,,Order Id"
        self.header_identifier = self.column_labels_line
        self.date_format = "%d-%m-%Y"
        self.header_map = {
            "Datum": "date",
            "Valutadatum": "tradeDate",
            "Omschrijving": "memo",
        }
        self.transaction_type_map = {}
        self.skip_transaction_types = []
        self.get_ticker_info = self.get_ticker_info_from_id

    @override
    def get_target_acct_custom(self, transaction, _ticker):
        if transaction.memo in ["B.T.W.", "Dividendbelasting"]:
            return self.config["tax"]
        if transaction.memo == "Service-fee":
            return self.config["service_fee"]
        return None

    @override
    def add_fee_postings(self, entry, ot):
        config = self.config
        if hasattr(ot, "fees") and getattr(ot, "fees", 0) != 0:
            data.create_simple_posting(entry, config["fees"], ot.fees, self.currency)
            # We create a second posting here, since this is not included in
            # the total yet. The total may not be the same currency either, so
            # we add it seperately.
            data.create_simple_posting(entry, config["cash_account"], -ot.fees, self.currency)

    def _fix_currency_for_buy_sell(self, entries: list[data.Directive]):
        """The currency for buy and sell transactions (not for currency
        conversions) has been set to `self.currency` at this point, even if
        there is an intermediate currency involved. We fix this here.
        """
        new_entries = []
        for entry in entries:
            if isinstance(entry, data.Transaction):
                # Get the correct currency from metadata and remove
                expected_currency = entry.meta["currency"]
                del entry.meta["currency"]

                # We find the transaction cost posting, so we can skip both
                # legs later on.
                transaction_cost_posting = None
                for posting in entry.postings:
                    if posting.account == self.config["fees"]:
                        transaction_cost_posting = posting
                        assert transaction_cost_posting.units is not None  # noqa: S101
                skipped_transaction_cost_other_leg = False

                if entry.postings[0].units is not None and entry.postings[0].units.currency != expected_currency:
                    for i in range(len(entry.postings)):
                        posting = entry.postings[i]

                        # Skip the transaction cost posting, as well as the opposit leg
                        if transaction_cost_posting is not None:
                            assert transaction_cost_posting.units is not None  # noqa: S101
                            # Always skip the transaction cost posting (there should only be 1)
                            if posting == transaction_cost_posting:
                                continue
                            # Since it could be that other legs have the same units, only skip this once.
                            if (
                                posting.units == -transaction_cost_posting.units
                                and not skipped_transaction_cost_other_leg
                            ):
                                skipped_transaction_cost_other_leg = True
                                continue

                        if posting.units is not None and posting.units.currency == self.currency:
                            posting = posting._replace(units=posting.units._replace(currency=expected_currency))
                            entry.postings[i] = posting
                        if posting.price is not None and posting.price.currency != expected_currency:
                            posting = posting._replace(price=posting.price._replace(currency=expected_currency))
                            entry.postings[i] = posting
                        if posting.cost is not None and posting.cost.currency != expected_currency:
                            posting = posting._replace(cost=posting.cost._replace(currency=expected_currency))
                            entry.postings[i] = posting
                        posting = posting._replace(account=posting.account.replace(self.currency, expected_currency))
                        entry.postings[i] = posting

            new_entries += [entry]
        return new_entries

    @override
    def custom_entry_mods(self, new_entries):
        new_entries = switch_payee_narration(new_entries)

        # There is one thing we should fix: for some buy/sell statements the
        # currency is incorrectly set to `self.currency`. It should be
        # `currency` instead. We can recognize these postings by the fact that
        # the first posting does not have this as a currency. (None of the
        # postings do, in fact.)
        #
        # A better solution would be to fix this in the importer.
        new_entries = self._fix_currency_for_buy_sell(new_entries)

        # At this point, the transactions are well-formed. Now we will merge
        # transactions with the same time & date.
        old_entries = new_entries
        new_entries = []
        for entry in old_entries:
            if (
                len(new_entries) > 0
                and new_entries[-1].date == entry.date
                and new_entries[-1].meta["time"] == entry.meta["time"]
            ):
                for posting in entry.postings:
                    existing_posting_pos = None
                    for i in range(len(new_entries[-1].postings)):
                        existing_posting = new_entries[-1].postings[i]
                        if posting.account == existing_posting.account:
                            if posting.units is None and existing_posting.units is None:
                                existing_posting_pos = i
                                break
                            if (
                                posting.units.currency == existing_posting.units.currency
                                and posting.price == existing_posting.price
                                and posting.cost == existing_posting.cost
                            ):
                                existing_posting_pos = i
                                break

                    if existing_posting_pos is None:
                        new_entries[-1].postings.append(posting)
                    # We can merge with an existing posting
                    elif new_entries[-1].postings[existing_posting_pos].units is not None:
                        new_entries[-1].postings[existing_posting_pos] = (
                            new_entries[-1]
                            .postings[existing_posting_pos]
                            ._replace(
                                units=new_entries[-1]
                                .postings[existing_posting_pos]
                                .units._replace(
                                    number=new_entries[-1].postings[existing_posting_pos].units.number
                                    + posting.units.number
                                )
                            )
                        )
            else:
                new_entries.append(entry)

        # We simplify transactions as follows: if there is a cost not
        # in `self.currency`, we convert it by using the cost of this currency
        # in terms of `self.currency`. We can then remove the adding and
        # subtracting of this other currency.
        old_entries = new_entries
        new_entries = []
        for entry in old_entries:
            costs = [[posting.units.currency, posting.cost] for posting in entry.postings if posting.cost is not None]

            for i in range(len(entry.postings)):
                posting = entry.postings[i]
                if posting.cost is not None and posting.cost.currency != self.currency:
                    correct_posting = None
                    for from_cur, cost in costs:
                        if from_cur == posting.cost.currency:
                            correct_posting = cost
                    if correct_posting is None:
                        continue
                    entry.postings[i] = posting._replace(
                        cost=posting.cost._replace(
                            currency=self.currency,
                            number=(posting.cost.number * correct_posting.number).quantize(decimal.Decimal("0.0001")),
                        )
                    )

            new_entries.append(entry)

        # We simplify one last time, to filter out combinations like 100 USD {0.8 EUR} & -100 USD {}
        old_entries = new_entries
        new_entries = []
        for entry in old_entries:
            cancelling_duo = None
            for i in range(len(entry.postings)):
                for j in range(i + 1, len(entry.postings)):
                    if (
                        entry.postings[i].account == entry.postings[j].account
                        and entry.postings[i].units is not None
                        and entry.postings[j].units is not None
                        and entry.postings[i].units.currency == entry.postings[j].units.currency
                        and entry.postings[i].units.number == -entry.postings[j].units.number
                    ):
                        cancelling_duo = [i, j]

            if cancelling_duo is not None:
                i, j = cancelling_duo
                entry.postings.pop(j)
                entry.postings.pop(i)

            new_entries.append(entry)

        return new_entries
