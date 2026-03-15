"""
ASN Bank Investing .csv importer.

# Example
The following code initializes this importer:
```python
acct = "Assets:AsnInvesting"
return asnbank_investing.Importer(
    {
        "account_number": "NL12345678901234567890",
        "main_account": f"{acct}:{{ticker}}",
        "cash_account": f"{acct}:{{currency}}",
        "transfer": "Assets:Zero-Sum-Accounts:AsnInvesting",
        "interest": "Income:Interest",
        "dividends": "Income:Dividend:{ticker}",
        "cg": "Income:CapitalGains",
        "fees": "Expenses:InvestingFees",
    }
)
```
See `Config` for an explanation of the keys.
"""

from __future__ import annotations

import decimal
from typing import TYPE_CHECKING, TypedDict, override

if TYPE_CHECKING:
    import itertools
    from collections.abc import Sequence

import petl as etl
import regex as re
from beancount.core import data
from beancount.core.data import Amount, Balance, Directive
from beancount_reds_importers.libreader import csvreader
from beancount_reds_importers.libtransactionbuilder import investments

from blikbonen.importers.util import switch_payee_narration


class Config(TypedDict):
    """
    This is the config used to initialize the importer.
    """

    account_number: str
    """
    The IBAN for this account.
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
    The beancount account used for transfers, for example a `zerosum` account.
    """

    interest: str
    """
    The beancount account used for interest income.
    """

    dividends: str
    """
    The beancount account used for dividends income.
    """

    cg: str
    """
    The beancount account for capital gains income.
    """

    fees: str
    """
    The beancount account for service fees.
    """


def _strip_quotes(a: str) -> str:
    if a.startswith("'") and a.endswith("'"):
        return a[1:-1]
    return a


def _regex(a: str) -> re.Match[str] | None:
    return re.match(
        "^Voor\\s+u\\s+([a-z]+kocht)\\s+via\\s+Euronext\\s+Fund\\s+Services:\\s+(\\d+ \\d+)\\s+Participaties\\s+(.*)a\\s+EUR\\s+(\\d+ \\d+)",
        a,
    )


def _units(a: etl.Record) -> str | None:
    m = _regex(a["Omschrijving"])
    if m is None:
        return None
    return m.group(2).replace(" ", ".")


def _unit_price(a: etl.Record) -> str | None:
    m = _regex(a["Omschrijving"])
    if m is None:
        return None
    return m.group(4).replace(" ", ".")


def _balance_security(a: etl.Record) -> decimal.Decimal | None:
    if "Uw positie in dit fonds na deze mutatie is nihil." in a["Omschrijving"]:
        return decimal.Decimal(0)
    m = re.search(r"Positie na transactie: (\d+ \d+)", a["Omschrijving"])
    if m is None:
        return None
    return decimal.Decimal(m.group(1).replace(" ", "."))


_FUND_INFO = {
    "fund_data": [
        ("ASNOB", "NL0014270209", "ASN Duurzaam Obligatiefonds"),
        ("ASNGF", "NL0014270258", "ASN Energie & Innovatiefonds"),
        ("ASNMF", "NL0014270266", "ASN Microkredietfonds"),
        ("ASNZD", "NL0014270274", "ASN Duurzaam Mixfonds Zeer Defensief"),
        ("ASND", "NL0014270282", "ASN Duurzaam Mixfonds Defensief"),
        ("ASNN", "NL0014270290", "ASN Duurzaam Mixfonds Neutraal"),
        ("ASNO", "NL0014270308", "ASN Duurzaam Mixfonds Offensief"),
        ("ASNZO", "NL0014270316", "ASN Duurzaam Aandelenfonds"),
        ("ASNML", "NL0014270233", "ASN Milieu & Waterfonds"),
        ("ASN5", "NL0014270217", "ASN Duurzaam Small & Midcapfonds"),
        ("ASNBD", "NL0015000JW4", "ASN Biodiversiteitsfonds"),
        ("ASNZO", "NL0014270316", "ASN Duurzaam Mixfonds Zeer Offensief"),
    ],
    "money_market": [],
}
_HEADER: Sequence[str] = [
    "Datum",
    "Je rekening",
    "Van / naar",
    "Naam",
    "unused1",
    "unused2",
    "unused3",
    "Valuta saldo",
    "Saldo voor boeking",
    "Valuta boeking",
    "Bedrag bij/af",
    "Verwerkingsdatum",
    "Valutadatum",
    "Code",
    "Type",
    "Volgnummer",
    "Betalingskenmerk",
    "Omschrijving",
    "Afschriftnummer",
    "Categorie",
]


class Importer(csvreader.Importer, investments.Importer):
    IMPORTER_NAME = "ASN Bank Investing CSV Importer"

    @override
    def __init__(self, config: Config):
        super().__init__(config)

    def _get_security_from_description(self, a: etl.Record) -> str | None:
        m = _regex(a["Omschrijving"])
        if m is None:
            return None
        for _ticker, isin, name in self.fund_data:
            # Sometimes, spaces are missing in the csv file. To combat this, we remove all spaces when comparing
            if name.replace(" ", "") == m.group(3).replace(" ", ""):
                return isin
        return None

    @override
    def prepare_raw_file(self, rdr: etl.Table):
        return etl.pushheader(rdr, _HEADER)

    @override
    def prepare_table(self, rdr: etl.Table):
        rdr = etl.convert(rdr, "Omschrijving", _strip_quotes)
        rdr = etl.convert(
            rdr,
            "Type",
            lambda _v: "fee",
            where=lambda r: r["Omschrijving"].startswith("Servicekosten "),
        )
        rdr = etl.convert(
            rdr,
            "Type",
            lambda _v: "dividends",
            where=lambda r: r["Omschrijving"].startswith("Uitkering cash dividend "),
        )
        rdr = etl.convert(
            rdr,
            "Type",
            lambda _v: "sellstock",
            where=lambda r: r["Omschrijving"].startswith("Voor u verkocht "),
        )
        rdr = etl.convert(
            rdr,
            "Type",
            lambda _v: "buystock",
            where=lambda r: r["Omschrijving"].startswith("Voor u gekocht "),
        )
        rdr = etl.addfield(rdr, "security", self._get_security_from_description)
        rdr = etl.addfield(rdr, "security_balance", _balance_security)
        rdr = etl.addfield(rdr, "units", _units)
        rdr = etl.addfield(rdr, "unit_price", _unit_price)
        return etl.addfield(rdr, "total", lambda x: x["Bedrag bij/af"])

    @override
    def custom_init(self):
        self.config["fund_info"] = _FUND_INFO
        self.config["capgainsd_lt"] = "UNUSED"
        self.config["capgainsd_st"] = "UNUSED"
        self.max_rounding_error = 0.00004
        self.filename_pattern_def = "transactie-historie_" + self.config["account_number"] + "_\\d+\\.csv"
        self.header_identifier = ""
        self.column_labels_line = ",".join(_HEADER)
        self.date_format = "%d-%m-%Y"
        self.skip_transaction_types = []
        self.currency = "EUR"
        self.header_map = {
            "Datum": "date",
            "Type": "type",
            # Payee and narration are flipped
            "Omschrijving": "payee",
            "Naam": "memo",
            "Bedrag bij/af": "amount",
            "Valuta boeking": "currency",
            "Saldo voor boeking": "balance",
            "Verwerkingsdatum": "tradeDate",
        }
        self.transaction_type_map = {
            "BIJ": "income",
            "NGI": "cash",
        }
        self.get_ticker_info = self.get_ticker_info_from_id

    @override
    def extract_balances_and_prices(self, file: str, counter: itertools.count):
        """Return the balance on the first and last dates"""

        # The assertion is for the start of the day, so we should find the
        # first transaction on the last day.
        last_date = self.get_max_transaction_date()
        transactions_at_date = etl.records(etl.select(self.rdr, lambda r: r.date.date() == last_date))
        balances = []
        for record in transactions_at_date:
            metadata = data.new_metadata(file, next(counter))
            main_acct = self.config["cash_account"]
            balances += [
                Balance(
                    metadata,
                    last_date,
                    main_acct,
                    Amount(record["balance"], record["currency"]),
                    None,
                    None,
                )
            ]
            break

        securities = etl.values(self.rdr, "security")
        unique_values = []
        for sec in securities:
            if sec is not None and sec not in unique_values:
                unique_values += [sec]

        last_for_security: dict[str, etl.Record] = {}
        for txn in etl.records(self.rdr):
            if txn.security is not None:
                sec = txn["security"]
                if sec not in last_for_security or last_for_security[sec]["date"] < txn["date"]:
                    last_for_security[sec] = txn

        balance_assertion_date = self.get_balance_assertion_date()
        for isin, txn in last_for_security.items():
            ticker, _long_name = self.get_ticker_info(isin)
            metadata = data.new_metadata(file, next(counter))
            main_acct = self.get_acct("main_account", txn, ticker)
            balances.append(
                Balance(
                    metadata,
                    balance_assertion_date,
                    main_acct,
                    Amount(txn["security_balance"], ticker),
                    None,
                    None,
                )
            )

        return balances

    @override
    def custom_entry_mods(self, new_entries: list[Directive]):
        return switch_payee_narration(new_entries)
