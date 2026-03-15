"""
Importer for DeGiro portfolio `.csv` files. These files contain the current
balance for all assets on your account. Use the
`blikbonen.importers.degiro.account` importer for transactions, buys and sells.

# Example
The following example configures the importer:

```python
from blikbonen.importers.degiro import portfolio

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
importer = portfolio.Importer(
    {
        "account_number": "NL12345678901234567890",
        "main_account": f"{ACCOUNT}:{{ticker}}",
        "cash_account": f"{ACCOUNT}:{{currency}}",
        "fund_info": FUND_INFO,
    }
)
```
"""

import datetime
import decimal
import pathlib
from typing import TypedDict, override

import petl as etl
from beancount_reds_importers.libreader import csvreader
from beancount_reds_importers.libtransactionbuilder import investments

from blikbonen.importers.util import FundInfo


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

    fund_info: FundInfo
    """
    Info about the funds that are included.
    """


_NO_CASH_BALANCE_FOUND = "could not recognize cash balance row"


class Importer(csvreader.Importer, investments.Importer):
    IMPORTER_NAME = "DeGiro Portfolio Importer"

    @override
    def __init__(self, config: Config):
        super().__init__(config)

    @override
    def prepare_table(self, rdr: etl.Table):
        # Remove unused columns
        rdr = etl.cut(rdr, *range(4), 6)
        return etl.convert(rdr, "Waarde in EUR", lambda v: decimal.Decimal(v.replace(",", ".")))

    @override
    def custom_init(self):
        self.config["currency"] = "EUR"
        self.config["fees"] = "Equity:Unknown"
        self.config["cg"] = "Equity:Unknown"
        self.config["capgainsd_lt"] = "Equity:Unknown"
        self.config["capgainsd_st"] = "Equity:Unknown"
        self.config["transfer"] = "Equity:Unknown"
        self.config["interest"] = "Equity:Unknown"
        self.config["dividends"] = "Equity:Unknown"
        self.config["rounding_error"] = "Equity:Unknown"
        self.config["tax"] = "Equity:Unknown"
        self.config["service_fee"] = "Equity:Unknown"
        self.max_rounding_error = 0.0004
        self.filename_pattern_def = r"Portfolio.*\.csv"
        self.column_labels_line = "Product,Symbool/ISIN,Aantal,Slotkoers,Lokale waarde,,Waarde in EUR"
        self.header_identifier = self.column_labels_line
        self.header_map = {
            "Product": "memo",
            "Symbool/ISIN": "security",
            "Aantal": "units",
            "Slotkoers": "unit_price",
            "Waarde in EUR": "eur_value",
        }
        self.transaction_type_map = {}
        self.skip_transaction_types = []
        self.get_ticker_info = self.get_ticker_info_from_id
        self.max_date = None

    @override
    def extract_transactions(self, file, counter):
        return []

    @override
    def date(self, file):
        self.read_file(file)
        if self.max_date is None:
            fname = pathlib.Path(file)
            mtime = datetime.datetime.fromtimestamp(fname.stat().st_mtime, tz=datetime.timezone.utc)
            self.max_date = mtime
        return self.max_date.date()

    @override
    def get_max_transaction_date(self):
        return self.date(self.file)

    @override
    def get_balance_positions(self):
        for pos in self.rdr.namedtuples():
            if pos.security != "":
                yield pos

    @override
    def get_available_cash(self, settlement_fund_balance=0):
        for r in self.rdr.namedtuples():
            if r.memo == "CASH & CASH FUND & FTX CASH (EUR)":
                return r.eur_value
        raise ValueError(_NO_CASH_BALANCE_FOUND)
