"""
An importer for NS travel transactions. Download these from the
'Journey history & transactions' / 'Reishistorie & betalingen' tab.

# Example
You can configure this importer as follows:
```python
from blikbonen.importers.ns import travel_history

travel_history.Importer(
    {
        "main_account": "Liabilities:PublicTransport:NS",
        "account_number": "0000000000000000",
        "travel_expenses": "Expenses:PublicTransport:NS",
    }
)
```
"""

import decimal
import re
from typing import TypedDict, override

import petl as etl
from beancount_reds_importers.libreader import csvreader
from beancount_reds_importers.libtransactionbuilder import banking

from blikbonen.importers.util import reverse_row_order

_AMOUNT_REGEX = r"^€ ?(\d+,\d+)$"
_NO_AF_BIJ = "could not parse file: no Af/Bij"


class Config(TypedDict):
    """
    The configuration to specify to initialize the importer.
    """

    main_account: str
    """
    The main account to specify transactions to. If you want to use
    `invoice.Importer`, this could be a `Liabilities:*` account. For travel
    expenses, this is where money is taken from.
    """

    account_number: str
    """
    The number of your OV chipkaart. This will be the part appearing after
    `reistransacties_` in the file name.
    """

    travel_expenses: str
    """
    The account to use for travel expenses.
    """


class Importer(csvreader.Importer, banking.Importer):
    IMPORTER_NAME = "NS travel transactions."

    def __init__(self, config: Config):
        super().__init__(config)

    def _amount_from_row(self, r: etl.Record) -> decimal.Decimal:
        af = re.match(_AMOUNT_REGEX, str(r["Af"]))
        bij = re.match(_AMOUNT_REGEX, str(r["Bij"]))
        if af is None or bij is None:
            raise ValueError(_NO_AF_BIJ)
        return decimal.Decimal(bij.group(1).replace(",", ".")) - decimal.Decimal(af.group(1).replace(",", "."))

    def _add_narration(self, r: etl.Record) -> str:
        return f"{r['Vertrek']} - {r['Bestemming']}"

    @override
    def prepare_raw_file(self, rdr: etl.Table):
        rdr = etl.addfield(rdr, "type", "transfer")
        rdr = etl.addfield(rdr, "amount", self._amount_from_row)
        rdr = etl.addfield(rdr, "payee", self._add_narration)
        return reverse_row_order(rdr)

    @override
    def custom_init(self):
        self.config["currency"] = "EUR"
        self.max_rounding_error = 0.004
        self.filename_pattern_def = r"reistransacties_" + self.config["account_number"] + "_.*\\.csv"
        self.header_identifier = (
            "Datum,Check in,Vertrek,Check uit,Bestemming,Af,Bij,Transactie,Kl,Product,Prive/ Zakelijk,Opmerking"
        )
        self.column_labels_line = self.header_identifier
        self.date_format = "%d-%m-%Y"
        self.skip_transaction_types = []
        self.header_map = {
            "Datum": "date",
            # Payee and narration are flipped
            "Product": "memo",
            "Check in": "check_in",
            "Check uit": "check_uit",
        }
        self.transaction_type_map = {}

    @override
    def get_target_account(self, ot: etl.Record):
        return self.config["travel_expenses"]

    @override
    def build_metadata(self, file, metatype=None, data=None):
        if data is None:
            data = {}
        meta = super().build_metadata(file, metatype, data)
        if data["transaction"].check_in != "":
            meta["check_in"] = data["transaction"].check_in
        if data["transaction"].check_uit != "":
            meta["check_uit"] = data["transaction"].check_uit
        return meta
