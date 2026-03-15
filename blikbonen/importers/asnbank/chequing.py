"""
An ASN Bank Chequing `.csv` importer. To use, download a `.csv` file from the
'Bij- en afschrijvingen downloaden' menu.

# Example
The following code initializes this importer:
```python
from importers.asnbank import chequing as asnbank_chequing

return asnbank_chequing.Importer(
    {
        "account_number": "NL12345678901234567890",
        "main_account": "Assets:Chequing",
        "invest": "Assets:Zero-Sum-Accounts:AsnInvesting",
    }
)
```
See `Config` for an explanation of the keys.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict, override

if TYPE_CHECKING:
    from collections.abc import Sequence

import petl as etl
from beancount_reds_importers.libreader import csvreader
from beancount_reds_importers.libtransactionbuilder import banking


def _strip_quotes(a: str):
    if a.startswith("'") and a.endswith("'"):
        return a[1:-1]
    return a


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


class Config(TypedDict):
    """
    This is the config used to initialize the importer.
    """

    account_number: str
    """
    The IBAN corresponding to this account, without whitespace.
    """

    main_account: str
    """
    The beancount account for the main leg of the transactions.
    """

    invest: str | None
    """
    If specified, this beancount account will be used for the second leg for
    transfers to and from the ASN investment account.
    """


class Importer(csvreader.Importer, banking.Importer):
    IMPORTER_NAME = "ASN Bank Chequing CSV Importer"

    @override
    def prepare_raw_file(self, rdr: etl.Table):
        return etl.pushheader(rdr, _HEADER)

    @override
    def prepare_table(self, rdr: etl.Table):
        return etl.convert(rdr, "Omschrijving", _strip_quotes)

    @override
    def __init__(self, config: Config):
        super().__init__(config)

    @override
    def custom_init(self):
        self.max_rounding_error = 0.004
        self.filename_pattern_def = "transactie-historie_" + self.config["account_number"] + "_\\d+\\.csv"
        self.header_identifier = ""
        self.column_labels_line = ",".join(_HEADER)
        self.date_format = "%d-%m-%Y"
        self.skip_transaction_types = []
        self.header_map = {
            "Datum": "date",
            "Type": "type",
            # Payee and narration are flipped
            "Omschrijving": "payee",
            "Naam": "memo",
            "Bedrag bij/af": "amount",
            "Valuta boeking": "currency",
            "Saldo voor boeking": "balance",
        }
        self.transaction_type_map = {"BIJ": "income", "NGI": "cash"}

    @override
    def get_target_account(self, ot: etl.Record):
        """This method is for importers to override. The overridden method can return a target account for
        special cases, or return None, which will let get_target_acct() decide the target account
        """
        investing_account = self.config.get("invest")
        if ot.memo == "ASN Themabeleggen" and investing_account is not None:
            return investing_account
        return None

    @override
    def get_balance_statement(self, file=None):
        """Return the balance on the first and last dates"""

        # The assertion is for the start of the day, so we should find the
        # first transaction on the last day.
        last_date = self.get_max_transaction_date()
        transactions_at_date = etl.records(etl.select(self.rdr, lambda r: r.date.date() == last_date))
        for record in transactions_at_date:
            return [banking.Balance(last_date, record["balance"], record["currency"])]
        return []
