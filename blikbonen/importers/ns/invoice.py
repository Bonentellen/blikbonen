"""
An importer for NS invoice `.pdf` files. Download these from the
'Overview of payments' / 'Betaaloverzicht' tab.

This importer usually works, but since it is based on reading PDF files it is
likely to fail at least some of the time. While
`blikbonen.importers.ns.travel_history.Importer` can be used to add expenses
as a liability, this importer can be used to collect these liabilities, add the
subscription costs, and collect them to a single payment posting, for example a
`zerosum` account.

See `Config` for the configuration options.

# Example
The following example initializes the importer:
```python
from blikbonen.importers.ns import invoice

invoice.Importer(
    {
        "main_account": "Assets:Zero-Sum-Accounts:PublicTransport:NS",
        "account_number": "12345678",
        "subscription_expense": "Expenses:PublicTransport:NS",
        "travel_expense": "Liabilities:PublicTransport:NS",
    }
)
```
"""

import datetime
import re
from collections.abc import Sequence
from typing import TypedDict, override

import petl as etl
import pytz
from beancount.core import data
from beancount.core.data import Amount
from beancount_reds_importers.libreader import pdfreader
from beancount_reds_importers.libtransactionbuilder import banking

_INVOICE_NUMBER_REGEX = r"Factuurnummer ((?:\d| )+)"
_INVOICE_DATE_REGEX = r"Factuurdatum (\d\d?) ([a-z]+) (\d\d\d\d)"
_DATE_UNRECOGNIZED = "the date could not be recognized"
_INVOICE_NUMBER_UNRECOGNIZED = "the invoice number could not be recognized"
_DATE_MAP: Sequence[str] = [
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


def _is_number(s: str) -> bool:
    try:
        float(s)
    except ValueError:
        return False
    else:
        return True


class Config(TypedDict):
    """
    The configuration to pass to the importer.
    """

    main_account: str
    """
    This is the account to map the final total invoice cost to.
    """

    account_number: str
    """
    Your account number. This is the number appearing as `factuur-<number>-`
    in the invoice file name.
    """

    travel_expense: str
    """
    The account to book travel expenses to. If you use the
    `blikbonen.importers.ns.travel_history` importer, this should be the same
    account as its [`main_account`](travel_history.html#Config.main_account), for example
    `Liabilities:PublicTransport:NS`.
    """

    subscription_expense: str
    """
    The account to book additional expenses to, for example subscription fees.
    """


class Importer(pdfreader.Importer, banking.Importer):
    IMPORTER_NAME = "NS Invoices Importer"

    def __init__(self, config: Config):
        super().__init__(config)

    def _convert_amount(self, v: str) -> str:
        """Remove trailing - and invert"""
        if v.endswith("-"):
            return v[:-1]
        return f"-{v}"

    @override
    def prepare_processed_table(self, rdr: etl.Table):
        rdr = etl.rename(rdr, 0, "narration")
        rdr = etl.rename(rdr, 1, "startdate")
        rdr = etl.rename(rdr, 3, "enddate")
        rdr = etl.rename(rdr, 5, "amount")
        rdr = etl.cutout(rdr, 4)
        rdr = etl.cutout(rdr, 2)

        # Process numbers
        rdr = etl.convert(rdr, "amount", self._convert_amount)
        rdr = etl.convert(rdr, "amount", lambda v: v.replace(",", "."))
        # Filter out extra rows this way
        rdr = etl.select(rdr, "amount", _is_number)
        rdr = etl.select(rdr, lambda r: r["narration"] != "")
        rdr = etl.addfield(
            rdr,
            "date",
            lambda _r: datetime.datetime(
                self.date(self.file).year,
                self.date(self.file).month,
                self.date(self.file).day,
                tzinfo=pytz.timezone("Europe/Amsterdam"),
            ),
        )
        rdr = etl.addfield(rdr, "memo", lambda r: f"Factuur {r['date'].date()}")
        return etl.addfield(rdr, "payee", "NS Reizigers")

    @override
    def custom_init(self):
        self.config["currency"] = "EUR"
        self.pdf_table_extraction_settings = {
            "vertical_strategy": "explicit",
            "horizontal_strategy": "text",
            "explicit_vertical_lines": [110, 255, 305, 315, 380, 420, 493],
            "text_y_tolerance": 50,
        }
        # self.debug = True
        self.pdf_table_extraction_crop = (105, 380, 90, 0)
        self.filename_pattern_def = r"factuur-" + self.config["account_number"] + r"-.*.pdf"
        self.pdf_table_title_height = 100
        self.pdf_page_break_top = 100
        self.transaction_table_section = "unknown"
        self.header_map = {}
        self.skip_transaction_types = []
        self.debug = False

    @override
    def date(self, file):
        self.read_file(file)
        m = re.search(_INVOICE_DATE_REGEX, self.meta_text)
        if m is None:
            raise ValueError(_DATE_UNRECOGNIZED)
        day = m.group(1)
        month = _DATE_MAP.index(m.group(2)) + 1
        year = m.group(3)
        return datetime.date(int(year), month, int(day))

    @override
    def get_transactions(self):
        """Provides the transactions to the transaction builder."""
        for transaction_table in self.alltables.values():
            yield from transaction_table.namedtuples()

    @override
    def get_target_account(self, ot):
        if ot.startdate != "":
            return self.config["subscription_expense"]
        return self.config["travel_expense"]

    invoice_number = None

    def _get_invoice_number(self) -> str:
        if self.invoice_number is None:
            m = re.search(_INVOICE_NUMBER_REGEX, self.meta_text)
            if m is None:
                raise ValueError(_INVOICE_NUMBER_UNRECOGNIZED)
            self.invoice_number = m.group(1).replace(" ", "")
        return self.invoice_number

    @override
    def build_metadata(self, file, metatype=None, data=None):
        if data is None:
            data = {}
        meta = super().build_metadata(file, metatype, data)
        if metatype == "transaction":
            meta["narration"] = data["transaction"].narration
            meta["invoice_number"] = self._get_invoice_number()
        return meta

    @override
    def find_and_fix_broken_tables(self, tables):
        tables = super().find_and_fix_broken_tables(tables)
        return tables[:1]

    @override
    def custom_entry_mods(self, new_entries: data.Entries):
        # We now have all parts in seperate transactions. Instead, we want to
        # combine them all into a single transaction. The transactions have two
        # postings, the first for `main_account`, the other for the particular
        # expense.

        def transform_transaction(entry: data.Directive) -> data.Directive:
            """
            Do two things:
            - Add the negative amount to the second posting.
            - Move the narration to the description of the second posting.
            """
            assert isinstance(entry, data.Transaction)  # noqa: S101
            assert entry.postings[0].units is not None  # noqa: S101
            assert entry.postings[0].units.number is not None  # noqa: S101

            narration = entry.meta["narration"]
            del entry.meta["narration"]
            currency = entry.postings[0].units.currency
            number = entry.postings[0].units.number
            entry.postings[1] = entry.postings[1]._replace(
                units=Amount(-number, currency), meta={"description": narration}
            )
            return entry

        new_entries = [transform_transaction(entry) for entry in new_entries]

        if len(new_entries) == 0:
            return []

        # Copy all postings to the first entry.
        first_entry = new_entries[0]
        assert isinstance(first_entry, data.Transaction)  # noqa: S101
        for other_entry in new_entries[1:]:
            assert isinstance(other_entry, data.Transaction)  # noqa: S101
            for posting in other_entry.postings:
                if posting.meta is not None and posting.meta["description"] is not None:
                    first_entry.postings.append(posting)
                else:
                    # Combine with existing posting
                    index = None
                    for i in range(len(first_entry.postings)):
                        if first_entry.postings[i].account == posting.account:
                            index = i
                            break
                    assert index is not None  # noqa: S101
                    first_posting = first_entry.postings[index]
                    assert first_posting.units is not None  # noqa: S101
                    assert posting.units is not None  # noqa: S101
                    assert posting.units.number is not None  # noqa: S101
                    assert first_posting.units.number is not None  # noqa: S101
                    first_entry.postings[index] = first_posting._replace(
                        units=first_posting.units._replace(
                            currency=first_posting.units.currency,
                            number=first_posting.units.number + posting.units.number,
                        )
                    )

        return [first_entry]
