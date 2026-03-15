"""
WieBetaaltWat Importer that reads the `list_items` (`.json`) file downloaded
when browsing a WieBetaaltWat list. To find these files, open a list in your
browser while you have the 'Network' tab open in the Developer Tools. You
should see a request for an URL that starts with `list_items`.

The account set in `main_account` indicates your balance. This importer creates entries of the following form:

- If the entry is by someone else, we will use `main_account` as the only leg,
  indicating the change in your balance. I.e. if someone spends €5 for you, this
  will add a leg `-5.00 EUR` to `main_account`. You still need to add the
  expense leg to balance the transaction.
```beancount
2026-01-01 * "WieBetaaltWat: Our List" "Dinner"
    filing_account: "Assets:WieBetaaltWat"
    category_main: "Food"
    category_sub: "Groceries"
    Assets:WieBetaaltWat -5.00 EUR
```
- If the entry is your transaction, it will additionally add a leg that
  transfers this amount from `transfer_account`. For example, if you have spent
  €20, divided among 4 people, this will add a leg `-20.00 EUR` for
  `transfer_account` and a leg `15.00 EUR` to `main_account`. The missing
  remainder is your own share of the expence.
```beancount
2026-01-01 * "WieBetaaltWat: Our List" "Dinner"
    filing_account: "Assets:WieBetaaltWat"
    category_main: "Food"
    category_sub: "Groceries"
    Assets:Zero-Sum-Accounts:WieBetaaltWat 20.00 EUR
    Assets:WieBetaaltWat 15.00 EUR
```

Since WieBetaaltWat supports cetegorizing transactions (automatically & manually), these are added via the `category_main` and `category_sub` metadata keys.

# Example
The following code initializes this importer:
```python
from blikbonen.importers import wiebetaaltwat

return wiebetaaltwat.Importer(
    {
        "list_id": "list-identifier",
        "account_number": "account-identifier",
        "main_account": "Assets:WieBetaaltWat",
        "transfer_account": "Assets:Zero-Sum-Accounts:WieBetaaltWat",
        "list_name": "Our List",
    }
)
```
See `Config` for an explanation of the keys.
"""

import datetime as dt
import decimal
from collections.abc import Sequence
from typing import NamedTuple, TypedDict, override

import pytz
from beancount.core import data
from beancount_reds_importers.libreader import jsonreader
from beancount_reds_importers.libtransactionbuilder import banking


class _TransactionOt(NamedTuple):
    payee: str
    memo: str
    total: decimal.Decimal
    currency: str
    date: dt.datetime
    by_id: str
    amount: decimal.Decimal
    income_expense: decimal.Decimal
    category_main: str
    category_sub: str


_UNKNOWN_TRANSACTION_ERROR_MSG = "could not find key `expense` or `income` in entry"


class Config(TypedDict):
    """
    This is the type of the dictionary used to configure the importer.
    """

    list_id: str
    """
    The identifier of this list. It is a random-looking hash-like value, which appears under the key `"list_id"`.
    """

    account_number: str
    """
    Like `list_id`, a value that looks like a hashed value, uniquely identifying you. Look for `"member_id"` keys in the input file.
    """

    main_account: str
    """
    This is the account representing your WieBetaaltWat balance.
    """

    transfer_account: str
    """
    The account which can be used to 'transer' money to and from another
    account. If you have added an expense to the list, this will be the input
    posting, with the other legs dividing the amount among the participants.
    """

    list_name: str
    """
    The name of this list. This will be used to fill the transaction payee.
    """


class Importer(jsonreader.Importer, banking.Importer):
    IMPORTER_NAME = "WieBetaaltWat Importer"
    FILE_EXTS: Sequence[str] = [""]

    @override
    def __init__(self, config: Config):
        super().__init__(config)

    @override
    def custom_init(self):
        self.max_rounding_error = 0.04
        self.filename_pattern_def = r"list_items"
        self.date_format = "%Y-%m-%d"

    @override
    def deep_identify(self, file: str):
        return (
            "data" in self.json_data
            and len(self.json_data["data"]) > 0
            and (
                "expense" in self.json_data["data"][0]
                and "list_id" in self.json_data["data"][0]["expense"]
                and self.json_data["data"][0]["expense"]["list_id"] == self.config["list_id"]
            )
        ) or (
            "income" in self.json_data["data"][0]
            and "list_id" in self.json_data["data"][0]["income"]
            and self.json_data["data"][0]["income"]["list_id"] == self.config["list_id"]
        )

    @override
    def get_transactions(self):
        for element in self.get_json_elements("data"):
            item = None
            prefix = None
            mul = None
            if "expense" in element:
                item = element["expense"]
                prefix = "payed"
                mul = decimal.Decimal(1)
            elif "income" in element:
                item = element["income"]
                prefix = "received"
                mul = decimal.Decimal(-1)

            if item is None or mul is None:
                raise ValueError(_UNKNOWN_TRANSACTION_ERROR_MSG)

            # The amount of the first posting will always be the change in
            # balance. If we created the posting, we will add an external
            # account with the total.

            def decimal_from_fractional(s: str) -> decimal.Decimal:
                return (decimal.Decimal(s) / decimal.Decimal(100)).quantize(decimal.Decimal(".01"))

            # The total amount.
            total = mul * decimal_from_fractional(item["amount"]["fractional"])

            # Find my share
            my_share = None
            for share_outer in item["shares"]:
                share = share_outer["share"]
                if share["member_id"] == self.config["account_number"]:
                    my_share = mul * decimal_from_fractional(share["amount"]["fractional"])

            by_id = item[f"{prefix}_by_id"]
            amount = decimal.Decimal(0).quantize(decimal.Decimal("0.01"))
            if my_share is not None:
                amount -= my_share
            else:
                my_share = decimal.Decimal(0).quantize(decimal.Decimal("0.01"))
            if by_id == self.config["account_number"]:
                amount += total
            yield _TransactionOt(
                payee=item["name"],
                memo=f"WieBetaaltWat: {self.config['list_name']}",
                by_id=by_id,
                income_expense=my_share,
                amount=amount,
                total=total,
                currency=item["amount"]["currency"],
                date=dt.datetime.strptime(item[f"{prefix}_on"], self.date_format).replace(
                    tzinfo=pytz.timezone("Europe/Amsterdam")
                ),
                category_main=item["category"]["main_description"],
                category_sub=item["category"]["sub_description"],
            )

    @override
    def build_metadata(self, file, metatype=None, data=None):
        if data is None:
            data = {}
        metadata = super().build_metadata(file, metatype, data)
        metadata["category_main"] = data["transaction"].category_main
        metadata["category_sub"] = data["transaction"].category_sub
        return metadata

    @override
    def add_custom_postings(self, entry, ot: _TransactionOt):
        if ot.by_id == self.config["account_number"]:
            data.create_simple_posting(entry, self.config["transfer_account"], -ot.total, ot.currency)
