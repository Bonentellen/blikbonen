# Blikbonen

This python package contains an assortment of `beangulp` importers and `beanprice` price sources. See the [documentation](https://bonentellen.github.io/blikbonen/) for usage examples.

### Importers

The importers are based on [`reds-importers`](https://github.com/redstreet/beancount_reds_importers/), although they are compatiple with [`beangulp`](https://github.com/beancount/beangulp/). They generally do not try to categorize postings, except where we are sure of the transaction's payee, such as with an investing account with a single pay-in account. (It is also compatible with the [`zerosum`](https://github.com/redstreet/beancount_reds_plugins) plugin.)

The importers included are the following:

- `blikbonen.importers.asnbank.chequing`, for a Dutch ASN Bank chequing account.
- `blikbonen.importers.asnbank.investing`, for a Dutch ASN Bank investing account.
- `blikbonen.importers.coinbase`, for Coinbase account statements.
- `blikbonen.importers.degiro.account`, for the DeGiro account statements, containing transfers & transactions.
- `blikbonen.importers.degiro.portfolio`, for the DeGiro portfolio files, for importing balances.
- `blikbonen.importers.ns.invoice`, for `.pdf` invoices from the Nederlandse Spoorwegen.
- `blikbonen.importers.ns.travel_history`, for NS tavel history declarations.
- `blikbonen.importers.wiebetaaltwat`, for importing WieBetaaltWat history.

### Price sources

There are also price sources, for use with [`beanprice`](https://github.com/beancount/beanprice). These are the following:

- `blikbonen.prices.co2`, for carbon pricing.
- `blikbonen.prices.hicp`, for the Dutch consumer price index.

## Testing, linting, formatting

This code contains few if any tests due to the changing and generally unreliable nature of import formats, but is supported by (private) regression tests. Feel free to propose or discuss changes by opening a PR or an issue. If you would like to add an importer however, I would like at least one (regression) test, since I am unlikely to test these in another way.

This code is linted and formatted, through the following commands:

- Linting:
```shell
ruff check
```
- Formatting:
```shell
ruff format
```
