"""
This module contains two importers for NS journeys. They work well together,
although they are usable seperately as well.

- The [`travel_history`](ns/travel_history.html) importer imports `.csv` files from the website and
  creates individual transactions for your travels. These add additional
  metadata such as check-in and check-out time.
- The [`invoice`](ns/invoice.html) importer imports the invoice `.pdf` files. Here the individual
  journeys are collected in one transaction, combined with the fixed
  subscription costs and put against a single total posting.

When you want to use both, the first can be used to add `Expenses` postings for
every journey, and take this money from a `Liabilities` account. The second
importer can then take these liabilities, add additional `Expenses` and combine
it into a single posting (say, `Liabilities:Zero-Sum-Accounts:NS`). The
`zerosum` plugin can nicely find a match with the monthly payment to your
chequing account.

## Debugging mistakes
Although this system should work quite well most of the time, there are no
balance entries on the `.csv` files, nor is it guaranteed that your 'balance'
is zero after an invoice. This is because an invoice does not take into account
the most recent transactions. This means that sometimes, you may get some
errors:
- *Invoice amount does not match:* The invoice was not read correctly. Check
  the invoice manually to see if anything was missed. This should be rare and
  easy to fix.
- *Transactions missing or double in `.csv`:* Some journeys may take a while to
  appear on the website, or you may have included journeys twice. This means
  that there is some trailing balance for the `Liabilities` account. Check
  that every invoice brings the `Liabilities` account close to zero. The
  remainder should be explained by recent transactions that were not yet added
  to this invoice, i.e. from the last few days.
"""
