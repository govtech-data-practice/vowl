---
title: Capping Failed Rows
description: >-
  What max_failed_rows changes, and what it leaves alone.
---

# Capping Failed Rows

This page explains how to cap the failed rows of each check, and what the cap
changes.

By default the
[row query](check-results.md#two-queries-from-one)
of each check returns every failed row. On a
very large table with many failed rows, you can cap how many vowl downloads
for each check with
[`max_failed_rows`](../../run-settings.md#max_failed_rows):

```python
from vowl import ValidationConfig

config = ValidationConfig(max_failed_rows=100)
result = validate_data("orders.yaml", df=df, config=config)
```

## What the cap changes in the query output

- **The scalar count stays the same,** and so does pass or fail. The count
  query is never capped.
- **The failed rows are cut short.** `show_failed_rows()`,
  `get_output_dfs()` and residues return at most the cap for each check.

## What the cap changes in the attributed rows

- **The row counts mostly stay the same.** Checks whose rows the data
  source attributes (`server_predicate` and `server_lookup`) don't use the downloaded
  failed rows, and neither do scalar counts under
  `row_counts="scalar"`. A `client_lookup` check that is cut
  short is [not attributable](counting-mechanisms.md#fallbacks), with the
  `reason` `truncated by max_failed_rows`. It is left out of the row counts,
  which become [approximate](counting-mechanisms.md#exact-numbers). Its
  failed rows are still annotated, up to the cap.
- **The annotated output can miss rows.** `get_annotated_output()` warns if
  a check that annotates rows had more failed rows than the cap. The
  annotated table is still built, but the check's rows past the cap look
  clean. Each of the check's `check_info` items carries `"truncated": true`,
  so you can tell which marks are incomplete. Remove the cap
  (`max_failed_rows=-1`, the default) to flag every row.

## How vowl knows the rows were cut short

vowl asks the data source for one row more than the cap. If that extra row
comes back, the check had more failed rows than the cap, and vowl drops the
extra row. So a check with exactly as many failed rows as the cap is not
reported as cut short, and one with more always is. The scalar count is
not used for this, because it does not always equal the number of failed
rows, for example under `DISTINCT` or a join.

`max_rows_for_statistics` is deprecated and no longer caps the row counts. vowl always counts
the whole table.
