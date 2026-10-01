---
title: Capping Failed Rows
description: >-
  What max_failed_rows changes, and what it leaves alone.
---

# Capping Failed Rows

By default the failed rows query of each check returns every failed row. On
very large tables you can cap how many vowl downloads for each check:

```python
from vowl import ValidationConfig

config = ValidationConfig(max_failed_rows=100)
result = validate_data("orders.yaml", df=df, config=config)
```

With a cap:

- **Pass or fail does not change.** It comes from each check's count, which is
  never capped.
- **The row counts do not change** for checks the data source counts itself
  (the `pushdown` and `table_match` [routes](levels.md#c1-collect-each-checks-failed-rows)).
  A check whose failed rows vowl had to download (`fetched_rows`) can be cut
  short. Its numbers are then [not exact](levels.md#exact-numbers).
- **`get_annotated_output()` stops with an error** if a check that annotates rows
  had more failed rows than the cap. Otherwise the rows past the cap would look
  clean. See [Annotating your table](levels.md#annotating-your-table). Remove the cap (`max_failed_rows=-1`, the default) to get annotated
  tables.

`max_rows_for_statistics` no longer caps the row counts. vowl always counts
the whole table.
