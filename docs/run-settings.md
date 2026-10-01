---
description: The ValidationConfig settings that apply to a whole vowl run. Capping failed rows, counting tolerated rows, TRY_CAST, save defaults and row statistics.
---

# Run Settings

`ValidationConfig` holds the settings that apply to a whole run. Pass it to
`validate_data` as `config=`:

```python
from vowl import ValidationConfig, validate_data

config = ValidationConfig(max_failed_rows=100, row_issue_scope="all_violations")
result = validate_data("contract.yaml", df=df, config=config)
```

Every setting has a default, so you only set the ones you want to change.

## Checks and failed rows

| Setting           | Default           | What it does                                                                                                                                                                                                         |
| ----------------- | ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `max_failed_rows` | `-1` (no cap)     | The most failed rows vowl downloads for each check. Use it on very large tables. See [Capping Failed Rows](design-considerations/failed-rows/capping.md).                                                            |
| `row_issue_scope` | `"failed_checks"` | Which failed rows count toward the row counts. `"all_violations"` also counts the tolerated rows of checks that passed. See [Tolerated rows](design-considerations/failed-rows/which-checks.md#tolerated-rows).      |
| `use_try_cast`    | `True`            | Turns `CAST` into `TRY_CAST` in check queries. A value that cannot be converted then becomes a failed row instead of making the check end in `ERROR`.                                                                |

## Saving results

These set what `save()` and `get_annotated_output()` do when you do not pass
the matching argument. An argument you pass always wins.

| Setting                | Default         | What it does                                                                                                                                                       |
| ---------------------- | --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `output_mode`          | `"annotated"`   | What `save()` writes. `"failed_rows"` and `"both"` are deprecated. See [Saving results](results.md#saving-results) and [Deprecated](results.md#deprecated).        |
| `annotated_check_info` | `"names"`       | How much detail the `check_info` column holds. See [What the annotated output holds](design-considerations/failed-rows/levels.md#what-the-annotated-output-holds). |

## Row statistics

The summary shows how many rows each table has. These settings control that
count.

| Setting                               | Default       | What it does                                                         |
| ------------------------------------- | ------------- | -------------------------------------------------------------------- |
| `enable_additional_schema_statistics` | `True`        | Counts the rows in each table for the summary. `False` skips it.     |
| `max_rows_for_statistics`             | `-1` (no cap) | The most rows counted in each table for the summary.                 |
