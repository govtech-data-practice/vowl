---
description: The ValidationConfig settings that apply to a whole vowl run. TRY_CAST, capping failed rows, row counts, save defaults and table sizes.
---

# Run Settings (`ValidationConfig`)

`ValidationConfig` holds the settings that apply to a whole run. Pass it to
`validate_data` as `config=`:

```python
from vowl import ValidationConfig, validate_data

config = ValidationConfig(max_failed_rows=100, row_issue_scope="all_violations")
result = validate_data("contract.yaml", df=df, config=config)
```

Every setting has a default, so you only set the ones you want to change.

## Running checks

These change what each check returns.

| Setting                                       | Default       | What it does                                                                                                                                                                                                                                                                            |
| --------------------------------------------- | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <a id="use_try_cast"></a>`use_try_cast`       | `True`        | Turns `CAST` into `TRY_CAST` in check queries. A value that cannot be converted then becomes a failed row instead of making the check end in `ERROR`.                                                                                                                                   |
| <a id="max_failed_rows"></a>`max_failed_rows` | `-1` (no cap) | The most failed rows vowl downloads for each check. Use it on very large tables. A capped check can make the row counts approximate, and its rows past the cap look clean in the annotated output. See [Capping Failed Rows](design-considerations/failed-rows/capping-failed-rows.md). |

## Row counts

These change the row counts: `failed_rows`, `passed_rows` and `pass_rate` per
table, dimension and check. They apply to `print_summary`, the DQ metrics and
`get_row_quality_df`.

| Setting                                                                       | Default           | What it does                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ----------------------------------------------------------------------------- | ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <a id="row_issue_scope"></a>`row_issue_scope`                                 | `"failed_checks"` | Which failed rows count. `"all_violations"` also counts the tolerated rows of checks that passed. It also decides which checks the annotated output flags. See [Tolerated rows](design-considerations/failed-rows/failed-row-results.md#tolerated-rows).                                                                                                                                                                                                                                                                          |
| <a id="disable_table_attributed_counts"></a>`disable_table_attributed_counts` | `False`           | `True` skips attributing failed rows to the source table, which can download a table or run extra queries. Every counted check is then not attributed, and the row counts add up each check's scalar count, so they can be approximate. With the default `False`, a check whose failed rows vowl can't attribute is left out of the row counts and counted in `checks_not_attributed`. The annotated output and check results do not change. See [Counting Mechanisms](design-considerations/failed-rows/counting-mechanisms.md). |

## Saving results

These set what `save()` and `get_annotated_output()` do when you do not pass
the matching argument. An argument you pass always wins.

| Setting                                                 | Default       | What it does                                                                                                                                                                            |
| ------------------------------------------------------- | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <a id="output_mode"></a>`output_mode`                   | `"annotated"` | What `save()` writes. `"failed_rows"` and `"both"` are deprecated. See [Saving results](results.md#saving-results) and [Deprecated](results.md#deprecated).                             |
| <a id="annotated_check_info"></a>`annotated_check_info` | `"names"`     | How much detail the `check_info` column holds. See [What the annotated output holds](design-considerations/failed-rows/annotating-the-source-table.md#what-the-annotated-output-holds). |

## Table sizes

The summary shows how many rows each table has. These settings control that
count. They do not change the row counts above.

| Setting                                                                               | Default       | What it does                                                     |
| ------------------------------------------------------------------------------------- | ------------- | ---------------------------------------------------------------- |
| <a id="enable_additional_schema_statistics"></a>`enable_additional_schema_statistics` | `True`        | Counts the rows in each table for the summary. `False` skips it. |
| <a id="max_rows_for_statistics"></a>`max_rows_for_statistics`                         | `-1` (no cap) | The most rows counted in each table for the summary.             |
