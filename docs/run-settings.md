---
description: The ValidationConfig settings that apply to a whole vowl run. TRY_CAST, capping failed rows, row counts, save defaults and table sizes.
---

# Run Settings (`ValidationConfig`)

`ValidationConfig` holds the settings that apply to a whole run. Pass it to
`validate_data` as `config=`:

```python
from vowl import ValidationConfig, validate_data

config = ValidationConfig(max_failed_rows=100, row_counts="scalar")
result = validate_data("contract.yaml", df=df, config=config)
```

Every setting has a default, so you only set the ones you want to change.

## Running checks

These change what each check returns.

| Setting                                       | Default       | What it does                                                                                                                                                                                                                                                                       |
| --------------------------------------------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <a id="use_try_cast"></a>`use_try_cast`       | `True`        | Turns `CAST` into `TRY_CAST` in check queries. A value that cannot be converted then becomes a failed row instead of making the check end in `ERROR`.                                                                                                                              |
| <a id="max_failed_rows"></a>`max_failed_rows` | `-1` (no cap) | The most failed rows vowl downloads for each check. Use it on very large tables. A capped check can make the row counts approximate, and its rows past the cap look clean in the annotated output. See [Capping Failed Rows](design-considerations/checks/capping-failed-rows.md). |

## Row counts

These change the row counts: `failed_rows`, `passed_rows` and `pass_rate` per
table, dimension and check. They apply to `print_summary`, the DQ metrics and
`get_dq_metrics_df`.

| Setting                                                         | Default        | What it does                                                                                                                                                                                                                                                                                                                                                                      |
| --------------------------------------------------------------- | -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <a id="row_counts"></a>`row_counts`                             | `"attributed"` | How vowl counts rows. `"attributed"` attributes each failed row to its table row, so a row that fails two checks counts once. `"scalar"` adds up the scalar counts of the failed checks instead. It runs no extra queries, but the row counts can be approximate. `"off"` computes no row counts. See [Counting Mechanisms](design-considerations/checks/counting-mechanisms.md). |
| <a id="attribute_tolerated_rows"></a>`attribute_tolerated_rows` | `False`        | `True` also attributes the tolerated rows of checks that passed, so they count as failed rows and are flagged in the annotated output. Applies only under `row_counts="attributed"`. See [Tolerated rows](design-considerations/checks/check-results.md#tolerated-rows).                                                                                                          |

## Saving results

These set what `save()` and `get_annotated_output()` do when you do not pass
the matching argument. An argument you pass always wins.

| Setting                                                 | Default       | What it does                                                                                                                                                                       |
| ------------------------------------------------------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <a id="output_mode"></a>`output_mode`                   | `"annotated"` | What `save()` writes. `"failed_rows"` and `"both"` are deprecated. See [Saving results](results.md#saving-results) and [Deprecated](results.md#deprecated).                        |
| <a id="annotated_check_info"></a>`annotated_check_info` | `"names"`     | How much detail the `check_info` column holds. See [What the annotated output holds](design-considerations/checks/annotating-the-source-table.md#what-the-annotated-output-holds). |

## Deprecated

These still work but emit a `DeprecationWarning`.

| Setting                                                                               | Default       | Use instead                                                                                                    |
| ------------------------------------------------------------------------------------- | ------------- | -------------------------------------------------------------------------------------------------------------- |
| <a id="enable_additional_schema_statistics"></a>`enable_additional_schema_statistics` | not set       | [`row_counts="off"`](#row_counts). `False` sets `row_counts="off"`.                                            |
| <a id="max_rows_for_statistics"></a>`max_rows_for_statistics`                         | `-1` (no cap) | Nothing. It only caps the table size in the summary, which makes the row counts approximate. Leave it at `-1`. |
