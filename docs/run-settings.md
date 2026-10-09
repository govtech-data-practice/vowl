---
description: The ValidationConfig settings that apply to a whole vowl run. TRY_CAST, capping failed rows, row counts, save defaults and table sizes.
---

# Run Settings (`ValidationConfig`)

`ValidationConfig` holds the settings that apply to a whole run. Pass it to
`validate_data` as `config=`:

```python
from vowl import ValidationConfig, validate_data

config = ValidationConfig(max_failed_rows=100)
result = validate_data("contract.yaml", df=df, config=config)
```

Every setting has a default, so you only set the ones you want to change.

## Running checks

These change what each check returns.

| Setting                                       | Default       | What it does                                                                                                                                                                                                                                                                       |
| --------------------------------------------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <a id="use_try_cast"></a>`use_try_cast`       | `True`        | Turns `CAST` into `TRY_CAST` in check queries. A value that cannot be converted then becomes a failed row instead of making the check end in `ERROR`.                                                                                                                              |
| <a id="max_failed_rows"></a>`max_failed_rows` | `-1` (no cap) | The most failed rows vowl downloads for each check. Use it on very large tables. A capped check can make the row counts approximate, and its rows past the cap look clean in the annotated output. See [Capping Failed Rows](design-considerations/checks/capping-failed-rows.md). |

## Row attribution

These change how vowl attributes failed rows to the rows of each table. They
change the [row attribution](results.md#what-each-method-costs) of
`get_dq_metrics`, `get_dq_metrics_df`, `export_otel` and the `"annotated_table"`
and `"dq_metrics"` outputs of `save()`. There they change the row counts,
`failed_rows`, `passed_rows` and `pass_rate` per table, dimension and check.
`fetch_tolerated_rows` also adds tolerated rows to `get_output_dfs`,
`get_consolidated_output_dfs` and `show_failed_rows`. No setting changes each
check's scalar `failed_rows_count` or its status. See
[How Attributed Rows Work](design-considerations/checks/how-attributed-rows-work.md).

| Setting                                                 | Default | What it does                                                                                                                                                                                                                                                                                                                                                                        |
| ------------------------------------------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <a id="fetch_tolerated_rows"></a>`fetch_tolerated_rows` | `False` | `True` also fetches the tolerated rows of checks that passed, an extra query per check. Every output then holds them, each marked as tolerated, and they count as failed rows in the DQ metrics. The check stays `PASSED`. OpenTelemetry's `failed_rows_sample` still holds failed checks only. See [Tolerated rows](design-considerations/checks/check-results.md#tolerated-rows). |

## Saving results

These set what `save()` and `get_annotated_output()` do when you do not pass
the matching argument. An argument you pass always wins.

| Setting                                                 | Default                                   | What it does                                                                                                                                                                       |
| ------------------------------------------------------- | ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <a id="outputs"></a>`outputs`                           | every output but `"failed_query_outputs"` | The list of files `save()` writes. See [Outputs](results.md#outputs).                                                                                                              |
| <a id="annotated_check_info"></a>`annotated_check_info` | `"names"`                                 | How much detail the `check_info` column holds. See [What the annotated output holds](design-considerations/checks/annotating-the-source-table.md#what-the-annotated-output-holds). |

## Deprecated

These emit a `DeprecationWarning` and have no effect.

| Setting                                                                               | Default       | Use instead                                                        |
| ------------------------------------------------------------------------------------- | ------------- | ------------------------------------------------------------------ |
| <a id="enable_additional_schema_statistics"></a>`enable_additional_schema_statistics` | not set       | Nothing. Row counts are computed only when you ask for DQ metrics. |
| <a id="max_rows_for_statistics"></a>`max_rows_for_statistics`                         | `-1` (no cap) | Nothing. vowl no longer caps the table size.                       |

<a id="output_mode"></a>`output_mode` still works with a `FutureWarning` and
maps to the outputs it wrote in v0.0.6. Use `outputs` instead. See
[Deprecated output_mode](results.md#deprecated-output_mode).
