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

| Setting              | Default           | What it does                                                                                                                                                                                                    |
| -------------------- | ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `max_failed_rows`    | `-1` (no cap)     | The most failed rows vowl downloads for each check. Use it on very large tables. See [Capping Failed Rows](design-considerations/failed-rows/capping.md).                                                       |
| `row_issue_scope`    | `"failed_checks"` | Which failed rows count toward the row counts. `"all_violations"` also counts the tolerated rows of checks that passed. See [Tolerated rows](design-considerations/failed-rows/which-checks.md#tolerated-rows). |
| `use_try_cast`       | `True`            | Turns `CAST` into `TRY_CAST` in check queries. A value that cannot be converted then becomes a failed row instead of making the check end in `ERROR`.                                                           |
| `row_count_accuracy` | `"accurate"`      | How the row counts count checks that are not plain row filters. `"balanced"` and `"fast"` download less. See [Row count accuracy](#row-count-accuracy).                                                         |

## Saving results

These set what `save()` and `get_annotated_output()` do when you do not pass
the matching argument. An argument you pass always wins.

| Setting                | Default       | What it does                                                                                                                                                       |
| ---------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `output_mode`          | `"annotated"` | What `save()` writes. `"failed_rows"` and `"both"` are deprecated. See [Saving results](results.md#saving-results) and [Deprecated](results.md#deprecated).        |
| `annotated_check_info` | `"names"`     | How much detail the `check_info` column holds. See [What the annotated output holds](design-considerations/failed-rows/levels.md#what-the-annotated-output-holds). |

## Row statistics

The summary shows how many rows each table has. These settings control that
count.

| Setting                               | Default       | What it does                                                     |
| ------------------------------------- | ------------- | ---------------------------------------------------------------- |
| `enable_additional_schema_statistics` | `True`        | Counts the rows in each table for the summary. `False` skips it. |
| `max_rows_for_statistics`             | `-1` (no cap) | The most rows counted in each table for the summary.             |

### Row count accuracy

`row_count_accuracy` sets how the row counts (`failed_rows`, `passed_rows` and
`pass_rate` per table, dimension and check) count each check. It applies to
`print_summary`, the OTel gauges and `get_row_quality_df`.

A check that is a plain row filter of its table is counted inside the data
source wherever the data source supports it. The setting only changes how vowl counts the other checks, such as
checks with `DISTINCT`, joins, cross-source checks and checks on sources
without pushdown.

| Level                  | Other checks are counted by                                                                                                              | Downloads the whole table                                 |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| `"accurate"` (default) | Matching their failed rows onto the downloaded table                                                                                     | When any counted check is not a plain row filter          |
| `"balanced"`           | Matching in the data source where it can. Matching onto the downloaded table where vowl would otherwise use only the fetched failed rows | Only for the checks that would otherwise use fetched rows |
| `"fast"`               | Matching in the data source, or the fetched failed rows                                                                                  | Never                                                     |

vowl downloads each table at most once per run. Annotated output reuses the
same download, so counting and annotated output flag the same rows.

Each number carries an `exact` flag, and `print_summary` shows `(approx.)` next
to a number that is not exact. Under `"accurate"` a number is approximate only
in these cases:

- The table could not be downloaded. The check then falls back to its fetched
  failed rows.
- A check's fetched rows were cut short by `max_failed_rows`.
- A check returns values that no table row holds, for example `c * 1.5` or
  `upper(s)`. vowl counts the rows that do match.

Under `"balanced"` and `"fast"` a check that is not a plain row filter and uses
the fetched failed rows is always approximate. A check that returns values no
table row holds is approximate at every level.

Downloading a table holds all of it in memory on the machine running vowl. At
6 columns this took about 3 s and 1 to 1.5 GiB for 1 million rows, and about
13 s and 3.5 GiB for 5 million rows. Use `"balanced"` or `"fast"` for large
tables. Use `"fast"` to never download a table. A table whose checks are all
plain row filters is never downloaded.

See [Which route a check takes](design-considerations/failed-rows/levels.md#which-route-a-check-takes)
for the routes behind each level.
