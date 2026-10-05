---
title: Annotating the Source Table
description: >-
  How get_annotated_output() writes the checks each row failed into your
  table, what becomes a residue, and when the annotated rows and the row
  counts differ.
---

# Annotating the Source Table

The annotated output shows a check's
[attributed rows](failed-row-results.md#failed-row-results) on your table.
The [row counts](how-rows-are-counted.md) tell you _how many_ rows
have issues, and the annotated output tells you _which_ ones. A check whose
failed rows can never be attributed keeps them as a [residue](#residues).
vowl takes your whole table and adds a `check_info`
column listing the checks each row failed. A row with an empty `check_info`
is a **clean row**.

For the [example](failed-row-results.md#the-example-used-in-this-section), the
annotated `orders` table is:

| order_id | item  | price | quantity | check_info                                                                         |
| -------- | ----- | ----- | -------- | ---------------------------------------------------------------------------------- |
| 1        | apple | 1.50  | 3        |                                                                                    |
| 2        | bread | -2.00 | 1        | `[{"check_name": "price_must_be_positive"}, {"check_name": "order_id_is_unique"}]` |
| 3        | milk  | 0.00  |          | `[{"check_name": "price_must_be_positive"}, {"check_name": "quantity_is_filled"}]` |
| 4        | eggs  | 3.20  | 2        |                                                                                    |
| 2        | bread | -2.00 | 1        | `[{"check_name": "price_must_be_positive"}, {"check_name": "order_id_is_unique"}]` |

Every copy of an attributed row is annotated, so there are 3 annotated rows, the
same as the 3 attributed rows in the [row counts](how-rows-are-counted.md).

To keep only the clean rows:

```python
annotated = result.get_annotated_output()["annotated"]["orders"].to_pandas()
clean = annotated[annotated["check_info"].isna()].drop(columns=["check_info"])
# 2 clean rows: apple and eggs
```

## Where each failed check ends up

Each failed check ends up in exactly one place:

| Where                                  | Which checks                                                                                                                                                                                 | Example                              |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------ |
| **Annotated on the table**             | [Counted checks](failed-row-results.md) whose failed rows hold the [match key](how-rows-are-counted.md#the-match-key). Most checks.                                                          | `price_must_be_positive`             |
| **A residue**, in `output["residues"]` | Checks whose failed rows can never be attributed to the table, such as rows with fewer columns                                                                                               | A check that returns only town names |
| **The summary only**                   | Checks that return no rows, such as an `AVG`, `SUM`, `MIN` or `MAX` aggregate, checks whose failed rows are good rows, such as a `rowCount` under `mustBeGreaterThan`, and checks in `ERROR` | `average_price_in_range`             |

!!! tip "Read the summary for the final result"

    A check in the last group leaves no mark on the table and has no residue.
    You would miss it if you only looked at the annotated table.

## How vowl annotates

Annotating runs on your machine, once per table. It uses the failed rows each
check returned, not the [routes](counting-mechanisms.md) used for counting.

1. **Download the whole table**, with your filter conditions applied. If
   counting already downloaded it, vowl reuses that copy, so a table is
   downloaded at most once per run.
2. **Keep the checks whose rows can be annotated.** These are the counted
   checks whose failed rows hold the match key, the same rule counting uses.
   A check that is not attributed only on this run, for example because
   `max_failed_rows` cut it short, is still kept.
3. **Warn if a check was cut short.** If a kept check caught more rows than
   `max_failed_rows`, `get_annotated_output()` warns, and the check's
   `check_info` items carry `"truncated": true`. Its rows past the cap look
   clean. See [Capping Failed Rows](capping-failed-rows.md).
4. **List each distinct failed row once**, with the names of the checks that
   caught it, as in [Merging the attributed rows](how-rows-are-counted.md#merging-the-failed-rows).
5. **Attribute and annotate.** vowl goes through every row of the table. If
   its match key values match a row in the list, vowl writes those check
   names into its `check_info`. The bread row in the list matches both rows 2
   and 5.
6. **Keep the rest as residues.**

When two checks return different types for the same column, such as `int32`
and `int64`, vowl converts each check's rows to the downloaded table's types
before it merges them.

### When the table can't be downloaded

vowl logs a warning and makes no annotated table for that schema. Its failed
rows become residues. This happens, for example, when no adapter was given
for the table (the log says "No adapter for schema ..., so its table cannot
be exported."), or the table has a DuckDB `INTERVAL`, `BIT` or `UNION` column
(the log says "Could not export the table of ...").

## Residues

A **residue** holds the failed rows of one check that could not be annotated.
vowl makes one for:

- counted checks left out in step 2,
- counted checks of a table that could not be downloaded,
- other failed checks that returned rows, unless those rows are the good
  rows, such as a check under `mustBeGreaterThan`.

An `AVG`, `SUM`, `MIN` or `MAX` aggregate returns no rows, so it has no
residue.

Each residue holds one check, without duplicate rows. Residues of different
checks are never merged.

## What the annotated output holds

```python
output = result.get_annotated_output()
output["annotated"]   # {"<schema>": your table + check_info}
output["residues"]    # {"<schema>::<check_name>": failed rows + check_info + tables_in_query}
```

<!-- prettier-ignore-start -->
<!-- Zensical needs the table indented 4 spaces to stay inside the list item. -->

- **Every schema gets an annotated table**, even when nothing failed. Then
  `check_info` is empty on every row.
- **`check_info` is a JSON list**, with one item per check the row failed.
  Choose what each item holds with `check_info=` (or
  [`annotated_check_info`](../../run-settings.md#annotated_check_info) in
  `ValidationConfig`):

    | `check_info`        | Each item holds                                             |
    | ------------------- | ----------------------------------------------------------- |
    | `"names"` (default) | `check_name`                                                |
    | `"summary"`         | `check_name`, `dimension`, `tags` and `target` (the column) |
    | `"full"`            | The whole check definition, plus `check_name` and `target`  |

- **Each residue holds one check**: its failed rows, a `check_info` column,
  and a `tables_in_query` column naming the tables the check read.
- **Tolerated rows** are annotated only under
  [`row_issue_scope="all_violations"`](../../run-settings.md#row_issue_scope). Their `check_info` item carries
  `"tolerated": true`. See [Tolerated rows](failed-row-results.md#tolerated-rows).
- **Checks cut short by `max_failed_rows`** carry `"truncated": true` in
  their `check_info` items, and `get_annotated_output()` warns. See
  [Capping Failed Rows](capping-failed-rows.md).
- **Unique-value, primary key and `duplicateValues` checks annotate rows.**
  vowl fetches every row whose value repeats (and, for a primary key, every
  row where it is empty), so the row counts and the annotated rows agree. The
  exception is `duplicateValues` with `unit: percent`, which gives a share,
  not rows.

<!-- prettier-ignore-end -->

## When counting and annotating differ

Usually `failed_rows` in `get_row_quality_df()` equals the number of
annotated rows. Here is when it doesn't:

| Situation                                                                      | Row counts                                                                                                                                                                                                                | Annotated table                                                                                           |
| ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| A check caught more rows than `max_failed_rows`                                | Right on `server_predicate` and `server_lookup`. On `client_lookup` the check is [not attributed](counting-mechanisms.md#fallbacks) and left out, and the numbers are [approximate](counting-mechanisms.md#exact-numbers) | A warning. The rows past the cap look clean, and the check's `check_info` items carry `"truncated": true` |
| The table has a column vowl can't download (`INTERVAL`, `BIT`, `UNION`)        | Right for plain filters. Other checks are [not attributed](counting-mechanisms.md#fallbacks) and left out                                                                                                                 | None. The failed rows become residues                                                                     |
| A check's failed rows can never be attributed, such as rows with fewer columns | The check is left out, and the numbers are approximate                                                                                                                                                                    | None. The failed rows become a residue                                                                    |
| An untested data source treats different values as equal (`a` and `A`)         | Two rows can count as one, marked not exact. When the table was downloaded, both rows count                                                                                                                               | Both rows are annotated                                                                                   |
| With attribution disabled, two checks catch the same row                       | The row counts twice, capped at the table's row count. Marked not exact                                                                                                                                                   | The row is annotated once                                                                                 |
| With attribution disabled, a `DISTINCT` check returns one of three copies      | One copy is counted. Marked not exact                                                                                                                                                                                     | All three copies are annotated                                                                            |
| The table changes while vowl reads it                                          | Read at one moment                                                                                                                                                                                                        | Read again later                                                                                          |

vowl can't detect the last case. It only matters if the table is written to
during the run.

Use the **row counts** for how many rows have issues, and for pass rates.
They stay cheap on large tables with
[`disable_table_attributed_counts`](../../run-settings.md#disable_table_attributed_counts)
set, or when every check is a plain filter. Use the **annotated table** or the
**residues** for which rows have issues.

## Other ways to see failed rows

The annotated output downloads your whole table. On a large table, these
give you the failed rows without it:

| Method                                | What you get                                                                    |
| ------------------------------------- | ------------------------------------------------------------------------------- |
| `result.show_failed_rows(max_rows=5)` | Prints a few failed rows for each failed check. `max_rows=-1` prints them all.  |
| `result.get_output_dfs()`             | Each check's failed rows as a separate table, under `"<schema>::<check_name>"`. |

`result.save()` saves the annotated tables, residues and `summary.json` as
files. The older `output_mode="failed_rows"` and
`get_consolidated_output_dfs()` will be removed.
