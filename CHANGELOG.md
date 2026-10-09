# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed (Breaking)
- Results come in two tiers. `print_summary()`, `get_check_results_df()`, `result.summary` and `save(outputs=["consolidated_query_outputs"])` run the check queries only and never attribute rows to the table. `get_dq_metrics()`, `get_dq_metrics_df()`, `export_otel()` and `save()` with `"annotated_table"` or `"dq_metrics"` attribute the rows once per result and share the numbers. See [Results](docs/results.md).
- `print_summary()` no longer prints the **Unique Passed Rows** line. Each schema's **Overall** block shows **Failed Rows (approximate)** instead, the sum of the scalar counts of its failed row-level checks. A check that passed within its tolerance adds nothing. A row that fails two checks counts twice. For exact row counts and pass rates use `get_dq_metrics_df(by="schema")`.
- summary.json and `result.summary["validation_summary"]`: `failed_rows` is renamed `failed_rows_approximate`. The value is the same sum of scalar counts.
- summary.json and `result.summary["validation_summary"]` no longer have `total_rows_by_schema`. `validate_data()` no longer counts each table's rows. The DQ metrics count them when they need them, without a cap. Table totals are the `total_rows` column of `get_dq_metrics_df(by="schema")` and `vowl.schema.row.count`.
- `ValidationConfig(enable_additional_schema_statistics=...)` and `ValidationConfig(max_rows_for_statistics=...)` have no effect. They still emit a `DeprecationWarning`.
- `save()` and `ValidationConfig` take `outputs=[...]`, a list of the files to write, in place of `output_mode`. `output_mode` is deprecated. Its names still work with a `FutureWarning` and keep the files they wrote in 0.0.6: `"failed_rows"` is `outputs=["consolidated_query_outputs"]`, `"annotated"` is `["annotated_table", "dq_metrics"]` and `"both"` is all three. It will be removed in a future release. Passing both `output_mode=` and `outputs=` raises `ValueError`. `ValidationConfig.to_dict()` and summary.json record `outputs`. See [Saving results](docs/results.md#saving-results).
- A `save()` with no `outputs` writes every output except `"failed_query_outputs"`, whose files `"all_query_outputs"` already writes: the per-check row files of every row-level check, the grouped failed-rows CSVs, the annotated tables with their residues, and `dq_metrics.json`. In 0.0.6 it wrote the grouped CSVs only. Pass `outputs=["consolidated_query_outputs"]` to keep that.
- `get_output_dfs()` and the `"residues"` of `get_annotated_output()` key a column-level check by `"<schema>.<column>::<check_name>"`, the check's `target` in `check_results.csv`. A schema-level check keeps `"<schema>::<check_name>"`. Two checks with the same name on different columns used to share a key, so one of them was lost.
- A contract with two quality checks of the same name on one column, or two schema-level checks of the same name, now raises `ValueError` when it loads. Their results used to overwrite each other. The same name on different columns is still allowed.
- Residue files are renamed from `<prefix>_<schema>_<check>_residue.csv` to `<prefix>_<schema>__<column>__<check>_residue.csv`, or `<prefix>_<schema>__<check>_residue.csv` for a schema-level check. The schema, the column and the check name are cleaned on their own and joined with `__`, so schema `a_b` with check `c` and schema `a` with check `b_c` no longer write the same file, and checks with the same name on different columns write different files.
- The grouped failed-rows CSVs and `get_consolidated_output_dfs()` hold one file per table set, `<prefix>_<tables>.csv`. Checks that return different columns of the same tables used to be split into `<prefix>_<tables>__1.csv`, `__2.csv` and so on, numbered by check order. They are now in one file, with nulls in the columns a check did not return, and a row a check returned only some columns of does not merge with the full row of the same record. The per-check row files keep each check's own columns.
- Per-check and residue file names spell out comparison operators, so check `amount > 0` writes `<schema>__<column>__amount_gt_0.csv`, not `amount_0`, and no longer clashes with a check named `amount_0`. Check names that still clean to the same file name, compared without case, such as `Amount` and `amount`, are numbered in contract order: the later one writes `amount_2`. `summary.json`'s `saved_outputs` maps each file to its check.
- `get_output_dfs()` leaves out a failed check whose operator sets no upper limit, such as `mustBeGreaterThan`. Its rows are the ones that passed. `get_output_dfs(scope="all")` still returns them.
- `get_output_dfs()` leaves out checks with no rows to report. It used to return an empty frame with no columns for every passed check. A passed check now appears only under `fetch_tolerated_rows=True`, when it passed within its threshold.

### Added
- `vowl.check.row.approximate`, `vowl.dimension.row.approximate`, `vowl.schema.row.approximate` and `vowl.run.row.approximate` gauges in the OTEL metrics and `dq_metrics.json`. Each is `1` when that level's row counts could be off and `0` when they are exact, so you can alert on approximate counts without traces. The check level is sent for every check with a scalar count, including one that is not attributable, and says whether the check made its schema's row counts approximate. See [Approximate row counts](docs/dq-metrics/understanding-metrics.md#approximate-row-counts).
- **Row-quality statistics**: `result.get_dq_metrics_df(by="schema" | "dimension" | "check")` reports, for each table, how many rows failed at least one row-level check, how many passed, the pass rate, and whether each number is approximate. `by="check"` lists which checks are row-level, the `attribution_method` their rows took and the `attribution_note` saying why a check was left out. The DQ metrics, the OTEL row gauges and annotated output all read the same numbers.
- Row counts are computed inside the data source where it can (the `server_predicate` and `server_lookup` attribution methods), so they are exact on large tables, count every copy of a duplicated row, and no longer depend on `max_failed_rows`. Tested on DuckDB, SQLite, Spark and Postgres. Other data sources match the failed rows onto the downloaded table and mark a number `approximate` where it could be off. See [How Attributed Rows Work](docs/design-considerations/checks/how-attributed-rows-work.md).
- On data sources without a tested key encoding (Snowflake, BigQuery, SQL Server and others), a table whose row-level checks are all plain row filters is counted in one scan that groups on which checks each row fails, not on the row values. Its numbers are exact and no longer marked **(approx.)**.
- `ValidationConfig(fetch_tolerated_rows=True)` fetches the failed rows of a check that passed within its tolerance and puts them into the row counts. Annotated output follows the setting and marks such items with `"tolerated": true`. Such a check can need its table downloaded, and the failed checks of that table then take `client_lookup` too. By default a passed check is not attributed: vowl runs no query for it, it adds nothing to the row counts or annotated output, and its `attribution_note` is `passed, not attributed`.
- `get_dq_metrics_df(by="check")` has a `scalar_count` column, the check's scalar count, next to `attributed_rows`, the rows of the table its failed rows are attributed to. They differ for a check with `DISTINCT`, a join or `COUNT(DISTINCT ...)`. `attributed_rows` is `None` when the check's rows are not in the row counts.
- The docs and the `by="check"` columns call a check whose failed rows can go into the row counts a **row-level check** (`row_level`), and one whose failed rows vowl can attribute to the table **attributable**. Earlier unreleased builds called these counted and attributed.
- `get_dq_metrics_df(by="schema" | "dimension")` has a `checks_not_attributable` column, after `checks_not_row_level`: the failed row-level checks (and passed ones under `fetch_tolerated_rows=True`) whose rows are not in the row counts. The `vowl.validate` span carries it as `row.checks_not_attributable`.
- `COUNT(DISTINCT x)` checks are now row-level checks. Their failed rows are the rows that hold the counted values, and the derived row query adds `x IS NOT NULL` for each argument. `COUNT(DISTINCT (a, b))` and `COUNT(DISTINCT ROW(a, b))` are still not row-level.
- `check_info` items of a check cut short by `max_failed_rows` carry `"truncated": true` in annotated output.
- `get_check_results_df()` has a `dimension` column, with `"unknown"` for a check that declares none.
- A new `client_lookup` attribution method in `get_dq_metrics_df(by="check")`. vowl downloads the table once, then attributes each check's failed rows to its rows by value, so every copy comes from the table. A `DISTINCT` check that returns one row for 3 identical copies counts 3, and a join that returns a row twice counts it once. When the table is downloaded, the table's total is its downloaded row count. Annotated output reuses the same download, so a run downloads each table at most once, and counting and annotated output flag the same rows.
- New `attribution_note` values for checks that cannot use `client_lookup`: `the table could not be downloaded`, `the table's rows could not be turned into match keys`, `the failed rows could not be fetched` and `truncated by max_failed_rows`. Such a check is not attributable: it adds nothing to the row counts, which are marked approximate if its rows are in scope. A check whose returned values, such as `c * 1.5` or `upper(s)`, the table may not hold is counted from the rows that do match and marked approximate. vowl reads this from the check's SELECT list: anything other than plain table columns or `*` is marked approximate on `server_lookup` and `client_lookup`.
- **DQ metrics** at four levels, check, dimension, schema and run, named `vowl.<level>.<unit>.<measure>`. New: `vowl.dimension.check.count`, `vowl.schema.check.count`, `vowl.run.check.count`, `vowl.run.check.pass_rate`, `vowl.run.row.count`, `vowl.run.row.pass_rate` and `vowl.run.schema.count`. Check counts send `PASSED`, `FAILED` and `ERROR` every run, zeros included. See [Understanding DQ Metrics](docs/dq-metrics/understanding-metrics.md).
- `save(outputs=[..., "dq_metrics"])` writes the DQ metrics to `<prefix>_dq_metrics.json`, and `result.get_dq_metrics()` returns them. It is in the default outputs. Annotated output and the file share one table download and one row attribution, so writing both costs no more than writing one. The file and the OpenTelemetry metrics come from one calculation, with the same names, attributes and values. `summary.json` is unchanged. See [Exporting to dq_metrics.json](docs/dq-metrics/json-export.md).
- `result.run_id`: every run gets an ID when it runs. `dq_metrics.json` and `export_otel()` both use it, so the saved files and the telemetry of one run share it without passing `run_id=`. Set it to use your own ID.
- `save()` can write each check's rows to its own file, `<prefix>_checks/<schema>__<column>__<check>.csv` (`<schema>__<check>.csv` for a schema-level check), with `check_id` and `tables_in_query` columns. `"failed_query_outputs"` writes them for the failed checks (and the tolerated ones under `fetch_tolerated_rows=True`). `"all_query_outputs"` writes them for every row-level check whatever its status, with a `status` column, and is in the default outputs. It runs one row query for each passed check. The two can't be combined.
- `get_output_dfs(scope="all")` returns the rows of every row-level check that did not error, including passed checks and checks whose operator sets no upper limit, with a `status` column. It ignores `fetch_tolerated_rows`. The default `scope="failed"` is the failed-rows view.
- summary.json has a `saved_outputs` key that lists, for each output `save()` wrote, its files with the schema, column, check or tables and the row count. A check with no rows writes no file and is listed with `"rows": 0`.
- `examples/6_dq_metrics/otel_stack/`: a local OpenTelemetry Collector, Prometheus, Tempo, Loki and Grafana stack with a DQ dashboard for a sample catalog of four contracts in two domains, for checking `export_otel` end to end. The collector also archives every batch to a local S3 bucket with no expiry, queryable with SQL through DuckDB. It has its own Makefile: run `make otel-up` and `make otel-add-vowl-runs` in that folder.

### Changed
- The row counts can now download whole tables. A table with a row-level check that the data source cannot attribute, such as a cross-source check or a check on a source that is not tested, is downloaded and held in memory on the machine running vowl. At 6 columns this took about 3 s and 1 to 1.5 GiB for 1 million rows, and about 13 s and 3.5 GiB for 5 million rows. This happens only when you ask for DQ metrics or annotated output. `print_summary()`, `get_check_results_df()` and `save(outputs=["consolidated_query_outputs"])` never download a table. A table whose checks are all plain row filters is never exported.
- The warnings when a table cannot be downloaded now read `No adapter for schema '<name>', so its table cannot be downloaded.` and `Could not download the table of '<name>': <error>`. They used to say `... skipping annotated output.` and `Annotated export failed for ...`, but the download now also serves the row counts.
- `save(output_mode="both")` now writes the residue files too, so it is everything `"annotated"` writes plus the grouped CSVs. Before, it skipped residues, so the rows of a non-mergeable check that passed within its tolerance under `fetch_tolerated_rows=True` were in no file.
- Under `ValidationConfig(fetch_tolerated_rows=True)` every output now holds the rows of a check that passed within its threshold, not only annotated output and the DQ metrics. `get_output_dfs()` returns them with a `tolerated` column. `get_consolidated_output_dfs()` and the grouped CSVs of `"consolidated_query_outputs"` add a `tolerated_check_ids` column, next to `check_ids`, to a group such a check contributed to. A row picked out by a failed check A and a tolerated check B has `check_ids = "A, B"` and `tolerated_check_ids = "B"`. `show_failed_rows()` lists such checks too, labelled `(tolerated)`. Every output reads the check's rows from one fetch. With the setting off nothing changes, and vowl runs no query for a passed check.
- `get_consolidated_output_dfs()` is no longer deprecated and no longer emits a `DeprecationWarning`. `save(outputs=["consolidated_query_outputs"])` is the cheap choice for large tables: it writes the grouped failed-rows CSVs with a `check_ids` column, runs no extra queries, never downloads a table and never attributes rows.
- The OTEL pass-rate metrics are renamed from `*_pass_rate` to `*.pass_rate`: `vowl.check.row.pass_rate`, `vowl.dimension.check.pass_rate`, `vowl.dimension.row.pass_rate`, `vowl.schema.check.pass_rate` and `vowl.schema.row.pass_rate`. `vowl.check.count` is now `vowl.check.check.count`, so every name follows `vowl.<level>.<unit>.<measure>`.
- The OTEL `vowl.validate` span's `total_checks`, `passed`, `failed`, `errors` and `success_rate` attributes are now `check.count.passed`, `check.count.failed`, `check.count.error` and `check.pass_rate`. The pass rate is from 0 to 1 instead of a percentage.
- The OTEL spans and log records carry the DQ metrics' numbers under the metrics' names, without the level. The `vowl.validate` span adds `schema.count.passed`, `schema.count.failed`, `schema.count.error`, `row.count.passed`, `row.count.failed`, `row.pass_rate` and `row.approximate`. Each `vowl.check` span and log record replaces `failed_rows_count` with `row.count.passed`, `row.count.failed` and `row.pass_rate`, sent only for the checks that get `vowl.check.row.count`. Those checks also carry `row.approximate`, `row.attribution_method` and `row.attribution_note`, so you can see which check made the row counts approximate. The metrics carry the flag as a separate gauge, not an attribute, so it never splits a series. An aggregate or errored check no longer reports a `0` that reads as every row passing.
- `vowl.check.row.count`, `vowl.check.row.pass_rate` and the check span and log `row.count.*` and `row.pass_rate` are the check's attributed rows, as at the dimension, schema and run levels. The new `vowl.check.row.scalar_count` and `vowl.check.row.scalar_pass_rate`, and the span and log `row.scalar_count.*` and `row.scalar_pass_rate`, carry the check's scalar count. The scalar `PASSED` and pass rate are reported as they are, so they are negative when a check counts more rows than the table holds, such as a join that fans out.
- Every check-level metric carries the same attributes: `check_name`, `schema_name`, `dimension`, `severity` and `engine`. `vowl.check.row.count` and `vowl.check.row.pass_rate` add `severity` and `engine`, and `vowl.check.duration` adds `dimension` and `severity`.
- The example notebooks follow the guides. `examples/5_outputs/outputs_tour.ipynb` is now `examples/5_results/results_tour.ipynb` and covers annotated output, residues and saving. Its OpenTelemetry section moved to the new `examples/6_dq_metrics/dq_metrics.ipynb`, which also reads the DQ metrics at each level and loads `dq_metrics.json` with pandas. The local OTel stack lives in `examples/6_dq_metrics/otel_stack/`.
- The guides are regrouped: [Understanding DQ Metrics](docs/dq-metrics/understanding-metrics.md) holds the metric vocabulary shared by [Exporting to OpenTelemetry](docs/dq-metrics/otel-export.md) and [Exporting to dq_metrics.json](docs/dq-metrics/json-export.md). The Reference tab is now Design Considerations. Its [Checks](docs/design-considerations/checks/check-results.md) pages cover what failed rows are, how they are derived and counted, the counting mechanisms, annotating the source table and capping. Its [Cross-Server and Cross-Table Checks](docs/design-considerations/cross-table/how-it-works.md) pages explain where those checks run and how relationship references are found. The old Design Considerations page redirects to the new pages.
- The OTEL `vowl.schema.row.count`, `vowl.schema.row.pass_rate`, `vowl.dimension.row.count` and `vowl.dimension.row.pass_rate` values change to the same counts of attributed rows, every copy counted. A pass rate is no longer sent for an empty table, or for a dimension without a row-level check, instead of reporting 100%.
- Checks whose operator does not mark the matched rows as bad, such as `mustBeGreaterThan` or `mustBe: 5`, are no longer row-level checks, so they are no longer in the row counts, shown on annotated tables or kept as residues. Their verdict is in the summary as before.
- On a default run, a row-level check whose failed rows vowl can't attribute to the table this run, for example because `max_failed_rows` cut them short or the table could not be downloaded, is no longer added to the row counts through its scalar count. It is left out, counted in `checks_not_attributable`, and the numbers are marked approximate if its rows are in scope: the check failed, or it passed under `fetch_tolerated_rows=True`. A table whose row-level checks are all not attributable shows `N/A`. A check whose failed rows have no match key is never attributable. It is now row-level but not attributable, instead of not row-level. Its failed rows still become a residue.
- The Failed Rows pages are now the Checks section, under `design-considerations/checks/`. What Are Failed Rows is renamed [Check Results](docs/design-considerations/checks/check-results.md) and leads with failed checks, which checks go into the row counts, and what a failed check gives you. How Failed Rows Are Counted is renamed [How Attributed Rows Work](docs/design-considerations/checks/how-attributed-rows-work.md). The old URLs, including every page under `design-considerations/failed-rows/`, redirect to them.
- Annotated output now merges a check whose failed rows hold only the table's declared primary key columns, when the row counts count it that way: the key is unique and the data source counts rows for vowl. Before, such a check was in the row counts but became a residue.
- A unique declared primary key is now the match key for every check of its table, not only when a check returns just some of the columns. The numbers are the same, the queries are smaller, and values outside the key are no longer compared. When the key has a repeated value, or vowl can't check it, a check that holds the key but not every column reports `primary key has duplicate values` or `primary key uniqueness could not be checked` in `get_dq_metrics_df(by="check")`.
- A cross-table check whose tables share one connection now runs in that database even when adapters have filter conditions. Each table keeps the filters of the adapter that reads it, so the answer matches a run on local copies. Previously any filter condition made vowl download every table in the join. For these checks `rendered_implementation` now shows the filter subqueries that ran. A custom adapter without `with_filter_conditions` still downloads the tables when their filters differ.
- Schemas given the same `PooledAdapter`, such as `adapter=pooled` on a contract with several schemas, now run their single-table checks side by side through the pool, up to `max_concurrency` in total. Previously the pool ran one schema's checks at a time.
- A cross-table check whose tables are all served by one `PooledAdapter` now runs in the database on one of the pool's connections, with each table's filter conditions. Previously vowl downloaded every table in the join. A join across two pools, or between a pool and another adapter, is still copied. See [PooledAdapter: Joins Across Pools Are Copied](docs/known-issues.md#pooledadapter-joins-across-pools-are-copied).
- `DataSourceMapper.get_adapter` given an adapter other than `IbisAdapter` now raises a `TypeError` that says to pass the adapter to `validate_data` as it is. The old message, `Only IbisAdapter is supported`, was wrong since `validate_data` accepts any adapter.
- The `rowCount` library check now behaves like a `type: sql` check with `SELECT COUNT(*) FROM t`. Its `failed_rows_count` is the table size, and under `mustBeGreaterThan` or another operator that does not identify bad rows it is not row-level, with the `attribution_note` `operator does not set an upper limit`, and has no residue. Before, it reported `0` failed rows and a failing `rowCount` made the whole table a residue. Under an upper limit, such as `mustBeLessThan`, it is now row-level and annotates every row.
- `vowl.check.row.count`, `vowl.check.row.pass_rate` and the check span and log `row.count.*` and `row.pass_rate` are now sent only for row-level checks, the checks whose operator identifies bad rows. A check under `mustBeGreaterThan`, `mustBe: n` with n above 0 or another such operator gets check counts only. A row-level check that passed within its tolerance still gets row counts.

### Deprecated
- `ValidationConfig(enable_additional_schema_statistics=...)` is deprecated, emits a `DeprecationWarning` and has no effect.
- `ValidationConfig(max_rows_for_statistics=...)` is deprecated, emits a `DeprecationWarning` when it is not `-1` and has no effect. Table totals are never capped.
- `ValidationResult.save_dataframe()` is deprecated and emits a `DeprecationWarning`. Write the frame with its own library instead, for example `pyarrow.parquet.write_table(df.to_arrow(), "s3://...", filesystem=fs)`, which takes the same URIs and `filesystem=` as `save()`.

### Fixed
- A SQL check whose query has a syntax error, such as `SELEC COUNT(*) FROM t`, now ends as `ERROR` and the other checks keep their results. Previously building the `ERROR` result parsed the query again, so `validate_data` raised the sqlglot `ParseError` and the whole run was lost. A SQL check with no query now ends as `ERROR` with `No query specified for SQL check`.
- An ODCS v3.0.2 quality rule that names a `rule:` and no `type` is now read as a library check, the v3.0.2 default, and ends as `ERROR` saying `rule:` is not supported and to use `metric:`. Previously it became a SQL check with no query and crashed the run. With `type: library`, the message now names the rule instead of `Unsupported library metric 'None'`.
- A quality rule with a `metric` and no `type` now runs as a library check, as ODCS defaults `type` to `library`. Previously it was treated as SQL and crashed on the empty query.
- External references next to a contract loaded from `s3://` now resolve next to it, including `../` paths. Previously they failed to resolve.
- The failed-row cap now uses `TOP` on SQL Server and Fabric and `FETCH FIRST` on Oracle. Previously vowl appended `LIMIT`, which those databases reject.
- The meter provider vowl builds for OTEL export no longer carries `vowl.run.id` on its Resource. Traces and logs keep it.
- The grouped failed-rows CSVs and `get_consolidated_output_dfs()` no longer list the rows of a failed check whose operator does not set an upper limit, such as `mustBeGreaterThan`, `mustBe: 5` or a table-level `rowCount` check. Those rows are the ones that passed. Annotated output already left them out. This was also the case in 0.0.6.
- On PySpark, downloading a table for annotated output and the row counts keeps NaN values. Previously the download went through pandas, which turned NaN into NULL, so rows a check failed for NaN were not flagged.
- `get_annotated_output()` no longer raises `ArrowTypeError` when two checks return different Arrow types for the same column, such as `int32` and `int64`. Each check's rows are now cast to the downloaded table's types before they are merged. Types that still differ (a column a check turned into text or a decimal) are promoted, or compared as text.
- A composite primary key, two or more properties with `primaryKey: true`, is now checked as one key. vowl generates one `<schema>_<col1>_<col2>_primary_key_check`, with its columns in `primaryKeyPosition` order, that fails rows with a NULL in any key column and rows whose key tuple repeats. Previously every key column got its own `<col>_primary_key_check`, so valid data failed whenever one column repeated on its own. A single-column key keeps its check unchanged. The per-column check names of a composite key are gone, so update any dashboards or alerts that use them. On SQL Server, BigQuery and Trino the check uses `EXISTS` in place of a row-value `IN`.
- A SQL check that joins a table the contract doesn't declare, such as a lookup table, now reads it through the adapter of the schema the check sits under. That is the same connection a check reading the table alone already used. Previously such a join always failed with `No adapter configured for table '...'`, and so did a foreign key to an external table with no adapter registered. The check now runs when that connection can see the table, and otherwise errors with the database's own "table not found" message.
- `validate_data` now accepts a `PooledAdapter` or a custom `BaseAdapter` subclass through `adapter=` or in `adapters={...}`. Previously it raised `TypeError: Unsupported adapter type: PooledAdapter` unless you wrapped the adapter in a `MultiSourceAdapter` yourself.
- A `PooledAdapter` shared by several schemas now keeps to `max_concurrency` connections in total. Previously each schema's copy of the pool kept its own count, so the copies together could open more connections than the limit.
- A `PooledAdapter` and an `IbisAdapter` on the same connection no longer route a join between their tables to the database through the pool, which could fail. The join is copied to a local DuckDB.
- Capped `sqlglot` at `<30.18`. With sqlglot 30.18.0 or newer, ibis's DuckDB `create_table` emits an incomplete `DROP VIEW/TABLE IF EXISTS`, so DataFrame and Arrow input (ibis 12.0.0) and multi-source checks (ibis 11.x and 12.0.0) failed with `Parser Error: syntax error at end of input`.
- A check whose scalar query aliases its count, such as `SELECT COUNT(*) AS n`, now derives a row query. Before, it was dropped from the row counts with `approximate = true` and added nothing to annotated output.
- The row query derived from a `COUNT(col)` check now adds `col IS NOT NULL`, so it returns only the rows the count counts. Before, it also returned the NULL rows, and annotated output flagged them.
- `PooledAdapter.get_total_rows` now leases an instance from the pool instead of using the primary one, which another thread may be using. Building annotated output from several threads over one DuckDB pool no longer crashes.
- The `max_failed_rows` cap is no longer skipped when a row query merely mentions `LIMIT`, for example in a `credit_limit` column, a string literal or a subquery. Only an outer `LIMIT`, `TOP` or `FETCH` now counts.
- `get_annotated_output()` now warns when `max_failed_rows` cut a check's failed rows short, including at `max_failed_rows=0`, instead of silently annotating the rows past the cap as passing. vowl fetches one row more than the cap to tell, so a check with exactly as many failed rows as the cap no longer looks cut short, and one whose count is lower than its failed rows, such as a `DISTINCT` check, is no longer missed.
- A failed-rows fetch that returns zero rows now keeps its column names, so the check is still recognised as mergeable.
- Annotated output now flags every copy of a failing row that holds NaN, keeps `-0.0` apart from `0.0`, supports list, struct, map and fixed-size array columns, and keeps nanosecond timestamps and `UBIGINT` values above 2^63 with their original types. Previously NaN rows were never flagged, a `-0.0` failure also flagged `0.0` rows, nested columns raised, and nanosecond timestamps were truncated so only one copy matched.

## [0.0.6] - 2026-09-21

### Added
- **ODCS v3.2.0 support**: the official v3.2.0 JSON schema is bundled and registered as the newest supported apiVersion. v3.2.0 is a strict additive superset of v3.1.0 (the DataQuality definition is unchanged), so contracts declaring `apiVersion` v3.2.0 validate and run with no engine changes. Adds coverage for 3.2-only fields (`context`, `synonyms`, `deprecated`, `enum`, `semanticType`, and the new `vector` logicalType) (#60).
- **Enum value-set auto-check**: any property declaring `enum` (the first-class property field introduced in ODCS v3.2.0) now generates a "value must be in the allowed set" check. Non-NULL values outside the allowed set are counted (NULLs stay with the required check), and allowed values are built as SQL literals rather than interpolated (#61).
- **Relationships / foreignKey auto-checks**: ODCS `relationships` (type `foreignKey`, available since ODCS v3.1.0) now generate executed referential-integrity checks at property and schema level, as `NOT EXISTS` anti-joins. Composite and self-referential keys, MATCH SIMPLE null semantics, and shorthand / fully-qualified / external-file reference resolution are supported. Failed rows project the referencing table's columns so they merge onto its annotated output, and an unresolvable reference surfaces as a per-check `ERROR` (carrying the reason) rather than aborting the whole run. External FK targets are recognised as valid adapter keys, so registering the adapter for a cross-file target no longer warns spuriously (#61).
- **Native array-type auto-checks**: array properties (`logicalType: array`, available since ODCS v3.1.0) now generate cardinality checks (`minItems` / `maxItems` / `uniqueItems`) and element validation from the `items` sub-schema (element type, options, and enum). NULL arrays are skipped, and non-array properties carrying array keys degrade to an unsupported check (#61).

### Docs
- Completed the README and `SECURITY.md` governance sections (#57).
- Updated the security acknowledgement target to 5 working days (#58).

### Security
- Closed SQL replacement-scan and DNS-rebinding SSRF gaps in contract and reference resolution (#59).
- Remediated verified security scanner findings (#55).

### Dependencies
- Bumped `tornado` from 6.5.7 to 6.5.8 (#53).
- Bumped `pymdown-extensions` (#52).
- Bumped `cryptography` (#51).
- Bumped `setuptools` from 82.0.1 to 83.0.0 (#49).
- Bumped `pillow` from 12.2.0 to 12.3.0 (#48).
- Bumped the `uv` dependency group with 2 updates (#50).

## [0.0.5] - 2026-07-17

### Added
- **Cross-table merge for annotated output**: referential checks that use a subquery projection now merge onto their anchor table instead of becoming a residue, giving you a single consolidated view across tables (#45).
- **SQL check execution design doc** (`docs/design-considerations.md`): covers the two-query model (scalar + failed-rows), automatic `COUNT(*)` ↔ `SELECT *` derivation, sqlglot-powered rewriting, and how query shape affects annotated output mergeability (#46).
- Restructured example notebooks into three focused, self-contained tutorials: core tutorial, multiple sources, and real databases (#45).
### Deprecated
- `get_consolidated_output_dfs()` and `output_mode="failed_rows"` / `"both"` now emit a `DeprecationWarning`. Use `get_annotated_output()` / `output_mode="annotated"` instead — pass `output_mode` explicitly to pin behaviour.

### Fixed
- Fixed flaky parallel SQLite test caused by `check_same_thread=True` default; pooled SQLite connections now use thread-safe mode (#44).

## [0.0.4] - 2026-07-06

### Fixed
- vowl now works with Spark Connect, including on Databricks. Previously, validating a Spark Connect DataFrame failed outright; now it's detected and runs as expected. Installing with `pip install vowl[spark]` gives you Spark Connect out of the box (this raises the minimum PySpark to `3.4.0`), and a new `spark-classic` option keeps support for older setups (PySpark `3.0.0`+) (#39, #40).
- Fixed Spark Connect / Databricks validation returning an error on every check (on PySpark 4.x). Checks now run and return real pass/fail results (#41).
- Fixed a crash when generating a report for an empty table; it now shows `N/A` instead (#41).
- Generated SQL now uses the correct syntax for your database (e.g. Databricks), both for the checks that run and for the SQL shown when a check fails (#41).
- Fixed installing vowl from source failing on older environments such as Databricks clusters, caused by how the license was declared requiring newer build tools (#41).

## [0.0.3] - 2026-06-29

### ✨ Annotated table output

This release introduces **annotated table output**: failed-row results can now be merged back into a single consolidated, annotated view of your source data, making it far easier to see *which* rows failed and *why* in context.

### Added
- **Annotated table output**: merge per-check failed rows into a consolidated, annotated copy of the source table. See the [Annotated Output](https://github.com/govtech-data-practice/vowl/blob/main/README.md#annotated-output) section in the README and the [usage patterns notebook](https://github.com/govtech-data-practice/vowl/blob/main/examples/vowl_usage_patterns_demo.ipynb) for usage (#35).
- **Pooled adapter** for concurrent query execution. The pooled adapter maintains a connection pool so multiple checks can run in parallel against a backend, with new tests covering concurrency and multi-source parallel execution (#32).
- Unique, primaryKey, and duplicateValues checks are now mergeable into the annotated output. Previously these auto-generated checks landed in `residues`; they now emit full participating rows that fold into the consolidated table (#36).
- **Preset-driven `check_info` column** on annotated output: a JSON array of objects (one per failing check) selectable via `names` (default), `summary`, or `full` presets, exposing each check's `dimension`, `tags`, and `target` per row. Set it on `ValidationConfig.annotated_check_info` or per call (`get_annotated_output(check_info=...)` / `save(check_info=...)`). Residues are now emitted one entry per non-mergeable check, carrying the same `check_info` column (#37).
- Security CI/CD pipeline for GitHub Actions, complementing existing security measures (#29).
- `SECURITY.md` security policy (#30).

### Changed
- Auto-generated unique, primaryKey, and duplicateValues checks now count *participating rows* (rather than duplicate groups), so `actual_value` / `failed_rows_count` match the annotated row count. The PASS/FAIL verdict is unchanged. The percent-unit `duplicateValues` variant remains non-mergeable as its result is a ratio (#36).

### Fixed
- Percent-unit library checks (`unit: "percent"`, e.g. `nullValues`, `missingValues`, `invalidValues`, `duplicateValues`) generated invalid SQL (aliased scalar subqueries) and came back as `ERROR` instead of a real pass/fail verdict. They now render valid SQL and return a correct ratio-based verdict, guarded by SQL re-parse and end-to-end DuckDB tests (#37).

### Dependencies
- Bumped `cryptography` (#34).
- Bumped `tornado` from 6.5.6 to 6.5.7 (#33) and from 6.5.5 to 6.5.6 (#31).
- Bumped `urllib3` from 2.6.3 to 2.7.0 (#27).
- Bumped the `uv` dependency group with 2 updates (#28).

## [0.0.2] - 2026-04-27

### 🎉 vowl is now an official ODCS vendor!

We're thrilled to announce that **vowl** has been recognised as an official [Open Data Contract Standard (ODCS)](https://bitol.io/open-data-contract-standard/) vendor. This is a proud milestone for the project and a testament to the community's commitment to open, interoperable data contracts.

### Fixed
- Getting outputs no longer crashes when a contract contains a list-valued `mustBe` field (#23).
- Broken link on the Known Issues documentation page (#19).
- Git link sanitisation issue in documentation (#15).

### Changed (Breaking)
- Check metadata now uses `check_definition` and `contract_definition` (#21, #26). This replaces several top-level metadata fields:
  - **Renamed fields:** `schema` → `schema_name`, `rule` → `rendered_implementation`.
  - **Removed from top-level:** `dimension`, `type`, `description`, `severity`, `unit` — these are no longer top-level keys in `CheckResultMetadata`.
  - **`check_definition`** carries the resolved/generated check definition dict. Auto-generated checks are tagged with `vowl_generated_check`.
  - **`contract_definition`** carries the raw ODCS contract content at the check's JSONPath.
  - **Output DataFrame:** `get_check_results_df()` no longer flattens definitions into top-level columns. Pass `include_check_definition=True` and/or `include_contract_definition=True` to include them as JSON-serialised columns. `save()` accepts the same keyword arguments.
  - Code that accesses `metadata["schema"]`, `metadata["rule"]`, `metadata["unit"]`, etc. must be updated to use the new key names or read from `metadata["check_definition"]` / `metadata["contract_definition"]`.

### Added
- Logical type `options.format` validation — contracts can now specify format constraints (e.g. date formats) on logical types, and vowl will auto-generate format checks. See the [Format Checks](https://govtech-data-practice.github.io/vowl/contracts/#format-checks) docs for details (#25).
- DuckDB attach example demonstrating cross-database validation (#18).
- Open Graph and SEO metadata for the documentation site (#14).
- Google site verification meta tag (#12).
- Updated Jupyter notebook examples (#16).
- README badge and data contract example link (#13, #22).

### Dependencies
- Bumped `pytest` from 9.0.2 to 9.0.3 (#20).
- Bumped `cryptography` (#17).
- Updated Python Docker tag to 3.14.3 (#11).

## [0.0.1] - 2026-04-02

### 🎉 Celebrating Open Source

Initial public release of **vowl**.

**Background:**

- vowl originated as an internal tool for demonstrating data contracts within our prototyping workflows. Over time, we recognised its potential value to the wider international community.
- With that in mind, we refined the library and published it as open source.
- As the project is still in its early stages, there may be rough edges and bugs. We appreciate your patience and warmly welcome contributions to help improve vowl for everyone.

### Added
- Core SQL-powered data quality validation engine backed by Ibis and DuckDB.
- Contract-based validation with YAML/JSON schema definitions.
- Adapters for pandas, Spark, and database backends (DuckDB attach).
- CTE wrapper for robust query transformation and complex query support.
- Multi-table and multi-source materialisation support.
- Export results as Arrow tables.
- Jupyter notebook examples and demo outputs.
- MkDocs documentation site (architecture, contracts, usage patterns).
- MIT license.
- GitHub Actions CI for testing, linting, and PyPI publishing.
- `THIRD_PARTY_NOTICES` and `LICENSE_AUDIT_REPORT.md`.
- `CONTRIBUTING.md` with development setup and release workflow.

[Unreleased]: https://github.com/govtech-data-practice/vowl/compare/v0.0.5...HEAD
[0.0.5]: https://github.com/govtech-data-practice/vowl/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/govtech-data-practice/vowl/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/govtech-data-practice/vowl/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/govtech-data-practice/vowl/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/govtech-data-practice/vowl/releases/tag/v0.0.1
