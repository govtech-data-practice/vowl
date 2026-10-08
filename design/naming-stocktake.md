# Naming stocktake

A review of the names added on `feat/otel-exporter`. Each item is a
proposal. We decide and apply them one at a time.

Status values: `todo`, `done`, `kept` (decided to leave as is), `dropped`
(proposal rejected).

## Public names

Users see these in the config, the row quality df, the DQ metrics or OTEL.

| #   | Status | Name                                                                                     | Where                            | Proposal                                                                                                                                        | Reason                                                                                                             |
| --- | ------ | ---------------------------------------------------------------------------------------- | -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| P1  | done   | `ValidationConfig.row_counts_enabled`                                                    | `config.py`                      | Remove, use `row_counts != "off"`                                                                                                               | Only 2 internal callers. A second way to say the same thing                                                        |
| P2  | done   | `table-level or not a row filter`                                                        | `reason` value                   | `not a row-level check`                                                                                                                         | The docs call this type "Non-row-level check"                                                                      |
| P3  | done   | `operator does not identify bad rows`                                                    | `reason` value                   | `operator does not set an upper limit`                                                                                                          | The docs define a non-limiting check by "sets an upper limit"                                                      |
| P4  | done   | `some failed rows match no table row`                                                    | `reason` value                   | `some failed rows could not be attributed to a table row`                                                                                       | Use "attribute", not "match"                                                                                       |
| P5  | done   | `the failed rows could not be keyed`                                                     | `reason` value                   | `the failed rows could not be turned into match keys`                                                                                           | "Keyed" is jargon. The docs already use the proposed phrase                                                        |
| P6  | kept   | `vowl.check.check.count`                                                                 | OTEL metric                      | Keep                                                                                                                                            | Reads oddly, but follows the `<grain>.check.count` pattern of the other grains                                     |
| P7  | kept   | `row.count.failed`, `row.count.passed`, `row.pass_rate`                                  | OTEL root span                   | Keep                                                                                                                                            | Renaming would break the example dashboards                                                                        |
| P8  | done   | `exact` (df column, `vowl.row_quality.exact`), `inexact` (internal)                      | row quality df, DQ metrics, OTEL | `approximate` everywhere                                                                                                                        | One word for one idea. Matches the docs and the summary's "(approx.)"                                              |
| P9  | done   | `vowl.row_quality.approximate`, `vowl.row_quality.checks_not_attributable` on row points | DQ metrics, OTEL metrics         | Off the metrics. On the `vowl.validate` span, plus per check `approximate`, `route`, `reason`, `attributed_rows` on `vowl.check` spans and logs | A changing attribute splits the series. Later added as the 0/1 gauge `vowl.<level>.row.approximate`, which names the check at check level |

Changing a `reason` value also means updating the docs tables and the test
assertions that compare against it.

## Internal names

No user impact. Code and tests only.

| #   | Status | Name                                                    | Where                       | Proposal                        | Reason                                                                    |
| --- | ------ | ------------------------------------------------------- | --------------------------- | ------------------------------- | ------------------------------------------------------------------------- |
| I1  | done   | `RowQualityReport.weighted_pass_rate`, `mean_pass_rate` | `rollup.py`                 | Remove                          | Computed but never read. The design doc calls them internal               |
| I2  | done   | `REASON_NOT_MERGEABLE`                                  | `selection.py`              | `REASON_NO_MATCH_KEY`           | The docs say "match key", not "mergeable"                                 |
| I3  | done   | `mergeable.py`, `rows_mergeable`                        | `row_quality/`              | `match_key.py`, `has_match_key` | Same as I2                                                                |
| I4  | done   | `REASON_PROBE_FAILURE`                                  | `selection.py`              | `REASON_QUERY_FAILED`           | Its text says "its row query failed"                                      |
| I5  | done   | `CheckSelection.row_count`                              | `selection.py`              | `scalar_count`                  | `CheckState` and the df column already say `scalar_count`                 |
| I6  | done   | `CheckSelection.blocks_exact`, `CheckState.inexact`     | `selection.py`, `rollup.py` | `inexact` for both              | Two names for one idea                                                    |
| I7  | done   | `CheckState.scalar`                                     | `rollup.py`                 | `from_scalar`                   | Easy to confuse with `scalar_count`                                       |
| I8  | done   | `RowQuality._attribution_disabled`                      | `row_quality/__init__.py`   | `_scalar_only`                  | It means `row_counts="scalar"`                                            |
| I9  | done   | `flagged_checks`                                        | `selection.py`              | `attributed_checks`             | It returns the checks whose rows are attributed                           |
| I10 | done   | `REASON_UNMATCHED`                                      | `selection.py`              | `REASON_UNATTRIBUTED`           | Matches its new text (P4)                                                 |
| I11 | done   | `REASON_UNKEYABLE`                                      | `selection.py`              | `REASON_MATCH_KEYS_FAILED`      | Matches its new text (P5). Avoids a near-clash with `REASON_NO_MATCH_KEY` |
