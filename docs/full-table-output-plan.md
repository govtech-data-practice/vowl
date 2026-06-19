# Plan: Annotated Output (Full Table with Failed Rows Marked)

## Context

Users currently receive only failed rows as output. They want the **full in-scope table** with failed rows **annotated** (via a `check_ids` column), so they can see what's wrong while still using the good rows downstream. Not all checks can participate — only those producing `SELECT * FROM table WHERE ...` (same columns as the full table) are "mergeable". Non-mergeable checks (aggregations, column subsets, cross-table joins) keep their existing failed-rows-only output, returned alongside as **residues**.

> **Terminology**
> - **"annotated"** — a full in-scope table with an added `check_ids` column (check name(s) for failed rows, `null` for passing rows). This is the headline feature; the term is used consistently across the method name, the dict key, the CSV suffix, and this prose.
> - **"full / in-scope table"** — the table as returned by `adapter.export_table_as_arrow(schema_name)`, which applies the adapter's `filter_conditions` before returning (see `src/vowl/adapters/ibis_adapter.py:240`). So output reflects the same in-scope rows the checks ran against — **not** the raw untouched source. This is intentional: it keeps annotation against the failed rows consistent (both share the same filtered scope).
> - **"residues"** — failed-rows output for checks that cannot be annotated onto a full table (cross-table, aggregation, column-subset). Same shape as today's `get_consolidated_output_dfs` entries.

---

## New Method on `ValidationResult`

### `get_annotated_output(checks=None, *, include_target=False) -> dict[str, dict[str, nw.DataFrame]]`

Returns a **nested dict with two reserved top-level keys** — an invariant contract that never changes shape based on arguments:

```python
{
    "annotated": {
        "orders":    <full orders table + check_ids (nullable)>,
        "customers": <full customers table + check_ids (nullable)>,
    },
    "residues": {
        "orders__1":         <non-mergeable subset, failed rows only>,
        "orders, customers": <cross-table, failed rows only>,
    },
}
```

- `"annotated"` and `"residues"` are **fixed reserved keys**, not schema names. Inner keys are schema names (annotated) or the existing consolidated keys (residues).
- **`annotated`** — one entry per schema with an available adapter, **always present** even if no eligible checks failed (all-null `check_ids`). Inner keys are plain schema names. No `tables_in_query` column.
- **`residues`** — exactly the non-mergeable entries from `get_consolidated_output_dfs` (failed rows + `check_ids` + `tables_in_query`). Empty dict if there are none.
- Mergeable failed-rows entries are **not** duplicated in `residues` — they are represented by the annotated table. Subsumption is decided by `check_ids` membership, not by key (see algorithm).

> **No per-check annotated variant.** Only the consolidated form is provided. `save()` consumes only this path, and annotating per-check would emit N redundant copies of the same full table (one per check on a schema). If a per-check consumer ever materializes, add `get_annotated_output_per_check()` then.

### Column naming

The marking column is **always named `check_ids`** across both `annotated` and `residues` entries. The existing `_append_output_metadata` writes a singular `check_id` (`src/vowl/validation/result.py:417`); residues are produced via the existing consolidated path which already yields `check_ids`, so no rename is needed there. Annotated entries produce `check_ids` directly.

---

## Eligibility for Merging

A check is mergeable (can be annotated onto the full table) when ALL of, **evaluated in this order** (cheap metadata predicates first, so the lazy `failed_rows` fetch is only triggered once the others pass):

1. `status != 'ERROR'`
2. `is_cross_table_check() == False`
3. `supports_row_level_output == True` (aggregation_type is "count" or "none")
4. **Column match** (touches `failed_rows.columns`, the only fetch-triggering predicate):
   `set(failed_rows.columns) - {'check_id', 'check_ids', 'tables_in_query'} == set(full_table.columns)`

> **Lazy-fetch caution:** `cr.failed_rows` is fetched lazily on first access (`src/vowl/executors/base.py:78`). Criterion 4 and the `len(cr.failed_rows)` check both trigger that SELECT. Keep them last, and never evaluate them for checks already excluded by 1–3.

Helper: `_is_mergeable_for_full_table(cr: CheckResult, full_table_columns: set[str]) -> bool`

---

## Algorithm

```
checks_set = set(checks) if checks else None

# Step 1: existing consolidated failed-rows output (the residue candidates)
consolidated_failed = self.get_consolidated_output_dfs(checks=checks)

# Step 2: build one annotated table per schema, tracking which checks were merged
annotated: dict[str, nw.DataFrame] = {}
merged_check_names: set[str] = set()

For each schema_name in self._schema_names:
    full_table = self._fetch_full_table(schema_name)
    if full_table is None:
        continue                       # no adapter/export — leave residues intact
    full_table_cols = set(full_table.columns)

    eligible_failed = [
        cr for cr in self.check_results
        if cr.metadata.get('schema_name') == schema_name
        and (not checks_set or cr.check_name in checks_set)
        and cr.status == 'FAILED'
        and _is_mergeable_for_full_table(cr, full_table_cols)   # evaluated last
        and len(cr.failed_rows) > 0
    ]

    if no eligible_failed:
        annotated[schema_name] = full_table + null check_ids
    else:
        tagged_failures = []
        for cr in eligible_failed:
            rows = strip_metadata_cols(cr.failed_rows).unique()
            rows = rows.with_columns(check_id = cr.check_name)
            tagged_failures.append(rows)
            merged_check_names.add(cr.check_name)

        # Collapse duplicate rows into comma-joined check_ids.
        # Use _group_check_ids_by_row (no tables_in_query dependency),
        # NOT _consolidate_grouped_output (which requires that column).
        union = concat(tagged_failures)
        consolidated = _group_check_ids_by_row(union)

        # Attach check_ids to matching full-table rows via the NULL-safe matcher.
        # ⚠ DO NOT use a plain `full_table.join(..., how='left')` here — that is the
        #   value-join HAZARD (caveats §1): rows with NULLs are left unannotated and
        #   appear to pass. Delegate to the chosen strategy (Candidate A or B).
        data_cols = [c for c in consolidated.columns if c != 'check_ids']
        annotated[schema_name] = self._annotate_full_table(
            full_table, consolidated, data_cols
        )   # NULL-safe; see "Annotation matching caveats §1"

# Step 3: residues = consolidated_failed entries NOT fully subsumed by an annotated table.
#         Decision is by check-name membership, NOT key equality.
residues: dict[str, nw.DataFrame] = {}
for key, df in consolidated_failed.items():
    entry_checks = _check_names_in_entry(df)   # parse the 'check_ids' column
    if entry_checks and entry_checks.issubset(merged_check_names):
        continue                       # fully represented by an annotated table → drop
    residues[key] = df

return {"annotated": annotated, "residues": residues}
```

> **Why membership, not key equality?** `get_consolidated_output_dfs` keys entries by `tables_in_query` (`src/vowl/validation/result.py:461`), which for any single-table check — mergeable *or not* — is just the bare schema name (e.g. `"orders"`). The `__N` suffix only appears when a schema yields multiple groups (`result.py:475`). So a bare `"orders"` key can be a **non-mergeable** entry. Deciding subsumption by key would silently drop it. The nested `{annotated, residues}` structure keeps the two views in separate keyspaces, and `check_ids` membership decides subsumption correctly.
>
> (Assumes check names contain no commas — they are identifiers, so this holds. Worth a comment at the parse site. If names could contain commas, `check_ids` needs a different delimiter.)

---

## Helpers

### Split `_consolidate_grouped_output`

The existing `_consolidate_grouped_output` unconditionally reads `tables_in_query` (`src/vowl/validation/result.py:502`), so it **cannot** be called on the stripped, tagged failures Step 2 builds (data cols + `check_id` only). Extract the row-grouping core into a variant with no `tables_in_query` dependency:

```python
@staticmethod
def _group_check_ids_by_row(combined: nw.DataFrame) -> nw.DataFrame:
    """Group identical data rows, collapsing 'check_id' into comma-joined
    'check_ids'. Does NOT require or emit 'tables_in_query'."""
    data_cols = [
        c for c in combined.columns
        if c not in ('check_id', 'check_ids', 'tables_in_query')
    ]
    arrow_table = combined.to_arrow()
    check_id_col = arrow_table.column('check_id')
    data_arrow_cols = [arrow_table.column(c) for c in data_cols]

    row_groups: dict[tuple[Any, ...], set[str]] = {}
    for i in range(arrow_table.num_rows):
        row_key = tuple(c[i].as_py() for c in data_arrow_cols)
        row_groups.setdefault(row_key, set()).add(check_id_col[i].as_py())

    result_data: dict[str, list] = {c: [] for c in data_cols}
    result_data['check_ids'] = []
    for row_key, ids in row_groups.items():
        for c, v in zip(data_cols, row_key, strict=False):
            result_data[c].append(v)
        result_data['check_ids'].append(', '.join(sorted(ids)))
    return nw.from_native(pa.table(result_data), eager_only=True)
```

The existing `_consolidate_grouped_output` may optionally be refactored to delegate to this and re-attach `tables_in_query`, but that is not required.

### Subsumption parser

```python
@staticmethod
def _check_names_in_entry(df: nw.DataFrame) -> set[str]:
    if 'check_ids' not in df.columns:
        return set()
    names: set[str] = set()
    for cell in df['check_ids'].to_list():
        if cell:
            names.update(p.strip() for p in cell.split(',') if p.strip())
    return names
```

---

## Full Table Fetching (cached, failure-tolerant)

`get_adapter` returns `None` for unknown schemas (`src/vowl/adapters/multi_source_adapter.py:145`), and adapters that don't support export raise `NotImplementedError` (`src/vowl/adapters/base.py:146`). Both must be handled without crashing the output path. Cache the **failure** (as `None`) so it isn't retried per-schema.

```python
self._full_table_cache: dict[str, nw.DataFrame | None] = {}  # added to __init__

def _fetch_full_table(self, schema_name: str) -> nw.DataFrame | None:
    if schema_name not in self._full_table_cache:
        result: nw.DataFrame | None = None
        adapter = self._multi_adapter.get_adapter(schema_name)
        if adapter is None:
            logger.warning("No adapter for schema %r; skipping annotated output.", schema_name)
        else:
            try:
                arrow_table = adapter.export_table_as_arrow(schema_name)
                result = nw.from_native(arrow_table, eager_only=True)
            except Exception as exc:   # NotImplementedError + any backend export error
                logger.warning("Annotated export failed for %r: %s", schema_name, exc)
        self._full_table_cache[schema_name] = result
    return self._full_table_cache[schema_name]
```

Callers treat `None` as "skip the annotated table for this schema; residues for it survive".

> **Logging note:** `src/vowl/validation/result.py` currently has **no logger** —
> the module uses `print()` for output (e.g. `result.py:331`). The `logger.warning(...)`
> calls in this plan (`_fetch_full_table`, the match-count safety net) therefore
> need a logger to be introduced: add `import logging` + `logger = logging.getLogger(__name__)`
> at module top. Decide deliberately whether warnings should use `logging` (preferred
> for library code — callers control verbosity) or match the existing `print()` style.
> Pick one and apply consistently; don't leave an undefined `logger`.

> **Memory note:** the cache holds every schema's full (filtered) table in memory simultaneously, consistent with how mode-2 materialization already works. For large multi-schema runs this can be heavy. Consider clearing `_full_table_cache` at the end of `save()` if the `ValidationResult` is long-lived.

---

## Annotation matching caveats (matching failed rows to the full table)

Annotation must match each tagged failed row back to its row in the full table
using **all data columns** as the identity. This value-based identity has two
limitations — the first is a **correctness hazard**, not a cosmetic gap, and must
be handled, not merely documented. Two candidate implementations (see Handling)
address it; the final choice is decided by the benchmark in Verification §9–10.

### 1. NULL in any column → failed row silently shown as passing (HAZARD)

The naive approach (an Arrow/narwhals LEFT JOIN on all columns) treats
`NULL != NULL`. Because matching is on *every* data column, a failed row with a
NULL in **any** column cannot match its own copy in the full table, so its
`check_ids` comes back `null` — **identical to a passing row**. A row that failed
is then presented as clean.

This was verified empirically (narwhals 2.18.1 / pyarrow 23.0.1): a row that
failed a `not_null`-style check, with a NULL in a join column, received
`check_ids = null` after a plain left join — annotated as passing.

> **Why this is the dangerous direction.** Over-annotation (flagging a clean row)
> is conservative — a user investigates and finds nothing. *Under*-annotation
> hides a real failure. And the trigger correlates with the checks most likely to
> fire: `not_null` / completeness checks target exactly the nullable columns whose
> NULLs break the join. So the rows most likely to fail are among the most likely
> to be mis-annotated.

**Handling — two candidate implementations, chosen by a benchmark.** Both fix the
hazard. We will implement both behind a common internal interface, run them
against the same correctness suite (§ Verification 7a–7h) and a performance
benchmark (§ Verification 9), and keep the winner. Decision criteria, in order:
(1) passes **all** correctness cases; (2) acceptable performance at expected table
sizes; (3) least code / most consistent with the existing file.

> Both candidates must be wired so the choice is a one-line swap (e.g. a private
> `_annotate_strategy` constant), so the benchmark can exercise each and the loser
> can be deleted cleanly once decided.

**Candidate A — NULL-safe equality join (vectorized).** Make the join treat
`NULL == NULL`: add a boolean `__<col>_isnull` mask column for every join column
to *both* frames, fill the NULLs in the value columns with a type-appropriate
placeholder, then join on `value columns + mask columns`. Two rows match iff same
filled value **and** same null-ness. The placeholder value is irrelevant (the
mask disambiguates), so no value-collision risk. Drop the mask columns after.
**Verified** (narwhals 2.18.1 / pyarrow 23.0.1): failed NULL row → annotated,
clean NULL row → stays null, no duplicates.

```python
# sketch — full and failed are narwhals DataFrames, join on `data_cols`
masks = [nw.col(c).is_null().alias(f"__{c}_isnull") for c in data_cols]
fills = [nw.col(c).fill_null(_placeholder_for(full.schema[c])) for c in data_cols]
keys = data_cols + [f"__{c}_isnull" for c in data_cols]
annotated = (
    full.with_columns(*masks).with_columns(*fills)
        .join(failed.with_columns(*masks).with_columns(*fills).select(keys + ["check_ids"]),
              on=keys, how="left")
        .drop([f"__{c}_isnull" for c in data_cols])
)
# NOTE: fill overwrites value cells; preserve the ORIGINAL columns (carry through
# unfilled, or restore after) so output shows true NULLs, not the placeholder.
```

- Pro: vectorized in C; scales to large/out-of-core tables.
- Con: fiddliest code (mask + fill + restore + type-appropriate placeholder); must
  also handle §1a (`pa.null()` columns crash the join unless cast first).

**Candidate B — Python dict matching (row-by-row).** Skip the engine join
entirely. Build a dict mapping each failed row's value-tuple → `check_ids`, then
walk the full table, look each row's tuple up, and attach the result. Python's
`None == None` is `True` and tuples containing `None` hash/compare correctly, so
**NULLs just work — no masks, no placeholder, no §1a crash.** This is the *same
pattern already used* by `_consolidate_grouped_output`
(`src/vowl/validation/result.py:505-515`) and the row-quality code, so it is the
most consistent with the file. **Verified**: failed NULL row → annotated, clean
NULL row → null, duplicates handled no worse than the join.

```python
# sketch — arr is the full table (pa.Table), data_cols the join columns
failed_map = {row_tuple: check_ids}            # built from tagged failures via .as_py()
cols = [arr.column(c) for c in data_cols]
check_ids_out = [
    failed_map.get(tuple(c[i].as_py() for c in cols))
    for i in range(arr.num_rows)
]
annotated = arr.append_column("check_ids", pa.array(check_ids_out, type=pa.string()))
```

- Pro: simplest; correct for NULLs for free; no §1a crash; matches existing code.
- Con: row-by-row Python loop with per-cell `.as_py()` materialization — O(rows)
  with a large constant; slower per row than the vectorized join and heavier on
  memory. Cost profile is identical to what `_consolidate_grouped_output` already
  incurs today.

**Rejected approach — anti-join + append (does NOT work).** Appending unmatched
failures to a plain value-join output leaves the original full-table row present
with `check_ids = null` *and* adds the surfaced copy — the failed row then appears
**twice**, one still looking like it passed. Verified to misbehave; do not use.

**Invariant / safety net (always assert, both candidates).** The number of
distinct annotated rows (non-null `check_ids`) must equal the number of distinct
failed rows fed in. If they differ, **log a warning** with both counts and the
schema name, so the gap is never silent.

**Proper fix (follow-up) — row-id join.** Join on a stable per-row key instead of
values. Eliminates this hazard *and* fixes #2 below, and is simpler than either
candidate. Requires the id to originate in the SQL layer so it exists identically
in both the failed-rows result and `export_table_as_arrow` output — a larger
change than this iteration scopes. Whichever candidate wins is the bridge until
then.

### 1a. All-NULL / untyped join columns crash the join (Candidate A only)

If a column reaching the join is typed `pa.null()` (e.g. an empty failed-rows
frame, or a column entirely NULL with no inferred type), pyarrow raises
`ArrowInvalid: Data type null is not supported in join key field` — the whole
annotation throws. **This affects Candidate A only**; Candidate B does no join and
is immune.

**Handling (Candidate A):** before joining, cast any `pa.null()`-typed join column
to the corresponding full-table column's type (the full table is the
authoritative schema). Mirrors the existing empty-frame column-typing logic in
`_append_output_metadata` (`src/vowl/validation/result.py:425`).

### 2. Duplicate identical rows (value-based matching limitation — both candidates)

Both candidates match by row *value*, so they cannot distinguish N byte-identical
rows. Candidate A (join) annotates **all N** if one failed (over-annotation, the
safe over-flagging direction). Candidate B (dict) annotates the rows whose tuple
is in the failed set — for truly identical rows it also cannot tell which physical
copy failed. Neither is fully correct here; only the row-id follow-up resolves it.
This is acceptable for the first iteration — the benchmark/correctness suite
should record each candidate's exact duplicate behavior (§ Verification 8) so the
trade-off is explicit, not assumed.

---

## `include_target` Parameter

When `include_target=True`:
- **annotated**: add `targets` column = comma-separated deduplicated targets per row (built alongside `check_ids` during grouping); null for passing rows.
- **residues**: no change (target info already in check metadata).

> Optional; may be deferred to a follow-up. The `check_ids` annotation is the core value.

---

## Output Mode

`save()` selects between failed-rows output and annotated output via a single
**mode enum** rather than a boolean. The three behaviors are mutually exclusive
modes, not independent toggles, so a boolean cannot express them cleanly (it can
encode at most two of three states, and "both" would force a second, interacting
flag). The enum is also future-proof: a new output style adds an enum value, not
another combinatorial boolean.

```python
from typing import Literal

OutputMode = Literal["failed_rows", "annotated", "both"]
```

| Mode | Files written (besides `check_results.csv` + `summary.json`) |
|------|---------------------------------------------------------------|
| `"failed_rows"` (default) | Existing consolidated failed-rows CSVs (unchanged behavior) |
| `"annotated"` | Annotated tables (`_annotated.csv`) + residues; **no** standalone failed-rows CSVs. `annotated + residues` is a complete, non-overlapping partition, so nothing is written twice. |
| `"both"` | Failed-rows CSVs **and** annotated tables + residues. Redundant by design — for consumers that genuinely need the raw failed-rows files alongside annotated tables. |

- **Validate the value** — reject anything outside the three literals at the top
  of `save()` with a clear `ValueError`, so a typo (`"anotated"`) fails at the
  call site, not silently.
- Shipping `"failed_rows"` and `"annotated"` first while leaving `"both"`
  available means the API never has to change if "both" turns out to be needed.
- The API method `get_annotated_output()` is always available regardless of mode;
  the mode only governs what `save()` writes.

## Configuration

Add to `ValidationConfig` in `src/vowl/config.py` — **mirror the enum, do not
re-encode it as a boolean** (a boolean config feeding an enum param would drift):

```python
output_mode: OutputMode = "failed_rows"
```

**Wiring (required — the setting must not be dead):** `save()`'s `output_mode`
param defaults to `None`; when `None`, `save()` falls back to
`config.output_mode`. This gives "set it once in config" without two settings
that can disagree.

```python
def save(self, ..., output_mode: OutputMode | None = None) -> ValidationResult:
    mode = output_mode if output_mode is not None else self._config.output_mode
    if mode not in ("failed_rows", "annotated", "both"):
        raise ValueError(f"Unknown output_mode: {mode!r}")
    ...
```

> **Prerequisite — `ValidationResult` does not currently hold a config.** The
> runner has `self._config` (`src/vowl/validation/runner.py:37`) but does **not**
> pass it when constructing the result (`runner.py:152-158`), and
> `ValidationResult.__init__` (`result.py:50`) takes no config. So `self._config`
> in the sketch above does not exist yet. Thread it through:
> 1. Add `config: ValidationConfig` (or `ValidationConfig | None`, defaulting to a
>    fresh `ValidationConfig()`) to `ValidationResult.__init__`, stored as `self._config`.
> 2. Pass `config=self._config` at the construction site in `runner.py:152`.
>
> Until this is wired, `save()` cannot read `config.output_mode`. If threading the
> config is undesirable, the fallback is to make `output_mode` a required-ish param
> with a literal default (`output_mode: OutputMode = "failed_rows"`) and drop the
> config field entirely — but then "set it once in config" is lost. Threading the
> config is preferred.

---

## File Changes

| File | Changes |
|------|---------|
| `src/vowl/validation/result.py` | Add `_full_table_cache` to `__init__`; add `_fetch_full_table()`; add `_is_mergeable_for_full_table()`; add `_group_check_ids_by_row()`; add `_check_names_in_entry()`; add the annotation matcher (build **both** Candidate A & B behind a swappable strategy for the bake-off, then keep the winner per Verification §10); add `get_annotated_output()`; extend `save()` with `output_mode` param (defaults to config) |
| `src/vowl/config.py` | Add `output_mode: OutputMode = "failed_rows"` to `ValidationConfig` (with `OutputMode = Literal["failed_rows", "annotated", "both"]`) |
| `src/vowl/validation/result.py` (`__init__`) | Add a `config: ValidationConfig` param stored as `self._config` so `save()` can read `output_mode` (does not exist today — see Output Mode prerequisite) |
| `src/vowl/validation/runner.py` | Pass `config=self._config` into `result_cls(...)` at `runner.py:152` |
| `src/vowl/validate.py` | None — the public `ValidationResult` subclass (`validate.py:26`) is empty and inherits `__init__` cleanly (confirmed) |
| `tests/` | New test file or extend existing coverage |

---

## Save Method Extension

```python
def save(self, ..., output_mode: OutputMode | None = None) -> ValidationResult:
    mode = output_mode if output_mode is not None else self._config.output_mode
    if mode not in ("failed_rows", "annotated", "both"):
        raise ValueError(f"Unknown output_mode: {mode!r}")

    # check_results.csv + summary.json: always written (unchanged)

    if mode in ("failed_rows", "both"):
        for key, df in self.get_consolidated_output_dfs().items():
            safe_key = key.replace(', ', '_').replace(' ', '_')
            csv_path = output_path / f"{prefix}_{safe_key}.csv"
            _pa_csv.write_csv(df.to_arrow(), str(csv_path))
            saved_files.append(str(csv_path))

    if mode in ("annotated", "both"):
        out = self.get_annotated_output(include_target=True)
        for schema, df in out["annotated"].items():
            safe_key = schema.replace(', ', '_').replace(' ', '_')
            csv_path = output_path / f"{prefix}_{safe_key}_annotated.csv"
            _pa_csv.write_csv(df.to_arrow(), str(csv_path))
            saved_files.append(str(csv_path))
        # In "annotated" mode, residues cover the non-mergeable checks.
        # In "both" mode, residues are a SUBSET of the failed-rows CSVs already
        # written above (same keys, same filenames) — skip them to avoid
        # rewriting identical files.
        if mode == "annotated":
            for key, df in out["residues"].items():
                safe_key = key.replace(', ', '_').replace(' ', '_')
                csv_path = output_path / f"{prefix}_{safe_key}.csv"
                _pa_csv.write_csv(df.to_arrow(), str(csv_path))
                saved_files.append(str(csv_path))
```

> **Filename collisions are now resolved by mode:**
> - `"annotated"`: annotated tables use the `_annotated.csv` suffix; residues use the plain consolidated filenames — but the standalone failed-rows CSVs are *not* written in this mode, so there is no collision.
> - `"both"`: the failed-rows CSVs are written, and residues would reuse those same filenames (residue keys are a subset of consolidated keys). Residues are therefore skipped in `"both"` mode — the failed-rows CSVs already contain them. Only the `_annotated.csv` files are added on top.

---

## Edge Cases

| Case | Handling |
|------|----------|
| Cross-table checks | Non-mergeable → `residues` entry |
| Aggregation checks | Non-mergeable → `residues` entry |
| Column-subset checks | Non-mergeable → `residues` entry |
| ERROR check | Excluded (eligibility criterion 1); not in `annotated`; `get_consolidated_output_dfs` already skips ERROR for residues |
| Schema with no eligible failed checks | `annotated[schema]` = full table with all-null `check_ids` |
| Adapter is `None` for schema | `_fetch_full_table` returns `None`; no `annotated` entry; residues for it survive; log warning |
| Adapter doesn't implement `export_table_as_arrow` | Caught (`NotImplementedError`); same as above |
| `export_table_as_arrow` raises at runtime | Caught (broad `Exception`); same as above; failure cached |
| Single-table **non-mergeable** check, bare schema key | Kept in `residues` — subsumption is by `check_ids` membership, not key |
| Schema with both mergeable + non-mergeable checks | `annotated[schema]` + surviving `residues` entry; no duplication |
| Duplicate rows in failed_rows | `.unique()` before matching |
| Duplicate rows in full table | Value-based matching can't distinguish copies — see caveats §2; behavior recorded per candidate |
| NULL in any join column of a failed row | **Hazard**: plain value-join leaves it `check_ids=null` (looks passing). Fixed by both candidates (NULL-safe join / dict matching). See caveats §1. Never ship as a silent false-negative. |
| Annotated-match count != failed-rows-in count | Invariant breach → log a warning with both counts + schema name (both candidates) |
| Filtered-out source rows | Absent from annotated table (export applies adapter filters) — by design |
| Marking column name | Always `check_ids` |

---

## Verification

1. `pytest tests/` — no regressions
2. Unit test eligibility logic with various `aggregation_type`/column combos, asserting predicate ordering doesn't fetch rows for ERROR/cross-table/agg checks
3. Unit test `get_annotated_output` shape: always returns `{"annotated": {...}, "residues": {...}}`; reserved keys present even when one bucket is empty
4. Unit test annotated entries: multiple checks on same schema, `check_ids` comma-separated correctly; passing rows null; PASSED/no-failure schema yields all-null column
5. **Unit test subsumption** (the critical fix): (a) schema with only a non-mergeable single-table check keeps its `residues` entry; (b) schema with mergeable + non-mergeable checks yields an annotated table + surviving residue, no duplicate
6. Unit test `_fetch_full_table` failure paths: `None` adapter, `NotImplementedError`, runtime export error — each skips cleanly, residues survive
7. **Annotation correctness suite — run against BOTH candidates (A: NULL-safe join, B: dict matching).** Parametrize the suite over the two strategies so identical assertions apply to each; a candidate that fails any case is disqualified regardless of speed. Each case below was proven distinct in prototyping:
   - **7a. Failed NULL-bearing row is annotated, not hidden.** A row with a NULL in a join column that *failed* a check → assert `check_ids` is the check name (NOT null). Core false-negative regression guard — the naive value-join produced `null` here.
   - **7b. Clean NULL-bearing row is NOT cross-annotated.** A *passing* row sharing the same NULL pattern as a failed row must keep `check_ids=null`. Guards against over-matching on null-ness alone.
   - **7c. No duplicate rows introduced.** Assert annotated row count == full-table row count (catches a regression to the rejected anti-join+append, which duplicated a row).
   - **7d. Multiple NULL columns.** A failed row with NULLs in several join columns is still annotated.
   - **7e. Untyped (`pa.null()`) join column.** Construct a failed-rows frame whose join column is typed `pa.null()` (empty or all-null). Candidate A must cast and complete (not raise `ArrowInvalid: Data type null is not supported in join key field`); Candidate B must be immune by construction. Assert both complete.
   - **7f. Original NULLs preserved in output.** Annotated table shows true NULLs in data columns, not any internal fill placeholder (Candidate A: fill must not leak; Candidate B: trivially holds).
   - **7g. Placeholder-value collision is safe (Candidate A).** A real data value equal to the chosen fill placeholder must still annotate correctly (the `__isnull` mask, not the value, carries match semantics). N/A for B but harmless to run.
   - **7h. Match-count invariant + warning (both).** Assert distinct-annotated-rows == distinct-failed-rows-in; when forced to differ, a warning is logged with both counts and the schema name.
8. **Duplicate full-table rows — record each candidate's behavior (both).** N byte-identical rows, one failed. Assert the run does not error, and snapshot what each candidate marks (A: all N; B: per its tuple-matching) so caveats §2 reflects measured behavior, not assumption.
9. **Performance benchmark — A vs B (the bake-off).** Annotate synthetic tables at increasing sizes (e.g. 10k, 100k, 1M, 10M rows) with a realistic failed-row fraction (e.g. 1–5%) and several columns including nullable ones. Measure wall-clock and peak memory for each candidate. Record results in the plan/PR. Cross-check the per-row Python cost of Candidate B against the existing `_consolidate_grouped_output` loop as a baseline (it is the same pattern, so B should be in the same ballpark). Define a "good enough" threshold for expected real table sizes; if B clears it, prefer B for simplicity, else prefer A.
10. **Decision record.** Document the chosen candidate and why (correctness parity assumed; tie broken by perf then simplicity), and delete the losing implementation and its now-dead handling (e.g. if B wins, remove the mask/placeholder code and §1a casting). The shipped code keeps only one strategy.
11. Integration test with DuckDB adapter: full round-trip, including a table with genuine NULLs in checked columns, asserting failed NULL rows are annotated end-to-end (run against the chosen candidate)
12. Test `save()` per mode: `"failed_rows"` (existing files only), `"annotated"` (annotated + residues, no standalone failed-rows CSVs), `"both"` (failed-rows CSVs + annotated, residues not rewritten). Assert no filename collisions in any mode.
13. Test `output_mode` defaulting: `save()` with no arg falls back to `config.output_mode`; an invalid mode raises `ValueError`
