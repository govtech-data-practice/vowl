"""The pushdown and table_match routes: count failing rows inside the data source.

Every counted check becomes one tagged branch. Branches are packed into chunks
that stay within the engine limits, each chunk is one statement, and a chunk
that fails is bisected until the failing branches are found and dropped. See
"Hardened form" in ``design/row-quality-statistics.md``.

The statements are assembled as text from identifiers and key expressions that
sqlglot rendered for the adapter's dialect. Parsing and re-rendering the whole
wrapper would let sqlglot rewrite functions such as ``encode``, ``typeof`` or
``printf``, and would cost time linear in the branches for each chunk anyway.
The top level is always a single ``WITH ... SELECT``, the only statement shape
the security validator accepts.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import sqlglot
from sqlglot import exp

from .keys import cast, column_ref, key_expression, null_safe_equal, quote, value_aggregate

logger = logging.getLogger(__name__)

# Chunk budgets. Module constants so tests can lower them.
MAX_BRANCHES = 250
MAX_SQL_BYTES = 512_000
MAX_OUTPUT_COLUMNS = 1000
WORD_BITS = 63

# Dialects where every UNION ALL branch is its own table scan, so certified
# checks are lifted into one scan with a CASE term per check.
SINGLE_SCAN_DIALECTS = {"spark", "databricks"}

ANCHOR_ALIAS = "_vowl_a"


@dataclass
class KeySpec:
    """The key and value columns every branch of one schema projects."""

    dialect: str
    anchor_sql: str
    columns: list[str]
    dtypes: list[Any]
    with_values: bool = False

    @property
    def key_aliases(self) -> list[str]:
        return [f"_vowl_c{i}" for i in range(len(self.columns))]

    @property
    def value_aliases(self) -> list[str]:
        return [f"_vowl_v{i}" for i in range(len(self.columns))] if self.with_values else []

    @property
    def data_column_count(self) -> int:
        return len(self.key_aliases) + len(self.value_aliases)

    def key(self, alias: str, index: int) -> str:
        column = column_ref(alias, self.columns[index], self.dialect)
        return key_expression(column, self.dtypes[index], self.dialect)

    def projections(self, alias: str) -> list[str]:
        """``key AS _vowl_c{i}`` and, when needed, ``value AS _vowl_v{i}``."""
        items = [f"{self.key(alias, i)} AS {name}" for i, name in enumerate(self.key_aliases)]
        items += [
            f"{column_ref(alias, self.columns[i], self.dialect)} AS {name}" for i, name in enumerate(self.value_aliases)
        ]
        return items

    def max_words(self) -> int:
        # keys + values + words + copies must stay under the column limit.
        return MAX_OUTPUT_COLUMNS - 2 - self.data_column_count

    def can_push_down(self) -> bool:
        return bool(self.columns) and self.max_words() >= 1


@dataclass
class Branch:
    """One check's contribution to a chunk.

    Attributes:
        check_id: Schema-local check id, the bit the check owns in the masks.
        kind: ``"pushdown"`` for a certified row filter, ``"table_match"`` for
            a check whose anchor rows are matched against its failed rows.
        query: The check's filtered failed-rows query.
        scan_from: For the single-scan form, the rendered FROM clause.
        scan_alias: For the single-scan form, the FROM clause's alias.
        scan_predicate: For the single-scan form, the lifted WHERE predicate.
    """

    check_id: int
    kind: str
    query: str
    scan_from: str | None = None
    scan_alias: str | None = None
    scan_predicate: str | None = None


@dataclass
class Chunk:
    branches: list[Branch]
    scan_from: str | None = None
    scan_alias: str | None = None

    @property
    def is_scan(self) -> bool:
        return self.scan_from is not None


def lift_scan(query: str, dialect: str) -> tuple[str, str, str] | None:
    """Split a certified failed-rows query into (FROM, alias, predicate)."""
    try:
        parsed = sqlglot.parse_one(query, dialect=dialect)
    except Exception:
        return None
    if not isinstance(parsed, exp.Select):
        return None
    from_clause = parsed.args.get("from_") or parsed.args.get("from")
    if from_clause is None:
        return None
    source = from_clause.this
    where = parsed.args.get("where")
    predicate = where.this.sql(dialect=dialect) if where is not None else "1 = 1"
    return source.sql(dialect=dialect), source.alias_or_name, predicate


def _position(index: int) -> tuple[int, int]:
    return index // WORD_BITS, 1 << (index % WORD_BITS)


def _words(branch_count: int) -> int:
    return (branch_count + WORD_BITS - 1) // WORD_BITS


def union_branch_sql(spec: KeySpec, branch: Branch, index: int) -> str:
    word, bit = _position(index)
    tags = f"{word} AS _vowl_w, {cast(str(bit), 'BIGINT', spec.dialect)} AS _vowl_bit"
    if branch.kind == "table_match":
        failed_keys = ", ".join(f"{spec.key('_vowl_f', i)} AS {name}" for i, name in enumerate(spec.key_aliases))
        matches = " AND ".join(
            null_safe_equal(f"{quote('_vowl_d', spec.dialect)}.{name}", spec.key(ANCHOR_ALIAS, i), spec.dialect)
            for i, name in enumerate(spec.key_aliases)
        )
        return (
            f"SELECT {', '.join(spec.projections(ANCHOR_ALIAS))}, {tags} "
            f"FROM ({spec.anchor_sql}) AS {quote(ANCHOR_ALIAS, spec.dialect)} "
            f"WHERE EXISTS (SELECT 1 FROM (SELECT DISTINCT {failed_keys} "
            f"FROM ({branch.query}) AS {quote('_vowl_f', spec.dialect)}) AS {quote('_vowl_d', spec.dialect)} "
            f"WHERE {matches})"
        )
    alias = f"_vowl_q{index}"
    return f"SELECT {', '.join(spec.projections(alias))}, {tags} FROM ({branch.query}) AS {quote(alias, spec.dialect)}"


def _per_row_ctes(spec: KeySpec, chunk: Chunk) -> list[tuple[str, str]]:
    keys = ", ".join(spec.key_aliases)
    values = "".join(f", {value_aggregate(name, spec.dialect)} AS {name}" for name in spec.value_aliases)
    words = _words(len(chunk.branches))

    if chunk.is_scan:
        masks = []
        for word in range(words):
            members = chunk.branches[word * WORD_BITS : (word + 1) * WORD_BITS]
            terms = " + ".join(
                f"CASE WHEN ({branch.scan_predicate}) THEN {cast(str(1 << bit), 'BIGINT', spec.dialect)} "
                f"ELSE {cast('0', 'BIGINT', spec.dialect)} END"
                for bit, branch in enumerate(members)
            )
            masks.append(f"{cast(terms, 'BIGINT', spec.dialect)} AS _vowl_m{word}")
        alias = chunk.scan_alias or ""
        any_row = " OR ".join(f"({branch.scan_predicate})" for branch in chunk.branches)
        scan = f"SELECT {', '.join(spec.projections(alias) + masks)} FROM {chunk.scan_from} WHERE {any_row}"
        mask_max = ", ".join(f"MAX(_vowl_m{word}) AS _vowl_m{word}" for word in range(words))
        per_row = f"SELECT {keys}, {mask_max}, COUNT(*) AS _vowl_copies{values} FROM _vowl_scan GROUP BY {keys}"
        return [("_vowl_scan", scan), ("_vowl_per_row", per_row)]

    tagged = " UNION ALL ".join(union_branch_sql(spec, branch, index) for index, branch in enumerate(chunk.branches))
    per_check = (
        f"SELECT {keys}, _vowl_w, _vowl_bit, COUNT(*) AS _vowl_copies{values} "
        f"FROM _vowl_tagged GROUP BY {keys}, _vowl_w, _vowl_bit"
    )
    mask_sums = ", ".join(
        f"{cast(f'SUM(CASE WHEN _vowl_w = {word} THEN _vowl_bit ELSE 0 END)', 'BIGINT', spec.dialect)} AS _vowl_m{word}"
        for word in range(words)
    )
    per_row = (
        f"SELECT {keys}, {mask_sums}, MAX(_vowl_copies) AS _vowl_copies{values} FROM _vowl_per_check GROUP BY {keys}"
    )
    return [("_vowl_tagged", tagged), ("_vowl_per_check", per_check), ("_vowl_per_row", per_row)]


def _with(ctes: Sequence[tuple[str, str]], final: str) -> str:
    return "WITH " + ", ".join(f"{name} AS ({sql})" for name, sql in ctes) + " " + final


def per_row_statement(spec: KeySpec, chunk: Chunk) -> str:
    """One row per distinct failing key: keys, mask words, copies, values."""
    return _with(_per_row_ctes(spec, chunk), "SELECT * FROM _vowl_per_row")


def histogram_statement(spec: KeySpec, chunk: Chunk) -> str:
    """One row per failure pattern, plus the table's total in the same statement."""
    words = _words(len(chunk.branches))
    masks = ", ".join(f"_vowl_m{word}" for word in range(words))
    rows = cast("SUM(_vowl_copies)", "BIGINT", spec.dialect)
    hist = f"SELECT {masks}, {rows} AS _vowl_rows FROM _vowl_per_row GROUP BY {masks}"
    anchor = quote(ANCHOR_ALIAS, spec.dialect)
    final = (
        "SELECT _vowl_t._vowl_total, _vowl_hist.* "
        f"FROM (SELECT COUNT(*) AS _vowl_total FROM ({spec.anchor_sql}) AS {anchor}) AS _vowl_t "
        "LEFT JOIN _vowl_hist ON 1 = 1"
    )
    return _with([*_per_row_ctes(spec, chunk), ("_vowl_hist", hist)], final)


def preflight_statement(spec: KeySpec) -> str:
    """Checks that the key expressions bind and can be grouped, without reading rows.

    The GROUP BY matters: a type the engine cannot group (JSON on some
    engines, for example) would otherwise pass here and fail every chunk.
    """
    anchor = quote(ANCHOR_ALIAS, spec.dialect)
    keys = ", ".join(spec.key_aliases)
    return (
        f"SELECT {keys} FROM (SELECT {', '.join(spec.projections(ANCHOR_ALIAS))} "
        f"FROM ({spec.anchor_sql}) AS {anchor} WHERE 1 = 0) AS {quote('_vowl_k', spec.dialect)} GROUP BY {keys}"
    )


def count_statement(anchor_sql: str, dialect: str) -> str:
    """Counts the rows of the filtered anchor."""
    return f"SELECT COUNT(*) AS _vowl_total FROM ({anchor_sql}) AS {quote(ANCHOR_ALIAS, dialect)}"


def column_probe_statement(query: str, dialect: str) -> str:
    """Returns a query's columns without reading rows.

    ``WHERE 1 = 0`` is used instead of ``LIMIT 0`` because not every dialect
    supports LIMIT.
    """
    return f"SELECT * FROM ({query}) AS {quote('_vowl_p', dialect)} WHERE 1 = 0"


def duplicate_key_statement(spec: KeySpec) -> str:
    """Counts the key values that appear on more than one row of the anchor."""
    anchor = quote(ANCHOR_ALIAS, spec.dialect)
    keys = [spec.key(ANCHOR_ALIAS, i) for i in range(len(spec.columns))]
    return (
        "SELECT COUNT(*) AS _vowl_dups FROM ("
        f"SELECT {', '.join(f'{key} AS _vowl_c{i}' for i, key in enumerate(keys))} "
        f"FROM ({spec.anchor_sql}) AS {anchor} "
        f"GROUP BY {', '.join(keys)} HAVING COUNT(*) > 1) AS _vowl_dup"
    )


def plan_chunks(spec: KeySpec, branches: Sequence[Branch]) -> list[Chunk]:
    """Pack branches into chunks that stay within every budget.

    In a single-scan dialect, certified branches are grouped by their FROM
    clause into scan chunks. Every other branch goes into UNION ALL chunks.
    """
    max_per_chunk = max(1, min(MAX_BRANCHES, spec.max_words() * WORD_BITS))
    scan_groups: dict[tuple[str, str], list[Branch]] = {}
    union: list[Branch] = []
    for branch in branches:
        if branch.scan_from is not None and spec.dialect in SINGLE_SCAN_DIALECTS:
            scan_groups.setdefault((branch.scan_from, branch.scan_alias or ""), []).append(branch)
        else:
            union.append(branch)

    # The parts of a statement that do not repeat per branch: the projections
    # (in the scan CTE or the anchor's COUNT), the CTE wrappers and the
    # histogram. Counted generously so the budget holds for the whole statement.
    fixed = 3 * _bytes(", ".join(spec.projections(ANCHOR_ALIAS))) + _bytes(spec.anchor_sql) + 4096
    budget = max(MAX_SQL_BYTES - fixed, 0)
    # Sizes are measured at the widest word and bit a chunk can hold.
    widest = max_per_chunk - 1

    def scan_size(branch: Branch) -> int:
        # Each predicate appears twice: in its CASE term and in the WHERE.
        high_bit = cast(str(1 << (WORD_BITS - 1)), "BIGINT", spec.dialect)
        term = f"CASE WHEN () THEN {high_bit} ELSE {cast('0', 'BIGINT', spec.dialect)} END +  OR ()"
        return 2 * _bytes(branch.scan_predicate or "") + _bytes(term)

    def union_size(branch: Branch) -> int:
        return _bytes(union_branch_sql(spec, branch, widest)) + len(" UNION ALL ")

    chunks: list[Chunk] = []
    for (scan_from, scan_alias), members in scan_groups.items():
        for group in _pack(members, max_per_chunk, scan_size, budget):
            chunks.append(Chunk(group, scan_from=scan_from, scan_alias=scan_alias))
    for group in _pack(union, max_per_chunk, union_size, budget):
        chunks.append(Chunk(group))
    return chunks


def _bytes(sql: str) -> int:
    return len(sql.encode("utf-8"))


def _pack(
    branches: Sequence[Branch], max_per_chunk: int, size: Callable[[Branch], int], budget: int
) -> list[list[Branch]]:
    """Greedily fill chunks up to *max_per_chunk* branches and *budget* bytes.

    A branch larger than the budget on its own still runs, alone.
    """
    groups: list[list[Branch]] = []
    current: list[Branch] = []
    current_bytes = 0
    for branch in branches:
        branch_bytes = size(branch)
        if current and (len(current) >= max_per_chunk or current_bytes + branch_bytes > budget):
            groups.append(current)
            current, current_bytes = [], 0
        current.append(branch)
        current_bytes += branch_bytes
    if current:
        groups.append(current)
    return groups


@dataclass
class PushdownOutcome:
    """What the pushdown statements of one schema returned.

    Attributes:
        rows: Per distinct binary key: ``[mask, copies, reference]``. The mask
            is a Python int over schema-local check ids. The reference is
            ``(table, row)`` into ``value_tables`` when values were requested.
            Empty when the histogram form ran.
        value_tables: One Arrow table of value columns per chunk that ran.
        histogram: ``(mask, rows)`` per failure pattern, when the whole
            schema ran as one histogram statement.
        total_rows: The table's row count, from the histogram statement.
        dropped: Check ids whose branch failed on its own and was dropped.
    """

    rows: dict[tuple[Any, ...], list[Any]] = field(default_factory=dict)
    value_tables: list[pa.Table] = field(default_factory=list)
    histogram: list[tuple[int, int]] | None = None
    total_rows: int | None = None
    dropped: set[int] = field(default_factory=set)


def _decode_mask(words: Sequence[Any], branches: Sequence[Branch]) -> int:
    mask = 0
    for word_index, word in enumerate(words):
        if not word:
            continue
        word = int(word)
        base = word_index * WORD_BITS
        while word:
            low = word & -word
            mask |= 1 << branches[base + low.bit_length() - 1].check_id
            word ^= low
    return mask


class PushdownRunner:
    """Runs the chunks of one schema against the anchor adapter."""

    def __init__(self, run_query: Callable[[str], pa.Table], spec: KeySpec) -> None:
        self._run = run_query
        self._spec = spec

    def run(self, branches: Sequence[Branch], *, histogram: bool) -> PushdownOutcome:
        outcome = PushdownOutcome()
        chunks = plan_chunks(self._spec, branches)
        if histogram and len(chunks) == 1 and not self._spec.with_values:
            if self._run_histogram(chunks[0], outcome):
                return outcome
        for chunk in chunks:
            self._run_per_row(chunk, outcome)
        return outcome

    def _run_histogram(self, chunk: Chunk, outcome: PushdownOutcome) -> bool:
        try:
            table = self._run(histogram_statement(self._spec, chunk))
        except Exception as exc:
            logger.debug("Row-quality histogram statement failed, retrying per row: %s", exc)
            return False
        words = _words(len(chunk.branches))
        totals = table.column("_vowl_total").to_pylist()
        counts = table.column("_vowl_rows").to_pylist()
        mask_columns = [table.column(f"_vowl_m{word}").to_pylist() for word in range(words)]
        outcome.total_rows = int(totals[0]) if totals and totals[0] is not None else None
        outcome.histogram = []
        for row, count in enumerate(counts):
            if count is None:
                continue
            mask = _decode_mask([column[row] for column in mask_columns], chunk.branches)
            if mask:
                outcome.histogram.append((mask, int(count)))
        return True

    def _run_per_row(self, chunk: Chunk, outcome: PushdownOutcome) -> None:
        try:
            table = self._run(per_row_statement(self._spec, chunk))
        except Exception as exc:
            if len(chunk.branches) == 1:
                logger.warning(
                    "Row-quality statistics dropped a check whose rows could not be counted in the data source: %s",
                    exc,
                )
                outcome.dropped.add(chunk.branches[0].check_id)
                return
            middle = len(chunk.branches) // 2
            for half in (chunk.branches[:middle], chunk.branches[middle:]):
                self._run_per_row(Chunk(half, scan_from=chunk.scan_from, scan_alias=chunk.scan_alias), outcome)
            return
        self._merge(table, chunk, outcome)

    def _merge(self, table: pa.Table, chunk: Chunk, outcome: PushdownOutcome) -> None:
        """Fold one chunk into the outcome: OR the masks, take the MAX of copies."""
        spec = self._spec
        words = _words(len(chunk.branches))
        keys = list(zip(*(table.column(name).to_pylist() for name in spec.key_aliases), strict=True))
        masks = [table.column(f"_vowl_m{word}").to_pylist() for word in range(words)]
        copies = table.column("_vowl_copies").to_pylist()
        table_index = len(outcome.value_tables)
        if spec.value_aliases:
            # Kept as Arrow, so the values keep their exact types for the
            # cross-route merge.
            outcome.value_tables.append(table.select(spec.value_aliases))
        for row, key in enumerate(keys):
            mask = _decode_mask([column[row] for column in masks], chunk.branches)
            entry = outcome.rows.get(key)
            if entry is None:
                reference = (table_index, row) if spec.value_aliases else None
                outcome.rows[key] = [mask, int(copies[row]), reference]
            else:
                entry[0] |= mask
                entry[1] = max(entry[1], int(copies[row]))
