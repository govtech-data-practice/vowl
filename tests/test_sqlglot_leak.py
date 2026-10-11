"""Rendering SQL for DuckDB must not leave sqlglot expressions alive.

The compiled sqlglot build (sqlglotc) of 30.2.0 and 30.4.0 leaks about 390
expression objects per ``.sql(dialect="duckdb")`` call. vowl renders several
queries per check, so a long-running process builds up a heap whose full GC
passes stall checks for hundreds of milliseconds. pyproject.toml floors
sqlglot above the leaking releases. This test catches the leak if a later
release brings it back.
"""

from __future__ import annotations

import gc

import pytest
import sqlglot
import sqlglot.expressions as exp

_CALLS = 50


def _live_expressions() -> int:
    gc.collect()
    return sum(1 for obj in gc.get_objects() if isinstance(obj, exp.Expression))


@pytest.mark.parametrize("dialect", ["duckdb", "postgres", "snowflake"])
def test_render_does_not_leak_expressions(dialect: str) -> None:
    ast = sqlglot.parse_one(
        'SELECT COUNT(*) FROM "t" WHERE "a" IS NULL OR "a" IN '
        '(SELECT "a" FROM "t" WHERE NOT "a" IS NULL GROUP BY "a" HAVING COUNT(*) > 1)',
        read="duckdb",
    )
    # The first call may build lazy, process-wide state. Only growth after it counts.
    ast.sql(dialect=dialect)
    before = _live_expressions()
    for _ in range(_CALLS):
        ast.sql(dialect=dialect)
    leaked = _live_expressions() - before

    assert leaked < _CALLS, f"{leaked} sqlglot expressions survived {_CALLS} renders to {dialect}"
