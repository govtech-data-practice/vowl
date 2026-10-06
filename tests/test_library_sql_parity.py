"""Library and auto-generated checks give the same results as their SQL twin.

Each case runs one contract that holds a library or auto-generated check and a
``type: sql`` check (the twin) with the query vowl generates for it and the
same operator, threshold and unit. Every user-visible result of the two checks
must be equal: the check result, the failed rows, the annotated output, the
row-quality statistics and the check-level DQ metrics.
"""

from __future__ import annotations

import copy
import json
from typing import Any

import conftest as test_conftest
import ibis
import pyarrow as pa
import pytest

import vowl.contracts.contract as contract_module
from vowl.adapters.ibis_adapter import IbisAdapter
from vowl.contracts.models import get_latest_version

_run_validation = test_conftest._ORIGINAL_VALIDATE_DATA

_TWIN = "sql_twin"
_OPERATOR_KEYS = (
    "mustBe",
    "mustNotBe",
    "mustBeGreaterThan",
    "mustBeGreaterOrEqualTo",
    "mustBeLessThan",
    "mustBeLessOrEqualTo",
    "mustBeBetween",
    "mustNotBeBetween",
    "unit",
)


@pytest.fixture(autouse=True)
def _skip_contract_validation(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(contract_module, "validate_contract", lambda data, version: None)


def _contract(schemas: list[dict]) -> contract_module.Contract:
    return contract_module.Contract(
        {
            "apiVersion": get_latest_version(),
            "kind": "DataContract",
            "version": "1.0.0",
            "id": "parity",
            "status": "active",
            "schema": copy.deepcopy(schemas),
        }
    )


def _lib(metric: str, name: str = "lib", **fields: Any) -> dict:
    return {"type": "library", "metric": metric, "name": name, **fields}


def _props(*names: str, **fields: dict) -> list[dict]:
    """Properties named *names* with no fields, then the keyword ones with their fields."""
    return [{"name": n} for n in names] + [{"name": n, **f} for n, f in fields.items()]


# Each case: schemas, the check under test, the table data when it passes and
# when it fails. The check is named "lib" for library checks and keeps its
# generated name otherwise.
_T_ID_C = pa.schema([("id", pa.int64()), ("c", pa.string())])


def _t(rows: list[tuple], schema: pa.Schema = _T_ID_C) -> pa.Table:
    return pa.Table.from_pylist([dict(zip(schema.names, row, strict=True)) for row in rows], schema=schema)


_CASES: dict[str, dict[str, Any]] = {
    "nullValues": {
        "schemas": [{"name": "t", "properties": _props("id", c={"quality": [_lib("nullValues", mustBe=0)]})}],
        "check": "lib",
        "pass": {"t": _t([(1, "a"), (2, "b")])},
        "fail": {"t": _t([(1, "a"), (2, None), (3, None)])},
    },
    "nullValues_percent": {
        "schemas": [
            {
                "name": "t",
                "properties": _props("id", c={"quality": [_lib("nullValues", mustBeLessThan=10, unit="percent")]}),
            }
        ],
        "check": "lib",
        "pass": {"t": _t([(1, "a"), (2, "b")])},
        "fail": {"t": _t([(1, "a"), (2, None), (3, None)])},
    },
    "missingValues": {
        "schemas": [
            {
                "name": "t",
                "properties": _props(
                    "id",
                    c={"quality": [_lib("missingValues", arguments={"missingValues": ["", "N/A"]}, mustBe=0)]},
                ),
            }
        ],
        "check": "lib",
        "pass": {"t": _t([(1, "a"), (2, None)])},
        "fail": {"t": _t([(1, "a"), (2, ""), (3, "N/A"), (4, None)])},
    },
    "invalidValues_validValues": {
        "schemas": [
            {
                "name": "t",
                "properties": _props(
                    "id",
                    c={"quality": [_lib("invalidValues", arguments={"validValues": ["a", "b"]}, mustBeLessThan=2)]},
                ),
            }
        ],
        "check": "lib",
        "pass": {"t": _t([(1, "a"), (2, "z")])},
        "fail": {"t": _t([(1, "a"), (2, "x"), (3, "y"), (4, None)])},
    },
    "invalidValues_pattern": {
        "schemas": [
            {
                "name": "t",
                "properties": _props(
                    "id", c={"quality": [_lib("invalidValues", arguments={"pattern": "^[a-z]+$"}, mustBe=0)]}
                ),
            }
        ],
        "check": "lib",
        "pass": {"t": _t([(1, "abc"), (2, None)])},
        "fail": {"t": _t([(1, "abc"), (2, "A1"), (3, "b c")])},
    },
    "duplicateValues_column": {
        "schemas": [{"name": "t", "properties": _props("id", c={"quality": [_lib("duplicateValues", mustBe=0)]})}],
        "check": "lib",
        "pass": {"t": _t([(1, "a"), (2, "b"), (3, None), (4, None)])},
        "fail": {"t": _t([(1, "a"), (2, "a"), (3, "b")])},
    },
    "duplicateValues_column_percent": {
        "schemas": [
            {
                "name": "t",
                "properties": _props("id", c={"quality": [_lib("duplicateValues", mustBe=0, unit="percent")]}),
            }
        ],
        "check": "lib",
        "pass": {"t": _t([(1, "a"), (2, "b")])},
        "fail": {"t": _t([(1, "a"), (2, "a"), (3, "b")])},
    },
    "duplicateValues_table": {
        "schemas": [
            {
                "name": "t",
                "properties": _props("id", "c"),
                "quality": [_lib("duplicateValues", arguments={"properties": ["id", "c"]}, mustBe=0)],
            }
        ],
        "check": "lib",
        "pass": {"t": _t([(1, "a"), (1, "b")])},
        "fail": {"t": _t([(1, "a"), (1, "a"), (2, "b")])},
    },
    "rowCount_lower_bound": {
        "schemas": [{"name": "t", "properties": _props("id", "c"), "quality": [_lib("rowCount", mustBeGreaterThan=2)]}],
        "check": "lib",
        "pass": {"t": _t([(1, "a"), (2, "b"), (3, "c")])},
        "fail": {"t": _t([(1, "a"), (2, "b")])},
    },
    "rowCount_upper_bound": {
        "schemas": [{"name": "t", "properties": _props("id", "c"), "quality": [_lib("rowCount", mustBeLessThan=3)]}],
        "check": "lib",
        "pass": {"t": _t([(1, "a"), (2, "b")])},
        "fail": {"t": _t([(1, "a"), (2, "b"), (3, "c")])},
    },
    "column_exists": {
        "schemas": [{"name": "t", "properties": _props("id", "c")}],
        "check": "c_column_exists_check",
        "pass": {"t": _t([(1, "a")])},
        "fail": {"t": pa.table({"id": [1]})},
    },
    "logicalType_integer": {
        "schemas": [{"name": "t", "properties": _props("id", c={"logicalType": "integer"})}],
        "check": "c_logical_type_check",
        "pass": {"t": _t([(1, "1"), (2, "-3"), (3, None)])},
        "fail": {"t": _t([(1, "1"), (2, "x"), (3, "1.5")])},
    },
    "logicalType_date": {
        "schemas": [{"name": "t", "properties": _props("id", c={"logicalType": "date"})}],
        "check": "c_logical_type_check",
        "pass": {"t": _t([(1, "2024-01-31"), (2, None)])},
        "fail": {"t": _t([(1, "2024-01-31"), (2, "not a date")])},
    },
    "logicalTypeOptions_maxLength": {
        "schemas": [
            {
                "name": "t",
                "properties": _props("id", c={"logicalType": "string", "logicalTypeOptions": {"maxLength": 2}}),
            }
        ],
        "check": "c_logical_type_options_maxLength_check",
        "pass": {"t": _t([(1, "ab"), (2, None)])},
        "fail": {"t": _t([(1, "ab"), (2, "abc")])},
    },
    "logicalTypeOptions_pattern": {
        "schemas": [
            {
                "name": "t",
                "properties": _props("id", c={"logicalType": "string", "logicalTypeOptions": {"pattern": "^[0-9]+$"}}),
            }
        ],
        "check": "c_logical_type_options_pattern_check",
        "pass": {"t": _t([(1, "12")])},
        "fail": {"t": _t([(1, "12"), (2, "1a")])},
    },
    "logicalTypeOptions_format_email": {
        "schemas": [
            {
                "name": "t",
                "properties": _props("id", c={"logicalType": "string", "logicalTypeOptions": {"format": "email"}}),
            }
        ],
        "check": "c_logical_type_options_format_check",
        "pass": {"t": _t([(1, "a@b.co")])},
        "fail": {"t": _t([(1, "a@b.co"), (2, "nope")])},
    },
    "logicalTypeOptions_minimum": {
        "schemas": [
            {
                "name": "t",
                "properties": [{"name": "id", "logicalType": "integer", "logicalTypeOptions": {"minimum": 2}}],
            }
        ],
        "check": "id_logical_type_options_minimum_check",
        "pass": {"t": pa.table({"id": [2, 3]})},
        "fail": {"t": pa.table({"id": [1, 2, 3]})},
    },
    "array_maxItems": {
        "schemas": [
            {
                "name": "t",
                "properties": [
                    {"name": "id"},
                    {"name": "a", "logicalType": "array", "logicalTypeOptions": {"maxItems": 2}},
                ],
            }
        ],
        "check": "a_logical_type_options_maxItems_check",
        "pass": {"t": pa.table({"id": [1, 2], "a": [[1, 2], [3]]})},
        "fail": {"t": pa.table({"id": [1, 2], "a": [[1, 2, 3], [3]]})},
        "backends": ("duckdb",),
        # Once ibis's Postgres backend is imported, sqlglot renders ArraySize
        # as CARDINALITY in Postgres, which DuckDB rejects on lists.
        "twin_dialect": "duckdb",
    },
    "array_items_enum": {
        "schemas": [
            {
                "name": "t",
                "properties": [
                    {"name": "id"},
                    {"name": "a", "logicalType": "array", "items": {"enum": [{"value": "x"}, {"value": "y"}]}},
                ],
            }
        ],
        "check": "a_array_items_enum_check",
        "pass": {"t": pa.table({"id": [1, 2], "a": [["x"], ["y", "x"]]})},
        "fail": {"t": pa.table({"id": [1, 2], "a": [["x", "z"], ["y"]]})},
        "backends": ("duckdb",),
    },
    "enum": {
        "schemas": [{"name": "t", "properties": _props("id", c={"enum": [{"value": "a"}, {"value": "b"}]})}],
        "check": "c_enum_check",
        "pass": {"t": _t([(1, "a"), (2, None)])},
        "fail": {"t": _t([(1, "a"), (2, "z")])},
    },
    "required": {
        "schemas": [{"name": "t", "properties": _props("id", c={"required": True})}],
        "check": "c_required_check",
        "pass": {"t": _t([(1, "a")])},
        "fail": {"t": _t([(1, "a"), (2, None)])},
    },
    "unique": {
        "schemas": [{"name": "t", "properties": _props("id", c={"unique": True})}],
        "check": "c_unique_check",
        "pass": {"t": _t([(1, "a"), (2, "b"), (3, None), (4, None)])},
        "fail": {"t": _t([(1, "a"), (2, "a"), (3, "b")])},
    },
    "primaryKey": {
        "schemas": [{"name": "t", "properties": _props("c", id={"primaryKey": True})}],
        "check": "id_primary_key_check",
        "pass": {"t": _t([(1, "a"), (2, "b")])},
        "fail": {"t": _t([(1, "a"), (1, "b"), (None, "c")])},
    },
    "composite_primaryKey": {
        "schemas": [{"name": "t", "properties": _props(id={"primaryKey": True}, c={"primaryKey": True})}],
        "check": "t_id_c_primary_key_check",
        "pass": {"t": _t([(1, "a"), (1, "b")])},
        "fail": {"t": _t([(1, "a"), (1, "a"), (2, None)])},
    },
    "property_foreignKey": {
        "schemas": [
            {"name": "p", "properties": [{"name": "id"}]},
            {
                "name": "t",
                "properties": _props("id", c={"relationships": [{"type": "foreignKey", "to": "p.id"}]}),
            },
        ],
        "check": "t_c_foreign_key_check",
        "pass": {"p": pa.table({"id": ["a", "b"]}), "t": _t([(1, "a"), (2, None)])},
        "fail": {"p": pa.table({"id": ["a", "b"]}), "t": _t([(1, "a"), (2, "z")])},
        "backends": ("duckdb",),
    },
    "schema_foreignKey": {
        "schemas": [
            {"name": "p", "properties": [{"name": "k1"}, {"name": "k2"}]},
            {
                "name": "t",
                "properties": _props("id", "c"),
                "relationships": [{"type": "foreignKey", "from": ["t.id", "t.c"], "to": ["p.k1", "p.k2"]}],
            },
        ],
        "check": "t_id_c_foreign_key_check",
        "pass": {"p": pa.table({"k1": [1, 2], "k2": ["a", "b"]}), "t": _t([(1, "a"), (2, None)])},
        "fail": {"p": pa.table({"k1": [1, 2], "k2": ["a", "b"]}), "t": _t([(1, "a"), (1, "b")])},
        "backends": ("duckdb",),
    },
}


def _find_ref(contract: contract_module.Contract, name: str):
    for refs in contract.get_check_references_by_schema().values():
        for ref in refs:
            if ref.get_check_name() == name:
                return ref
    raise AssertionError(f"no check named {name!r}")


def _with_twin(schemas: list[dict], ref, case_dialect: str = "postgres") -> list[dict]:
    """The same schemas plus a ``type: sql`` check with the query vowl generates for *ref*."""
    check = ref.get_check()
    twin = {
        "type": "sql",
        "name": _TWIN,
        "query": ref.get_query(case_dialect),
        **{key: check[key] for key in _OPERATOR_KEYS if key in check},
    }
    if check.get("dimension"):
        twin["dimension"] = check["dimension"]
    schemas = copy.deepcopy(schemas)
    anchor = next(schema for schema in schemas if schema["name"] == ref.get_schema_name())
    anchor.setdefault("quality", []).append(twin)
    return schemas


def _run(case: dict, data: dict[str, pa.Table], backend: str):
    ref = _find_ref(_contract(case["schemas"]), case["check"])
    contract = _contract(_with_twin(case["schemas"], ref, case.get("twin_dialect", "postgres")))
    if backend == "pandas":
        (table,) = data.values()
        return _run_validation(contract, df=table.to_pandas())
    con = ibis.duckdb.connect()
    for name, table in data.items():
        con.create_table(name, table)
    return _run_validation(contract, adapters={name: IbisAdapter(con) for name in data})


def _view(result, name: str) -> dict[str, Any]:
    """Every user-visible result of the check *name*, with the name left out."""
    (cr,) = [cr for cr in result.check_results if cr.check_name == name]
    key = result._output_key(cr)
    outputs = result.get_output_dfs()
    failed_rows = None
    if key in outputs:
        frame = outputs[key].to_arrow().drop_columns(["check_id"])
        failed_rows = sorted(json.dumps(row, sort_keys=True, default=str) for row in frame.to_pylist())

    annotated = result.get_annotated_output()
    flags = {}
    for schema, frame in annotated["annotated"].items():
        flags[schema] = [
            any(info["check_name"] == name for info in json.loads(row["check_info"] or "[]"))
            for row in frame.to_arrow().to_pylist()
        ]

    (row_quality,) = [
        row for row in result.get_row_quality_df(by="check").to_arrow().to_pylist() if row["check_name"] == name
    ]
    metric_points = sorted(
        (point["name"], point["attributes"].get("status"), point["value"])
        for point in result.get_dq_metrics()["points"]
        if point["name"].startswith("vowl.check.row.") and point["attributes"].get("check_name") == name
    )
    return {
        "status": cr.status,
        "actual_value": cr.actual_value,
        "failed_rows_count": cr.failed_rows_count,
        "supports_row_level_output": cr.supports_row_level_output,
        "failed_rows": failed_rows,
        "residue": key in annotated["residues"],
        "annotated_flags": flags,
        "row_quality": {
            field: row_quality[field] for field in ("row_level", "route", "reason", "scalar_count", "attributed_rows")
        },
        "check_row_metrics": metric_points,
    }


_PARAMS = [
    pytest.param(case_name, outcome, backend, id=f"{case_name}-{outcome}-{backend}")
    for case_name, case in _CASES.items()
    for outcome in ("pass", "fail")
    for backend in case.get("backends", ("duckdb", "pandas"))
]


@pytest.mark.parametrize(("case_name", "outcome", "backend"), _PARAMS)
def test_check_matches_its_sql_twin(case_name: str, outcome: str, backend: str):
    case = _CASES[case_name]
    result = _run(case, case[outcome], backend)

    check, twin = _view(result, case["check"]), _view(result, _TWIN)

    assert check == twin
    if case_name == "column_exists" and outcome == "fail":
        assert check["status"] == "ERROR"
    else:
        assert check["status"] == ("PASSED" if outcome == "pass" else "FAILED")


def test_failing_row_count_reports_the_table_size_and_no_rows():
    """rowCount mustBeGreaterThan behaves like SELECT COUNT(*) FROM t: the
    count is the table size and the operator does not set an upper limit."""
    case = _CASES["rowCount_lower_bound"]
    result = _run(case, case["fail"], "duckdb")

    view = _view(result, "lib")

    assert view["failed_rows_count"] == 2
    assert view["supports_row_level_output"] is True
    assert view["residue"] is False
    assert view["row_quality"]["row_level"] is False
    assert view["row_quality"]["reason"] == "operator does not set an upper limit"
    assert view["check_row_metrics"] == []
    assert not any(view["annotated_flags"]["t"])
