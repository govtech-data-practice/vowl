"""``save(outputs=[...])``: choosing outputs, per-check file names and the deprecated ``output_mode``."""

from __future__ import annotations

import json
import warnings

import pyarrow as pa
import pyarrow.csv as pa_csv
import pytest
from test_annotated_output import _FakeAdapter, _make_check, _make_result

from vowl.config import DEFAULT_SAVE_OUTPUTS, SAVE_OUTPUTS, ValidationConfig

_FULL = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})


def _check(name, schema="orders", **kwargs):
    kwargs.setdefault("failed_rows", pa.table({"id": [2], "name": ["b"]}))
    kwargs.setdefault("tables_in_query", schema)
    return _make_check(name, schema, **kwargs)


def _result(*checks, config=None, schemas=("orders",)):
    return _make_result(list(checks), {s: _FakeAdapter(_FULL) for s in schemas}, config=config)


def _files(root):
    return {str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()}


def _saved_outputs(root, prefix="r"):
    return json.loads((root / f"{prefix}_summary.json").read_text())["saved_outputs"]


# --------------------------------------------------------------------------- #
# Choosing outputs
# --------------------------------------------------------------------------- #


def test_default_is_every_output_but_all_query_outputs():
    assert set(SAVE_OUTPUTS) - set(DEFAULT_SAVE_OUTPUTS) == {"all_query_outputs"}
    assert ValidationConfig().outputs == list(DEFAULT_SAVE_OUTPUTS)


def test_default_file_set(tmp_path):
    _result(_check("c")).save(str(tmp_path), prefix="r")
    assert _files(tmp_path) == {
        "r_check_results.csv",
        "r_summary.json",
        "r_checks/orders__c.csv",
        "r_orders.csv",
        "r_orders_annotated.csv",
        "r_dq_metrics.json",
    }


@pytest.mark.parametrize(
    ("output", "written"),
    [
        ("failed_query_outputs", {"r_checks/orders__c.csv"}),
        ("all_query_outputs", {"r_checks/orders__c.csv"}),
        ("consolidated_query_outputs", {"r_orders.csv"}),
        ("annotated_table", {"r_orders_annotated.csv"}),
        ("dq_metrics", {"r_dq_metrics.json"}),
    ],
)
def test_each_output_alone(tmp_path, output, written):
    _result(_check("c")).save(str(tmp_path), prefix="r", outputs=[output])
    assert _files(tmp_path) == {"r_check_results.csv", "r_summary.json"} | written
    assert list(_saved_outputs(tmp_path)) == [output]


def test_empty_list_writes_the_summary_only(tmp_path):
    _result(_check("c")).save(str(tmp_path), prefix="r", outputs=[])
    assert _files(tmp_path) == {"r_check_results.csv", "r_summary.json"}
    assert _saved_outputs(tmp_path) == {}


def test_unknown_output_raises(tmp_path):
    with pytest.raises(ValueError, match="annotated_tables"):
        _result(_check("c")).save(str(tmp_path), outputs=["annotated_tables"])
    assert not list(tmp_path.iterdir())


def test_a_bare_string_raises(tmp_path):
    with pytest.raises(TypeError):
        _result(_check("c")).save(str(tmp_path), outputs="dq_metrics")


def test_failed_and_all_query_outputs_together_raise(tmp_path):
    with pytest.raises(ValueError, match="failed_query_outputs.*all_query_outputs"):
        _result(_check("c")).save(str(tmp_path), outputs=["failed_query_outputs", "all_query_outputs"])
    with pytest.raises(ValueError, match="failed_query_outputs.*all_query_outputs"):
        ValidationConfig(outputs=["all_query_outputs", "failed_query_outputs"])


def test_outputs_from_the_config(tmp_path):
    result = _result(_check("c"), config=ValidationConfig(outputs=["dq_metrics"]))
    result.save(str(tmp_path), prefix="r")
    assert _files(tmp_path) == {"r_check_results.csv", "r_summary.json", "r_dq_metrics.json"}


def test_save_outputs_override_the_config(tmp_path):
    result = _result(_check("c"), config=ValidationConfig(outputs=["dq_metrics"]))
    result.save(str(tmp_path), prefix="r", outputs=["annotated_table"])
    assert "r_dq_metrics.json" not in _files(tmp_path)
    assert "r_orders_annotated.csv" in _files(tmp_path)


def test_config_records_outputs_not_output_mode():
    as_dict = ValidationConfig(outputs=["dq_metrics"]).to_dict()
    assert as_dict["outputs"] == ["dq_metrics"]
    assert "output_mode" not in as_dict


def test_check_info_without_annotated_table_warns(tmp_path):
    with pytest.warns(UserWarning, match="check_info"):
        _result(_check("c")).save(str(tmp_path), outputs=["dq_metrics"], check_info="summary")


def test_check_info_and_definitions_with_annotated_table_do_not_warn(tmp_path):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _result(_check("c")).save(
            str(tmp_path),
            outputs=["annotated_table"],
            check_info="summary",
            include_check_definition=True,
            include_contract_definition=True,
        )
        _result(_check("c")).save(str(tmp_path), outputs=[], include_check_definition=True)


# --------------------------------------------------------------------------- #
# Deprecated output_mode
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("mode", "outputs"),
    [
        ("failed_rows", ["consolidated_query_outputs"]),
        ("annotated", ["annotated_table", "dq_metrics"]),
        ("both", ["consolidated_query_outputs", "annotated_table", "dq_metrics"]),
    ],
)
def test_output_mode_maps_to_outputs_with_a_warning(tmp_path, mode, outputs):
    with pytest.warns(FutureWarning, match=rf"output_mode='{mode}' is deprecated, use outputs=.*v0\.1\.0"):
        _result(_check("c")).save(str(tmp_path), prefix="r", output_mode=mode)
    assert list(_saved_outputs(tmp_path)) == outputs
    with pytest.warns(FutureWarning, match=rf"output_mode='{mode}' is deprecated"):
        config = ValidationConfig(output_mode=mode)
    assert config.outputs == outputs
    assert config.output_mode is None


@pytest.mark.parametrize("mode", ["as_is", "attributed", "anotated"])
def test_unknown_output_mode_raises(tmp_path, mode):
    with pytest.raises(ValueError, match="output_mode"):
        _result(_check("c")).save(str(tmp_path), output_mode=mode)


def test_output_mode_and_outputs_together_raise(tmp_path):
    with pytest.raises(ValueError, match="outputs=.*output_mode="):
        _result(_check("c")).save(str(tmp_path), outputs=["dq_metrics"], output_mode="both")
    with pytest.raises(ValueError, match="outputs=.*output_mode="):
        ValidationConfig(outputs=["dq_metrics"], output_mode="both")


# --------------------------------------------------------------------------- #
# Per-check files and saved_outputs
# --------------------------------------------------------------------------- #


def test_per_check_file_columns(tmp_path):
    _result(_check("c")).save(str(tmp_path), prefix="r", outputs=["failed_query_outputs"])
    table = pa_csv.read_csv(tmp_path / "r_checks" / "orders__c.csv")
    assert table.column_names == ["id", "name", "check_id", "tables_in_query"]
    assert table.column("id").to_pylist() == [2]


def test_all_query_outputs_cover_passed_checks_with_a_status(tmp_path):
    passed = _check("p", status="PASSED", failed_rows=pa.table({"id": [3], "name": ["c"]}))
    _result(_check("c"), passed).save(str(tmp_path), prefix="r", outputs=["all_query_outputs"])
    assert _files(tmp_path) - {"r_check_results.csv", "r_summary.json"} == {
        "r_checks/orders__c.csv",
        "r_checks/orders__p.csv",
    }
    table = pa_csv.read_csv(tmp_path / "r_checks" / "orders__p.csv")
    assert table.column("status").to_pylist() == ["PASSED"]


def test_saved_outputs_lists_every_file_and_zero_row_checks(tmp_path):
    empty = _check("empty", failed_rows=pa.table({"id": pa.array([], pa.int64()), "name": pa.array([], pa.string())}))
    _result(_check("c"), empty).save(str(tmp_path), prefix="r", outputs=["all_query_outputs", "dq_metrics"])
    assert _saved_outputs(tmp_path) == {
        "all_query_outputs": [
            {"schema": "orders", "check": "c", "rows": 1, "file": "r_checks/orders__c.csv"},
            {"schema": "orders", "check": "empty", "rows": 0},
        ],
        "dq_metrics": [{"file": "r_dq_metrics.json"}],
    }
    assert not (tmp_path / "r_checks" / "orders__empty.csv").exists()


def test_no_checks_folder_when_no_check_has_rows(tmp_path):
    passed = _check("p", status="PASSED")
    _result(passed).save(str(tmp_path), prefix="r", outputs=["failed_query_outputs"])
    assert not (tmp_path / "r_checks").exists()


def test_double_underscore_join_keeps_schema_and_check_apart(tmp_path):
    first = _check("c", schema="a_b")
    second = _check("b_c", schema="a")
    _result(first, second, schemas=("a_b", "a")).save(str(tmp_path), prefix="r", outputs=["failed_query_outputs"])
    assert {p.name for p in (tmp_path / "r_checks").iterdir()} == {"a_b__c.csv", "a__b_c.csv"}


def test_check_names_are_cleaned_on_their_own(tmp_path):
    _result(_check("amount > 0")).save(str(tmp_path), prefix="r", outputs=["failed_query_outputs"])
    assert {p.name for p in (tmp_path / "r_checks").iterdir()} == {"orders__amount_0.csv"}


@pytest.mark.parametrize(("first", "second"), [("amount > 0", "amount_0"), ("Amount", "amount")])
def test_names_that_clean_to_the_same_file_raise_before_writing(tmp_path, first, second):
    result = _result(_check(first), _check(second))
    with pytest.raises(ValueError, match=rf"'{first}' and '{second}'.*Rename one"):
        result.save(str(tmp_path), prefix="r")
    assert not list(tmp_path.iterdir())


def test_clashing_names_are_fine_when_no_per_check_output_is_written(tmp_path):
    _result(_check("amount > 0"), _check("amount_0")).save(str(tmp_path), outputs=["consolidated_query_outputs"])


def test_duplicate_check_names_raise_in_save_and_warn_in_get_output_dfs(tmp_path):
    result = _result(_check("c"), _check("c", failed_rows=pa.table({"id": [3], "name": ["c"]})))
    with pytest.raises(ValueError, match="both named 'c'.*Rename one"):
        result.save(str(tmp_path), prefix="r")
    assert not list(tmp_path.iterdir())
    with pytest.warns(UserWarning, match=r"orders::c.*Rename one"):
        out = result.get_output_dfs()
    assert out["orders::c"]["id"].to_list() == [3]  # the later check wins


# --------------------------------------------------------------------------- #
# Residues and grouped CSVs
# --------------------------------------------------------------------------- #


def _residue_check(name, schema="orders"):
    return _check(
        name,
        schema,
        failed_rows=pa.table({"id": [3], "name": ["c"], "ref_id": [None]}),
        tables_in_query=f"{schema}, customers",
    )


def test_residue_files_use_the_double_underscore_join(tmp_path):
    _result(_residue_check("join check")).save(str(tmp_path), prefix="r", outputs=["annotated_table"])
    assert (tmp_path / "r_orders__join_check_residue.csv").exists()
    assert _saved_outputs(tmp_path)["annotated_table"][-1] == {
        "schema": "orders",
        "check": "join check",
        "rows": 1,
        "file": "r_orders__join_check_residue.csv",
    }


def test_residue_names_that_clash_raise(tmp_path):
    result = _result(_residue_check("join check"), _residue_check("Join_Check"))
    with pytest.raises(ValueError, match="residue files"):
        result.save(str(tmp_path), prefix="r", outputs=["annotated_table"])
    assert not list(tmp_path.iterdir())


def test_mixed_column_sets_give_one_grouped_file_with_nulls(tmp_path):
    both = _check("both_cols")
    one = _check("id_only", failed_rows=pa.table({"id": [3]}))
    result = _result(both, one)
    rows = result.get_consolidated_output_dfs()["orders"].to_arrow().to_pylist()
    assert {row["id"]: (row["name"], row["check_ids"]) for row in rows} == {
        2: ("b", "both_cols"),
        3: (None, "id_only"),
    }
    result.save(str(tmp_path), prefix="r", outputs=["consolidated_query_outputs"])
    assert _files(tmp_path) == {"r_check_results.csv", "r_summary.json", "r_orders.csv"}
    assert pa_csv.read_csv(tmp_path / "r_orders.csv").num_rows == 2
