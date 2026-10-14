"""Unit tests for the row-key helpers used to match failed rows to table rows."""

from __future__ import annotations

import struct

import pyarrow as pa

from vowl.validation.result_row_quality import (
    align_to_schema,
    first_occurrence_indices,
    row_keys,
)


def _keys(values, arrow_type=None):
    return [key[0] for key in row_keys(pa.table({"c": pa.array(values, type=arrow_type)}), ["c"])]


def test_nan_keys_match_each_other_including_other_payloads():
    other_nan = struct.unpack(">d", bytes.fromhex("7ff8000000000001"))[0]
    keys = _keys([float("nan"), float("nan"), other_nan])
    assert keys[0] == keys[1] == keys[2]
    assert len({*keys}) == 1


def test_negative_zero_is_distinct_from_positive_zero():
    keys = _keys([-0.0, 0.0, 0.0])
    assert keys[0] != keys[1]
    assert keys[1] == keys[2]


def test_plain_floats_still_equal_matching_ints():
    # Promoted sides can pair float64 with int64, so 1.0 must still match 1.
    assert _keys([1.0])[0] == _keys([1])[0]


def test_nulls_match():
    assert _keys([None, None], pa.float64()) == [None, None]


def test_nested_floats_are_normalised():
    keys = _keys([[1.0, float("nan")], [1.0, float("nan")], [-0.0], [0.0]])
    assert keys[0] == keys[1]
    assert keys[2] != keys[3]
    hash(keys[0])


def test_struct_keys_follow_field_order_and_are_hashable():
    struct_type = pa.struct([("a", pa.int32()), ("b", pa.string())])
    keys = _keys([{"a": 1, "b": "x"}, {"a": 1, "b": "x"}, {"a": 2, "b": "x"}], struct_type)
    assert keys[0] == keys[1] == (("a", 1), ("b", "x"))
    assert keys[0] != keys[2]


def test_map_keys_are_hashable():
    map_type = pa.map_(pa.string(), pa.int32())
    keys = _keys([[("k", 1)], [("k", 1)], [("j", 2)]], map_type)
    assert keys[0] == keys[1] != keys[2]


def test_timestamp_ns_keys_keep_nanoseconds():
    keys = _keys([1_000_000_001, 1_000_000_002, 1_000_000_001], pa.timestamp("ns"))
    assert keys == [1_000_000_001, 1_000_000_002, 1_000_000_001]


def test_uint64_above_int64_range_is_kept():
    assert _keys([2**64 - 1], pa.uint64()) == [2**64 - 1]


def test_row_keys_with_no_columns_gives_one_empty_key_per_row():
    assert row_keys(pa.table({"c": [1, 2]}), []) == [(), ()]


def test_first_occurrence_indices_keeps_first_seen_order():
    first = first_occurrence_indices([("b",), ("a",), ("b",), ("c",)])
    assert list(first.items()) == [(("b",), 0), (("a",), 1), (("c",), 3)]


def test_align_to_schema_casts_to_target_type():
    table = pa.table({"c": pa.array([1_000], type=pa.timestamp("us"))})
    target = pa.schema([("c", pa.timestamp("ns"))])
    aligned = align_to_schema(table, target, ["c"])
    assert aligned.schema.field("c").type == pa.timestamp("ns")
    assert row_keys(aligned, ["c"]) == [(1_000_000,)]


def test_align_to_schema_leaves_column_when_cast_fails():
    table = pa.table({"c": ["not a number"]})
    target = pa.schema([("c", pa.int64())])
    aligned = align_to_schema(table, target, ["c"])
    assert aligned.schema.field("c").type == pa.string()


def test_align_to_schema_ignores_columns_missing_from_either_side():
    table = pa.table({"c": [1]})
    assert align_to_schema(table, pa.schema([("d", pa.int32())]), ["c", "d"]).equals(table)
