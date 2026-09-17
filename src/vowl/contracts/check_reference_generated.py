"""Generated contract check references."""

from __future__ import annotations

import math
import warnings
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

import sqlglot
from sqlglot import exp

from .check_reference_sql import LOGICAL_TYPE_TO_SQL, SQLCheckReference

if TYPE_CHECKING:
    from vowl.adapters.models import FilterCondition

    from .contract import Contract, ResolvedRef
    from .models.ODCS_types import DataQuality

    FilterConditionType = FilterCondition | list[FilterCondition] | dict[str, Any]
else:
    FilterConditionType = Any


class GeneratedColumnCheckReference(SQLCheckReference, ABC):
    """Base class for auto-generated column-level checks."""

    def __init__(self, contract: Contract, property_path: str, path_suffix: str):
        super().__init__(contract, f"{property_path}.{path_suffix}")
        self._property_path = property_path
        self._generated_check: DataQuality | None = None
        self._cached_ast: exp.Expression | None = None

    @abstractmethod
    def _build_ast(self) -> exp.Expression:
        """Build and cache the sqlglot AST for this check."""
        ...

    @abstractmethod
    def _generate_check(self) -> DataQuality:
        """Generate and return the synthetic DataQuality check definition."""
        ...

    def get_query(
        self,
        dialect: str,
        filter_conditions: dict[str, FilterConditionType] | None = None,
        use_try_cast: bool = False,
    ) -> str:
        ast = self._build_ast()
        query = self._render_sql(ast, dialect)
        if filter_conditions:
            query = self.apply_filters(query, dialect, filter_conditions)
        if use_try_cast:
            query, _ = self.apply_try_cast(query, dialect)
        return query

    def get_schema_name(self) -> str | None:
        schema_path = self.get_schema_path()
        return self._contract.resolve(f"{schema_path}.name")

    def get_schema_path(self) -> str:
        return self._contract.resolve_parent(self._property_path, levels=1)

    def get_column_path(self) -> str:
        return self._property_path

    def get_column_name(self) -> str | None:
        return self._contract.resolve(f"{self._property_path}.name")

    def get_logical_type(self) -> str | None:
        return self._contract.resolve(f"{self._property_path}.logicalType")

    def get_logical_type_options(self) -> dict[str, Any] | None:
        return self._contract.resolve(f"{self._property_path}.logicalTypeOptions")

    def is_generated(self) -> bool:
        return True


class GeneratedTableCheckReference(SQLCheckReference, ABC):
    """Base class for auto-generated table-level checks."""

    def __init__(self, contract: Contract, quality_path: str):
        super().__init__(contract, quality_path)
        self._generated_check: DataQuality | None = None
        self._cached_ast: exp.Expression | None = None

    @abstractmethod
    def _build_ast(self) -> exp.Expression:
        """Build and cache the sqlglot AST for this check."""
        ...

    @abstractmethod
    def _generate_check(self) -> DataQuality:
        """Generate and return the synthetic DataQuality check definition."""
        ...

    def get_query(
        self,
        dialect: str,
        filter_conditions: dict[str, FilterConditionType] | None = None,
        use_try_cast: bool = False,
    ) -> str:
        ast = self._build_ast()
        query = self._render_sql(ast, dialect)
        if filter_conditions:
            query = self.apply_filters(query, dialect, filter_conditions)
        if use_try_cast:
            query, _ = self.apply_try_cast(query, dialect)
        return query

    def get_schema_name(self) -> str | None:
        schema_path = self.get_schema_path()
        return self._contract.resolve(f"{schema_path}.name")

    def get_schema_path(self) -> str:
        return self._contract.resolve_parent(self._path, levels=1)

    def is_generated(self) -> bool:
        return True


class DeclaredColumnExistsCheckReference(GeneratedColumnCheckReference):
    """Reference to an auto-generated column existence check."""

    def __init__(self, contract: Contract, property_path: str):
        super().__init__(contract, property_path, "name")

    def get_check(self) -> DataQuality:
        if self._generated_check is None:
            self._generated_check = self._generate_check()
        return self._generated_check

    def _build_ast(self) -> exp.Expression:
        if self._cached_ast is not None:
            return self._cached_ast

        col_name = self.get_column_name()
        schema_name = self.get_schema_name()

        if not col_name or not schema_name:
            warnings.warn(
                f"Could not generate column existence check at {self._path}: "
                f"col_name={col_name}, schema_name={schema_name}",
                UserWarning,
                stacklevel=2,
            )
            raise ValueError(f"Cannot generate column existence check for {self._path}")

        col = exp.Column(this=exp.to_identifier(col_name, quoted=True))
        table = exp.Table(this=exp.to_identifier(schema_name, quoted=True))
        inner_query = sqlglot.select(col).from_(table).limit(0)

        self._cached_ast = sqlglot.select(exp.Count(this=exp.Star())).from_(
            inner_query.subquery(alias="_vowl_column_exists")
        )
        return self._cached_ast

    def _generate_check(self) -> DataQuality:
        col_name = self.get_column_name()
        schema_name = self.get_schema_name()
        ast = self._build_ast()

        return {
            "name": f"{col_name}_column_exists_check",
            "type": "sql",
            "dimension": "conformity",
            "description": f"Column '{col_name}' must exist in '{schema_name}'",
            "query": ast.sql(dialect=self._INTERNAL_DIALECT),
            "mustBe": 0,
        }

    def get_column_name(self) -> str | None:
        return self._contract.resolve(self._path)


class LogicalTypeCheckReference(GeneratedColumnCheckReference):
    """Reference to an auto-generated logical type check."""

    def __init__(self, contract: Contract, property_path: str):
        super().__init__(contract, property_path, "logicalType")

    def get_check(self) -> DataQuality:
        if self._generated_check is None:
            self._generated_check = self._generate_check()
        return self._generated_check

    def _build_ast(self) -> exp.Expression:
        if self._cached_ast is not None:
            return self._cached_ast

        col_name = self.get_column_name()
        logical_type = self.get_logical_type()
        schema_name = self.get_schema_name()
        sql_type = LOGICAL_TYPE_TO_SQL.get(logical_type or "")

        if not col_name or not sql_type or not schema_name:
            warnings.warn(
                f"Could not generate type check at {self._path}: "
                f"col_name={col_name}, schema_name={schema_name}, sql_type={sql_type}",
                UserWarning,
                stacklevel=2,
            )
            raise ValueError(f"Cannot generate type check for {self._path}")

        col = exp.Column(this=exp.to_identifier(col_name, quoted=True))
        table = exp.Table(this=exp.to_identifier(schema_name, quoted=True))

        if logical_type == "integer":
            as_double = exp.TryCast(this=col, to=exp.DataType.build("DOUBLE PRECISION"), safe=True)
            as_integer = exp.TryCast(this=col, to=exp.DataType.build("BIGINT"), safe=True)
            invalid_integer = as_double.is_(exp.Null()).or_(as_double.neq(as_integer))

            self._cached_ast = (
                sqlglot.select(exp.Count(this=exp.Star()))
                .from_(table)
                .where(col.is_(exp.Null()).not_())
                .where(invalid_integer)
            )
            return self._cached_ast

        self._cached_ast = (
            sqlglot.select(exp.Count(this=exp.Star()))
            .from_(table)
            .where(col.is_(exp.Null()).not_())
            .where(exp.TryCast(this=col, to=exp.DataType.build(sql_type), safe=True).is_(exp.Null()))
        )
        return self._cached_ast

    def _generate_check(self) -> DataQuality:
        col_name = self.get_column_name()
        logical_type = self.get_logical_type()
        ast = self._build_ast()

        return {
            "name": f"{col_name}_logical_type_check",
            "type": "sql",
            "dimension": "conformity",
            "description": f"Values in '{col_name}' must be valid {logical_type}",
            "query": ast.sql(dialect=self._INTERNAL_DIALECT),
            "mustBe": 0,
        }

    def get_logical_type(self) -> str | None:
        return self._contract.resolve(self._path)


# ---------------------------------------------------------------------------
# Format option support: constants and helpers
# ---------------------------------------------------------------------------

_INTEGER_FORMAT_RANGES: dict[str, tuple[int, int]] = {
    "i8": (-128, 127),
    "i16": (-32_768, 32_767),
    "i32": (-2_147_483_648, 2_147_483_647),
    "i64": (-9_223_372_036_854_775_808, 9_223_372_036_854_775_807),
    "u8": (0, 255),
    "u16": (0, 65_535),
    "u32": (0, 4_294_967_295),
    "u64": (0, 18_446_744_073_709_551_615),
}

_STRING_FORMAT_PATTERNS: dict[str, str] = {
    "uuid": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    "email": r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
    "ipv4": r"^((25[0-5]|2[0-4][0-9]|[01]?[0-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9]?[0-9])$",
    "ipv6": r"^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$",
    "hostname": r"^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)*[a-zA-Z]{2,}$",
    "uri": r"^[a-zA-Z][a-zA-Z0-9+.\-]*:",
}

# Formats that are recognized but produce no SQL check.
_FORMAT_SKIP_SILENT: set[tuple[str, str]] = {
    ("number", "f32"),
    ("number", "f64"),
    ("string", "password"),
    ("string", "byte"),
    ("string", "binary"),
}

_FORMAT_SKIP_WARN: set[tuple[str, str]] = {
    ("integer", "i128"),
    ("integer", "u128"),
}

# JDK DateTimeFormatter pattern letters (subset we recognize).
_JDK_PATTERN_LETTERS = set("GyYMLdQqwWeFaAhHKkmsSNnVzZOXxpB")

_JDK_TOKEN_MAP: dict[str, str] = {
    "MM": r"(0[1-9]|1[0-2])",
    "M": r"(0?[1-9]|1[0-2])",
    "dd": r"(0[1-9]|[12]\d|3[01])",
    "d": r"(0?[1-9]|[12]\d|3[01])",
    "HH": r"([01]\d|2[0-3])",
    "H": r"(\d|1\d|2[0-3])",
    "hh": r"(0[1-9]|1[0-2])",
    "h": r"(0?[1-9]|1[0-2])",
    "mm": r"[0-5]\d",
    "m": r"\d{1,2}",
    "ss": r"[0-5]\d",
    "s": r"\d{1,2}",
    "a": r"(AM|PM|am|pm)",
    "XXX": r"(Z|[+-]\d{2}:\d{2})",
    "XX": r"(Z|[+-]\d{4})",
    "X": r"(Z|[+-]\d{2})",
    "ZZZZZ": r"(Z|[+-]\d{2}:\d{2})",
    "ZZZ": r"[+-]\d{4}",
    "ZZ": r"[+-]\d{4}",
    "Z": r"[+-]\d{4}",
}


# Characters that are special in a regex pattern (outside character classes).
_REGEX_META = frozenset(r"\.^$*+?{}[]|()")


def _escape_literal(ch: str) -> str:
    """Escape *ch* for use in a regex pattern outside a character class.

    Unlike ``re.escape`` this leaves benign characters (``-``, `` ``, etc.)
    unescaped so the resulting pattern is cleaner and avoids surprises in
    SQL ``REGEXP_LIKE`` implementations.
    """
    return "\\" + ch if ch in _REGEX_META else ch


def _jdk_format_to_regex(fmt: str) -> str | None:
    """Convert a JDK DateTimeFormatter pattern string to a regex.

    Returns ``None`` when the pattern contains tokens we cannot translate
    (caller should emit a warning and skip the check).
    """
    result: list[str] = []
    i = 0
    n = len(fmt)

    while i < n:
        ch = fmt[i]

        # Quoted literal section: 'text' or '' for a literal single-quote.
        if ch == "'":
            i += 1
            if i < n and fmt[i] == "'":
                result.append("'")
                i += 1
                continue
            literal: list[str] = []
            while i < n and fmt[i] != "'":
                literal.append(_escape_literal(fmt[i]))
                i += 1
            if i < n:
                i += 1  # skip closing quote
            result.append("".join(literal))
            continue

        # JDK pattern letter — collect consecutive identical letters.
        if ch.isalpha() and ch in _JDK_PATTERN_LETTERS:
            start = i
            while i < n and fmt[i] == ch:
                i += 1
            token = fmt[start:i]

            # Year tokens: any count of 'y' or 'Y'.
            if ch in ("y", "Y"):
                count = len(token)
                result.append(rf"\d{{{count}}}" if count > 1 else r"\d{1,4}")
                continue

            # Fractional-second tokens: any count of 'S'.
            if ch == "S":
                result.append(rf"\d{{{len(token)}}}")
                continue

            mapped = _JDK_TOKEN_MAP.get(token)
            if mapped is None:
                return None  # unrecognized JDK token
            result.append(mapped)
            continue

        # Non-pattern alphabetic character (e.g. 'T') — literal.
        if ch.isalpha():
            result.append(_escape_literal(ch))
            i += 1
            continue

        # Any other character — literal.
        result.append(_escape_literal(ch))
        i += 1

    return "^" + "".join(result) + "$"


def _validate_format(logical_type: str | None, val: Any, path: str) -> None:
    """Check that a ``logical_type`` + ``format`` combo is actionable.

    Shared by column-level ``logicalTypeOptions.format`` checks and array
    ``items`` element-format checks. Raises ``ValueError`` (caught by the
    caller in *contract.py* and downgraded to an unsupported check) when the
    combination is recognised but cannot produce a SQL check.
    """
    # Known silent skips (metadata-only, not checkable).
    if (logical_type, val) in _FORMAT_SKIP_SILENT:
        raise ValueError(f"Format '{val}' is metadata-only for logical type '{logical_type}'")

    # Known warn-and-skip (exceeds SQL numeric range).
    if (logical_type, val) in _FORMAT_SKIP_WARN:
        warnings.warn(
            f"Format '{val}' exceeds SQL numeric range; skipping check at {path}",
            UserWarning,
            stacklevel=3,
        )
        raise ValueError(f"Format '{val}' exceeds SQL numeric range")

    if logical_type == "integer":
        if val not in _INTEGER_FORMAT_RANGES:
            warnings.warn(
                f"Unknown integer format '{val}' at {path}",
                UserWarning,
                stacklevel=3,
            )
            raise ValueError(f"Unknown integer format: {val}")

    elif logical_type == "string":
        if val not in _STRING_FORMAT_PATTERNS:
            # Try interpreting as a JDK DateTimeFormatter pattern.
            regex = _jdk_format_to_regex(val)
            if regex is None:
                warnings.warn(
                    f"Unknown string format '{val}' at {path}",
                    UserWarning,
                    stacklevel=3,
                )
                raise ValueError(f"Unknown string format: {val}")

    elif logical_type in ("date", "timestamp", "time"):
        regex = _jdk_format_to_regex(val)
        if regex is None:
            warnings.warn(
                f"Could not convert JDK format '{val}' to regex at {path}",
                UserWarning,
                stacklevel=3,
            )
            raise ValueError(f"Cannot convert JDK format to regex: {val}")

    elif logical_type is not None:
        warnings.warn(
            f"Format option not supported for logical type '{logical_type}' at {path}",
            UserWarning,
            stacklevel=3,
        )
        raise ValueError(f"Format not supported for logical type '{logical_type}'")


def _format_violation_predicate(
    target: exp.Expression,
    val: Any,
    logical_type: str | None,
) -> exp.Expression:
    """Return the "value is INVALID" predicate for a *validated* ``format`` option.

    ``target`` is the expression under test — a column for column-level checks
    or an unnested element for array-``items`` checks. Assumes the combination
    of ``logical_type`` and ``val`` has already been accepted by the caller's
    validation (see ``_validate_format``), so the RuntimeError guards here only
    fire on a genuine internal bug.
    """
    if logical_type == "integer":
        min_val, max_val = _INTEGER_FORMAT_RANGES[val]
        cast_col = exp.TryCast(this=target, to=exp.DataType.build("DOUBLE PRECISION"), safe=True)
        return exp.Or(
            this=cast_col < exp.Literal.number(min_val),
            expression=cast_col > exp.Literal.number(max_val),
        )

    if logical_type == "string":
        pattern = _STRING_FORMAT_PATTERNS.get(val)
        if pattern is None:
            # Fall through to JDK format pattern (validated by _validate_format).
            pattern = _jdk_format_to_regex(val)
            if pattern is None:
                raise RuntimeError(
                    f"_jdk_format_to_regex returned None for '{val}' — _validate_format should have rejected this"
                )
        cast_col = exp.TryCast(this=target, to=exp.DataType.build("VARCHAR"), safe=True)
        return exp.Not(this=exp.RegexpLike(this=cast_col, expression=exp.Literal.string(pattern)))

    # date / timestamp / time — already validated by _validate_format.
    pattern = _jdk_format_to_regex(val)
    if pattern is None:
        raise RuntimeError(
            f"_jdk_format_to_regex returned None for '{val}' — _validate_format should have rejected this"
        )
    cast_col = exp.TryCast(this=target, to=exp.DataType.build("VARCHAR"), safe=True)
    return exp.Not(this=exp.RegexpLike(this=cast_col, expression=exp.Literal.string(pattern)))


def _scalar_violation_predicate(
    target: exp.Expression,
    key: str,
    val: Any,
    logical_type: str | None,
) -> exp.Expression:
    """Return the boolean "this value is INVALID" predicate for one scalar option.

    Shared by column-level checks (``target`` is the column) and array-element
    checks (``target`` is the unnested element), so both apply identical
    minLength / maxLength / pattern / minimum / … / format logic. The predicate
    is the ``NOT P`` form — TRUE for a value that *violates* the option — so a
    caller simply counts rows where it holds. Assumes ``val`` has already been
    coerced (and, for ``format``, validated against ``logical_type``).
    """
    if key == "minLength":
        length_check = exp.Length(this=exp.TryCast(this=target, to=exp.DataType.build("VARCHAR"), safe=True))
        return length_check < exp.Literal.number(val)
    if key == "maxLength":
        length_check = exp.Length(this=exp.TryCast(this=target, to=exp.DataType.build("VARCHAR"), safe=True))
        return length_check > exp.Literal.number(val)
    if key == "pattern":
        cast_col = exp.TryCast(this=target, to=exp.DataType.build("VARCHAR"), safe=True)
        return exp.Not(this=exp.RegexpLike(this=cast_col, expression=exp.Literal.string(val)))
    if key == "minimum":
        cast_col = exp.TryCast(this=target, to=exp.DataType.build("DOUBLE PRECISION"), safe=True)
        return cast_col < exp.Literal.number(val)
    if key == "maximum":
        cast_col = exp.TryCast(this=target, to=exp.DataType.build("DOUBLE PRECISION"), safe=True)
        return cast_col > exp.Literal.number(val)
    if key == "exclusiveMinimum":
        cast_col = exp.TryCast(this=target, to=exp.DataType.build("DOUBLE PRECISION"), safe=True)
        return cast_col <= exp.Literal.number(val)
    if key == "exclusiveMaximum":
        cast_col = exp.TryCast(this=target, to=exp.DataType.build("DOUBLE PRECISION"), safe=True)
        return cast_col >= exp.Literal.number(val)
    if key == "multipleOf":
        cast_col = exp.TryCast(this=target, to=exp.DataType.build("DOUBLE PRECISION"), safe=True)
        mod_check = exp.Mod(this=cast_col, expression=exp.Literal.number(val))
        return mod_check.neq(exp.Literal.number(0))
    if key == "format":
        return _format_violation_predicate(target, val, logical_type)

    raise ValueError(
        f"No predicate implementation for scalar option '{key}'. "
        f"This is a bug - please add predicate logic for '{key}' in _scalar_violation_predicate()."
    )


class LogicalTypeOptionsCheckReference(GeneratedColumnCheckReference):
    """Reference to an auto-generated logicalTypeOptions check."""

    # Options that constrain the elements/cardinality of a native array column.
    # These are only emitted when the property also declares ``logicalType:
    # array`` (the metadata gate lives in contract.py); the SQL they generate
    # (ARRAY_LENGTH / ARRAY_DISTINCT) is only valid on array-capable engines.
    ARRAY_OPTION_KEYS = frozenset({"minItems", "maxItems", "uniqueItems"})

    SUPPORTED_OPTIONS = {
        "minLength",
        "maxLength",
        "pattern",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "format",
        "minItems",
        "maxItems",
        "uniqueItems",
    }

    # Options whose value is emitted into generated SQL as a bare numeric
    # literal via ``exp.Literal.number``. sqlglot renders that value verbatim
    # (unquoted), so a non-numeric value would be injected into the query. The
    # value is coerced to a real number at construction time to prevent SQL
    # injection through ``logicalTypeOptions``.
    _NON_NEGATIVE_INT_OPTIONS = frozenset({"minLength", "maxLength", "minItems", "maxItems"})
    _NUMERIC_OPTIONS = frozenset({"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf"})

    def __init__(self, contract: Contract, property_path: str, option_key: str, option_value: Any):
        if option_key not in self.SUPPORTED_OPTIONS:
            warnings.warn(
                f"Unsupported logicalTypeOptions key '{option_key}' at {property_path}. "
                f"Supported options: {', '.join(sorted(self.SUPPORTED_OPTIONS))}",
                UserWarning,
                stacklevel=2,
            )
            raise ValueError(f"Unsupported logicalTypeOptions key: {option_key}")

        super().__init__(contract, property_path, f"logicalTypeOptions.{option_key}")
        self._option_key = option_key
        # Coerce numeric-bound options to real numbers so they cannot inject
        # arbitrary SQL when rendered as literals in _build_ast().
        if option_key in self._NON_NEGATIVE_INT_OPTIONS:
            self._option_value = self._coerce_non_negative_int(option_key, option_value)
        elif option_key in self._NUMERIC_OPTIONS:
            self._option_value = self._coerce_number(option_key, option_value)
        else:
            self._option_value = option_value

        if option_key == "format":
            self._validate_format()

        # uniqueItems only imposes a constraint when true; `uniqueItems: false`
        # is a no-op, so it degrades to an unsupported (no-check) reference
        # rather than emitting an always-passing query.
        if option_key == "uniqueItems" and self._option_value is not True:
            raise ValueError("uniqueItems: false imposes no constraint; no check generated")

    @staticmethod
    def _coerce_number(option_key: str, value: Any) -> int | float:
        """Return *value* as an int/float, rejecting anything non-numeric.

        Accepts JSON numbers directly and strictly-numeric strings; rejects
        booleans, NaN/inf, and any value that is not a clean number (which is
        how SQL-injection payloads would arrive). Raises ``ValueError`` — caught
        by the caller in ``contract.py`` and downgraded to an unsupported check.
        """
        # bool is an int subclass but is never a valid numeric bound.
        if isinstance(value, bool):
            raise ValueError(f"logicalTypeOptions '{option_key}' must be numeric, got boolean: {value!r}")

        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                raise ValueError(f"logicalTypeOptions '{option_key}' must be finite, got: {value!r}")
            return value
        if isinstance(value, str):
            text = value.strip()
            try:
                num: int | float = int(text)
            except ValueError:
                try:
                    num = float(text)
                except ValueError:
                    raise ValueError(f"logicalTypeOptions '{option_key}' must be numeric, got: {value!r}") from None
            if isinstance(num, float) and (math.isnan(num) or math.isinf(num)):
                raise ValueError(f"logicalTypeOptions '{option_key}' must be finite, got: {value!r}")
            return num

        raise ValueError(f"logicalTypeOptions '{option_key}' must be numeric, got: {value!r}")

    @classmethod
    def _coerce_non_negative_int(cls, option_key: str, value: Any) -> int:
        """Return *value* as a non-negative int, rejecting anything else."""
        num = cls._coerce_number(option_key, value)
        if isinstance(num, float):
            if not num.is_integer():
                raise ValueError(f"logicalTypeOptions '{option_key}' must be an integer, got: {value!r}")
            num = int(num)
        if num < 0:
            raise ValueError(f"logicalTypeOptions '{option_key}' must be non-negative, got: {value!r}")
        return num

    def _validate_format(self) -> None:
        """Check that this logical_type + format combo is actionable.

        Raises ``ValueError`` (caught by the caller in *contract.py*) when
        the combination is recognised but cannot produce a SQL check.
        """
        _validate_format(self.get_logical_type(), self._option_value, self._path)

    def get_check(self) -> DataQuality:
        if self._generated_check is None:
            self._generated_check = self._generate_check()
        return self._generated_check

    def _generate_check(self) -> DataQuality:
        col_name = self.get_column_name() or ""
        ast = self._build_ast()
        description = self._build_description(col_name)

        return {
            "name": f"{col_name}_logical_type_options_{self._option_key}_check",
            "type": "sql",
            "dimension": "conformity",
            "description": description,
            "query": ast.sql(dialect=self._INTERNAL_DIALECT),
            "mustBe": 0,
        }

    def _build_ast(self) -> exp.Expression:
        if self._cached_ast is not None:
            return self._cached_ast

        col_name = self.get_column_name()
        schema_name = self.get_schema_name()

        if not col_name or not schema_name:
            warnings.warn(
                f"Could not generate {self._option_key} check at {self._path}: "
                f"col_name={col_name}, schema_name={schema_name}",
                UserWarning,
                stacklevel=2,
            )
            raise ValueError(f"Cannot generate {self._option_key} check for {self._path}")

        key = self._option_key
        val = self._option_value

        col = exp.Column(this=exp.to_identifier(col_name, quoted=True))
        table = exp.Table(this=exp.to_identifier(schema_name, quoted=True))
        not_null = col.is_(exp.Null()).not_()

        def count_where(*conditions: exp.Expression) -> exp.Expression:
            query = sqlglot.select(exp.Count(this=exp.Star())).from_(table)
            for cond in conditions:
                query = query.where(cond)
            return query

        # Array cardinality options operate on the array column directly. The
        # SQL (ARRAY_LENGTH / ARRAY_DISTINCT) transpiles per dialect and is only
        # valid on array-capable engines — the metadata gate in contract.py
        # ensures these are only emitted for logicalType: array columns.
        if key == "minItems":
            return count_where(not_null, exp.ArraySize(this=col) < exp.Literal.number(val))
        if key == "maxItems":
            return count_where(not_null, exp.ArraySize(this=col) > exp.Literal.number(val))
        if key == "uniqueItems":
            distinct_size = exp.ArraySize(this=exp.ArrayDistinct(this=col))
            return count_where(not_null, distinct_size.neq(exp.ArraySize(this=col)))

        # Scalar options (string length, numeric bounds, pattern, format) share
        # their violation predicate with array-element (items) validation.
        return count_where(not_null, _scalar_violation_predicate(col, key, val, self.get_logical_type()))

    def _build_description(self, col_name: str) -> str:
        key = self._option_key
        val = self._option_value

        descriptions = {
            "minLength": f"Column '{col_name}' must have minimum length of {val}",
            "maxLength": f"Column '{col_name}' must have maximum length of {val}",
            "pattern": f"Column '{col_name}' must match pattern '{val}'",
            "minimum": f"Column '{col_name}' must be >= {val}",
            "maximum": f"Column '{col_name}' must be <= {val}",
            "exclusiveMinimum": f"Column '{col_name}' must be > {val}",
            "exclusiveMaximum": f"Column '{col_name}' must be < {val}",
            "multipleOf": f"Column '{col_name}' must be a multiple of {val}",
            "minItems": f"Array column '{col_name}' must contain at least {val} items",
            "maxItems": f"Array column '{col_name}' must contain at most {val} items",
            "uniqueItems": f"Array column '{col_name}' must contain only unique items",
        }

        if key == "format":
            return self._format_description(col_name)

        return descriptions.get(key, f"Column '{col_name}' must satisfy {key}={val}")

    def _format_description(self, col_name: str) -> str:
        logical_type = self.get_logical_type()
        val = self._option_value

        if logical_type == "integer":
            min_val, max_val = _INTEGER_FORMAT_RANGES[val]
            return f"Column '{col_name}' must fit in {val} range ({min_val} to {max_val})"

        if logical_type == "string":
            return f"Column '{col_name}' must match {val} format"

        # date / timestamp / time
        return f"Column '{col_name}' must match format {val}"


class EnumCheckReference(GeneratedColumnCheckReference):
    """Reference to an auto-generated allowed-value-set (enum) check.

    ODCS v3.2.0 promotes ``enum`` to a first-class property field: an explicit
    list of allowed values for a column (each entry an ``EnumValue`` object with
    at least a ``value``). This check flags any non-NULL row whose value is not
    one of those allowed values. NULLs are excluded — null enforcement belongs to
    the ``required`` check.
    """

    def __init__(self, contract: Contract, property_path: str):
        super().__init__(contract, property_path, "enum")
        # Extract and validate the allowed-value set eagerly so an invalid enum
        # raises ValueError at construction time — matching how the wiring in
        # contract.py degrades an invalid logicalTypeOptions to an unsupported
        # check via try/except ValueError.
        enum_list = self._contract.resolve(f"{property_path}.enum")
        if not isinstance(enum_list, list):
            raise ValueError(f"enum must be a list of allowed values, got: {enum_list!r}")

        allowed = [e["value"] for e in enum_list if isinstance(e, dict) and "value" in e]
        # NULL entries are excluded: the check ignores NULLs (required owns
        # null-enforcement), so a NULL allowed value contributes nothing.
        allowed = [v for v in allowed if v is not None]
        if not allowed:
            raise ValueError(f"enum at {property_path} has no usable allowed values")

        # Convert each allowed value to a sqlglot literal node up front so nothing
        # is ever string-interpolated into SQL (the values may be attacker-supplied
        # in the contract). Reject anything that cannot be a scalar literal.
        self._literals = [self._to_literal(v) for v in allowed]

    @staticmethod
    def _to_literal(value: Any) -> exp.Expression:
        """Convert an allowed enum value to a safe sqlglot literal node.

        Never string-interpolates: sqlglot builds and escapes the literal. Rejects
        non-scalar values (dict/list) and non-finite numbers with ``ValueError``,
        which the caller downgrades to an unsupported check.
        """
        # bool is an int subclass, so it MUST be checked before int.
        if isinstance(value, bool):
            return exp.Boolean(this=value)
        if isinstance(value, int):
            return exp.Literal.number(value)
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                raise ValueError(f"enum value must be finite, got: {value!r}")
            return exp.Literal.number(value)
        if isinstance(value, str):
            return exp.Literal.string(value)
        raise ValueError(f"enum value must be a scalar (str/int/float/bool), got: {value!r}")

    def get_check(self) -> DataQuality:
        if self._generated_check is None:
            self._generated_check = self._generate_check()
        return self._generated_check

    def _build_ast(self) -> exp.Expression:
        if self._cached_ast is not None:
            return self._cached_ast

        col_name = self.get_column_name()
        schema_name = self.get_schema_name()

        if not col_name or not schema_name:
            warnings.warn(
                f"Could not generate enum check at {self._path}: col_name={col_name}, schema_name={schema_name}",
                UserWarning,
                stacklevel=2,
            )
            raise ValueError(f"Cannot generate enum check for {self._path}")

        col = exp.Column(this=exp.to_identifier(col_name, quoted=True))
        table = exp.Table(this=exp.to_identifier(schema_name, quoted=True))
        not_null = col.is_(exp.Null()).not_()

        # col IS NOT NULL AND col NOT IN (<literals>). No CAST — enum values are
        # expected to match the column's logicalType.
        not_in = exp.Not(this=exp.In(this=col, expressions=list(self._literals)))

        self._cached_ast = sqlglot.select(exp.Count(this=exp.Star())).from_(table).where(not_null).where(not_in)
        return self._cached_ast

    def _generate_check(self) -> DataQuality:
        col_name = self.get_column_name()
        ast = self._build_ast()

        return {
            "name": f"{col_name}_enum_check",
            "type": "sql",
            "dimension": "conformity",
            "description": f"Column '{col_name}' must be one of the allowed enum values",
            "query": ast.sql(dialect=self._INTERNAL_DIALECT),
            "mustBe": 0,
        }


class ArrayItemsCheckReference(GeneratedColumnCheckReference):
    """Reference to an auto-generated array-element (``items``) check.

    ODCS lets an ``array`` property describe its element schema under ``items``
    (element ``logicalType``, ``logicalTypeOptions``, ``enum``). This reference
    validates every element of each non-NULL array via
    ``EXISTS (SELECT 1 FROM UNNEST(col) AS _vowl_arr(_vowl_elem) WHERE <element
    violates>)`` — flagging any row that contains at least one bad element.
    NULL arrays are skipped; an empty array passes vacuously (no element can
    violate), matching the ``required`` check owning null-enforcement.

    One instance maps to a single element sub-check (one COUNT query / one
    golden file):

    - ``kind="logicalType"`` — every element casts to the declared element type
    - ``kind="option"``      — every element satisfies one ``items`` option
    - ``kind="enum"``        — every element is in the ``items.enum`` set

    Element validation via ``UNNEST`` executes correctly on
    duckdb/postgres/trino/bigquery; Spark/Snowflake are best-effort and
    scalar-only engines ERROR (documented in docs/known-issues.md).
    """

    # Quoted identifiers for the unnested element and its lateral alias. Fixed
    # internal names (never taken from the contract), so no injection surface.
    _ELEM_IDENT = "_vowl_elem"
    _ARR_ALIAS = "_vowl_arr"

    # ``items`` may only carry *scalar* element options; array-of-array
    # cardinality (minItems/maxItems/uniqueItems on the elements themselves) is
    # not supported, so the array-cardinality keys are excluded here.
    _SUPPORTED_ITEM_OPTIONS = (
        LogicalTypeOptionsCheckReference.SUPPORTED_OPTIONS - LogicalTypeOptionsCheckReference.ARRAY_OPTION_KEYS
    )

    def __init__(
        self,
        contract: Contract,
        property_path: str,
        *,
        kind: str,
        option_key: str | None = None,
    ):
        if kind == "logicalType":
            suffix = "items.logicalType"
        elif kind == "enum":
            suffix = "items.enum"
        elif kind == "option":
            if option_key is None:
                raise ValueError("option_key is required when kind='option'")
            suffix = f"items.logicalTypeOptions.{option_key}"
        else:
            raise ValueError(f"Unknown ArrayItemsCheckReference kind: {kind!r}")

        super().__init__(contract, property_path, suffix)
        self._kind = kind
        self._option_key = option_key
        # Element logical type drives cast targets and format validation. It is
        # the *element* type (items.logicalType), not the "array" type of the
        # column itself (which get_logical_type() returns).
        self._item_logical_type = contract.resolve(f"{property_path}.items.logicalType")

        # Validate/coerce eagerly so an unactionable items schema raises
        # ValueError at construction — contract.py catches it and degrades to an
        # UnsupportedColumnCheckReference (mirrors the option/enum classes).
        if kind == "logicalType":
            self._sql_type = LOGICAL_TYPE_TO_SQL.get(self._item_logical_type or "")
            if not self._sql_type:
                raise ValueError(
                    f"items.logicalType '{self._item_logical_type}' has no SQL cast check at {self._path}"
                )
        elif kind == "option":
            if option_key not in self._SUPPORTED_ITEM_OPTIONS:
                raise ValueError(f"Unsupported items logicalTypeOptions key: {option_key}")
            raw = contract.resolve(f"{property_path}.items.logicalTypeOptions.{option_key}")
            self._option_value = self._coerce_item_option(option_key, raw)
        else:  # enum
            enum_list = contract.resolve(f"{property_path}.items.enum")
            if not isinstance(enum_list, list):
                raise ValueError(f"items.enum must be a list of allowed values, got: {enum_list!r}")
            allowed = [e["value"] for e in enum_list if isinstance(e, dict) and "value" in e]
            allowed = [v for v in allowed if v is not None]
            if not allowed:
                raise ValueError(f"items.enum at {self._path} has no usable allowed values")
            # Reuse the injection-safe literal builder from the enum check.
            self._literals = [EnumCheckReference._to_literal(v) for v in allowed]

    def _coerce_item_option(self, option_key: str, value: Any) -> Any:
        """Coerce/validate one element option, reusing the column-check logic."""
        if option_key in LogicalTypeOptionsCheckReference._NON_NEGATIVE_INT_OPTIONS:
            return LogicalTypeOptionsCheckReference._coerce_non_negative_int(option_key, value)
        if option_key in LogicalTypeOptionsCheckReference._NUMERIC_OPTIONS:
            return LogicalTypeOptionsCheckReference._coerce_number(option_key, value)
        if option_key == "format":
            _validate_format(self._item_logical_type, value, self._path)
        # pattern (and validated format) pass through as their literal string.
        return value

    def get_check(self) -> DataQuality:
        if self._generated_check is None:
            self._generated_check = self._generate_check()
        return self._generated_check

    def _element_violation(self, elem: exp.Expression) -> exp.Expression:
        """Return the "this element is INVALID" predicate for one element."""
        if self._kind == "logicalType":
            if self._item_logical_type == "integer":
                # Mirror LogicalTypeCheckReference: a value that casts to a float
                # but is not integral (e.g. 1.5) is an invalid integer.
                as_double = exp.TryCast(this=elem, to=exp.DataType.build("DOUBLE PRECISION"), safe=True)
                as_integer = exp.TryCast(this=elem, to=exp.DataType.build("BIGINT"), safe=True)
                return as_double.is_(exp.Null()).or_(as_double.neq(as_integer))
            return exp.TryCast(this=elem, to=exp.DataType.build(self._sql_type), safe=True).is_(exp.Null())
        if self._kind == "option":
            return _scalar_violation_predicate(elem, self._option_key, self._option_value, self._item_logical_type)
        # enum: element is not in the allowed set.
        return exp.Not(this=exp.In(this=elem, expressions=list(self._literals)))

    def _build_ast(self) -> exp.Expression:
        if self._cached_ast is not None:
            return self._cached_ast

        col_name = self.get_column_name()
        schema_name = self.get_schema_name()

        if not col_name or not schema_name:
            warnings.warn(
                f"Could not generate array items check at {self._path}: "
                f"col_name={col_name}, schema_name={schema_name}",
                UserWarning,
                stacklevel=2,
            )
            raise ValueError(f"Cannot generate array items check for {self._path}")

        col = exp.Column(this=exp.to_identifier(col_name, quoted=True))
        table = exp.Table(this=exp.to_identifier(schema_name, quoted=True))
        elem = exp.Column(this=exp.to_identifier(self._ELEM_IDENT, quoted=True))
        not_null = col.is_(exp.Null()).not_()

        # EXISTS (SELECT 1 FROM UNNEST(col) AS _vowl_arr(_vowl_elem)
        #         WHERE <element violates>) — the row has at least one bad
        # element. NULL arrays are excluded by the outer not_null; empty arrays
        # produce no rows to unnest, so they pass vacuously.
        unnest = exp.Unnest(
            expressions=[col],
            alias=exp.TableAlias(
                this=exp.to_identifier(self._ARR_ALIAS, quoted=True),
                columns=[exp.to_identifier(self._ELEM_IDENT, quoted=True)],
            ),
        )
        inner = sqlglot.select(exp.Literal.number(1)).from_(unnest).where(self._element_violation(elem))
        exists = exp.Exists(this=inner)

        self._cached_ast = (
            sqlglot.select(exp.Count(this=exp.Star())).from_(table).where(not_null).where(exists)
        )
        return self._cached_ast

    def _check_name(self, col_name: str) -> str:
        if self._kind == "logicalType":
            return f"{col_name}_array_items_logical_type_check"
        if self._kind == "enum":
            return f"{col_name}_array_items_enum_check"
        return f"{col_name}_array_items_{self._option_key}_check"

    def _describe(self, col_name: str) -> str:
        if self._kind == "logicalType":
            return f"Every element of array column '{col_name}' must be valid {self._item_logical_type}"
        if self._kind == "enum":
            return f"Every element of array column '{col_name}' must be one of the allowed enum values"
        return f"Every element of array column '{col_name}' must satisfy {self._option_key}={self._option_value}"

    def _generate_check(self) -> DataQuality:
        col_name = self.get_column_name() or ""
        ast = self._build_ast()

        return {
            "name": self._check_name(col_name),
            "type": "sql",
            "dimension": "conformity",
            "description": self._describe(col_name),
            "query": ast.sql(dialect=self._INTERNAL_DIALECT),
            "mustBe": 0,
        }


class RequiredCheckReference(GeneratedColumnCheckReference):
    """Reference to an auto-generated required (not null) check."""

    def __init__(self, contract: Contract, property_path: str):
        super().__init__(contract, property_path, "required")

    def get_check(self) -> DataQuality:
        if self._generated_check is None:
            self._generated_check = self._generate_check()
        return self._generated_check

    def _build_ast(self) -> exp.Expression:
        if self._cached_ast is not None:
            return self._cached_ast

        col_name = self.get_column_name()
        schema_name = self.get_schema_name()

        if not col_name or not schema_name:
            warnings.warn(
                f"Could not generate required check at {self._path}: col_name={col_name}, schema_name={schema_name}",
                UserWarning,
                stacklevel=2,
            )
            raise ValueError(f"Cannot generate required check for {self._path}")

        col = exp.Column(this=exp.to_identifier(col_name, quoted=True))
        table = exp.Table(this=exp.to_identifier(schema_name, quoted=True))

        self._cached_ast = sqlglot.select(exp.Count(this=exp.Star())).from_(table).where(col.is_(exp.Null()))
        return self._cached_ast

    def _generate_check(self) -> DataQuality:
        col_name = self.get_column_name()
        ast = self._build_ast()

        return {
            "name": f"{col_name}_required_check",
            "type": "sql",
            "dimension": "completeness",
            "description": f"Column '{col_name}' must not contain NULL values",
            "query": ast.sql(dialect=self._INTERNAL_DIALECT),
            "mustBe": 0,
        }


class UniqueCheckReference(GeneratedColumnCheckReference):
    """Reference to an auto-generated uniqueness check."""

    def __init__(self, contract: Contract, property_path: str):
        super().__init__(contract, property_path, "unique")

    def get_check(self) -> DataQuality:
        if self._generated_check is None:
            self._generated_check = self._generate_check()
        return self._generated_check

    def _build_ast(self) -> exp.Expression:
        if self._cached_ast is not None:
            return self._cached_ast

        col_name = self.get_column_name()
        schema_name = self.get_schema_name()

        if not col_name or not schema_name:
            warnings.warn(
                f"Could not generate unique check at {self._path}: col_name={col_name}, schema_name={schema_name}",
                UserWarning,
                stacklevel=2,
            )
            raise ValueError(f"Cannot generate unique check for {self._path}")

        col = exp.Column(this=exp.to_identifier(col_name, quoted=True))
        table = exp.Table(this=exp.to_identifier(schema_name, quoted=True))

        # Count the *participating rows* (every row whose value belongs to a
        # duplicate group), not the number of duplicate groups, so the failed
        # rows auto-derived from this query (SELECT * FROM table WHERE <pred>)
        # are full rows that merge into the annotated table.  The verdict is
        # unchanged: zero duplicate groups <=> zero participating rows.
        dup_subquery = (
            sqlglot.select(col)
            .from_(table)
            .where(col.is_(exp.Null()).not_())
            .group_by(col)
            .having(exp.Count(this=exp.Star()) > exp.Literal.number(1))
        )

        self._cached_ast = (
            sqlglot.select(exp.Count(this=exp.Star()))
            .from_(table)
            .where(exp.In(this=col, query=dup_subquery.subquery()))
        )
        return self._cached_ast

    def _generate_check(self) -> DataQuality:
        col_name = self.get_column_name()
        ast = self._build_ast()

        return {
            "name": f"{col_name}_unique_check",
            "type": "sql",
            "dimension": "consistency",
            "description": f"Column '{col_name}' must contain unique values",
            "query": ast.sql(dialect=self._INTERNAL_DIALECT),
            "mustBe": 0,
        }


class PrimaryKeyCheckReference(GeneratedColumnCheckReference):
    """Reference to an auto-generated primary key check."""

    def __init__(self, contract: Contract, property_path: str):
        super().__init__(contract, property_path, "primaryKey")

    def get_check(self) -> DataQuality:
        if self._generated_check is None:
            self._generated_check = self._generate_check()
        return self._generated_check

    def _build_ast(self) -> exp.Expression:
        if self._cached_ast is not None:
            return self._cached_ast

        col_name = self.get_column_name()
        schema_name = self.get_schema_name()

        if not col_name or not schema_name:
            warnings.warn(
                f"Could not generate primary key check at {self._path}: col_name={col_name}, schema_name={schema_name}",
                UserWarning,
                stacklevel=2,
            )
            raise ValueError(f"Cannot generate primary key check for {self._path}")

        col = exp.Column(this=exp.to_identifier(col_name, quoted=True))
        table = exp.Table(this=exp.to_identifier(schema_name, quoted=True))

        # Count the *participating rows*: NULL primary keys plus every row whose
        # value belongs to a duplicate group.  A single COUNT(*) over the base
        # table with this predicate lets the failed rows auto-derive as
        # SELECT * FROM table WHERE <pred> (full rows -> mergeable into the
        # annotated table).  Verdict unchanged: zero violations <=> zero rows.
        dup_subquery = (
            sqlglot.select(col)
            .from_(table)
            .where(col.is_(exp.Null()).not_())
            .group_by(col)
            .having(exp.Count(this=exp.Star()) > exp.Literal.number(1))
        )
        pred = exp.Or(
            this=col.is_(exp.Null()),
            expression=exp.In(this=col, query=dup_subquery.subquery()),
        )

        self._cached_ast = sqlglot.select(exp.Count(this=exp.Star())).from_(table).where(pred)
        return self._cached_ast

    def _generate_check(self) -> DataQuality:
        col_name = self.get_column_name()
        ast = self._build_ast()

        return {
            "name": f"{col_name}_primary_key_check",
            "type": "sql",
            "dimension": "consistency",
            "description": f"Primary key column '{col_name}' must be unique and not null",
            "query": ast.sql(dialect=self._INTERNAL_DIALECT),
            "mustBe": 0,
        }


class _ForeignKeyMixin:
    """Shared referential-integrity (foreign-key) AST + check generation.

    Mixed in *before* a ``Generated{Column,Table}CheckReference`` base so its
    ``_build_ast``/``_generate_check``/``get_check`` implementations satisfy the
    abstract base. Subclasses resolve their endpoints and call :meth:`_init_fk`.

    The generated check counts rows in the ``from`` table whose (non-NULL) key
    has no matching row in the ``to`` table — a ``NOT EXISTS`` anti-join. NULL
    keys are excluded (MATCH SIMPLE semantics: a row with any NULL FK column
    does not participate). Identifiers are always emitted as quoted sqlglot
    nodes, never string-interpolated, so contract-supplied names are safe.
    """

    _FROM_ALIAS = "_vowl_fk_from"
    _TO_ALIAS = "_vowl_fk_to"

    # Populated by _init_fk; declared here for type-checkers.
    _from_schema: str
    _from_cols: list[str]
    _to_schema: str
    _to_cols: list[str]
    _external: bool

    def _init_fk(self, from_schema: str, from_cols: list[str], target: ResolvedRef) -> None:
        self._from_schema = from_schema
        self._from_cols = from_cols
        self._to_schema = target.schema_name
        self._to_cols = target.columns
        self._external = target.external
        if len(self._from_cols) != len(self._to_cols):
            raise ValueError(
                f"composite foreign key arity mismatch: {len(self._from_cols)} source column(s) "
                f"vs {len(self._to_cols)} target column(s)"
            )
        if not target.target_unique:
            warnings.warn(
                f"foreign key target '{self._to_schema}({', '.join(self._to_cols)})' is not declared "
                "unique or primaryKey; referential-integrity results may be ambiguous",
                UserWarning,
                stacklevel=3,
            )

    def get_check(self) -> DataQuality:
        if self._generated_check is None:  # type: ignore[attr-defined]
            self._generated_check = self._generate_check()  # type: ignore[attr-defined]
        return self._generated_check  # type: ignore[attr-defined]

    def _build_ast(self) -> exp.Expression:
        if self._cached_ast is not None:  # type: ignore[attr-defined]
            return self._cached_ast  # type: ignore[attr-defined]

        f_alias, t_alias = self._FROM_ALIAS, self._TO_ALIAS
        from_tbl = exp.Table(
            this=exp.to_identifier(self._from_schema, quoted=True),
            alias=exp.TableAlias(this=exp.to_identifier(f_alias, quoted=True)),
        )
        to_tbl = exp.Table(
            this=exp.to_identifier(self._to_schema, quoted=True),
            alias=exp.TableAlias(this=exp.to_identifier(t_alias, quoted=True)),
        )

        def fcol(name: str) -> exp.Expression:
            return exp.column(name, table=f_alias, quoted=True)

        def tcol(name: str) -> exp.Expression:
            return exp.column(name, table=t_alias, quoted=True)

        # Correlated equality chain: to.x_i = from.k_i
        join_pred: exp.Expression | None = None
        for from_col, to_col in zip(self._from_cols, self._to_cols, strict=True):
            eq = tcol(to_col).eq(fcol(from_col))
            join_pred = eq if join_pred is None else exp.And(this=join_pred, expression=eq)

        not_exists = exp.Not(this=exp.Exists(this=sqlglot.select(exp.Literal.number(1)).from_(to_tbl).where(join_pred)))

        # MATCH SIMPLE: only rows whose every FK column is non-NULL participate.
        where: exp.Expression | None = None
        for from_col in self._from_cols:
            non_null = fcol(from_col).is_(exp.Null()).not_()
            where = non_null if where is None else exp.And(this=where, expression=non_null)
        where = exp.And(this=where, expression=not_exists) if where is not None else not_exists

        self._cached_ast = sqlglot.select(exp.Count(this=exp.Star())).from_(from_tbl).where(where)  # type: ignore[attr-defined]
        return self._cached_ast  # type: ignore[attr-defined]

    def _generate_check(self) -> DataQuality:
        ast = self._build_ast()
        from_cols_disp = ", ".join(self._from_cols)
        to_cols_disp = ", ".join(self._to_cols)
        return {
            "name": f"{self._from_schema}_{'_'.join(self._from_cols)}_foreign_key_check",
            "type": "sql",
            "dimension": "consistency",
            "description": (
                f"Every non-null ({from_cols_disp}) in '{self._from_schema}' must exist in "
                f"'{self._to_schema}' ({to_cols_disp})"
            ),
            "query": ast.sql(dialect=self._INTERNAL_DIALECT),  # type: ignore[attr-defined]
            "mustBe": 0,
        }


class PropertyForeignKeyCheckReference(_ForeignKeyMixin, GeneratedColumnCheckReference):
    """Auto-generated referential-integrity check for a property-level relationship.

    A property may declare a ``relationships`` entry of ``type: foreignKey`` with
    a ``to`` reference to another property. The owning property is the single
    foreign-key column (property-level relationships cannot be composite).
    """

    def __init__(self, contract: Contract, property_path: str, rel_index: int):
        super().__init__(contract, property_path, f"relationships[{rel_index}]")
        rel = self._contract.resolve(f"{property_path}.relationships[{rel_index}]")
        if not isinstance(rel, dict):
            raise ValueError(f"relationship at {self._path} is not an object")
        rel_type = rel.get("type", "foreignKey")
        if rel_type != "foreignKey":
            raise ValueError(f"unsupported relationship type '{rel_type}'; only 'foreignKey' is supported")

        to = rel.get("to")
        if to is None:
            raise ValueError(f"relationship at {self._path} has no 'to' target")
        if isinstance(to, list) and len(to) > 1:
            raise ValueError("property-level relationship cannot be composite; use a schema-level relationship")

        from_schema = self.get_schema_name()
        from_col = self.get_column_name()
        if not from_schema or not from_col:
            raise ValueError(f"cannot resolve source column/schema for relationship at {self._path}")

        target = self._contract.resolve_reference(to)
        self._init_fk(from_schema=from_schema, from_cols=[from_col], target=target)


class SchemaForeignKeyCheckReference(_ForeignKeyMixin, GeneratedTableCheckReference):
    """Auto-generated referential-integrity check for a schema-level relationship.

    A schema may declare a ``relationships`` entry of ``type: foreignKey`` with
    both ``from`` and ``to`` references. Both may be composite (lists), in which
    case the two sides must have equal arity and each side's columns must resolve
    within a single schema.
    """

    def __init__(self, contract: Contract, schema_index: int, rel_index: int):
        super().__init__(contract, f"$.schema[{schema_index}].relationships[{rel_index}]")
        rel = self._contract.resolve(self._path)
        if not isinstance(rel, dict):
            raise ValueError(f"relationship at {self._path} is not an object")
        rel_type = rel.get("type", "foreignKey")
        if rel_type != "foreignKey":
            raise ValueError(f"unsupported relationship type '{rel_type}'; only 'foreignKey' is supported")

        frm = rel.get("from")
        to = rel.get("to")
        if frm is None or to is None:
            raise ValueError(f"schema-level relationship at {self._path} requires both 'from' and 'to'")

        from_ref = self._contract.resolve_reference(frm)
        target = self._contract.resolve_reference(to)

        # The 'from' side must live on the schema this relationship is declared on.
        own_schema = self.get_schema_name()
        if own_schema and from_ref.schema_name != own_schema:
            raise ValueError(
                f"schema-level relationship 'from' ({from_ref.schema_name}) does not match its "
                f"declaring schema '{own_schema}'"
            )

        self._init_fk(from_schema=from_ref.schema_name, from_cols=from_ref.columns, target=target)


__all__ = [
    "ArrayItemsCheckReference",
    "DeclaredColumnExistsCheckReference",
    "EnumCheckReference",
    "GeneratedColumnCheckReference",
    "GeneratedTableCheckReference",
    "LogicalTypeCheckReference",
    "LogicalTypeOptionsCheckReference",
    "PrimaryKeyCheckReference",
    "PropertyForeignKeyCheckReference",
    "RequiredCheckReference",
    "SchemaForeignKeyCheckReference",
    "UniqueCheckReference",
]
