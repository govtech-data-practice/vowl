"""Generated contract check references."""

from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

import sqlglot
from sqlglot import exp

from .check_reference_sql import LOGICAL_TYPE_TO_SQL, SQLCheckReference

if TYPE_CHECKING:
    from vowl.adapters.models import FilterCondition

    from .contract import Contract
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


class LogicalTypeOptionsCheckReference(GeneratedColumnCheckReference):
    """Reference to an auto-generated logicalTypeOptions check."""

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
    }

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
        self._option_value = option_value

        if option_key == "format":
            self._validate_format()

    def _validate_format(self) -> None:
        """Check that this logical_type + format combo is actionable.

        Raises ``ValueError`` (caught by the caller in *contract.py*) when
        the combination is recognised but cannot produce a SQL check.
        """
        logical_type = self.get_logical_type()
        val = self._option_value

        # Known silent skips (metadata-only, not checkable).
        if (logical_type, val) in _FORMAT_SKIP_SILENT:
            raise ValueError(
                f"Format '{val}' is metadata-only for logical type '{logical_type}'"
            )

        # Known warn-and-skip (exceeds SQL numeric range).
        if (logical_type, val) in _FORMAT_SKIP_WARN:
            warnings.warn(
                f"Format '{val}' exceeds SQL numeric range; "
                f"skipping check at {self._path}",
                UserWarning,
                stacklevel=3,
            )
            raise ValueError(f"Format '{val}' exceeds SQL numeric range")

        if logical_type == "integer":
            if val not in _INTEGER_FORMAT_RANGES:
                warnings.warn(
                    f"Unknown integer format '{val}' at {self._path}",
                    UserWarning,
                    stacklevel=3,
                )
                raise ValueError(f"Unknown integer format: {val}")

        elif logical_type == "string":
            if val not in _STRING_FORMAT_PATTERNS:
                warnings.warn(
                    f"Unknown string format '{val}' at {self._path}",
                    UserWarning,
                    stacklevel=3,
                )
                raise ValueError(f"Unknown string format: {val}")

        elif logical_type in ("date", "timestamp", "time"):
            regex = _jdk_format_to_regex(val)
            if regex is None:
                warnings.warn(
                    f"Could not convert JDK format '{val}' to regex at {self._path}",
                    UserWarning,
                    stacklevel=3,
                )
                raise ValueError(f"Cannot convert JDK format to regex: {val}")

        elif logical_type is not None:
            warnings.warn(
                f"Format option not supported for logical type "
                f"'{logical_type}' at {self._path}",
                UserWarning,
                stacklevel=3,
            )
            raise ValueError(
                f"Format not supported for logical type '{logical_type}'"
            )

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

        if key == "minLength":
            length_check = exp.Length(this=exp.TryCast(this=col, to=exp.DataType.build("VARCHAR"), safe=True))
            return count_where(not_null, length_check < exp.Literal.number(val))
        elif key == "maxLength":
            length_check = exp.Length(this=exp.TryCast(this=col, to=exp.DataType.build("VARCHAR"), safe=True))
            return count_where(not_null, length_check > exp.Literal.number(val))
        elif key == "pattern":
            cast_col = exp.TryCast(this=col, to=exp.DataType.build("VARCHAR"), safe=True)
            pattern_check = exp.Not(this=exp.RegexpLike(this=cast_col, expression=exp.Literal.string(val)))
            return count_where(not_null, pattern_check)
        elif key == "minimum":
            cast_col = exp.TryCast(this=col, to=exp.DataType.build("DOUBLE PRECISION"), safe=True)
            return count_where(not_null, cast_col < exp.Literal.number(val))
        elif key == "maximum":
            cast_col = exp.TryCast(this=col, to=exp.DataType.build("DOUBLE PRECISION"), safe=True)
            return count_where(not_null, cast_col > exp.Literal.number(val))
        elif key == "exclusiveMinimum":
            cast_col = exp.TryCast(this=col, to=exp.DataType.build("DOUBLE PRECISION"), safe=True)
            return count_where(not_null, cast_col <= exp.Literal.number(val))
        elif key == "exclusiveMaximum":
            cast_col = exp.TryCast(this=col, to=exp.DataType.build("DOUBLE PRECISION"), safe=True)
            return count_where(not_null, cast_col >= exp.Literal.number(val))
        elif key == "multipleOf":
            cast_col = exp.TryCast(this=col, to=exp.DataType.build("DOUBLE PRECISION"), safe=True)
            mod_check = exp.Mod(this=cast_col, expression=exp.Literal.number(val))
            return count_where(not_null, mod_check.neq(exp.Literal.number(0)))
        elif key == "format":
            logical_type = self.get_logical_type()

            if logical_type == "integer":
                min_val, max_val = _INTEGER_FORMAT_RANGES[val]
                cast_col = exp.TryCast(
                    this=col, to=exp.DataType.build("DOUBLE PRECISION"), safe=True
                )
                range_check = exp.Or(
                    this=cast_col < exp.Literal.number(min_val),
                    expression=cast_col > exp.Literal.number(max_val),
                )
                return count_where(not_null, range_check)

            if logical_type == "string":
                pattern = _STRING_FORMAT_PATTERNS[val]
                cast_col = exp.TryCast(
                    this=col, to=exp.DataType.build("VARCHAR"), safe=True
                )
                pattern_check = exp.Not(
                    this=exp.RegexpLike(
                        this=cast_col, expression=exp.Literal.string(pattern)
                    )
                )
                return count_where(not_null, pattern_check)

            # date / timestamp / time — already validated in _validate_format
            pattern = _jdk_format_to_regex(val)
            assert pattern is not None  # guaranteed by _validate_format
            cast_col = exp.TryCast(
                this=col, to=exp.DataType.build("VARCHAR"), safe=True
            )
            pattern_check = exp.Not(
                this=exp.RegexpLike(
                    this=cast_col, expression=exp.Literal.string(pattern)
                )
            )
            return count_where(not_null, pattern_check)

        raise ValueError(
            f"No query implementation for logicalTypeOptions key '{key}'. "
            f"This is a bug - please add query logic for '{key}' in _build_ast()."
        )

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
        }

        if key == "format":
            return self._format_description(col_name)

        return descriptions.get(key, f"Column '{col_name}' must satisfy {key}={val}")

    def _format_description(self, col_name: str) -> str:
        logical_type = self.get_logical_type()
        val = self._option_value

        if logical_type == "integer":
            min_val, max_val = _INTEGER_FORMAT_RANGES[val]
            return (
                f"Column '{col_name}' must fit in {val} range "
                f"({min_val} to {max_val})"
            )

        if logical_type == "string":
            return f"Column '{col_name}' must match {val} format"

        # date / timestamp / time
        return f"Column '{col_name}' must match format {val}"


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
                f"Could not generate required check at {self._path}: "
                f"col_name={col_name}, schema_name={schema_name}",
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
                f"Could not generate unique check at {self._path}: "
                f"col_name={col_name}, schema_name={schema_name}",
                UserWarning,
                stacklevel=2,
            )
            raise ValueError(f"Cannot generate unique check for {self._path}")

        col = exp.Column(this=exp.to_identifier(col_name, quoted=True))
        table = exp.Table(this=exp.to_identifier(schema_name, quoted=True))

        subquery = (
            sqlglot.select(col)
            .from_(table)
            .where(col.is_(exp.Null()).not_())
            .group_by(col)
            .having(exp.Count(this=exp.Star()) > exp.Literal.number(1))
        )

        self._cached_ast = sqlglot.select(exp.Count(this=exp.Star())).from_(subquery.subquery())
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
                f"Could not generate primary key check at {self._path}: "
                f"col_name={col_name}, schema_name={schema_name}",
                UserWarning,
                stacklevel=2,
            )
            raise ValueError(f"Cannot generate primary key check for {self._path}")

        col = exp.Column(this=exp.to_identifier(col_name, quoted=True))
        table = exp.Table(this=exp.to_identifier(schema_name, quoted=True))

        null_count = sqlglot.select(exp.Count(this=exp.Star())).from_(table).where(col.is_(exp.Null()))
        dup_subquery = (
            sqlglot.select(col)
            .from_(table)
            .where(col.is_(exp.Null()).not_())
            .group_by(col)
            .having(exp.Count(this=exp.Star()) > exp.Literal.number(1))
        )
        dup_count = sqlglot.select(exp.Count(this=exp.Star())).from_(dup_subquery.subquery())

        self._cached_ast = sqlglot.select(
            exp.Add(
                this=exp.Paren(this=null_count.subquery()),
                expression=exp.Paren(this=dup_count.subquery()),
            )
        )
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


__all__ = [
    "DeclaredColumnExistsCheckReference",
    "GeneratedColumnCheckReference",
    "GeneratedTableCheckReference",
    "LogicalTypeCheckReference",
    "LogicalTypeOptionsCheckReference",
    "PrimaryKeyCheckReference",
    "RequiredCheckReference",
    "UniqueCheckReference",
]
