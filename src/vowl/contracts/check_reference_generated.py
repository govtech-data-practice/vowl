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
            as_double = exp.TryCast(this=col, to=exp.DataType.build("DOUBLE PRECISION"))
            as_integer = exp.TryCast(this=col, to=exp.DataType.build("BIGINT"))
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
            .where(exp.TryCast(this=col, to=exp.DataType.build(sql_type)).is_(exp.Null()))
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
            length_check = exp.Length(this=exp.TryCast(this=col, to=exp.DataType.build("VARCHAR")))
            return count_where(not_null, length_check < exp.Literal.number(val))
        elif key == "maxLength":
            length_check = exp.Length(this=exp.TryCast(this=col, to=exp.DataType.build("VARCHAR")))
            return count_where(not_null, length_check > exp.Literal.number(val))
        elif key == "pattern":
            cast_col = exp.TryCast(this=col, to=exp.DataType.build("VARCHAR"))
            pattern_check = exp.Not(this=exp.RegexpLike(this=cast_col, expression=exp.Literal.string(val)))
            return count_where(not_null, pattern_check)
        elif key == "minimum":
            cast_col = exp.TryCast(this=col, to=exp.DataType.build("DOUBLE PRECISION"))
            return count_where(not_null, cast_col < exp.Literal.number(val))
        elif key == "maximum":
            cast_col = exp.TryCast(this=col, to=exp.DataType.build("DOUBLE PRECISION"))
            return count_where(not_null, cast_col > exp.Literal.number(val))
        elif key == "exclusiveMinimum":
            cast_col = exp.TryCast(this=col, to=exp.DataType.build("DOUBLE PRECISION"))
            return count_where(not_null, cast_col <= exp.Literal.number(val))
        elif key == "exclusiveMaximum":
            cast_col = exp.TryCast(this=col, to=exp.DataType.build("DOUBLE PRECISION"))
            return count_where(not_null, cast_col >= exp.Literal.number(val))
        elif key == "multipleOf":
            cast_col = exp.TryCast(this=col, to=exp.DataType.build("DOUBLE PRECISION"))
            mod_check = exp.Mod(this=cast_col, expression=exp.Literal.number(val))
            return count_where(not_null, mod_check.neq(exp.Literal.number(0)))

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

        return descriptions.get(key, f"Column '{col_name}' must satisfy {key}={val}")


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
