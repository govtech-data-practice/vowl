"""
Models for adapter configuration.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Union, cast

from sqlglot import exp


# Supported filter operators
FilterOperator = Literal["=", "!=", ">", ">=", "<", "<=", "IN", "NOT IN", "LIKE", "NOT LIKE", "IS NULL", "IS NOT NULL"]

# Type for dict-based filter condition
FilterConditionDict = Dict[str, Any]  # {"field": str, "operator": str, "value": Any}


@dataclass
class FilterCondition:
    """
    Structured filter condition for table queries.
    
    Represents a single filter condition that can be applied to a table.
    Multiple conditions can be combined using AND logic when applied to the same table.
    
    Example:
        >>> # Simple comparison
        >>> FilterCondition(field="date_dt", operator=">=", value="2024-01-01")
        
        >>> # IN clause
        >>> FilterCondition(field="status", operator="IN", value=["active", "pending"])
        
        >>> # NULL check (value is ignored)
        >>> FilterCondition(field="deleted_at", operator="IS NULL")
    """
    field: str
    operator: FilterOperator
    value: Optional[Any] = None
    
    def to_ast(self) -> exp.Expression:
        """
        Convert the filter condition to a sqlglot AST expression.

        Returns a dialect-agnostic AST node that can be rendered in any
        SQL dialect via ``.sql(dialect=...)``.

        Returns:
            A sqlglot Expression node representing this condition.
        """
        col = exp.Column(this=exp.to_identifier(self.field, quoted=True))

        # NULL operators
        if self.operator == "IS NULL":
            return col.is_(exp.Null())
        if self.operator == "IS NOT NULL":
            return col.is_(exp.Null()).not_()

        # IN / NOT IN operators
        if self.operator in ("IN", "NOT IN"):
            values = self.value if isinstance(self.value, (list, tuple)) else [self.value]
            literal_values = [self._to_literal(v) for v in values]
            in_expr = exp.In(this=col, expressions=literal_values)
            if self.operator == "NOT IN":
                return exp.Not(this=in_expr)
            return in_expr

        # LIKE / NOT LIKE operators
        if self.operator in ("LIKE", "NOT LIKE"):
            like_expr = exp.Like(this=col, expression=self._to_literal(self.value))
            if self.operator == "NOT LIKE":
                return exp.Not(this=like_expr)
            return like_expr

        # Standard comparison operators
        op_map: Dict[str, type] = {
            "=": exp.EQ,
            "!=": exp.NEQ,
            ">": exp.GT,
            ">=": exp.GTE,
            "<": exp.LT,
            "<=": exp.LTE,
        }
        op_cls = op_map.get(self.operator)
        if op_cls is None:
            raise ValueError(f"Unsupported operator: {self.operator}")
        return op_cls(this=col, expression=self._to_literal(self.value))

    @staticmethod
    def _to_literal(value: Any) -> exp.Expression:
        """Convert a Python value to a sqlglot literal expression."""
        if value is None:
            return exp.Null()
        elif isinstance(value, bool):
            return exp.Boolean(this=value)
        elif isinstance(value, int):
            return exp.Literal.number(value)
        elif isinstance(value, float):
            return exp.Literal.number(value)
        else:
            return exp.Literal.string(str(value))

def build_filter_ast(
    conditions: Union[FilterCondition, List[FilterCondition], FilterConditionDict, List[FilterConditionDict]],
) -> exp.Expression:
    """
    Build a sqlglot AST expression from filter condition(s).

    Args:
        conditions: A single FilterCondition, list of FilterConditions,
                   or a dict with {field, operator, value} keys.

    Returns:
        A sqlglot Expression node. Multiple conditions are combined with AND.
    """
    def _to_filter_condition(cond: Union[FilterCondition, FilterConditionDict]) -> FilterCondition:
        if isinstance(cond, dict):
            cond_dict = cast(FilterConditionDict, cond)
            return FilterCondition(
                field=str(cond_dict["field"]),
                operator=cast(FilterOperator, cond_dict["operator"]),
                value=cond_dict.get("value"),
            )
        return cond

    # Single condition (or dict)
    if isinstance(conditions, (FilterCondition, dict)):
        return _to_filter_condition(conditions).to_ast()

    # List of conditions
    ast_nodes = [_to_filter_condition(c).to_ast() for c in conditions]
    result = ast_nodes[0]
    for node in ast_nodes[1:]:
        result = exp.And(this=result, expression=node)
    return result
