"""
SQL Security module for data quality checks.

Provides security validation for SQL queries to:
1. Ensure only read operations (SELECT) are allowed
2. Detect and block SQL injection patterns
3. Prevent dangerous operations that could modify or delete data
"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple

import sqlglot
from sqlglot import exp


class SQLSecurityError(Exception):
    """Raised when a SQL security violation is detected."""

    def __init__(self, message: str, violation_type: str, query: str):
        """
        Initialize a SQL security error.

        Args:
            message: Human-readable error message.
            violation_type: Type of violation (e.g., 'write_operation', 'injection').
            query: The offending query (truncated for safety).
        """
        super().__init__(message)
        self.violation_type = violation_type
        self.query = query[:500] if query else ""


# SQL statement types that are NOT allowed (write/modify operations)
FORBIDDEN_STATEMENT_TYPES = frozenset({
    # Data modification
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    # Schema modification
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.AlterColumn,
    # Transaction control (could be used to manipulate state)
    exp.Transaction,
    exp.Commit,
    exp.Rollback,
    # Permission management
    exp.Grant,
    exp.Revoke,
    # Other dangerous operations
    exp.Command,  # Generic commands like TRUNCATE, etc.
})

# Statement type names for the allowlist approach (more restrictive)
ALLOWED_STATEMENT_TYPES = frozenset({
    exp.Select,
    # EXPLAIN/DESCRIBE are read-only analysis operations
})

# Patterns that indicate potential SQL injection attempts
# These are checked against the raw query string
SQL_INJECTION_PATTERNS = [
    # Multiple statements (statement stacking)
    (r";\s*(?:INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|EXEC|EXECUTE)\b", "statement_stacking"),
    # UNION-based injection (suspicious UNION patterns)
    (r"UNION\s+(?:ALL\s+)?SELECT\s+(?:NULL|[0-9]+|'[^']*')\s*(?:,\s*(?:NULL|[0-9]+|'[^']*'))*\s*(?:--|#|$)", "union_injection"),
    # Time-based blind injection
    (r"(?:WAITFOR\s+DELAY|SLEEP\s*\(|PG_SLEEP\s*\(|BENCHMARK\s*\()", "time_based_injection"),
    # Error-based injection
    (r"(?:EXTRACTVALUE|UPDATEXML|XMLTYPE)\s*\(", "error_based_injection"),
    # Stacked queries with dangerous operations
    (r";\s*(?:TRUNCATE|GRANT|REVOKE)\s+", "dangerous_operation"),
    # Information schema probing (could be reconnaissance)
    (r"(?:INFORMATION_SCHEMA|SYS\.|SYSOBJECTS|SYSCOLUMNS|PG_CATALOG)\s*\.", "schema_probing"),
    # File operations
    (r"(?:LOAD_FILE|INTO\s+(?:OUT|DUMP)FILE|BULK\s+INSERT)", "file_operation"),
    # Command execution
    (r"(?:XP_CMDSHELL|SP_OACREATE|DBMS_PIPE)", "command_execution"),
]

# Compiled regex patterns for efficiency
_COMPILED_INJECTION_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(pattern, re.IGNORECASE | re.DOTALL), name)
    for pattern, name in SQL_INJECTION_PATTERNS
]


def validate_read_only_query(query: str, dialect: str = "postgres") -> None:
    """
    Validate that a SQL query is read-only (SELECT only).

    Uses sqlglot to parse the query and verify it's a SELECT statement.
    Blocks all write operations (INSERT, UPDATE, DELETE, DROP, etc.).

    Args:
        query: The SQL query string to validate.
        dialect: The SQL dialect for parsing (default: postgres).

    Raises:
        SQLSecurityError: If the query contains non-SELECT operations.
    """
    if not query or not query.strip():
        raise SQLSecurityError(
            "Empty query is not allowed",
            violation_type="empty_query",
            query=query or "",
        )

    try:
        # Parse all statements (handles multiple statements separated by ;)
        statements = sqlglot.parse(query, dialect=dialect)
    except Exception as e:
        # If parsing fails, be conservative and reject
        raise SQLSecurityError(
            f"Failed to parse SQL query for security validation: {e}",
            violation_type="parse_error",
            query=query,
        ) from e

    if not statements:
        raise SQLSecurityError(
            "No valid SQL statements found in query",
            violation_type="no_statements",
            query=query,
        )

    # Check each statement
    for i, stmt in enumerate(statements):
        if stmt is None:
            continue

        stmt_type = type(stmt)

        # Allowlist approach: only permit SELECT statements
        if stmt_type not in ALLOWED_STATEMENT_TYPES:
            # Get a human-readable name for the statement type
            stmt_name = stmt_type.__name__ if hasattr(stmt_type, "__name__") else str(stmt_type)
            raise SQLSecurityError(
                f"Only SELECT queries are allowed for data quality checks. "
                f"Found: {stmt_name}",
                violation_type="write_operation",
                query=query,
            )

        _check_for_select_side_effects(stmt, query)

        # Additional check: ensure no subqueries contain write operations
        _check_for_write_subqueries(stmt, query)


def _check_for_select_side_effects(ast: exp.Expression, original_query: str) -> None:
    """Reject SELECT forms that still mutate state, such as SELECT INTO."""
    if isinstance(ast, exp.Select) and ast.args.get("into") is not None:
        raise SQLSecurityError(
            "SELECT INTO is not allowed for data quality checks because it creates a new table.",
            violation_type="write_operation",
            query=original_query,
        )


def _check_for_write_subqueries(ast: exp.Expression, original_query: str) -> None:
    """
    Recursively check for write operations hidden in subqueries or CTEs.

    Args:
        ast: The parsed SQL AST to check.
        original_query: The original query string for error reporting.

    Raises:
        SQLSecurityError: If write operations are found in subqueries.
    """
    # Check all nested expressions for forbidden statement types
    for node in ast.walk():
        node_type = type(node)
        if node_type in FORBIDDEN_STATEMENT_TYPES:
            stmt_name = node_type.__name__ if hasattr(node_type, "__name__") else str(node_type)
            raise SQLSecurityError(
                f"Write operation '{stmt_name}' found in subquery or CTE. "
                "Only read operations are allowed.",
                violation_type="write_in_subquery",
                query=original_query,
            )


def detect_sql_injection(query: str) -> Optional[Tuple[str, str]]:
    """
    Detect potential SQL injection patterns in a query.

    Checks for known injection patterns that could indicate malicious input.
    This is a defense-in-depth measure alongside parameterized queries.

    Args:
        query: The SQL query string to check.

    Returns:
        Tuple of (pattern_name, matched_text) if injection detected, None otherwise.
    """
    if not query:
        return None

    for pattern, pattern_name in _COMPILED_INJECTION_PATTERNS:
        match = pattern.search(query)
        if match:
            return (pattern_name, match.group(0)[:100])

    return None


def validate_query_security(query: str, dialect: str = "postgres") -> None:
    """
    Perform comprehensive security validation on a SQL query.

    Combines read-only validation and injection detection into a single check.
    This is the main entry point for query security validation.

    Args:
        query: The SQL query string to validate.
        dialect: The SQL dialect for parsing (default: postgres).

    Raises:
        SQLSecurityError: If any security violation is detected.
    """
    # First check for injection patterns (before parsing, as injection
    # could break the parser in unexpected ways)
    injection_result = detect_sql_injection(query)
    if injection_result:
        pattern_name, matched_text = injection_result
        raise SQLSecurityError(
            f"Potential SQL injection detected: {pattern_name}. "
            f"Matched pattern: '{matched_text}'",
            violation_type=f"injection_{pattern_name}",
            query=query,
        )

    # Then validate it's a read-only query
    validate_read_only_query(query, dialect)


def sanitize_identifier(identifier: str) -> str:
    """
    Sanitize a SQL identifier (table name, column name, etc.).

    Only allows alphanumeric characters, underscores, and dots (for schema.table).
    This prevents injection through identifier names.

    Args:
        identifier: The identifier to sanitize.

    Returns:
        The sanitized identifier.

    Raises:
        SQLSecurityError: If the identifier contains invalid characters.
    """
    if not identifier:
        raise SQLSecurityError(
            "Empty identifier is not allowed",
            violation_type="empty_identifier",
            query="",
        )

    # Allow alphanumeric, underscore, and dot (for schema.table notation)
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*$", identifier):
        raise SQLSecurityError(
            f"Invalid identifier: '{identifier}'. "
            "Identifiers must start with a letter or underscore and contain only "
            "alphanumeric characters, underscores, and dots.",
            violation_type="invalid_identifier",
            query=identifier,
        )

    return identifier


def to_table_expression(identifier: str) -> exp.Table:
    """
    Build a sqlglot Table expression from a sanitized identifier.

    Args:
        identifier: Table identifier in table, schema.table, or project.schema.table form.

    Returns:
        Parsed sqlglot Table expression.

    Raises:
        SQLSecurityError: If the identifier cannot be parsed as a table expression.
    """
    sanitized = sanitize_identifier(identifier)
    try:
        table_expr = sqlglot.parse_one(sanitized, into=exp.Table)
    except Exception as e:
        raise SQLSecurityError(
            f"Failed to parse identifier '{sanitized}' as table expression: {e}",
            violation_type="invalid_identifier",
            query=sanitized,
        ) from e

    if not isinstance(table_expr, exp.Table):
        raise SQLSecurityError(
            f"Identifier '{sanitized}' did not produce a table expression",
            violation_type="invalid_identifier",
            query=sanitized,
        )

    return table_expr
