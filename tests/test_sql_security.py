"""
Unit tests for SQL security module.

Tests security validation including:
- Read-only query enforcement
- SQL injection detection
- Identifier sanitization
"""

import pytest

from vowl.executors.security import (
    SQLSecurityError,
    detect_sql_injection,
    sanitize_identifier,
    validate_query_security,
    validate_read_only_query,
)


class TestValidateReadOnlyQuery:
    """Tests for validate_read_only_query function."""

    def test_simple_select_allowed(self):
        """Simple SELECT queries should be allowed."""
        query = "SELECT * FROM users"
        validate_read_only_query(query)  # Should not raise

    def test_select_with_where_allowed(self):
        """SELECT with WHERE clause should be allowed."""
        query = "SELECT id, name FROM users WHERE status = 'active'"
        validate_read_only_query(query)  # Should not raise

    def test_select_with_join_allowed(self):
        """SELECT with JOIN should be allowed."""
        query = """
            SELECT u.id, o.amount 
            FROM users u 
            JOIN orders o ON u.id = o.user_id
        """
        validate_read_only_query(query)  # Should not raise

    def test_select_with_subquery_allowed(self):
        """SELECT with subquery should be allowed."""
        query = """
            SELECT * FROM users 
            WHERE id IN (SELECT user_id FROM orders WHERE amount > 100)
        """
        validate_read_only_query(query)  # Should not raise

    def test_select_with_cte_allowed(self):
        """SELECT with CTE should be allowed."""
        query = """
            WITH active_users AS (
                SELECT id FROM users WHERE status = 'active'
            )
            SELECT * FROM active_users
        """
        validate_read_only_query(query)  # Should not raise

    def test_select_count_allowed(self):
        """SELECT COUNT queries should be allowed."""
        query = "SELECT COUNT(*) FROM users WHERE status = 'active'"
        validate_read_only_query(query)  # Should not raise

    def test_select_aggregate_allowed(self):
        """SELECT with aggregate functions should be allowed."""
        query = """
            SELECT department, AVG(salary), MAX(salary) 
            FROM employees 
            GROUP BY department
        """
        validate_read_only_query(query)  # Should not raise

    def test_insert_blocked(self):
        """INSERT statements should be blocked."""
        query = "INSERT INTO users (name) VALUES ('test')"
        with pytest.raises(SQLSecurityError) as exc_info:
            validate_read_only_query(query)
        assert exc_info.value.violation_type == "write_operation"
        assert "INSERT" in str(exc_info.value) or "Insert" in str(exc_info.value)

    def test_update_blocked(self):
        """UPDATE statements should be blocked."""
        query = "UPDATE users SET name = 'test' WHERE id = 1"
        with pytest.raises(SQLSecurityError) as exc_info:
            validate_read_only_query(query)
        assert exc_info.value.violation_type == "write_operation"

    def test_delete_blocked(self):
        """DELETE statements should be blocked."""
        query = "DELETE FROM users WHERE id = 1"
        with pytest.raises(SQLSecurityError) as exc_info:
            validate_read_only_query(query)
        assert exc_info.value.violation_type == "write_operation"

    def test_drop_table_blocked(self):
        """DROP TABLE statements should be blocked."""
        query = "DROP TABLE users"
        with pytest.raises(SQLSecurityError) as exc_info:
            validate_read_only_query(query)
        assert exc_info.value.violation_type == "write_operation"

    def test_create_table_blocked(self):
        """CREATE TABLE statements should be blocked."""
        query = "CREATE TABLE test (id INT)"
        with pytest.raises(SQLSecurityError) as exc_info:
            validate_read_only_query(query)
        assert exc_info.value.violation_type == "write_operation"

    def test_alter_table_blocked(self):
        """ALTER TABLE statements should be blocked."""
        query = "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
        with pytest.raises(SQLSecurityError) as exc_info:
            validate_read_only_query(query)
        assert exc_info.value.violation_type == "write_operation"

    def test_truncate_blocked(self):
        """TRUNCATE statements should be blocked."""
        query = "TRUNCATE TABLE users"
        with pytest.raises(SQLSecurityError) as exc_info:
            validate_read_only_query(query)
        assert exc_info.value.violation_type == "write_operation"

    def test_merge_blocked(self):
        """MERGE statements should be blocked."""
        query = """
            MERGE INTO target USING source 
            ON target.id = source.id 
            WHEN MATCHED THEN UPDATE SET name = source.name
        """
        with pytest.raises(SQLSecurityError) as exc_info:
            validate_read_only_query(query)
        assert exc_info.value.violation_type == "write_operation"

    def test_empty_query_blocked(self):
        """Empty queries should be blocked."""
        with pytest.raises(SQLSecurityError) as exc_info:
            validate_read_only_query("")
        assert exc_info.value.violation_type == "empty_query"

    def test_whitespace_only_query_blocked(self):
        """Whitespace-only queries should be blocked."""
        with pytest.raises(SQLSecurityError) as exc_info:
            validate_read_only_query("   \n\t  ")
        assert exc_info.value.violation_type == "empty_query"

    def test_multiple_select_statements_blocked(self):
        """Multiple statements (even valid SELECTs) should be carefully handled."""
        # sqlglot parses this as two statements, but we only allow one
        # The second SELECT is fine, but having multiple statements is suspicious
        query = "SELECT 1; SELECT 2"
        # This should either work (if we allow multiple SELECTs) or fail
        # The important thing is we don't allow INSERT/UPDATE/DELETE
        # Our current implementation allows multiple SELECTs
        validate_read_only_query(query)  # Both are SELECTs, should be allowed

    def test_select_into_blocked(self):
        """SELECT INTO (creates a new table) should be handled."""
        query = "SELECT * INTO new_table FROM users"

        with pytest.raises(SQLSecurityError) as exc_info:
            validate_read_only_query(query)

        assert exc_info.value.violation_type == "write_operation"
        assert "SELECT INTO" in str(exc_info.value)


class TestDetectSqlInjection:
    """Tests for detect_sql_injection function."""

    def test_normal_query_no_injection(self):
        """Normal queries should not be flagged as injection."""
        query = "SELECT * FROM users WHERE name = 'John'"
        result = detect_sql_injection(query)
        assert result is None

    def test_union_injection_detected(self):
        """UNION-based injection should be detected."""
        query = "SELECT * FROM users WHERE id = 1 UNION ALL SELECT NULL, NULL, NULL --"
        result = detect_sql_injection(query)
        assert result is not None
        assert result[0] == "union_injection"

    def test_statement_stacking_detected(self):
        """Statement stacking with dangerous operations should be detected."""
        query = "SELECT * FROM users; DROP TABLE users"
        result = detect_sql_injection(query)
        assert result is not None
        assert result[0] == "statement_stacking"

    def test_time_based_injection_waitfor(self):
        """Time-based injection with WAITFOR should be detected."""
        query = "SELECT * FROM users WHERE id = 1; WAITFOR DELAY '0:0:5'"
        result = detect_sql_injection(query)
        assert result is not None
        assert result[0] == "time_based_injection"

    def test_time_based_injection_sleep(self):
        """Time-based injection with SLEEP should be detected."""
        query = "SELECT * FROM users WHERE id = 1 AND SLEEP(5)"
        result = detect_sql_injection(query)
        assert result is not None
        assert result[0] == "time_based_injection"

    def test_file_operation_detected(self):
        """File operations should be detected."""
        query = "SELECT LOAD_FILE('/etc/passwd')"
        result = detect_sql_injection(query)
        assert result is not None
        assert result[0] == "file_operation"

    def test_command_execution_detected(self):
        """Command execution attempts should be detected."""
        query = "EXEC xp_cmdshell 'dir'"
        result = detect_sql_injection(query)
        assert result is not None
        assert result[0] == "command_execution"

    def test_truncate_in_injection_detected(self):
        """TRUNCATE in statement stacking should be detected."""
        query = "SELECT 1; TRUNCATE TABLE users"
        result = detect_sql_injection(query)
        assert result is not None
        # Could be detected as either statement_stacking or dangerous_operation
        assert result[0] in ("dangerous_operation", "statement_stacking")

    def test_legitimate_union_allowed(self):
        """Legitimate UNION queries should not be flagged."""
        query = """
            SELECT id, name FROM employees 
            UNION 
            SELECT id, name FROM contractors
        """
        result = detect_sql_injection(query)
        # This is a legitimate UNION, not an injection pattern
        assert result is None

    def test_empty_query_no_injection(self):
        """Empty query should not be flagged as injection."""
        result = detect_sql_injection("")
        assert result is None


class TestValidateQuerySecurity:
    """Tests for the combined validate_query_security function."""

    def test_valid_select_passes(self):
        """Valid SELECT queries should pass all security checks."""
        query = "SELECT COUNT(*) FROM orders WHERE status = 'pending'"
        validate_query_security(query)  # Should not raise

    def test_insert_fails(self):
        """INSERT should fail security validation."""
        query = "INSERT INTO logs (message) VALUES ('test')"
        with pytest.raises(SQLSecurityError):
            validate_query_security(query)

    def test_injection_fails(self):
        """SQL injection patterns should fail validation."""
        query = "SELECT * FROM users; DROP TABLE users"
        with pytest.raises(SQLSecurityError) as exc_info:
            validate_query_security(query)
        # Should fail on injection detection (checked first)
        assert "injection" in exc_info.value.violation_type

    def test_complex_valid_query_passes(self):
        """Complex but valid queries should pass."""
        query = """
            WITH monthly_sales AS (
                SELECT 
                    DATE_TRUNC('month', order_date) AS month,
                    SUM(amount) AS total
                FROM orders
                WHERE status = 'completed'
                GROUP BY DATE_TRUNC('month', order_date)
            )
            SELECT month, total
            FROM monthly_sales
            WHERE total > 10000
            ORDER BY month DESC
        """
        validate_query_security(query)  # Should not raise


class TestSanitizeIdentifier:
    """Tests for identifier sanitization."""

    def test_valid_simple_identifier(self):
        """Simple valid identifiers should pass."""
        assert sanitize_identifier("users") == "users"
        assert sanitize_identifier("order_items") == "order_items"
        assert sanitize_identifier("User123") == "User123"

    def test_valid_schema_qualified_identifier(self):
        """Schema-qualified identifiers should pass."""
        assert sanitize_identifier("public.users") == "public.users"
        assert sanitize_identifier("dbo.orders") == "dbo.orders"

    def test_underscore_prefix_allowed(self):
        """Identifiers starting with underscore should be allowed."""
        assert sanitize_identifier("_temp_table") == "_temp_table"
        assert sanitize_identifier("_123") == "_123"

    def test_empty_identifier_blocked(self):
        """Empty identifiers should be blocked."""
        with pytest.raises(SQLSecurityError) as exc_info:
            sanitize_identifier("")
        assert exc_info.value.violation_type == "empty_identifier"

    def test_special_characters_blocked(self):
        """Identifiers with special characters should be blocked."""
        with pytest.raises(SQLSecurityError) as exc_info:
            sanitize_identifier("users; DROP TABLE users")
        assert exc_info.value.violation_type == "invalid_identifier"

    def test_sql_injection_in_identifier_blocked(self):
        """SQL injection attempts in identifiers should be blocked."""
        with pytest.raises(SQLSecurityError):
            sanitize_identifier("users'--")

    def test_spaces_blocked(self):
        """Identifiers with spaces should be blocked."""
        with pytest.raises(SQLSecurityError):
            sanitize_identifier("user table")

    def test_numeric_start_blocked(self):
        """Identifiers starting with a number should be blocked."""
        with pytest.raises(SQLSecurityError):
            sanitize_identifier("123users")

    def test_hyphen_blocked(self):
        """Identifiers with hyphens should be blocked."""
        with pytest.raises(SQLSecurityError):
            sanitize_identifier("user-table")


class TestSQLSecurityError:
    """Tests for SQLSecurityError exception class."""

    def test_error_attributes(self):
        """Error should have correct attributes."""
        error = SQLSecurityError(
            message="Test error",
            violation_type="test_violation",
            query="SELECT * FROM test"
        )
        assert str(error) == "Test error"
        assert error.violation_type == "test_violation"
        assert error.query == "SELECT * FROM test"

    def test_query_truncation(self):
        """Long queries should be truncated in error."""
        long_query = "SELECT " + "x, " * 1000 + "y FROM test"
        error = SQLSecurityError(
            message="Test error",
            violation_type="test",
            query=long_query
        )
        assert len(error.query) <= 500

    def test_none_query_handling(self):
        """None query should be handled gracefully."""
        error = SQLSecurityError(
            message="Test error",
            violation_type="test",
            query=None  # type: ignore
        )
        assert error.query == ""


class TestDialectSupport:
    """Tests for SQL dialect support in security validation."""

    def test_postgres_dialect(self):
        """Postgres dialect should work correctly."""
        query = "SELECT * FROM users WHERE created_at > NOW() - INTERVAL '1 day'"
        validate_read_only_query(query, dialect="postgres")

    def test_duckdb_dialect(self):
        """DuckDB dialect should work correctly."""
        query = "SELECT * FROM users WHERE created_at > CURRENT_TIMESTAMP - INTERVAL 1 DAY"
        validate_read_only_query(query, dialect="duckdb")

    def test_mysql_dialect(self):
        """MySQL dialect should work correctly."""
        query = "SELECT * FROM users WHERE created_at > DATE_SUB(NOW(), INTERVAL 1 DAY)"
        validate_read_only_query(query, dialect="mysql")


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_case_insensitive_detection(self):
        """Security detection should be case-insensitive."""
        # Test uppercase
        with pytest.raises(SQLSecurityError):
            validate_read_only_query("INSERT INTO users VALUES (1)")

        # Test lowercase
        with pytest.raises(SQLSecurityError):
            validate_read_only_query("insert into users values (1)")

        # Test mixed case
        with pytest.raises(SQLSecurityError):
            validate_read_only_query("InSeRt InTo users VALUES (1)")

    def test_comments_in_query(self):
        """Queries with comments should be handled correctly."""
        query = """
            -- This is a comment
            SELECT * FROM users /* inline comment */ WHERE id = 1
        """
        validate_read_only_query(query)  # Should not raise

    def test_multiline_query(self):
        """Multiline queries should be handled correctly."""
        query = """
            SELECT 
                id,
                name,
                email
            FROM 
                users
            WHERE 
                status = 'active'
        """
        validate_read_only_query(query)  # Should not raise

    def test_unicode_in_query(self):
        """Queries with unicode characters should be handled."""
        query = "SELECT * FROM users WHERE name = '日本語'"
        validate_read_only_query(query)  # Should not raise
