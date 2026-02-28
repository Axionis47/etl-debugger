"""Unit tests for agent tools."""

import os
import sqlite3
import tempfile
from pathlib import Path

import duckdb
import pytest

from src.tools.sql_executor import execute_sql, set_db_path
from src.tools.schema_inspector import inspect_schema
from src.tools.log_parser import parse_logs
from src.tools.file_reader import read_file, set_base_dir


@pytest.fixture
def duckdb_path(tmp_path):
    """Create a temporary DuckDB database with test data."""
    db_path = str(tmp_path / "test.duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")
    con.execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
    con.close()
    set_db_path("duckdb", db_path)
    return db_path


@pytest.fixture
def sqlite_path(tmp_path):
    """Create a temporary SQLite database with test data."""
    db_path = str(tmp_path / "test.sqlite")
    con = sqlite3.connect(db_path)
    con.execute("CREATE TABLE products (id INTEGER, name TEXT, price REAL)")
    con.execute("INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)")
    con.commit()
    con.close()
    set_db_path("sqlite", db_path)
    return db_path


class TestSqlExecutor:
    def test_duckdb_select(self, duckdb_path):
        result = execute_sql("SELECT * FROM users", "duckdb")
        assert "Alice" in result
        assert "Bob" in result

    def test_sqlite_select(self, sqlite_path):
        result = execute_sql("SELECT * FROM products", "sqlite")
        assert "Widget" in result
        assert "9.99" in result

    def test_duckdb_count(self, duckdb_path):
        result = execute_sql("SELECT COUNT(*) as cnt FROM users", "duckdb")
        assert "2" in result

    def test_invalid_sql(self, duckdb_path):
        result = execute_sql("SELECT * FROM nonexistent", "duckdb")
        assert "Error" in result or "error" in result.lower()

    def test_no_db_configured(self):
        set_db_path("test_engine", "")
        result = execute_sql("SELECT 1", "unknown_engine")
        assert "Error" in result


class TestSchemaInspector:
    def test_duckdb_schema(self, duckdb_path):
        result = inspect_schema("users", "duckdb")
        assert "users" in result
        assert "id" in result
        assert "name" in result
        assert "2" in result  # row count

    def test_sqlite_schema(self, sqlite_path):
        result = inspect_schema("products", "sqlite")
        assert "products" in result
        assert "price" in result
        assert "2" in result  # row count

    def test_nonexistent_table(self, sqlite_path):
        result = inspect_schema("nonexistent", "sqlite")
        assert "not found" in result.lower() or "products" in result


class TestLogParser:
    SAMPLE_LOG = """2024-11-15 08:32:00 INFO   Starting pipeline
2024-11-15 08:32:01 ERROR  Pipeline failed at transform step
2024-11-15 08:32:01 ERROR  Binder Error: column not found
2024-11-15 08:32:01 WARN   Retrying connection
2024-11-15 08:32:02 INFO   Pipeline execution time: 1.2s"""

    def test_default_filter(self):
        result = parse_logs(self.SAMPLE_LOG)
        assert "ERROR" in result
        assert "WARN" in result
        assert "Starting pipeline" not in result

    def test_regex_filter(self):
        result = parse_logs(self.SAMPLE_LOG, "Binder")
        assert "Binder Error" in result
        assert "Retrying" not in result

    def test_empty_log(self):
        result = parse_logs("")
        assert "No log content" in result

    def test_no_matches(self):
        result = parse_logs("INFO all good\nINFO everything fine", "ERROR")
        assert "No matching" in result


class TestFileReader:
    def test_read_file(self, tmp_path):
        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello, World!")
        set_base_dir(str(tmp_path))
        result = read_file(str(test_file))
        assert "Hello, World!" in result

    def test_file_not_found(self, tmp_path):
        set_base_dir(str(tmp_path))
        result = read_file(str(tmp_path / "nonexistent.txt"))
        assert "not found" in result.lower() or "Error" in result

    def test_truncation(self, tmp_path):
        test_file = tmp_path / "big.txt"
        test_file.write_text("x" * 3000)
        set_base_dir(str(tmp_path))
        result = read_file(str(test_file))
        assert "truncated" in result.lower()
