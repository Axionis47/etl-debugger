from __future__ import annotations

import sqlite3

import duckdb

from src.tools.sql_executor import get_db_path


def inspect_schema(table_name: str, engine: str = "duckdb") -> str:
    """Inspect the schema of a database table. Returns column names, data types, and row count.

    Args:
        table_name: Name of the table to inspect.
        engine: Database engine - either 'sqlite' or 'duckdb'.

    Returns:
        Table schema as formatted text with columns, types, and row count.
    """
    db_path = get_db_path(engine)
    if not db_path:
        return f"Error: No database configured for engine '{engine}'."

    try:
        if engine == "duckdb":
            return _inspect_duckdb(table_name, db_path)
        elif engine == "sqlite":
            return _inspect_sqlite(table_name, db_path)
        else:
            return f"Error: Unknown engine '{engine}'."
    except Exception as e:
        return f"Schema Error ({engine}): {e}"


def _inspect_duckdb(table_name: str, db_path: str) -> str:
    """Inspect schema in DuckDB."""
    con = duckdb.connect(db_path)
    try:
        # Get columns
        cols = con.execute(f"DESCRIBE {table_name}").fetchall()
        # Get row count
        count_result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = count_result[0] if count_result else 0

        lines = [f"Table: {table_name}", f"Row count: {row_count}", ""]
        lines.append(f"{'Column':<20} {'Type':<15} {'Nullable':<10}")
        lines.append(f"{'-'*20} {'-'*15} {'-'*10}")
        for col in cols:
            name, dtype, nullable = col[0], col[1], col[2]
            null_str = "YES" if nullable == "YES" else "NO"
            lines.append(f"{name:<20} {dtype:<15} {null_str:<10}")

        return "\n".join(lines)
    finally:
        con.close()


def _inspect_sqlite(table_name: str, db_path: str) -> str:
    """Inspect schema in SQLite."""
    con = sqlite3.connect(db_path)
    try:
        cursor = con.execute(f"PRAGMA table_info({table_name})")
        cols = cursor.fetchall()

        if not cols:
            # List available tables
            tables = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            table_list = ", ".join(t[0] for t in tables)
            return f"Table '{table_name}' not found. Available tables: {table_list}"

        count_result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = count_result[0] if count_result else 0

        lines = [f"Table: {table_name}", f"Row count: {row_count}", ""]
        lines.append(f"{'Column':<20} {'Type':<15} {'Nullable':<10}")
        lines.append(f"{'-'*20} {'-'*15} {'-'*10}")
        for col in cols:
            # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            name, dtype, notnull = col[1], col[2], col[3]
            null_str = "NO" if notnull else "YES"
            lines.append(f"{name:<20} {dtype:<15} {null_str:<10}")

        return "\n".join(lines)
    finally:
        con.close()
